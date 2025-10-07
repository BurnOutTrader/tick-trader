use crate::http::client::PxHttpClient;
use crate::http::credentials::PxCredential;
use crate::http::error::PxError;
use crate::http::models::{RetrieveBarsReq, RetrieveBarsResponse};
use crate::websocket::client::{parse_px_instrument, px_format_from_instrument, PxWebSocketClient};
use ahash::AHashMap;
use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, NaiveDate, NaiveDateTime, ParseResult, Utc};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;
use tt_bus::Router;
use tt_types::data::core::Candle;
use tt_types::data::models::Resolution;
use tt_types::history::{HistoricalRequest, HistoryEvent};
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::futures_helpers::{extract_month_year, extract_root};
use tt_types::securities::symbols::Exchange;
use tt_types::securities::symbols::Instrument;
use tt_types::server_side::traits::{
    CommandAck, ConnectionState, DisconnectReason, ExecutionProvider, HistoricalDataProvider,
    MarketDataProvider, ProviderSessionSpec,
};

pub struct PXClient {
    pub provider_kind: ProviderKind,
    http: Arc<PxHttpClient>,
    websocket: Arc<PxWebSocketClient>,
    http_connection_state: Arc<RwLock<ConnectionState>>,
    account_subscriptions: Arc<RwLock<Vec<AccountKey>>>,
}

impl PXClient {
    pub async fn instruments_map_snapshot(
        &self,
    ) -> AHashMap<
        tt_types::securities::symbols::Instrument,
        tt_types::securities::security::FuturesContract,
    > {
        self.http.instruments_snapshot().await
    }
    #[allow(dead_code)]
    pub async fn new_from_session(
        kind: ProviderKind,
        session: ProviderSessionSpec,
        bus: Arc<Router>,
    ) -> anyhow::Result<Self> {
        let user_name: String = match session.user_names.get(&kind) {
            Some(s) => s.clone(),
            None => {
                anyhow::bail!(
                    "PXClient missing username for {:?}. Ensure PX_{{TENANT}}_USERNAME is set for this provider.",
                    kind
                )
            }
        };
        let api_key: String = match session.api_keys.get(&kind) {
            Some(s) => s.clone(),
            None => {
                anyhow::bail!(
                    "PXClient missing api_key for {:?}. Ensure PX_{{TENANT}}_APIKEY is set for this provider.",
                    kind
                )
            }
        };
        let firm = match kind {
            ProviderKind::ProjectX(firm) => firm,
            _ => anyhow::bail!("PXClient requires a ProjectX provider kind"),
        };
        let px_credentials = PxCredential::new(firm, user_name, api_key);
        let http = PxHttpClient::new(px_credentials, None, None, None, None)?;
        let base = http.inner.rtc_base();
        let token = http.inner.token_string().await;
        let websocket = PxWebSocketClient::new(base, token, firm, bus);
        Ok(Self {
            provider_kind: kind,
            http: Arc::new(http),
            websocket: Arc::new(websocket),
            http_connection_state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            account_subscriptions: Arc::new(RwLock::new(vec![])),
        })
    }

    async fn state(&self) -> ConnectionState {
        let http_state = self.http_connection_state.read().await;
        if *http_state == ConnectionState::Disconnected
            || self.websocket.is_user_connected() == false
            || self.websocket.is_market_connected() == false
        {
            return ConnectionState::Disconnected;
        }
        ConnectionState::Connected
    }

    async fn connect_all(&self) -> anyhow::Result<()> {
        let mut conn_state = self.http_connection_state.write().await;
        if *conn_state == ConnectionState::Disconnected {
            *conn_state = ConnectionState::Connecting;

            match self.http.start().await {
                Ok(_) => {
                    *conn_state = ConnectionState::Connected;
                }
                Err(PxError::Other(e)) => {
                    *conn_state = ConnectionState::Disconnected;
                    return Err(e);
                }
                Err(e) => {
                    log::error!("Error starting HTTP client: {:?}", e);
                    *conn_state = ConnectionState::Disconnected;
                }
            }
        }
        if self.websocket.is_market_connected() == false {
            *conn_state = ConnectionState::Connecting;
            match self.websocket.connect_market().await {
                Ok(_conn) => {}
                Err(e) => {
                    log::error!("Error starting WebSocket client: {:?}", e);
                    return Err(e);
                }
            };
        }
        if self.websocket.is_user_connected() == false {
            match self.websocket.connect_user().await {
                Ok(_conn) => {}
                Err(e) => {
                    log::error!("Error starting WebSocket client: {:?}", e);
                }
            }
        }
        self.http.manual_update_instruments(false).await?;
        self.http.account_snapshots().await?;
        Ok(())
    }

    /// Retrieve historical bars for a given instrument and topic over [start, end).
    /// Paginates using the ProjectX 20,000-bars limit and normalizes timestamps to UTC.
    pub async fn retrieve_bars(&self, req: &HistoricalRequest) -> anyhow::Result<Vec<HistoryEvent>> {
        // Map Topic -> Resolution and PX unit fields
        let (resolution, unit, unit_number) = match req.topic {
            Topic::Candles1s => (Resolution::Seconds(1), 1, 1),
            Topic::Candles1m => (Resolution::Minutes(1), 2, 1),
            Topic::Candles1h => (Resolution::Hours(1), 3, 1),
            Topic::Candles1d => (Resolution::Daily, 4, 1),
            _ => anyhow::bail!("retrieve_bars only supports candle topics (1s/1m/1h/1d)"),
        };

        // Resolve contract id and exchange (as you had)
        let (contract_id, exchange) = {
            let mut cid: Option<String> = None;
            let mut ex: Option<Exchange> = None;
            let map = self.http.instruments_snapshot().await;
            if let Some(fc) = map.get(&req.instrument) {
                cid = Some(fc.provider_contract_name.clone());
                ex = Some(fc.exchange);
            }
            if cid.is_none() {
                let map = self.http.manual_update_instruments(false).await?;
                let map = map.read().await;
                if let Some(fc) = map.get(&req.instrument) {
                    cid = Some(fc.provider_contract_name.clone());
                    ex = Some(fc.exchange);
                }
            }
            (cid.unwrap_or_else(|| px_format_from_instrument(&req.instrument)),
             ex.expect("exchange must be present for known instrument"))
        };

        let limit: i32 = 20_000;
        let step = resolution.as_duration();
        let chunk_span = step * (limit as i32);

        let mut out: Vec<Candle> = Vec::new();
        let mut cur_start: DateTime<Utc> = req.start;
        let mut prev_max_start: Option<DateTime<Utc>> = None; // drive pagination by last bar start
        let end = req.end;

        'main: while cur_start < end {
            let cur_end = (cur_start + chunk_span).min(end);

            let body = RetrieveBarsReq {
                contract_id: contract_id.clone(),
                live: false,
                start_time: cur_start.to_rfc3339(),
                end_time: cur_end.to_rfc3339(),
                unit,
                unit_number,
                limit,
                include_partial_bar: false,
            };

            let resp: RetrieveBarsResponse = self.http.inner.retrieve_bars(&body).await?;
            if !resp.success {
                anyhow::bail!(
                "ProjectX retrieveBars error: code={:?} msg={:?}",
                resp.error_code,
                resp.error_message
            );
            }

            if resp.bars.is_empty() {
                // no data in this window — move the window forward
                cur_start = cur_end;
                continue;
            }

            // Parse to engine candles
            let mut batch = match resp.to_engine_candles(req.instrument.clone(), resolution, exchange) {
                Ok(c) => c,
                Err(e) => {
                    log::error!("Error parsing ProjectX bars: {}", e);
                    break 'main;
                }
            };

            // Keep only *new* bars strictly after prev_max_start
            if let Some(pms) = prev_max_start {
                batch.retain(|c| c.time_start > pms);
            }

            if batch.is_empty() {
                // Provider likely returned inclusive overlap at the boundary — nudge forward.
                // We advance by 1 second because API granularity is seconds.
                cur_start = cur_start + chrono::Duration::seconds(1);
                continue;
            }

            // Append and update pagination anchor by *max start* in this batch
            let (min_s, max_s) = (
                batch.iter().map(|c| c.time_start).min().unwrap(),
                batch.iter().map(|c| c.time_start).max().unwrap(),
            );
            log::info!("candles: {} from start={} to start={}", batch.len(), min_s, max_s);

            out.extend(batch);
            prev_max_start = Some(match prev_max_start {
                Some(prev) => prev.max(max_s),
                None => max_s,
            });

            // **Critical**: advance by last bar start + 1s (provider accuracy is seconds)
            cur_start = prev_max_start.unwrap() + chrono::Duration::seconds(1);
        }

        // Finalize: sort, dedup by start, clip
        out.sort_by_key(|c| c.time_start);
        out.dedup_by_key(|c| c.time_start); // skip duplicates if provider re-sent same timestamp
        out.retain(|c| c.time_start >= req.start && c.time_start < req.end);

        Ok(out.into_iter().map(HistoryEvent::Candle).collect())
    }
}

fn parse_symbol_key(key: SymbolKey) -> anyhow::Result<String> {
    let instrument = key.instrument; // already an Instrument
    let root = extract_root(&instrument);
    Ok(match extract_month_year(&instrument) {
        None => format!("CON.F.US.{root}"),
        Some((month, year)) => format!("CON.F.US.{root}.{month}{year}"),
    })
}

#[async_trait]
impl MarketDataProvider for PXClient {
    fn id(&self) -> ProviderKind {
        self.provider_kind
    }

    fn supports(&self, topic: Topic) -> bool {
        match topic {
            Topic::Ticks | Topic::Quotes | Topic::Depth => true,
            _ => false,
        }
    }

    async fn connect_to_market(
        &self,
        _kind: ProviderKind,
        _session: ProviderSessionSpec,
    ) -> anyhow::Result<()> {
        self.connect_all().await
    }

    async fn disconnect(&self, _reason: DisconnectReason) {
        self.websocket.kill().await;
        self.http.kill()
    }

    async fn connection_state(&self) -> ConnectionState {
        self.state().await
    }

    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey) -> anyhow::Result<()> {
        let instrument = parse_symbol_key(key.clone())?;
        match topic {
            Topic::Ticks => {
                self.websocket
                    .subscribe_contract_ticks(instrument.as_str())
                    .await
            }
            Topic::Quotes => {
                self.websocket
                    .subscribe_contract_quotes(instrument.as_str())
                    .await
            }
            Topic::Depth => {
                self.websocket
                    .subscribe_contract_market_depth(instrument.as_str())
                    .await
            }
            _ => anyhow::bail!("Unsupported topic: {:?}", topic),
        }
    }

    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> anyhow::Result<()> {
        let instrument = parse_symbol_key(key.clone())?;
        match topic {
            Topic::Ticks => {
                self.websocket
                    .unsubscribe_contract_ticks(instrument.as_str())
                    .await
            }
            Topic::Quotes => {
                self.websocket
                    .unsubscribe_contract_quotes(instrument.as_str())
                    .await
            }
            Topic::Depth => {
                self.websocket
                    .unsubscribe_contract_market_depth(instrument.as_str())
                    .await
            }
            _ => anyhow::bail!("Unsupported topic: {:?}", topic),
        }
    }

    async fn active_md_subscriptions(&self) -> AHashMap<Topic, Vec<SymbolKey>> {
        // Collect current active subscriptions from the websocket client which tracks
        // contract ids per topic internally.
        fn symbol_from_contract_id(firm: ProjectXTenant, instrument: &str) -> Option<SymbolKey> {
            let provider = ProviderKind::ProjectX(firm);
            let instrument = Instrument::from_str(instrument).ok()?;
            Some(SymbolKey {
                instrument,
                provider,
            })
        }
        let firm = match self.provider_kind {
            ProviderKind::ProjectX(firm) => firm,
            _ => return AHashMap::new(),
        };

        let ticks = {
            let g = self.websocket.active_contract_ids_ticks().await;
            g.iter()
                .filter_map(|s| symbol_from_contract_id(firm, s))
                .collect::<Vec<_>>()
        };
        let quotes = {
            let g = self.websocket.active_contract_ids_quotes().await;
            g.iter()
                .filter_map(|s| symbol_from_contract_id(firm, s))
                .collect::<Vec<_>>()
        };
        let depth = {
            let g = self.websocket.active_contract_ids_depth().await;
            g.iter()
                .filter_map(|s| symbol_from_contract_id(firm, s))
                .collect::<Vec<_>>()
        };

        let mut map = AHashMap::new();
        map.insert(Topic::Ticks, ticks);
        map.insert(Topic::Quotes, quotes);
        map.insert(Topic::Depth, depth);
        map
    }

    async fn list_instruments(
        &self,
        pattern: Option<String>,
    ) -> anyhow::Result<Vec<tt_types::securities::symbols::Instrument>> {
        // Use HTTP snapshot maintained by PxHttpClient; filter by optional pattern (case-insensitive contains)
        let map = self.instruments_map_snapshot().await;
        let mut v: Vec<_> = map.keys().cloned().collect();
        if let Some(mut pat) = pattern {
            pat.make_ascii_uppercase();
            v.retain(|inst| inst.to_string().to_uppercase().contains(&pat));
        }
        Ok(v)
    }

    async fn instruments_map(
        &self,
    ) -> anyhow::Result<
        ahash::AHashMap<
            tt_types::securities::symbols::Instrument,
            tt_types::securities::security::FuturesContract,
        >,
    > {
        Ok(self.instruments_map_snapshot().await)
    }

    async fn auto_update(&self) -> anyhow::Result<()> {
        self.http.auto_update().await
    }
}

#[async_trait]
impl ExecutionProvider for PXClient {
    fn id(&self) -> ProviderKind {
        self.provider_kind
    }

    async fn connect_to_broker(
        &self,
        _kind: ProviderKind,
        _session: ProviderSessionSpec,
    ) -> anyhow::Result<()> {
        self.connect_all().await
    }

    async fn disconnect(&self, reason: DisconnectReason) {
        self.websocket.kill().await;
        self.http.kill();
        log::info!("Disconnected from ProjectX: {:?}", reason);
    }

    async fn connection_state(&self) -> ConnectionState {
        self.state().await
    }

    async fn subscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        match self.websocket.subscribe_user_account(id).await {
            Ok(_) => {
                let mut lock = self.account_subscriptions.write().await;
                lock.push(account_key.clone());
                Ok(())
            }
            Err(_) => Err(anyhow::anyhow!(
                "Failed to subscribe to account events: {}",
                account_key.account_name
            )),
        }
    }

    async fn unsubscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        let lock = self.account_subscriptions.write().await;
        let _ = lock.iter().position(|k| k == account_key);
        self.websocket.unsubscribe_user_account(id).await
    }

    async fn subscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        self.websocket.subscribe_account_positions(id).await
    }

    async fn unsubscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        self.websocket.unsubscribe_account_positions(id).await
    }

    async fn active_account_subscriptions(&self) -> Vec<AccountKey> {
        let lock = self.account_subscriptions.read().await;
        lock.clone()
    }

    async fn subscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        self.websocket.subscribe_account_orders(id).await
    }

    async fn unsubscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        self.websocket.unsubscribe_account_orders(id).await
    }

    async fn place_order(&self, spec: tt_types::wire::PlaceOrder) -> CommandAck {
        use crate::http::models::{BracketCfg, PlaceOrderReq};
        // Map typed spec to ProjectX PlaceOrderReq
        let type_i = match spec.r#type {
            tt_types::wire::OrderTypeWire::Limit => 1,
            tt_types::wire::OrderTypeWire::Market => 2,
            tt_types::wire::OrderTypeWire::Stop => 4,
            tt_types::wire::OrderTypeWire::TrailingStop => 5,
            tt_types::wire::OrderTypeWire::JoinBid => 6,
            tt_types::wire::OrderTypeWire::JoinAsk => 7,
            tt_types::wire::OrderTypeWire::StopLimit => 4, // PX uses Stop with limit/stop provided
        };
        let side_i = match spec.side {
            tt_types::accounts::events::Side::Buy => 0,
            tt_types::accounts::events::Side::Sell => 1,
        };
        fn map_type(t: tt_types::wire::OrderTypeWire) -> i32 {
            match t {
                tt_types::wire::OrderTypeWire::Limit => 1,
                tt_types::wire::OrderTypeWire::Market => 2,
                tt_types::wire::OrderTypeWire::Stop => 4,
                tt_types::wire::OrderTypeWire::TrailingStop => 5,
                tt_types::wire::OrderTypeWire::JoinBid => 6,
                tt_types::wire::OrderTypeWire::JoinAsk => 7,
                tt_types::wire::OrderTypeWire::StopLimit => 4,
            }
        }
        let stop_loss_bracket = spec.stop_loss.map(|b| BracketCfg {
            ticks: b.ticks,
            type_: map_type(b.r#type),
        });
        let take_profit_bracket = spec.take_profit.map(|b| BracketCfg {
            ticks: b.ticks,
            type_: map_type(b.r#type),
        });
        let req = PlaceOrderReq {
            account_id: spec.account_id,
            contract_id: spec.key.instrument.to_string(),
            type_: type_i,
            side: side_i,
            size: spec.qty,
            limit_price: spec.limit_price,
            stop_price: spec.stop_price,
            trail_price: spec.trail_price,
            custom_tag: spec.custom_tag,
            stop_loss_bracket,
            take_profit_bracket,
        };
        let res = self.http.inner.place_order(&req).await;
        match res {
            Ok(r) => CommandAck {
                ok: r.success,
                message: r.error_message,
            },
            Err(e) => CommandAck {
                ok: false,
                message: Some(format!("place_order error: {}", e)),
            },
        }
    }

    async fn cancel_order(&self, spec: tt_types::wire::CancelOrder) -> CommandAck {
        // Requires provider_order_id to be present for PX
        let Some(poid) = spec.provider_order_id else {
            return CommandAck {
                ok: false,
                message: Some("provider_order_id required for cancel on ProjectX".to_string()),
            };
        };
        let Ok(order_id) = poid.parse::<i64>() else {
            return CommandAck {
                ok: false,
                message: Some("invalid provider_order_id; expected numeric string".to_string()),
            };
        };
        let res = self
            .http
            .inner
            .cancel_order(spec.account_id, order_id)
            .await;
        match res {
            Ok(r) => CommandAck {
                ok: r.success,
                message: r.error_message,
            },
            Err(e) => CommandAck {
                ok: false,
                message: Some(format!("cancel_order error: {}", e)),
            },
        }
    }

    async fn replace_order(&self, spec: tt_types::wire::ReplaceOrder) -> CommandAck {
        use crate::http::models::ModifyOrderReq;
        let Some(poid) = spec.provider_order_id else {
            return CommandAck {
                ok: false,
                message: Some("provider_order_id required for replace on ProjectX".to_string()),
            };
        };
        let Ok(order_id) = poid.parse::<i64>() else {
            return CommandAck {
                ok: false,
                message: Some("invalid provider_order_id; expected numeric string".to_string()),
            };
        };
        let req = ModifyOrderReq {
            account_id: spec.account_id,
            order_id,
            size: spec.new_qty,
            limit_price: spec.new_limit_price,
            stop_price: spec.new_stop_price,
            trail_price: spec.new_trail_price,
        };
        let res = self.http.inner.modify_order(&req).await;
        match res {
            Ok(r) => CommandAck {
                ok: r.success,
                message: r.error_message,
            },
            Err(e) => CommandAck {
                ok: false,
                message: Some(format!("modify_order error: {}", e)),
            },
        }
    }

    async fn auto_update(&self) -> anyhow::Result<()> {
        self.http.auto_update().await
    }
}
#[async_trait::async_trait]
impl HistoricalDataProvider for PXClient {
    fn name(&self) -> ProviderKind {
        self.provider_kind
    }

    async fn ensure_connected(&self) -> anyhow::Result<()> {
        let mut conn_state = self.http_connection_state.write().await;
        if *conn_state == ConnectionState::Disconnected {
            *conn_state = ConnectionState::Connecting;

            match self.http.start().await {
                Ok(_) => {
                    *conn_state = ConnectionState::Connected;
                }
                Err(PxError::Other(e)) => {
                    *conn_state = ConnectionState::Disconnected;
                    return Err(e);
                }
                Err(e) => {
                    log::error!("Error starting HTTP client: {:?}", e);
                    *conn_state = ConnectionState::Disconnected;
                }
            }
        }
        Ok(())
    }

    async fn fetch(&self, req: HistoricalRequest) -> anyhow::Result<Vec<HistoryEvent>> {
        self.retrieve_bars(&req).await
    }

    fn supports(&self, topic: Topic) -> bool {
        if matches!(
            topic,
            Topic::Candles1d | Topic::Candles1s | Topic::Candles1h | Topic::Candles1m
        ) {
            return true;
        }
        false
    }

    async fn earliest_available(
        &self,
        instrument: Instrument,
        _topic: Topic,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        self.http.manual_update_instruments(false).await?;
        if let Some(contract) = self.http.instruments_snapshot().await.get(&instrument) {
            if let Some(ad) = contract.activation_date {
                let dt = NaiveDateTime::from(ad);
                let utc_time = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
                return Ok(Some(utc_time));
            }
        }
        match NaiveDate::from_ymd_opt(2023, 1, 1) {
            Some(date) => {
                let dt = NaiveDateTime::from(date);
                let utc_time = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
                Ok(Some(utc_time))
            }
            None => Ok(None),
        }
    }
}
