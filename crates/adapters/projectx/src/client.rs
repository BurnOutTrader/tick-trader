use crate::http::client::PxHttpClient;
use crate::http::credentials::PxCredential;
use crate::http::error::PxError;
use crate::http::models::{ContractSearchResponse, RetrieveBarsReq, RetrieveBarsResponse};
use crate::websocket::client::{PxWebSocketClient, px_format_from_instrument};
use ahash::AHashMap;
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, NaiveDateTime, Utc};
use dashmap::DashMap;
use log::info;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;
use tt_bus::Router;
use tt_types::accounts::account::{AccountName, AccountSnapShot};
use tt_types::data::core::Candle;
use tt_types::data::models::Resolution;
use tt_types::history::{HistoricalRangeRequest, HistoryEvent};
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::futures_helpers::{extract_month_year, extract_root};
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::Instrument;
use tt_types::securities::symbols::{Currency, Exchange, get_symbol_info};
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
    internal_accounts: Arc<DashMap<AccountName, AccountSnapShot>>,
    pub(crate) instruments: Arc<DashMap<Instrument, FuturesContract>>,
}

impl PXClient {
    pub async fn instruments_map_snapshot(
        &self,
    ) -> anyhow::Result<Vec<tt_types::securities::security::FuturesContract>> {
        self.manual_update_instruments(false).await?;
        let v: Vec<tt_types::securities::security::FuturesContract> =
            self.instruments.iter().map(|r| r.value().clone()).collect();
        Ok(v)
    }
    pub async fn auto_update_client(&self) -> anyhow::Result<()> {
        self.account_snapshots().await?;
        self.manual_update_instruments(false).await?;
        Ok(())
    }

    pub async fn manual_update_instruments(&self, live_only: bool) -> anyhow::Result<()> {
        let resp: ContractSearchResponse = self
            .http
            .inner
            .list_all_contracts(live_only)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut new = 0;
        for inst in &resp.contracts {
            let instrument = match Instrument::try_parse_dotted(&inst.id) {
                Ok(instrument) => instrument,
                Err(e) => {
                    log::error!("Failed to parse instrument from id {}: {:?}", inst.id, e);
                    continue;
                }
            };
            if self.instruments.contains_key(&instrument) {
                continue;
            }
            new += 1;
            let symbol = extract_root(&instrument);
            let symbol_info = match get_symbol_info(&symbol) {
                Some(info) => info,
                None => {
                    log::error!("Failed to get symbol info for: {}", symbol);
                    continue;
                }
            };
            let tick_value = match Decimal::from_f64(inst.tick_value) {
                Some(tick) => tick,
                None => return Err(anyhow::anyhow!("Failed to convert tick value to decimal")),
            };
            let tick_size = match Decimal::from_f64(inst.tick_size) {
                Some(tick) => tick,
                None => return Err(anyhow::anyhow!("Failed to convert tick size to decimal")),
            };
            let mut s = match FuturesContract::from_root_with(
                &instrument,
                symbol_info.exchange,
                inst.id.clone(),
                self.provider_kind,
                tick_value,
                Some(Currency::USD),
                tick_size,
            ) {
                None => continue,
                Some(s) => s,
            };
            s.value_per_tick =
                Decimal::from_f64(inst.tick_value).unwrap_or(symbol_info.value_per_tick);
            s.tick_size = Decimal::from_f64(inst.tick_size).unwrap_or(symbol_info.tick_size);
            self.instruments.insert(instrument, s);
        }
        info!(
            "ProjectX Manual Updated instruments Successfully, {} new instruments",
            new
        );
        Ok(())
    }

    /// Initialise or refresh the account id using the api
    pub async fn account_snapshots(
        &self,
    ) -> anyhow::Result<Arc<DashMap<AccountName, AccountSnapShot>>> {
        let resp = self.http.inner.search_accounts(true).await?;

        for acc in &resp.accounts {
            let name = match AccountName::from_str(&acc.name) {
                Ok(name) => name,
                Err(e) => {
                    log::error!("Failed to parse account id: {}", e);
                    continue;
                }
            };
            let snap_shot = AccountSnapShot {
                name: name.clone(),
                id: acc.id,
                balance: Decimal::from_f64(acc.balance).unwrap_or_default(),
                can_trade: acc.can_trade,
            };
            self.internal_accounts.insert(name, snap_shot);
        }
        Ok(self.internal_accounts.clone())
    }

    /// Find a correlated nautilus account id from an account name
    pub async fn account_id(&self, account_name: AccountName) -> anyhow::Result<i64> {
        match self.internal_accounts.get(&account_name) {
            None => {
                // if we don't have the account id cached, we need to refresh the list
                let map = self.account_snapshots().await?;
                match map.get(&account_name) {
                    None => Err(anyhow::anyhow!(
                        "Failed to find account id for {}, please check the account name and try again",
                        account_name
                    )),
                    Some(acc) => Ok(acc.id),
                }
            }
            Some(acc) => Ok(acc.id),
        }
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
            internal_accounts: Arc::new(Default::default()),
            instruments: Arc::new(Default::default()),
        })
    }

    async fn state(&self) -> ConnectionState {
        let http_state = self.http_connection_state.read().await;
        if *http_state == ConnectionState::Disconnected
            || !self.websocket.is_user_connected()
            || !self.websocket.is_market_connected()
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
        if !self.websocket.is_market_connected() {
            *conn_state = ConnectionState::Connecting;
            match self.websocket.connect_market().await {
                Ok(_conn) => {}
                Err(e) => {
                    log::error!("Error starting WebSocket client: {:?}", e);
                    return Err(e);
                }
            };
        }
        if !self.websocket.is_user_connected() {
            match self.websocket.connect_user().await {
                Ok(_conn) => {}
                Err(e) => {
                    log::error!("Error starting WebSocket client: {:?}", e);
                }
            }
        }
        self.auto_update_client().await?;
        Ok(())
    }

    /// Retrieve historical bars for a given instrument and topic over [start, end).
    /// Paginates using the ProjectX 20,000-bars limit and normalizes timestamps to UTC.
    pub async fn retrieve_bars(
        &self,
        req: &HistoricalRangeRequest,
    ) -> anyhow::Result<Vec<HistoryEvent>> {
        // Map Topic -> Resolution and PX unit fields
        let (resolution, unit, unit_number) = match req.topic {
            Topic::Candles1s => (Resolution::Seconds(1), 1, 1),
            Topic::Candles1m => (Resolution::Minutes(1), 2, 1),
            Topic::Candles1h => (Resolution::Hours(1), 3, 1),
            Topic::Candles1d => (Resolution::Daily, 4, 1),
            _ => anyhow::bail!("retrieve_bars only supports candle topics (1s/1m/1h/1d)"),
        };

        // Resolve contract id and exchange
        let (contract_id, exchange) = {
            // Try the current in-memory snapshot first, then force a manual update if missing.
            let mut cid: Option<String> = None;
            let mut ex: Option<Exchange> = None;
            if let Some(fc) = self.instruments.get(&req.instrument) {
                let fc = fc.value();
                cid = Some(fc.provider_contract_name.clone());
                ex = Some(fc.exchange);
            }

            if cid.is_none() {
                self.manual_update_instruments(false).await?;
                if let Some(fc) = self.instruments.get(&req.instrument) {
                    let fc = fc.value();
                    cid = Some(fc.provider_contract_name.clone());
                    ex = Some(fc.exchange);
                }
            }

            (
                cid.unwrap_or_else(|| px_format_from_instrument(&req.instrument)),
                ex.expect("exchange must be present for known instrument"),
            )
        };

        let limit: i32 = 5_000;
        let step = resolution.as_duration();
        let chunk_span = step * limit;

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
            let mut batch =
                match resp.to_engine_candles(req.instrument.clone(), resolution, exchange) {
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
                cur_start += chrono::Duration::seconds(1);
                continue;
            }

            // Append and update pagination anchor by *max start* in this batch
            let (min_s, max_s) = (
                batch.iter().map(|c| c.time_start).min().unwrap(),
                batch.iter().map(|c| c.time_start).max().unwrap(),
            );
            log::info!(
                "candles: {} from start={} to start={}",
                batch.len(),
                min_s,
                max_s
            );

            // Compute max END in this batch before moving it into out
            let max_end_in_batch = batch.iter().map(|c| c.time_end).max().unwrap();

            out.extend(batch);
            // Update anchors: keep start-based watermark for filtering/logging
            prev_max_start = Some(match prev_max_start {
                Some(prev) => prev.max(max_s),
                None => max_s,
            });

            // Advance by last bar END + 1s to avoid start-boundary stalls on 1s bars
            cur_start = (max_end_in_batch + chrono::Duration::seconds(1)).min(end);
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
        matches!(
            topic,
            Topic::Ticks
                | Topic::Quotes
                | Topic::MBP10
                | Topic::Candles1s
                | Topic::Candles1m
                | Topic::Candles1h
                | Topic::Candles1d
        )
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
        info!("Subscribing to: {}", instrument);
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
            Topic::MBP10 => {
                self.websocket
                    .subscribe_contract_market_depth(instrument.as_str())
                    .await
            }
            Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d => {
                self.websocket
                    .subscribe_contract_candles(instrument.as_str())
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
            Topic::MBP10 => {
                self.websocket
                    .unsubscribe_contract_market_depth(instrument.as_str())
                    .await
            }
            Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d => {
                self.websocket
                    .unsubscribe_contract_candles(instrument.as_str())
                    .await
            }
            _ => anyhow::bail!("Unsupported topic: {:?}", topic),
        }
    }

    async fn active_md_subscriptions(&self) -> AHashMap<Topic, Vec<SymbolKey>> {
        // Collect current active subscriptions from the websocket client which tracks
        // contract ids per topic internally.
        fn symbol_from_contract_id(firm: ProjectXTenant, instrument: &str) -> Option<SymbolKey> {
            // Convert PX contract id (e.g., "CON.F.US.MNQ.Z25") to our Instrument format
            let provider = ProviderKind::ProjectX(firm);
            let dotted = crate::websocket::client::parse_px_instrument(instrument);
            let instrument = Instrument::try_parse_dotted(&dotted).ok()?;
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
        map.insert(Topic::MBP10, depth);
        map
    }

    async fn list_instruments(
        &self,
        pattern: Option<String>,
    ) -> anyhow::Result<Vec<tt_types::securities::symbols::Instrument>> {
        // Use HTTP snapshot maintained by PxHttpClient; filter by optional pattern (case-insensitive contains)
        // Snapshot returns Vec<FuturesContract>; extract their Instrument values
        let contracts = self.instruments_map_snapshot().await?;
        // Map to Instrument and optionally filter by pattern (case-insensitive)
        let mut instruments: Vec<tt_types::securities::symbols::Instrument> =
            contracts.into_iter().map(|c| c.instrument).collect();
        if let Some(mut pat) = pattern {
            pat.make_ascii_uppercase();
            instruments.retain(|inst| inst.to_string().to_uppercase().contains(&pat));
        }
        Ok(instruments)
    }

    async fn instruments(
        &self,
    ) -> anyhow::Result<Vec<tt_types::securities::security::FuturesContract>> {
        self.instruments_map_snapshot().await
    }

    async fn auto_update(&self) -> anyhow::Result<()> {
        self.auto_update_client().await
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
        let id = self.account_id(account_key.account_name.clone()).await?;
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
        let id = self.account_id(account_key.account_name.clone()).await?;
        let lock = self.account_subscriptions.write().await;
        let _ = lock.iter().position(|k| k == account_key);
        self.websocket.unsubscribe_user_account(id).await
    }

    async fn subscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.account_id(account_key.account_name.clone()).await?;
        self.websocket.subscribe_account_positions(id).await
    }

    async fn unsubscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.account_id(account_key.account_name.clone()).await?;
        self.websocket.unsubscribe_account_positions(id).await
    }

    async fn active_account_subscriptions(&self) -> Vec<AccountKey> {
        let lock = self.account_subscriptions.read().await;
        lock.clone()
    }

    async fn subscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.account_id(account_key.account_name.clone()).await?;
        self.websocket.subscribe_account_orders(id).await
    }

    async fn unsubscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.account_id(account_key.account_name.clone()).await?;
        self.websocket.unsubscribe_account_orders(id).await
    }

    async fn place_order(&self, spec: tt_types::wire::PlaceOrder) -> CommandAck {
        //info!("Order Received in ProjectX: {:?}", spec);
        let s = spec.clone();
        use crate::http::models::{BracketCfg, PlaceOrderReq};
        // Map typed spec to ProjectX PlaceOrderReq
        let type_i = match spec.order_type {
            tt_types::wire::OrderType::Limit => 1,
            tt_types::wire::OrderType::Market => 2,
            tt_types::wire::OrderType::Stop => 4,
            tt_types::wire::OrderType::TrailingStop => 5,
            tt_types::wire::OrderType::JoinBid => 6,
            tt_types::wire::OrderType::JoinAsk => 7,
            tt_types::wire::OrderType::StopLimit => 4, // PX uses Stop with limit/stop provided
        };
        let side_i = match spec.side {
            tt_types::accounts::events::Side::Buy => 0,
            tt_types::accounts::events::Side::Sell => 1,
        };
        fn map_type(t: tt_types::wire::OrderType) -> i32 {
            match t {
                tt_types::wire::OrderType::Limit => 1,
                tt_types::wire::OrderType::Market => 2,
                tt_types::wire::OrderType::Stop => 4,
                tt_types::wire::OrderType::TrailingStop => 5,
                tt_types::wire::OrderType::JoinBid => 6,
                tt_types::wire::OrderType::JoinAsk => 7,
                tt_types::wire::OrderType::StopLimit => 4,
            }
        }
        let stop_loss_bracket = spec.stop_loss.map(|b| BracketCfg {
            ticks: b.ticks,
            type_: map_type(b.order_type),
        });
        let take_profit_bracket = spec.take_profit.map(|b| BracketCfg {
            ticks: b.ticks,
            type_: map_type(b.order_type),
        });

        if let Some(contract) = self.instruments.get(&spec.instrument) {
            let contract = contract.value();
            if let Some(account) = self.internal_accounts.get(&spec.account_key.account_name) {
                let req = PlaceOrderReq {
                    account_id: account.id,
                    contract_id: contract.provider_contract_name.clone(),
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
                //info!("place order request: {:?}", req);
                let res = self.http.inner.place_order(&req).await;
                return match res {
                    Ok(r) => {
                        info!("PlaceOrder Success: {:?}", r);
                        CommandAck {
                            ok: r.success,
                            message: r.error_message,
                        }
                    }
                    Err(e) => {
                        info!("PlaceOrder Error: {:?}", e);
                        CommandAck {
                            ok: false,
                            message: Some(format!("place_order error: {}", e)),
                        }
                    }
                };
            }
            error!("PlaceOrder Error No Instrument: {:?}", s);
            return CommandAck {
                ok: false,
                message: Some(format!(
                    "cancel_order error: No instrument found on {:?} for : {:?}",
                    self.provider_kind, spec.instrument
                )),
            };
        }
        error!("PlaceOrder Error No account: {:?}", s);
        CommandAck {
            ok: false,
            message: Some(format!(
                "cancel_order error: No account found on {:?} client for : {}",
                self.provider_kind, spec.account_key.account_name
            )),
        }
    }

    async fn cancel_order(&self, spec: tt_types::wire::CancelOrder) -> CommandAck {
        let Ok(order_id) = spec.provider_order_id.parse::<i64>() else {
            return CommandAck {
                ok: false,
                message: Some("invalid provider_order_id; expected numeric string".to_string()),
            };
        };
        if let Some(account) = self.internal_accounts.get(&spec.account_name) {
            let res = self.http.inner.cancel_order(account.id, order_id).await;
            return match res {
                Ok(r) => CommandAck {
                    ok: r.success,
                    message: r.error_message,
                },
                Err(e) => CommandAck {
                    ok: false,
                    message: Some(format!("cancel_order error: {}", e)),
                },
            };
        }
        CommandAck {
            ok: false,
            message: Some(format!(
                "cancel_order error: No account found on {:?} client for : {}",
                self.provider_kind, spec.account_name
            )),
        }
    }

    async fn replace_order(&self, spec: tt_types::wire::ReplaceOrder) -> CommandAck {
        use crate::http::models::ModifyOrderReq;
        let Ok(order_id) = spec.provider_order_id.parse::<i64>() else {
            return CommandAck {
                ok: false,
                message: Some("invalid provider_order_id; expected numeric string".to_string()),
            };
        };
        if let Some(account) = self.internal_accounts.get(&spec.account_name) {
            let req = ModifyOrderReq {
                account_id: account.id,
                order_id,
                size: spec.new_qty,
                limit_price: spec.new_limit_price,
                stop_price: spec.new_stop_price,
                trail_price: spec.new_trail_price,
            };
            let res = self.http.inner.modify_order(&req).await;
            return match res {
                Ok(r) => CommandAck {
                    ok: r.success,
                    message: r.error_message,
                },
                Err(e) => CommandAck {
                    ok: false,
                    message: Some(format!("modify_order error: {}", e)),
                },
            };
        }
        CommandAck {
            ok: false,
            message: Some(format!(
                "cancel_order error: No account found on {:?} client for : {}",
                self.provider_kind, spec.account_name
            )),
        }
    }

    async fn auto_update(&self) -> anyhow::Result<()> {
        self.auto_update_client().await
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

    async fn fetch(&self, req: HistoricalRangeRequest) -> anyhow::Result<Vec<HistoryEvent>> {
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
        topic: Topic,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        self.manual_update_instruments(false).await?;
        // Look for the matching instrument in the DashMap without taking async locks.
        if let Some(c) = self
            .instruments
            .iter()
            .find(|r| r.value().instrument == instrument)
            .map(|r| r.value().activation_date)
        {
            let dt = match topic {
                Topic::Candles1s => Utc::now() - Duration::weeks(2),
                Topic::Candles1m => Utc::now() - Duration::weeks(12),
                Topic::Candles1h => Utc::now() - Duration::days(365),
                Topic::Candles1d => Utc::now() - Duration::days(365),
                _ => return Err(anyhow!("Unsupported topic for historical data".to_string())),
            };
            return Ok(Some(dt));
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
