use crate::models::{Command, DataTopic};
use crate::runtime::EngineRuntimeShared;
use anyhow::anyhow;
use crossbeam::queue::ArrayQueue;
use dashmap::DashMap;
use rust_decimal::prelude::ToPrimitive;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::oneshot;
use tt_bus::ClientMessageBus;
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::PositionSide;
use tt_types::consolidators::ConsolidatorKey;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{Request, Response};

#[derive(Clone)]
pub struct EngineHandle {
    pub(crate) inner: Arc<EngineRuntimeShared>,
    pub(crate) cmd_q: Arc<ArrayQueue<Command>>,
}

impl EngineHandle {
    // === TIME ===
    /// Current engine time. In backtests, this is the deterministic simulation clock; in live, system time.
    #[inline]
    pub fn time(&self) -> chrono::DateTime<chrono::Utc> {
        self.inner.now_dt()
    }
    /// Current engine time in nanoseconds since UNIX_EPOCH.
    #[inline]
    pub fn time_ns(&self) -> u64 {
        self.inner.now_ns()
    }

    // === REGISTRATION ===
    /// Register a consolidator to be driven by the engine for the given data stream key.
    /// The consolidator will be invoked whenever data for (topic,key) arrives.
    /// The data will be sent to the strategies on_bar(&mut self, candle: Candle) function.
    #[inline]
    pub fn add_consolidator(
        &self,
        from_data_topic: DataTopic,
        for_key: SymbolKey,
        cons: Box<dyn tt_types::consolidators::Consolidator + Send>,
    ) {
        let topic = from_data_topic.to_topic_or_err().unwrap();
        let key = ConsolidatorKey::new(for_key.instrument, for_key.provider, topic);
        self.inner.consolidators.insert(key, cons);
    }

    // === Removal ===
    /// Remove a consolidator bring driven by the engine.
    pub fn remove_consolidator(&self, from_data_topic: DataTopic, for_key: SymbolKey) {
        let topic = from_data_topic.to_topic_or_err().unwrap();
        let key = ConsolidatorKey::new(for_key.instrument, for_key.provider, topic);
        self.inner.consolidators.remove(&key);
    }

    // === FIRE-AND-FORGET ===
    /// Enqueue a subscribe request to be handled by the engine task.
    ///
    /// Parameters:
    /// - topic: Logical data stream to subscribe to (e.g., Ticks, Quotes, MBP10, Candles).
    /// - key: SymbolKey including instrument and provider.
    #[inline]
    pub fn subscribe_now(&self, topic: DataTopic, key: SymbolKey) {
        let _ = self.cmd_q.push(Command::Subscribe {
            topic: topic.to_topic_or_err().unwrap(),
            key,
        });
    }
    /// Enqueue an unsubscribe request for a previously subscribed stream.
    ///
    /// Parameters:
    /// - topic: Logical data stream to unsubscribe from.
    /// - key: SymbolKey including instrument and provider.
    #[inline]
    pub fn unsubscribe_now(&self, topic: DataTopic, key: SymbolKey) {
        let _ = self.cmd_q.push(Command::Unsubscribe {
            topic: topic.to_topic_or_err().unwrap(),
            key,
        });
    }
    /// Enqueue a new order for asynchronous placement and return its EngineUuid immediately.
    ///
    /// Parameters:
    /// - spec: Complete order specification (account, key, side, qty, type, prices, etc.).
    ///
    /// Returns: EngineUuid assigned locally for correlation with subsequent order updates.
    #[inline]
    pub fn place_now(&self, mut spec: tt_types::wire::PlaceOrder) -> EngineUuid {
        let id = EngineUuid::new();
        spec.custom_tag = Some(EngineUuid::append_engine_tag(spec.custom_tag.take(), id));
        let _ = self.cmd_q.push(Command::Place(spec));
        id
    }

    // === SYNC GETTERS (instant) ===
    #[inline]
    pub fn is_long(&self, account: &tt_types::keys::AccountKey, instr: &Instrument) -> bool {
        self.inner.portfolio_manager.is_long_account(
            &account.provider,
            &account.account_name,
            instr,
        )
    }
    #[inline]
    pub fn is_short(&self, account: &tt_types::keys::AccountKey, instr: &Instrument) -> bool {
        self.inner.portfolio_manager.is_short_account(
            &account.provider,
            &account.account_name,
            instr,
        )
    }
    #[inline]
    pub fn is_flat(&self, account: &tt_types::keys::AccountKey, instr: &Instrument) -> bool {
        self.inner.portfolio_manager.is_flat_account(
            &account.provider,
            &account.account_name,
            instr,
        )
    }
    #[inline]
    pub fn open_orders_for_instrument(
        &self,
        instr: &Instrument,
    ) -> Vec<tt_types::accounts::events::OrderUpdate> {
        self.inner
            .portfolio_manager
            .open_orders_for_instrument(instr)
    }

    async fn request_with_corr<F>(&self, make: F) -> oneshot::Receiver<Response>
    where
        F: FnOnce(u64) -> Request,
    {
        let corr_id = self.inner.next_corr_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.inner.pending.insert(corr_id, tx);
        let req = make(corr_id);
        // forward and ignore the result here; the response path uses corr_id
        self.inner
            .bus
            .handle_request(&self.inner.sub_id, req)
            .await
            .ok();
        rx
    }

    // Market data
    /// Subscribe to a market data stream for a specific instrument and provider.
    ///
    /// Parameters:
    /// - data_topic: Logical stream to subscribe to (Ticks, Quotes, MBP10, Candles).
    /// - key: SymbolKey composed of instrument and provider.
    ///
    /// On success, a SubscribeResponse will be delivered to the strategy via on_subscribe.
    pub async fn subscribe_key(&self, data_topic: DataTopic, key: SymbolKey) -> anyhow::Result<()> {
        let topic = data_topic.to_topic_or_err()?;
        // Ensure vendor watch is running for this provider
        self.ensure_vendor_securities_watch(key.provider).await;
        self.inner
            .bus
            .handle_request(
                &self.inner.sub_id,
                Request::SubscribeKey(tt_types::wire::SubscribeKey {
                    topic,
                    key,
                    latest_only: false,
                    from_seq: 0,
                }),
            )
            .await?;
        Ok(())
    }

    /// Unsubscribe from a market data stream previously requested with subscribe_key.
    ///
    /// Parameters:
    /// - data_topic: Logical stream to unsubscribe from.
    /// - key: SymbolKey composed of instrument and provider.
    pub async fn unsubscribe_key(
        &self,
        data_topic: DataTopic,
        key: SymbolKey,
    ) -> anyhow::Result<()> {
        let topic = data_topic.to_topic_or_err()?;
        self.inner
            .bus
            .handle_request(
                &self.inner.sub_id,
                Request::UnsubscribeKey(tt_types::wire::UnsubscribeKey { topic, key }),
            )
            .await?;
        Ok(())
    }

    /// List instruments for a provider, optionally filtered by a pattern understood by the server.
    ///
    /// Parameters:
    /// - provider: Data provider to query.
    /// - pattern: Optional filter string (provider-specific; None returns all).
    ///
    /// Returns: Vec of instruments or empty vec on timeout/unsupported response.
    pub async fn list_instruments(
        &self,
        provider: ProviderKind,
        pattern: Option<String>,
    ) -> anyhow::Result<Vec<Instrument>> {
        use std::time::Duration;
        use tokio::time::timeout;
        use tt_types::wire::{InstrumentsRequest, Response as WireResp};
        let rx = self
            .request_with_corr(|corr_id| {
                Request::InstrumentsRequest(InstrumentsRequest {
                    provider,
                    pattern,
                    corr_id,
                })
            })
            .await;
        match timeout(Duration::from_millis(750), rx).await {
            Ok(Ok(WireResp::InstrumentsResponse(ir))) => Ok(ir.instruments),
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => Ok(vec![]),
        }
    }

    pub async fn get_instruments_map(
        &self,
        provider: ProviderKind,
    ) -> anyhow::Result<Vec<FuturesContract>> {
        use std::time::Duration;
        use tokio::time::timeout;
        use tt_types::wire::{InstrumentsMapRequest, Response as WireResp};
        let rx = self
            .request_with_corr(|corr_id| {
                Request::InstrumentsMapRequest(InstrumentsMapRequest { provider, corr_id })
            })
            .await;
        match timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(WireResp::InstrumentsMapResponse(imr))) => Ok(imr.instruments),
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => Ok(vec![]),
        }
    }

    pub async fn update_historical_latest_by_key_async(
        &self,
        provider: ProviderKind,
        topic: Topic,
        instrument: Instrument,
    ) -> anyhow::Result<()> {
        use anyhow::anyhow;
        use tt_types::wire::{DbUpdateKeyLatest, Response as WireResp};
        let instr_clone = instrument.clone();
        let rx = self
            .request_with_corr(|corr_id| {
                Request::DbUpdateKeyLatest(DbUpdateKeyLatest {
                    provider,
                    instrument: instr_clone,
                    topic,
                    corr_id,
                })
            })
            .await;
        match rx.await {
            Ok(WireResp::DbUpdateComplete {
                success, error_msg, ..
            }) => {
                if success {
                    Ok(())
                } else {
                    Err(anyhow!(
                        error_msg.unwrap_or_else(|| "historical update failed".to_string())
                    ))
                }
            }
            Ok(other) => Err(anyhow!(format!("unexpected response: {:?}", other))),
            Err(_canceled) => Err(anyhow!("engine response channel closed")),
        }
    }

    // Fire-and-forget: enqueue cancel; engine task performs I/O
    pub fn cancel_order(&self, order_id: EngineUuid) -> anyhow::Result<()> {
        // Look up provider order ID from map populated by order updates
        if let Some(pid) = self.inner.provider_order_ids.get(&order_id) {
            // Resolve account from our engine_order_accounts map
            if let Some(ak) = self.inner.engine_order_accounts.get(&order_id) {
                let spec = tt_types::wire::CancelOrder {
                    account_name: ak.account_name.clone(),
                    provider_order_id: pid.value().clone(),
                };
                let _ = self.cmd_q.push(Command::Cancel(spec));
                Ok(())
            } else {
                Err(anyhow!("account not known for engine order {}", order_id))
            }
        } else {
            // Provider id not yet known: record a pending cancel with full spec to be sent when first order update arrives
            if let Some(ak) = self.inner.engine_order_accounts.get(&order_id) {
                let spec = tt_types::wire::CancelOrder {
                    account_name: ak.account_name.clone(),
                    provider_order_id: String::new(),
                };
                self.inner.pending_cancels.insert(order_id, spec);
                Ok(())
            } else {
                Err(anyhow!("account not known for engine order {}", order_id))
            }
        }
    }
    // Fire-and-forget: enqueue replace; engine task performs I/O
    pub fn replace_order(
        &self,
        incoming: tt_types::wire::ReplaceOrder,
        order_id: EngineUuid,
    ) -> anyhow::Result<()> {
        if let Some(pid) = self.inner.provider_order_ids.get(&order_id) {
            // Resolve account from our engine_order_accounts map
            if let Some(ak) = self.inner.engine_order_accounts.get(&order_id) {
                let spec = tt_types::wire::ReplaceOrder {
                    account_name: ak.account_name.clone(),
                    provider_order_id: pid.value().clone(),
                    new_qty: incoming.new_qty,
                    new_limit_price: incoming.new_limit_price,
                    new_stop_price: incoming.new_stop_price,
                    new_trail_price: incoming.new_trail_price,
                };
                let _ = self.cmd_q.push(Command::Replace(spec));
                Ok(())
            } else {
                Err(anyhow!("account not known for engine order {}", order_id))
            }
        } else {
            // Provider id not yet known: record a pending replace with full spec to be sent when first order update arrives
            if let Some(ak) = self.inner.engine_order_accounts.get(&order_id) {
                let spec = tt_types::wire::ReplaceOrder {
                    account_name: ak.account_name.clone(),
                    provider_order_id: String::new(),
                    new_qty: incoming.new_qty,
                    new_limit_price: incoming.new_limit_price,
                    new_stop_price: incoming.new_stop_price,
                    new_trail_price: incoming.new_trail_price,
                };
                self.inner.pending_replaces.insert(order_id, spec);
                Ok(())
            } else {
                Err(anyhow!("account not known for engine order {}", order_id))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    /// Convenience: construct a `PlaceOrder` from parameters and enqueue it.
    /// This keeps strategies from having to allocate and own a full `PlaceOrder` struct.
    /// Do not use +oId: as part of your order's custom tag, it is reserved for internal order management
    pub fn place_order(
        &self,
        account_key: tt_types::keys::AccountKey,
        instrument: Instrument,
        side: tt_types::accounts::events::Side,
        qty: i64,
        order_type: tt_types::wire::OrderType,
        limit_price: Option<rust_decimal::Decimal>,
        stop_price: Option<rust_decimal::Decimal>,
        trail_price: Option<rust_decimal::Decimal>,
        custom_tag: Option<String>,
        stop_loss: Option<tt_types::wire::BracketWire>,
        take_profit: Option<tt_types::wire::BracketWire>,
    ) -> anyhow::Result<EngineUuid> {
        let engine_uuid = EngineUuid::new();
        let tag = EngineUuid::append_engine_tag(custom_tag, engine_uuid);
        let account_key_clone = account_key.clone();
        // Normalize quantity sign to platform standard
        let qty_norm = side.normalize_qty(qty);
        let spec = tt_types::wire::PlaceOrder {
            account_key,
            instrument,
            side,
            qty: qty_norm,
            order_type,
            limit_price: limit_price.and_then(|d| d.to_f64()),
            stop_price: stop_price.and_then(|d| d.to_f64()),
            trail_price: trail_price.and_then(|d| d.to_f64()),
            custom_tag: Some(tag),
            stop_loss,
            take_profit,
        };
        // Record mapping so we can cancel/replace with minimal info later
        self.inner
            .engine_order_accounts
            .insert(engine_uuid, account_key_clone);
        // Fire-and-forget: enqueue; engine task will send to execution provider
        let _ = self.cmd_q.push(Command::Place(spec));
        Ok(engine_uuid)
    }
    pub fn orders_for_instrument(
        &self,
        instr: &Instrument,
    ) -> Vec<tt_types::accounts::events::OrderUpdate> {
        let guard = self
            .inner
            .state
            .last_orders
            .read()
            .expect("orders lock poisoned");
        guard
            .as_ref()
            .map(|ob| {
                ob.orders
                    .iter()
                    .filter(|o| &o.instrument == instr)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn find_position_delta(
        &self,
        instr: &Instrument,
    ) -> Option<tt_types::accounts::events::PositionDelta> {
        let guard = self
            .inner
            .state
            .last_positions
            .read()
            .expect("positions lock poisoned");
        guard.as_ref().and_then(|pb| {
            pb.positions
                .iter()
                .find(|p| &p.instrument == instr)
                .cloned()
        })
    }
    pub async fn position_state(
        &self,
        _account_name: &AccountName,
        _instr: &Instrument,
    ) -> PositionSide {
        todo!()
    }
    pub async fn position_state_delta(&self, _instr: &Instrument) -> PositionSide {
        todo!()
    }

    pub fn is_long_delta(&self, instr: &Instrument) -> bool {
        self.find_position_delta(instr)
            .map(|p| p.net_qty > rust_decimal::Decimal::ZERO)
            .unwrap_or(false)
    }
    pub fn is_short_delta(&self, instr: &Instrument) -> bool {
        self.find_position_delta(instr)
            .map(|p| p.net_qty < rust_decimal::Decimal::ZERO)
            .unwrap_or(false)
    }
    pub fn is_flat_delta(&self, instr: &Instrument) -> bool {
        self.find_position_delta(instr)
            .map(|p| p.net_qty == rust_decimal::Decimal::ZERO)
            .unwrap_or(true)
    }

    // Reference data cache access
    pub fn securities_for(&self, provider: ProviderKind) -> Vec<Instrument> {
        self.inner
            .securities_by_provider
            .get(&provider)
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }

    async fn ensure_vendor_securities_watch(&self, provider: ProviderKind) {
        if self.inner.backtest_mode {
            return;
        }
        if self.inner.watching_providers.contains_key(&provider) {
            return;
        }
        self.inner.watching_providers.insert(provider, ());
        // Immediate fetch using the client bus correlation helper
        let bus = self.inner.bus.clone();
        let sec_map = self.inner.securities_by_provider.clone();

        async fn fetch_into(
            bus: Arc<ClientMessageBus>,
            provider: ProviderKind,
            sec_map: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
        ) {
            use std::time::Duration;
            use tokio::time::timeout;
            use tt_types::wire::{InstrumentsRequest, Request as WireReq, Response as WireResp};
            let rx = bus
                .request_with_corr(|corr_id| {
                    WireReq::InstrumentsRequest(InstrumentsRequest {
                        provider,
                        pattern: None,
                        corr_id,
                    })
                })
                .await;
            if let Ok(Ok(WireResp::InstrumentsResponse(ir))) =
                timeout(Duration::from_secs(3), rx).await
            {
                sec_map.insert(provider, ir.instruments);
            }
        }
        fetch_into(bus.clone(), provider, sec_map.clone()).await;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tick.tick().await;
                fetch_into(bus.clone(), provider, sec_map.clone()).await;
            }
        });
    }
}
