use std::future::pending;
use crate::backtest::backtest_clock::BacktestClock;
use crate::engine::EngineAccountsState;
use crate::models::{Command, DataTopic};
use crate::statics::portfolio::Portfolio;
use crate::traits::Strategy;
use ahash::AHashMap;
use crossbeam::queue::ArrayQueue;
use dashmap::DashMap;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use chrono::Utc;
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::info;
use tt_bus::{ClientMessageBus, ClientSubId};
use tt_types::accounts::events::PositionSide;
use tt_types::consolidators::ConsolidatorKey;
use tt_types::data::core::Candle;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{
    AccountDeltaBatch, BarBatch, InstrumentsMapRequest, Kick, OrdersBatch, PositionsBatch,
    QuoteBatch, Request, Response, TickBatch,
};
use crate::statics::bus::{bus, initialize_accounts};
use crate::statics::clock::CLOCK;
use crate::statics::consolidators::CONSOLIDATORS;
use crate::statics::subscriptions::CMD_Q;

pub struct EngineRuntime {
    backtest_mode: bool,
    backtest_notify: Option<Arc<Notify>>,
    sub_id: Option<ClientSubId>,
    rx: Option<mpsc::Receiver<Response>>,
    task: Option<tokio::task::JoinHandle<()>>,
    state: Arc<EngineAccountsState>,
    // Vendor securities cache and watchers
    securities_by_provider: Arc<DashMap<ProviderKind, AHashMap<Instrument, FuturesContract>>>,
    watching_providers: Arc<DashMap<ProviderKind, ()>>,
    // Active SHM polling tasks keyed by (topic,key)
    shm_tasks: Arc<DashMap<(Topic, SymbolKey), tokio::task::JoinHandle<()>>>,
    // Keys for which SHM has been disabled due to fatal errors; fall back to UDS frames per-strategy
    shm_blacklist: Arc<DashMap<(Topic, SymbolKey), ()>>,
    slow_spin_ns: Option<u64>,
    consolidators:
        Arc<DashMap<ConsolidatorKey, Box<dyn tt_types::consolidators::Consolidator + Send>>>,
}

impl EngineRuntime {
    

    /// Create a new EngineRuntime bound to a ClientMessageBus.
    ///
    /// Parameters:
    /// - bus: Connected ClientMessageBus used for all requests and streaming responses.
    /// - slow_spin: Optional nanos to sleep when polling SHM without new data (to avoid tight loops).
    pub fn new(slow_spin: Option<u64>) -> Self {
        let instruments = Arc::new(DashMap::new());
        Self {
            backtest_mode: false,
            backtest_notify: None,
            sub_id: None,
            rx: None,
            task: None,
            state: Arc::new(EngineAccountsState {
                last_orders: Arc::new(RwLock::new(None)),
                last_positions: Arc::new(RwLock::new(None)),
                last_accounts: Arc::new(RwLock::new(None)),
            }),
            securities_by_provider: instruments.clone(),
            watching_providers: Arc::new(DashMap::new()),
            shm_tasks: Arc::new(DashMap::new()),
            shm_blacklist: Arc::new(DashMap::new()),
            slow_spin_ns: slow_spin,
            consolidators: Arc::new(DashMap::new()),
        }
    }

    /// Construct an EngineRuntime in Backtest mode.
    /// Live-only features (SHM, Kick, vendor watch timers) are disabled.
    pub fn new_backtest(
        slow_spin: Option<u64>,
        backtest_notify: Option<Arc<Notify>>,
    ) -> Self {
        let mut rt = Self::new(slow_spin);
        rt.backtest_mode = true;
        rt.backtest_notify = backtest_notify;
        rt
    }

    /// Start the engine processing loop and hand an EngineHandle to the strategy.
    ///
    /// Parameters:
    /// - strategy: Your Strategy implementation; on_start will be invoked with an EngineHandle.
    ///
    /// Returns: EngineHandle for issuing subscriptions and orders from your strategy.
    pub async fn start<S: Strategy>(&mut self, mut strategy: S) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel::<Response>(2048);
        let tx_for_task = tx.clone();
        let sub_id = bus().add_client(tx).await;
        self.sub_id = Some(sub_id.clone());
        self.rx = Some(rx);
        let rx = self.rx.take().expect("rx present after start");
        let shm_tasks = self.shm_tasks.clone();
        let shm_blacklist_for_task = self.shm_blacklist.clone();
        let sub_id_for_task = sub_id.clone();
        let backtest_notify_for_task = self.backtest_notify.clone();
        // Call on_start before moving strategy
        info!("engine: invoking strategy.on_start");
        strategy.on_start();
        info!("engine: strategy.on_start returned");
        // Auto-subscribe to account streams declared by the strategy, if any.
        // We lock briefly to fetch the list, then drop before awaiting network calls.
        let accounts_to_init: Vec<AccountKey> = strategy.accounts();
        if !accounts_to_init.is_empty() {
            info!(
                "engine: initializing {} account(s) for strategy",
                accounts_to_init.len()
            );
            initialize_accounts(accounts_to_init, &sub_id).await?;
        }
        // Move strategy into the spawned task after on_start and accounts have been called
        let mut strategy_for_task = strategy;
        let slow_spin_ns = self.slow_spin_ns;
        let handle_task = tokio::spawn(async move {
            let mut rx = rx;
            // Initial drain to process any commands enqueued during on_start (e.g., subscribe_now)
            Self::drain_commands_for_task(
                sub_id_for_task.clone(),
            )
            .await;

            // Create a time ticker to drive consolidators' on_time about 50ms after each whole second
            let mut ticker = {
                use tokio::time::{Instant, MissedTickBehavior, interval_at};
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let sub_ns = now.subsec_nanos();
                let target_ns: u32 = 50_000_000; // 50ms after the whole second
                let wait_ns: u64 = if sub_ns <= target_ns {
                    (target_ns - sub_ns) as u64
                } else {
                    1_000_000_000u64 - (sub_ns as u64 - target_ns as u64)
                };
                let start = Instant::now() + Duration::from_nanos(wait_ns);
                let mut iv = interval_at(start, Duration::from_secs(1));
                iv.set_missed_tick_behavior(MissedTickBehavior::Skip);
                iv
            };

            loop {
                tokio::select! {
                    biased;
                    // Drive time-based consolidation first if due
                    _ = ticker.tick() => {
                        if !self.backtest_mode {
                            // 1) Walk consolidators mutably and collect outputs.
                            if !CONSOLIDATORS.is_empty() {
                                let mut outs: SmallVec<[(ProviderKind, Candle); 16]> = SmallVec::new();
                                for mut entry in CONSOLIDATORS.iter_mut() {
                                    let provider = entry.key().provider;        // read key (cheap)
                                    if let Some(tt_types::consolidators::ConsolidatedOut::Candle(c)) =
                                        entry.value_mut().on_time(Utc::now())
                                    {
                                        outs.push((provider, c));
                                    }
                                } // all MapRefMut guards dropped here; no locks held
                                for (prov, c) in outs {
                                    strategy_for_task.on_bar(&c, prov);
                                }
                            }
                        }
                    },
                    resp_opt = rx.recv() => {
                        if let Some(resp) = resp_opt {
                            // Fulfill engine-local correlated callbacks first
                            match &resp {
                                Response::InstrumentsResponse(ir) => {
                                    if let Some((_k, tx)) = pending.remove(&ir.corr_id) {
                                        let _ = tx.send(resp.clone());
                                        continue;
                                    }
                                }
                                Response::InstrumentsMapResponse(imr) => {
                                    if let Some((_k, tx)) = pending.remove(&imr.corr_id) {
                                        let _ = tx.send(resp.clone());
                                        continue;
                                    }
                                }
                                Response::AccountInfoResponse(air) => {
                                    if let Some((_k, tx)) = pending.remove(&air.corr_id) {
                                        let _ = tx.send(Response::AccountInfoResponse(air.clone()));
                                        continue;
                                    }
                                }
                                Response::DbUpdateComplete { corr_id, .. } => {
                                    let cid: u64 = *corr_id;
                                    if let Some((_k, tx)) = pending.remove(&cid) {
                                        let _ = tx.send(resp.clone());
                                        continue;
                                    }
                                }
                                _ => {}
                            }
                match resp {
                    Response::BacktestTimeUpdated { now } => {
                        if self.backtest_mode && let Some(prev_now) = last_bt_now && prev_now == now { continue; }
                        // Record logical time for other emissions
                        last_bt_now = Some(now);
                        // 1) Drive time-based consolidators using orchestrator-provided logical time
                        if !handle_inner_for_task.consolidators.is_empty() {
                            let mut outs: SmallVec<[(ProviderKind, Candle); 16]> = SmallVec::new();
                            for mut entry in handle_inner_for_task.consolidators.iter_mut() {
                                let provider = entry.key().provider;
                                if let Some(tt_types::consolidators::ConsolidatedOut::Candle(c)) = entry.value_mut().on_time(now) {
                                    outs.push((provider, c));
                                }
                            }
                            for (prov, c) in outs {
                                strategy_for_task.on_bar(&c, prov);
                            }
                        }
                        // 2) In backtests, periodically synthesize and emit snapshots only if content changed
                        if handle_inner_for_task.backtest_mode {
                            let should_emit = match last_snapshot_emit { Some(prev) => (now - prev) >= chrono::Duration::seconds(1), None => true };
                            if should_emit {
                                // Positions snapshot (content signature ignoring time)
                                let pb = pm.positions_snapshot(now);
                                let mut pos_sig_now: Vec<String> = Vec::with_capacity(pb.positions.len());
                                for p in &pb.positions {
                                    pos_sig_now.push(format!(
                                        "{:?}|{}|{}|{}|{}|{}",
                                        p.provider_kind, p.account_name, p.instrument, p.net_qty, p.average_price, p.open_pnl
                                    ));
                                }
                                let mut emitted_any = false;
                                if last_pos_sig.as_ref().map(|s| s != &pos_sig_now).unwrap_or(!pos_sig_now.is_empty()) {
                                    // Update engine state and notify strategy
                                    {
                                        let mut g = state.last_positions.write().expect("positions lock poisoned");
                                        *g = Some(pb.clone());
                                    }
                                    if !pb.positions.is_empty() {
                                        strategy_for_task.on_positions_batch(&pb);
                                        emitted_any = true;
                                    }
                                    last_pos_sig = Some(pos_sig_now);
                                }
                                // Accounts snapshot (content signature ignoring time)
                                let ab = pm.accounts_snapshot(now);
                                let mut acct_sig_now: Vec<String> = Vec::with_capacity(ab.accounts.len());
                                for a in &ab.accounts {
                                    acct_sig_now.push(format!(
                                        "{:?}|{}|{}|{}",
                                        a.provider_kind, a.name, a.equity, a.day_realized_pnl
                                    ));
                                }
                                if last_acct_sig.as_ref().map(|s| s != &acct_sig_now).unwrap_or(!acct_sig_now.is_empty()) {
                                    {
                                        let mut g = state.last_accounts.write().expect("accounts lock poisoned");
                                        *g = Some(ab.clone());
                                    }
                                    if !ab.accounts.is_empty() {
                                        strategy_for_task.on_account_delta(&ab.accounts);
                                        emitted_any = true;
                                    }
                                    last_acct_sig = Some(acct_sig_now);
                                }
                                if emitted_any { last_snapshot_emit = Some(now); }
                            }
                        }
                        // 3) Flat-by-close enforcement is handled by backtest risk models; do not enforce in runtime
                    },
                    Response::BacktestCompleted { end: _ } => {
                        // Graceful shutdown: stop strategy and terminate engine task
                        strategy_for_task.on_stop();
                        return;
                    },
                    Response::WarmupComplete{ .. } => {
                       strategy_for_task.on_warmup_complete();
                    },
                    Response::TickBatch(TickBatch {
                        ticks,
                        provider_kind,
                        ..
                    }) => {
                        for t in ticks {
                            // Update portfolio marks then deliver to strategy
                            pm.update_apply_last_price(provider_kind, &t.instrument, t.price, handle_inner_for_task.now_dt());
                            strategy_for_task.on_tick(&t, provider_kind);
                            // Drive any consolidators registered for ticks on this key
                            let key = ConsolidatorKey::new(t.instrument.clone(), provider_kind, Topic::Ticks);
                            if let Some(mut cons) = handle_inner_for_task
                                .consolidators
                                .get_mut(&key)
                                && let Some(out) = cons.on_tick(&t)
                                && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                            {
                                strategy_for_task.on_bar(c, provider_kind);
                            }
                        }
                    }
                    Response::QuoteBatch(QuoteBatch {
                        quotes,
                        provider_kind,
                        ..
                    }) => {
                        for q in quotes {
                            // Drive consolidators for quotes (BBO)
                            pm.update_apply_last_price(provider_kind, &q.instrument, q.bid, handle_inner_for_task.now_dt());
                            strategy_for_task.on_quote(&q, provider_kind);
                            let key = ConsolidatorKey::new(q.instrument.clone(), provider_kind, Topic::Quotes);
                            if let Some(mut cons) = handle_inner_for_task
                                .consolidators
                                .get_mut(&key)
                                && let Some(out) = cons.on_bbo(&q)
                                && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                            {
                                strategy_for_task.on_bar(c, provider_kind);
                            }
                        }
                    }
                    Response::BarBatch(BarBatch {
                        bars,
                        provider_kind,
                        ..
                    }) => {
                        for b in bars {
                            // Use close as mark
                            pm.update_apply_last_price(provider_kind, &b.instrument, b.close, handle_inner_for_task.now_dt());
                            strategy_for_task.on_bar(&b, provider_kind);
                            // Drive consolidators for incoming candles (candle-to-candle)
                            let tpc = match b.resolution {
                                tt_types::data::models::Resolution::Seconds(1) => Topic::Candles1s,
                                tt_types::data::models::Resolution::Minutes(1) => Topic::Candles1m,
                                tt_types::data::models::Resolution::Hours(1) => Topic::Candles1h,
                                tt_types::data::models::Resolution::Daily => Topic::Candles1d,
                                tt_types::data::models::Resolution::Weekly => Topic::Candles1d,
                                _ => Topic::Candles1d,
                            };
                            let key = ConsolidatorKey::new(b.instrument.clone(), provider_kind, tpc);
                            if let Some(mut cons) =
                                handle_inner_for_task.consolidators.get_mut(&key)
                                && let Some(out) = cons.on_candle(&b)
                                && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                            {
                                strategy_for_task.on_bar(c, provider_kind);
                            }
                        }
                    }
                    Response::MBP10Batch(ob) => {
                        //todo, the way we apply Mpd10 will not actually be this way, it will be broken down by types.
                         // trades and bbo can build bars from the single feed.
                        let resp2 =
                            pm.process_response(tt_types::wire::Response::MBP10Batch(ob.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::MBP10Batch(ob2) = resp2 {
                            strategy_for_task.on_mbp10(&ob2.event, ob2.provider_kind);
                        }
                    }
                    Response::OrdersBatch(ob) => {
                        let resp2 =
                            pm.process_response(tt_types::wire::Response::OrdersBatch(ob.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::OrdersBatch(ob2) = resp2 {
                            {
                                let mut g =
                                    state.last_orders.write().expect("orders lock poisoned");
                                *g = Some(ob2.clone());
                            }
                            {
                                for o in ob2.orders.iter() {
                                    if let Some(pid) = &o.provider_order_id {
                                        handle_inner_for_task
                                            .provider_order_ids
                                            .insert(o.order_id, pid.0.clone());
                                        // If there is a pending cancel for this engine order, dispatch it now
                                        if let Some((_, mut spec)) = handle_inner_for_task.pending_cancels.remove(&o.order_id) {
                                            // fill provider order id and send
                                            spec.provider_order_id = pid.0.clone();
                                            let _ = bus_for_task
                                                .handle_request(&sub_id_for_task, Request::CancelOrder(spec))
                                                .await;
                                        }
                                        // If there is a pending replace order, dispatch it now
                                        else if let Some((_, mut spec)) = handle_inner_for_task.pending_replaces.remove(&o.order_id) {
                                            // fill provider order id and send
                                            spec.provider_order_id = pid.0.clone();
                                            let _ = bus_for_task
                                                .handle_request(&sub_id_for_task, Request::ReplaceOrder(spec))
                                                .await;
                                        }
                                    }
                                    if o.state.is_eol() {
                                        handle_inner_for_task.engine_order_accounts.remove(&o.order_id);
                                        handle_inner_for_task.provider_order_ids.remove(&o.order_id);
                                    }

                                }
                            }
                            strategy_for_task.on_orders_batch(&ob2);
                        }
                    }
                    Response::PositionsBatch(pb) => {
                        let resp2 = pm
                            .process_response(tt_types::wire::Response::PositionsBatch(pb.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::PositionsBatch(pb2) = resp2 {
                            {
                                let mut g = state
                                    .last_positions
                                    .write()
                                    .expect("positions lock poisoned");
                                *g = Some(pb2.clone());
                            }
                            strategy_for_task.on_positions_batch(&pb2);
                            // In backtest mode, whenever positions structurally change, emit an account snapshot too.
                        }
                    }
                    Response::AccountDeltaBatch(ab) => {
                        let resp2 = pm.process_response(tt_types::wire::Response::AccountDeltaBatch(ab.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::AccountDeltaBatch(ab2) = resp2 {
                            {
                                let mut g =
                                    state.last_accounts.write().expect("accounts lock poisoned");
                                *g = Some(ab2.clone());
                            }
                            strategy_for_task.on_account_delta(&ab2.accounts);
                        }
                    }
                    Response::ClosedTrades(t) => {
                        let resp2 =
                            pm.process_response(tt_types::wire::Response::ClosedTrades(t.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::ClosedTrades(t2) = resp2 {
                            strategy_for_task.on_trades_closed(t2);
                        }
                    }
                    Response::Tick {
                        tick,
                        provider_kind,
                    } => {
                        pm.update_apply_last_price(provider_kind, &tick.instrument, tick.price, handle_inner_for_task.now_dt());
                        strategy_for_task.on_tick(&tick, provider_kind);
                    }
                    Response::Quote { bbo, provider_kind } => {
                        let mid = (bbo.bid + bbo.ask) / rust_decimal::Decimal::from(2);
                        pm.update_apply_last_price(provider_kind, &bbo.instrument, mid, handle_inner_for_task.now_dt());
                        strategy_for_task.on_quote(&bbo, provider_kind);
                    }
                    Response::Bar {
                        candle,
                        provider_kind,
                    } => {
                        pm.update_apply_last_price(provider_kind, &candle.instrument, candle.close, handle_inner_for_task.now_dt());
                        strategy_for_task.on_bar(&candle, provider_kind);
                    }
                    Response::Mbp10 {
                        mbp10,
                        provider_kind,
                    } => {
                        //todo, the way we apply Mpd10 will not actually be this way, it will be broken down by types.
                        // trades and bbo can build bars from the single feed.
                        pm.update_apply_last_price(provider_kind, &mbp10.instrument, mbp10.price, handle_inner_for_task.now_dt());
                        strategy_for_task.on_mbp10(&mbp10, provider_kind);
                    }
                    Response::AnnounceShm(ann) => {
                        if handle_inner_for_task.backtest_mode { continue; }
                        use tokio::task::JoinHandle;
                        use tracing::error;
                        use tt_types::data::core::{Bbo, Tick};
                        use tt_types::data::mbp10::Mbp10;
                        use tt_types::wire::Bytes;
                        let topic = ann.topic;
                        let key = ann.key.clone();
                        // If this (topic,key) was blacklisted due to SHM errors, skip spawning reader
                        if shm_blacklist_for_task.get(&(topic, key.clone())).is_some() {
                            info!(?topic, ?key, "SHM disabled for this stream; staying on UDS");
                        } else if shm_tasks.get(&(topic, key.clone())).is_none() {
                            let tx_shm = tx_for_task.clone();
                            let value = key.clone();
                            let bus = bus_for_task.clone();
                            let sub_id = sub_id_for_task.clone();
                            let shm_blacklist = shm_blacklist_for_task.clone();

                            let handle: JoinHandle<()> = tokio::spawn(async move {
                                let mut last_seq: u32 = 0;
                                let mut consecutive_failures: u32 = 0;
                                loop {
                                    let mut progressed = false;
                                    #[allow(clippy::collapsible_if)]
                                    if let Some(reader) = tt_shm::ensure_reader(topic, &value) {
                                        if let Some((seq, buf)) = reader.read_with_seq() {
                                            if seq != last_seq {
                                                progressed = true;
                                                last_seq = seq;
                                                let maybe_resp = match topic {
                                                    Topic::Quotes => Bbo::from_bytes(&buf)
                                                        .ok()
                                                        .map(|bbo| Response::Quote {
                                                            bbo,
                                                            provider_kind: key.provider,
                                                        }),
                                                    Topic::Ticks => Tick::from_bytes(&buf)
                                                        .ok()
                                                        .map(|t| Response::Tick {
                                                            tick: t,
                                                            provider_kind: key.provider,
                                                        }),
                                                    Topic::MBP10 => Mbp10::from_bytes(&buf)
                                                        .ok()
                                                        .map(|m| Response::Mbp10 {
                                                            mbp10: m,
                                                            provider_kind: key.provider,
                                                        }),
                                                    _ => None,
                                                };
                                                if let Some(resp) = maybe_resp {
                                                    consecutive_failures = 0;
                                                    if tx_shm.is_closed() {
                                                        error!(
                                                            "tx shm channel closed; terminating SHM task"
                                                        );
                                                        break;
                                                    }
                                                    if let Err(e) = tx_shm.send(resp).await {
                                                        error!(
                                                            "tx shm send failed: {e}; terminating SHM task"
                                                        );
                                                        break;
                                                    }
                                                } else {
                                                    // Parse failure or unsupported topic buffer
                                                    consecutive_failures =
                                                        consecutive_failures.saturating_add(1);
                                                    if consecutive_failures >= 8 {
                                                        // Mark this (topic,key) as SHM-failed and request framed fallback
                                                        info!(?topic, key = ?value, fails = consecutive_failures, "SHM decode failures; falling back to UDS for this stream");
                                                        shm_blacklist
                                                            .insert((topic, value.clone()), ());
                                                        // Ask server to (re)subscribe; providers may resume framed publishing when SHM is disabled client-side
                                                        let _ = bus
                                                            .handle_request(
                                                                &sub_id,
                                                                Request::SubscribeKey(
                                                                    tt_types::wire::SubscribeKey {
                                                                        topic,
                                                                        key: value.clone(),
                                                                        latest_only: false,
                                                                        from_seq: 0,
                                                                    },
                                                                ),
                                                            )
                                                            .await;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // If no progress (no reader, no new seq, or decode produced no message),
                                    // yield/sleep so task can be aborted and we avoid busy-spin.
                                    if !progressed {
                                        if let Some(ns) = slow_spin_ns {
                                            tokio::time::sleep(Duration::from_nanos(ns)).await;
                                        } else {
                                            tokio::task::yield_now().await;
                                        }
                                    }
                                }
                            });
                            shm_tasks.insert((topic, key), handle);
                        }
                    }
                    Response::SubscribeResponse {
                        topic,
                        instrument,
                        success,
                    } => {
                        let data_topic = DataTopic::from(topic);
                        strategy_for_task.on_subscribe(instrument, data_topic, success);
                    }
                    Response::UnsubscribeResponse { topic, instrument } => {
                        // Stop any SHM polling task(s) for this topic/instrument
                        let to_remove: Vec<(Topic, SymbolKey)> = shm_tasks
                            .iter()
                            .filter_map(|e| {
                                let (t, k) = (e.key().0, e.key().1.clone());
                                if t == topic && k.instrument == instrument {
                                    Some((t, k))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        for (tk, kk) in to_remove {
                            if let Some((_, handle)) = shm_tasks.remove(&(tk, kk)) {
                                handle.abort();
                            }
                        }
                        let data_topic = DataTopic::from(topic);
                        strategy_for_task.on_unsubscribe(instrument, data_topic);
                    }
                    Response::Pong(_)
                    | Response::InstrumentsResponse(_)
                    | Response::InstrumentsMapResponse(_)
                    | Response::VendorData(_)
                    | Response::AccountInfoResponse(_) => {}
                    Response::DbUpdateComplete { .. } => {}
                }
                // After processing a message in backtest mode, notify feeder so it can proceed to the next one.
                if handle_inner_for_task.backtest_mode && let Some(notify) = &backtest_notify_for_task {
                    notify.notify_one();
                }
                // Flush any commands the strategy enqueued during this message
                Self::drain_commands_for_task(
                    cmd_q_for_task.clone(),
                    bus_for_task.clone(),
                    sub_id_for_task.clone(),
                )
                .await;
                        } else { break; }
                    }
                }
            }

            strategy_for_task.on_stop();
        });
        self.task = Some(handle_task);
        Ok(handle)
    }

    async fn drain_commands_for_task(
        sub_id: ClientSubId,
    ) {
        use tt_types::wire::{SubscribeKey, UnsubscribeKey};

        while let Some(cmd) = CMD_Q.pop() {
            match cmd {
                Command::Subscribe { topic, key } => {
                    let _ = bus()
                        .handle_request(
                            Request::SubscribeKey(SubscribeKey {
                                topic,
                                key,
                                latest_only: false,
                                from_seq: 0,
                            }),
                            &sub_id,
                        )
                        .await;
                }
                Command::Unsubscribe { topic, key } => {
                    let _ = bus()
                        .handle_request(
                            Request::UnsubscribeKey(UnsubscribeKey { topic, key }),
                            &sub_id,
                        )
                        .await;
                }
                Command::Place(spec) => {
                    let _ = bus().handle_request(Request::PlaceOrder(spec), &sub_id).await;
                }
                Command::Cancel(spec) => {
                    let _ = bus()
                        .handle_request(Request::CancelOrder(spec), &sub_id)
                        .await;
                }
                Command::Replace(spec) => {
                    let _ = bus()
                        .handle_request(Request::ReplaceOrder(spec), &sub_id)
                        .await;
                }
            }
        }
    }

    #[allow(dead_code)]
    /// Trigger a historical DB update for the latest data for a given provider/topic/instrument.
    /// This sends a DbUpdateKeyLatest request and blocks until the DbUpdateComplete response arrives.
    /// Returns Ok(()) when the update completed successfully; otherwise returns Err with the server error message.
    fn update_historical_latest_by_key(
        provider: ProviderKind,
        topic: Topic,
        instrument: Instrument,
    ) -> anyhow::Result<()> {
        use anyhow::anyhow;
        use tt_types::wire::{DbUpdateKeyLatest, Response as WireResp};
        let fut = async {
            let instr_clone = instrument.clone();
            let rx = bus()
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
        };
        // Execute the async flow in a blocking manner
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Ensure we don't block the runtime reactor; run in a blocking section
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else {
            let rt = tokio::runtime::Runtime::new().map_err(|e| anyhow!(e))?;
            rt.block_on(fut)
        }
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        info!("engine: stopping");
        // Send Kick to bus in live modes; skip in backtest
        if !self.backtest_mode {
            bus()
                .handle_request(
                    tt_types::wire::Request::Kick(Kick {
                        reason: Some("Shutting Down".to_string()),
                    }),
                    self.sub_id.as_ref().expect("engine started"),
                )
                .await
                .ok();
        }
        if let Some(handle) = self.task.take() {
            handle.abort();
        }
        for task in self.shm_tasks.iter() {
            task.abort();
        }
        self.rx.take();
        info!("engine: shutdown complete");
        Ok(())
    }
}

#[cfg(test)]
mod engine_shm_tests {
    use crate::runtime::EngineRuntime;
    use crate::traits::Strategy;
    use std::str::FromStr;
    use tokio::time::{Duration, sleep};
    use tt_bus::ClientMessageBus;
    use tt_types::keys::{SymbolKey, Topic};
    use tt_types::providers::ProviderKind;
    use tt_types::securities::symbols::Instrument;
    use tt_types::wire::Response;

    struct NopStrategy;
    impl Strategy for NopStrategy {}

    // When an AnnounceShm arrives for a non-blacklisted (topic,key), the engine should spawn an SHM reader task.
    #[tokio::test]
    async fn shm_reader_is_spawned_on_announce_when_not_blacklisted() {
        let (req_tx, mut _req_rx) = tokio::sync::mpsc::channel::<tt_types::wire::Request>(8);
        let bus = ClientMessageBus::new_with_transport(req_tx);

        let mut rt = EngineRuntime::new(Some(1));
        let _handle = rt.start(NopStrategy).await.expect("engine start");

        let key = SymbolKey::new(
            Instrument::from_str("TST.Z25").unwrap(),
            ProviderKind::ProjectX(tt_types::providers::ProjectXTenant::Topstep),
        );
        let ann = tt_types::wire::AnnounceShm {
            topic: Topic::MBP10,
            key: key.clone(),
            name: "test.mbp10".to_string(),
            layout_ver: 1,
            size: 4096,
        };

        // Deliver AnnounceShm to the engine via in-memory bus routing.
        let _ = bus.route_response(Response::AnnounceShm(ann)).await;

        // Give the engine loop a short moment to spawn the task.
        sleep(Duration::from_millis(20)).await;

        assert!(
            rt.shm_tasks.get(&(Topic::MBP10, key.clone())).is_some(),
            "expected SHM task to be spawned for non-blacklisted stream"
        );
        let _ = tokio::time::timeout(Duration::from_secs(2), rt.stop())
            .await
            .expect("engine stop should not hang");
    }

    //todo fix test make sure it works
    // If the (topic,key) is blacklisted due to previous SHM errors, AnnounceShm should not spawn a reader task.
    #[tokio::test]
    async fn shm_reader_is_skipped_when_blacklisted() {
        let (req_tx, mut _req_rx) = tokio::sync::mpsc::channel::<tt_types::wire::Request>(8);
        let bus = ClientMessageBus::new_with_transport(req_tx);

        let mut rt = EngineRuntime::new(bus.clone(), Some(1));
        let key = SymbolKey::new(
            Instrument::from_str("TST.Z25").unwrap(),
            ProviderKind::ProjectX(tt_types::providers::ProjectXTenant::Topstep),
        );
        // Pre-mark the stream as SHM-disabled
        rt.shm_blacklist.insert((Topic::MBP10, key.clone()), ());

        let _handle = rt.start(NopStrategy).await.expect("engine start");

        let ann = tt_types::wire::AnnounceShm {
            topic: Topic::MBP10,
            key: key.clone(),
            name: "test.mbp10".to_string(),
            layout_ver: 1,
            size: 4096,
        };
        let _ = bus.route_response(Response::AnnounceShm(ann)).await;

        // Give the engine loop a short moment; it should NOT spawn a task for blacklisted key.
        sleep(Duration::from_millis(20)).await;

        assert!(
            rt.shm_tasks.get(&(Topic::MBP10, key.clone())).is_none(),
            "SHM task should not be spawned for blacklisted stream"
        );

        let _ = tokio::time::timeout(Duration::from_secs(2), rt.stop())
            .await
            .expect("engine stop should not hang");
    }
}
