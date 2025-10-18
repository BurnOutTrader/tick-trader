use crate::models::{Command, DataTopic};
use crate::statics::bus::{bus, initialize_accounts};
use crate::statics::consolidators::CONSOLIDATORS;
use crate::statics::core::{LAST_ASK, LAST_BID, LAST_PRICE};
use crate::statics::subscriptions::CMD_Q;
use crate::traits::Strategy;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::info;
use tt_types::data::core::{Bbo, Tick};
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::wire::{BarsBatch, Request, Response, TickBatch};
use tt_types::wire::{Bytes, QuoteBatch};

// Live warmup state for a candle subscription (per (topic,key))
struct WarmupState {
    awaiting_db: bool,
    cached_ticks: VecDeque<Tick>,
    cached_quotes: VecDeque<Bbo>,
    last_hist_end: Option<DateTime<Utc>>, // last candle time_end seen during warmup
}

pub struct EngineRuntime {
    backtest_mode: bool,
    backtest_notify: Option<Arc<Notify>>,
    task: Option<tokio::task::JoinHandle<()>>,
    // Vendor securities cache and watchers
    // Active SHM polling tasks keyed by (topic,key)
    shm_tasks: Arc<DashMap<(Topic, SymbolKey), tokio::task::JoinHandle<()>>>,
    // Keys for which SHM has been disabled due to fatal errors; fall back to UDS frames per-strategy
    shm_blacklist: Arc<DashMap<(Topic, SymbolKey), ()>>,
    slow_spin_ns: Option<u64>,
}

impl EngineRuntime {
    /// Create a new EngineRuntime bound to a ClientMessageBus.
    ///
    /// Parameters:
    /// - bus: Connected ClientMessageBus used for all requests and streaming responses.
    /// - slow_spin: Optional nanos to sleep when polling SHM without new data (to avoid tight loops).
    pub fn new(slow_spin: Option<u64>) -> Self {
        Self {
            backtest_mode: false,
            backtest_notify: None,
            task: None,
            shm_tasks: Arc::new(DashMap::new()),
            shm_blacklist: Arc::new(DashMap::new()),
            slow_spin_ns: slow_spin,
        }
    }

    /// Construct an EngineRuntime in Backtest mode.
    /// Live-only features (SHM, Kick, vendor watch timers) are disabled.
    pub fn new_backtest(slow_spin: Option<u64>, backtest_notify: Option<Arc<Notify>>) -> Self {
        let mut rt = Self::new(slow_spin);
        rt.backtest_mode = true;
        rt.backtest_notify = backtest_notify;
        rt
    }

    /// Start the engine processing loop and invoke strategy.on_start().
    ///
    /// Parameters:
    /// - strategy: Your Strategy implementation; on_start will be invoked synchronously.
    ///
    /// Returns: Ok(()) after starting; the engine task continues running until stopped.
    pub async fn start<S: Strategy>(
        &mut self,
        mut strategy: S,
        backtest_mode: bool,
    ) -> anyhow::Result<()> {
        let mut receiver = bus().add_client();
        let shm_tasks = self.shm_tasks.clone();
        let shm_blacklist_for_task = self.shm_blacklist.clone();

        // Call on_start before moving strategy
        info!("engine: invoking strategy.on_start");
        strategy.on_start();
        info!("engine: strategy.on_start returned");
        // Initialize live warmup state map (per (topic,key)) and flush any pending commands that on_start may have enqueued.
        let warmups: Arc<DashMap<(Topic, SymbolKey), WarmupState>> = Arc::new(DashMap::new());
        Self::drain_commands_for_task_with_orchestration(backtest_mode, warmups.clone()).await;
        // Auto-subscribe to account streams declared by the strategy, if any.
        // We lock briefly to fetch the list, then drop before awaiting network calls.
        let accounts_to_init: Vec<AccountKey> = strategy.accounts();
        if !accounts_to_init.is_empty() {
            info!(
                "engine: initializing {} account(s) for strategy",
                accounts_to_init.len()
            );
            initialize_accounts(accounts_to_init).await?;
        }
        let notify = self.backtest_notify.clone();
        let slow_spin_ns = self.slow_spin_ns;
        let warmups_for_task = warmups.clone();
        let handle_task = tokio::spawn(async move {
            let mut warmup_complete = false;
            'main_loop: while let Ok(resp) = receiver.recv().await {
                match resp {
                    Response::BacktestTimeUpdated { now, .. } => {
                        if !backtest_mode {
                            panic!("Backtest time created in live mode")
                        }
                        // Sync global backtest clock facade so all time reads use this logical time
                        crate::statics::clock::backtest_advance_to(now);
                        // 1) Drive time-based consolidators using orchestrator-provided logical time
                        let outs = crate::statics::consolidators::drive_time(now);
                        for (prov, c) in outs {
                            strategy.on_bar(&c, prov);
                        }
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::BacktestCompleted { end: _ } => {
                        // Graceful shutdown: stop strategy and terminate engine task
                        strategy.on_stop();
                        break 'main_loop;
                    }
                    Response::WarmupComplete { .. } => {
                        warmup_complete = true;
                        strategy.on_warmup_complete();
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::TickBatch(TickBatch {
                        ticks,
                        provider_kind,
                        ..
                    }) => {
                        for t in ticks {
                            // If any live warmup is active for this instrument/provider, cache the tick instead of processing now
                            let symbol_key = SymbolKey::new(t.instrument.clone(), provider_kind);
                            LAST_PRICE.insert(symbol_key, t.price);
                            crate::statics::portfolio::apply_mark(
                                provider_kind,
                                &t.instrument,
                                t.price,
                                t.time,
                            );
                            strategy.on_tick(&t, provider_kind);
                            // Drive any consolidators registered for ticks on this key
                            if let Some((_prov, c)) =
                                crate::statics::consolidators::drive_tick(&t, provider_kind)
                            {
                                info!(
                                    "emit:consolidator(tick) candle {} {} {}..{} O:{} H:{} L:{} C:{}",
                                    c.instrument,
                                    c.resolution.as_key().unwrap_or_else(|| "na".to_string()),
                                    c.time_start,
                                    c.time_end,
                                    c.open,
                                    c.high,
                                    c.low,
                                    c.close
                                );
                                strategy.on_bar(&c, provider_kind);
                            }
                        }
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::QuoteBatch(QuoteBatch {
                        quotes,
                        provider_kind,
                        ..
                    }) => {
                        for q in quotes {
                            let symbol_key = SymbolKey::new(q.instrument.clone(), provider_kind);
                            LAST_ASK.insert(symbol_key.clone(), q.ask);
                            LAST_BID.insert(symbol_key, q.bid);
                            strategy.on_quote(&q, provider_kind);
                            if let Some((_prov, c)) =
                                crate::statics::consolidators::drive_bbo(&q, provider_kind)
                            {
                                info!(
                                    "emit:consolidator(bbo) candle {} {} {}..{} O:{} H:{} L:{} C:{}",
                                    c.instrument,
                                    c.resolution.as_key().unwrap_or_else(|| "na".to_string()),
                                    c.time_start,
                                    c.time_end,
                                    c.open,
                                    c.high,
                                    c.low,
                                    c.close
                                );
                                strategy.on_bar(&c, provider_kind);
                            }
                        }
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::BarBatch(BarsBatch {
                        bars,
                        provider_kind,
                        topic,
                        ..
                    }) => {
                        for b in bars {
                            // Record emitted candle window for backtest duplicate suppression
                            let symbol_key = SymbolKey::new(b.instrument.clone(), provider_kind);
                            // Use close as mark
                            LAST_PRICE.insert(symbol_key, b.close);
                            crate::statics::portfolio::apply_mark(
                                provider_kind,
                                &b.instrument,
                                b.close,
                                b.time_end,
                            );
                            strategy.on_bar(&b, provider_kind);
                            // Route consolidator lookup by the actual incoming topic
                            if let Some((_prov, c)) = crate::statics::consolidators::drive_candle(
                                topic,
                                &b,
                                provider_kind,
                            ) {
                                strategy.on_bar(&c, provider_kind);
                            }
                        }
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::MBP10Batch(ob) => {
                        strategy.on_mbp10(&ob.event, ob.provider_kind);
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::OrdersBatch(ob) => {
                        // Update portfolios in backtest using order updates; live updates flow via provider events
                        if backtest_mode {
                            use std::collections::HashMap;
                            let mut groups: HashMap<
                                AccountKey,
                                Vec<tt_types::accounts::events::OrderUpdate>,
                            > = HashMap::new();
                            for o in &ob.orders {
                                let key = AccountKey::new(o.provider_kind, o.account_name.clone());
                                groups.entry(key).or_default().push(o.clone());
                            }
                            for (key, orders) in groups {
                                let batch = tt_types::wire::OrdersBatch {
                                    topic: ob.topic,
                                    seq: ob.seq,
                                    orders,
                                };
                                crate::statics::portfolio::apply_orders_batch(key, batch);
                            }
                        }
                        // Forward order updates to the strategy
                        strategy.on_orders_batch(&ob);
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::PositionsBatch(pb) => {
                        // Update per-account portfolios from positions snapshot; not forwarded to strategies
                        use std::collections::HashMap;
                        let topic = pb.topic;
                        let seq = pb.seq;
                        let mut groups: HashMap<
                            AccountKey,
                            Vec<tt_types::accounts::events::PositionDelta>,
                        > = HashMap::new();
                        for p in pb.positions.into_iter() {
                            let key = AccountKey::new(p.provider_kind, p.account_name.clone());
                            groups.entry(key).or_default().push(p);
                        }
                        let now = crate::statics::clock::time_now();
                        for (key, positions) in groups {
                            let batch = tt_types::wire::PositionsBatch {
                                topic,
                                seq,
                                positions,
                            };
                            crate::statics::portfolio::apply_positions_batch(key, batch, now);
                        }
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::AccountDeltaBatch(ab) => {
                        // Record account deltas into portfolios
                        crate::statics::portfolio::apply_account_delta_batch(ab.clone());
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::ClosedTrades(_t) => {
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::Tick {
                        tick,
                        provider_kind,
                    } => {
                        // Cache tick if any warmup is active for this instrument/provider
                        let mut cached = false;
                        for entry in warmups_for_task.iter() {
                            let (tp, sk) = (entry.key().0, entry.key().1.clone());
                            if entry.value().awaiting_db
                                && sk.provider == provider_kind
                                && sk.instrument == tick.instrument
                                && let Some(mut e) = warmups_for_task.get_mut(&(tp, sk))
                            {
                                e.value_mut().cached_ticks.push_back(tick.clone());
                                cached = true;
                            }
                        }
                        if cached {
                            continue;
                        }

                        let symbol_key = SymbolKey::new(tick.instrument.clone(), provider_kind);
                        LAST_PRICE.insert(symbol_key, tick.price);
                        crate::statics::portfolio::apply_mark(
                            provider_kind,
                            &tick.instrument,
                            tick.price,
                            tick.time,
                        );
                        strategy.on_tick(&tick, provider_kind);
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::Quote { bbo, provider_kind } => {
                        // Cache quote if any warmup is active for this instrument/provider
                        let mut cached = false;
                        for entry in warmups_for_task.iter() {
                            let (tp, sk) = (entry.key().0, entry.key().1.clone());
                            if entry.value().awaiting_db
                                && sk.provider == provider_kind
                                && sk.instrument == bbo.instrument
                                && let Some(mut e) = warmups_for_task.get_mut(&(tp, sk))
                            {
                                e.value_mut().cached_quotes.push_back(bbo.clone());
                                cached = true;
                            }
                        }
                        if cached {
                            continue;
                        }
                        let symbol_key = SymbolKey::new(bbo.instrument.clone(), provider_kind);
                        LAST_BID.insert(symbol_key.clone(), bbo.bid);
                        LAST_ASK.insert(symbol_key.clone(), bbo.ask);
                        strategy.on_quote(&bbo, provider_kind);
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::Bar {
                        candle,
                        provider_kind,
                    } => {
                        // Normalize single vendor candle
                        let candle = crate::statics::consolidators::normalize_candle_window(candle);
                        let symbol_key = SymbolKey::new(candle.instrument.clone(), provider_kind);
                        LAST_PRICE.insert(symbol_key, candle.close);
                        crate::statics::portfolio::apply_mark(
                            provider_kind,
                            &candle.instrument,
                            candle.close,
                            candle.time_end,
                        );
                        strategy.on_bar(&candle, provider_kind);
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::Mbp10 {
                        mbp10,
                        provider_kind,
                    } => {
                        //let symbol_key = SymbolKey::new(mbp10.instrument.clone(), provider_kind);
                        //LAST_PRICE.insert(symbol_key, mbp10.price); //todo
                        strategy.on_mbp10(&mbp10, provider_kind);
                        if let Some(notify) = &notify {
                            notify.notify_one();
                        }
                    }
                    Response::AnnounceShm(ann) => {
                        if backtest_mode {
                            continue;
                        }

                        let topic = ann.topic;
                        let key = ann.key.clone();
                        // If this (topic,key) was blacklisted due to SHM errors, skip spawning reader
                        if shm_blacklist_for_task.get(&(topic, key.clone())).is_some() {
                            info!(?topic, ?key, "SHM disabled for this stream; staying on UDS");
                        } else if shm_tasks.get(&(topic, key.clone())).is_none() {
                            let value = key.clone();
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
                                                    // Broadcast directly to the local bus so engine loop receives it
                                                    if let Err(e) = bus().broadcast(resp) {
                                                        error!(
                                                            "tx shm broadcast failed: {e}; terminating SHM task"
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
                                                        let _ = bus()
                                                            .handle_request(Request::SubscribeKey(
                                                                tt_types::wire::SubscribeKey {
                                                                    topic,
                                                                    key: value.clone(),
                                                                    latest_only: false,
                                                                    from_seq: 0,
                                                                },
                                                            ))
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
                        info!(
                            "subscribe: ack {:?} {} success={}",
                            topic, instrument, success
                        );
                        let data_topic = DataTopic::from(topic);
                        strategy.on_subscribe(instrument, data_topic, success);
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
                        strategy.on_unsubscribe(instrument, data_topic);
                    }
                    Response::Pong(_)
                    | Response::InstrumentsResponse(_)
                    | Response::InstrumentsMapResponse(_)
                    | Response::VendorData(_)
                    | Response::AccountInfoResponse(_) => {}
                    Response::DbUpdateComplete {
                        provider,
                        instrument,
                        topic,
                        success: _,
                        ..
                    } => {
                        // Complete warmup for this (topic,key): replay cached ticks newer than last_hist_end
                        let sk = SymbolKey::new(instrument.clone(), provider);
                        if let Some((_k, st)) = warmups_for_task.remove(&(topic, sk.clone())) {
                            let cutoff = st.last_hist_end;
                            for t in st.cached_ticks.into_iter() {
                                if let Some(cu) = cutoff
                                    && t.time < cu
                                {
                                    continue;
                                }
                                let symbol_key = SymbolKey::new(t.instrument.clone(), provider);
                                LAST_PRICE.insert(symbol_key, t.price);
                                crate::statics::portfolio::apply_mark(
                                    provider,
                                    &t.instrument,
                                    t.price,
                                    t.time,
                                );
                                strategy.on_tick(&t, provider);
                                // Drive consolidators for replayed ticks (hybrid will synthesize candles)
                                if let Some((_prov, c)) =
                                    crate::statics::consolidators::drive_tick(&t, provider)
                                {
                                    info!(
                                        "emit:consolidator(replay) candle {} {} {}..{} O:{} H:{} L:{} C:{}",
                                        c.instrument,
                                        c.resolution.as_key().unwrap_or_else(|| "na".to_string()),
                                        c.time_start,
                                        c.time_end,
                                        c.open,
                                        c.high,
                                        c.low,
                                        c.close
                                    );
                                    strategy.on_bar(&c, provider);
                                }
                            }
                            // Replay cached quotes strictly after cutoff as well
                            for q in st.cached_quotes.into_iter() {
                                if let Some(cu) = cutoff
                                    && q.time < cu
                                {
                                    continue;
                                }
                                let symbol_key = SymbolKey::new(q.instrument.clone(), provider);
                                LAST_BID.insert(symbol_key.clone(), q.bid);
                                LAST_ASK.insert(symbol_key.clone(), q.ask);
                                strategy.on_quote(&q, provider);
                                if let Some((_prov, c)) =
                                    crate::statics::consolidators::drive_bbo(&q, provider)
                                {
                                    info!(
                                        "emit:consolidator(replay) candle {} {} {}..{} O:{} H:{} L:{} C:{}",
                                        c.instrument,
                                        c.resolution.as_key().unwrap_or_else(|| "na".to_string()),
                                        c.time_start,
                                        c.time_end,
                                        c.open,
                                        c.high,
                                        c.low,
                                        c.close
                                    );
                                    strategy.on_bar(&c, provider);
                                }
                            }
                            // Announce warmup complete when all keys finished
                            if warmups_for_task.is_empty() && !warmup_complete {
                                info!("warmup: complete for all keys; signaling strategy");
                                strategy.on_warmup_complete();
                                warmup_complete = true;
                            } else {
                                info!("warmup: finished for key {} on {:?}", instrument, provider);
                            }
                        }
                    }
                }
                // Flush any pending commands emitted by strategy callbacks.
                let _ = Self::drain_commands_for_task_with_orchestration(
                    backtest_mode,
                    warmups_for_task.clone(),
                )
                .await;
            }
        });

        self.task = Some(handle_task);
        Ok(())
    }

    async fn drain_commands_for_task_with_orchestration(
        backtest_mode: bool,
        warmups: Arc<DashMap<(Topic, SymbolKey), WarmupState>>,
    ) -> usize {
        use tt_types::wire::{DbUpdateKeyLatest, SubscribeKey, UnsubscribeKey};
        let bus = bus();
        let mut count = 0usize;
        while let Some(cmd) = CMD_Q.pop() {
            match cmd {
                Command::Subscribe { topic, key } => {
                    // Log subscribe intent
                    info!(
                        "subscribe: sending {:?} for {} via {:?}",
                        topic, key.instrument, key.provider
                    );
                    // Live warmup orchestration for candle subscriptions (1s/1m):
                    if !backtest_mode
                        && matches!(
                            topic,
                            Topic::Candles1s
                                | Topic::Candles1m
                                | Topic::Candles1h
                                | Topic::Candles1d
                        )
                    {
                        // Track warmup state for this (topic,key)
                        warmups.insert(
                            (topic, key.clone()),
                            WarmupState {
                                awaiting_db: true,
                                cached_ticks: VecDeque::new(),
                                cached_quotes: VecDeque::new(),
                                last_hist_end: None,
                            },
                        );
                        // Kick off a DB update to fetch the latest completed bars
                        let _ = bus
                            .handle_request(Request::DbUpdateKeyLatest(DbUpdateKeyLatest {
                                provider: key.provider,
                                instrument: key.instrument.clone(),
                                topic,
                                corr_id: 0,
                            }))
                            .await;
                        info!(
                            "warmup: requested DbUpdateKeyLatest for {:?} {} on {:?}",
                            topic, key.instrument, key.provider
                        );
                    }
                    // Invariant: provider on subscribe should match consolidator provider used for this instrument (coarse check)
                    for entry in CONSOLIDATORS.iter() {
                        let k = entry.key();
                        if k.instrument == key.instrument && k.provider != key.provider {
                            info!(
                                "invariant: provider mismatch: consolidator provider={:?} vs subscribe provider={:?} for {} on {:?}",
                                k.provider, key.provider, key.instrument, topic
                            );
                        }
                    }
                    // Forward the original subscribe request
                    let _ = bus
                        .handle_request(Request::SubscribeKey(SubscribeKey {
                            topic,
                            key,
                            latest_only: false,
                            from_seq: 0,
                        }))
                        .await;
                    count += 1;
                }
                Command::Unsubscribe { topic, key } => {
                    let _ = bus
                        .handle_request(Request::UnsubscribeKey(UnsubscribeKey { topic, key }))
                        .await;
                    count += 1;
                }
                Command::Place(spec) => {
                    let _ = bus.handle_request(Request::PlaceOrder(spec)).await;
                    count += 1;
                }
                Command::Cancel(spec) => {
                    let _ = bus.handle_request(Request::CancelOrder(spec)).await;
                    count += 1;
                }
                Command::Replace(spec) => {
                    let _ = bus.handle_request(Request::ReplaceOrder(spec)).await;
                    count += 1;
                }
            }
        }
        count
    }
}
/*
#[cfg(test)]
mod engine_shm_tests {
    use crate::client::ClientMessageBus;
    use crate::runtime::EngineRuntime;
    use crate::traits::Strategy;
    use std::str::FromStr;
    use tokio::time::{Duration, sleep};
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
        rt.start(NopStrategy, true).await.expect("engine start");

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
        let _ = bus.broadcast(Response::AnnounceShm(ann));

        // Give the engine loop a short moment to spawn the task.
        sleep(Duration::from_millis(20)).await;

        assert!(
            rt.shm_tasks.get(&(Topic::MBP10, key.clone())).is_some(),
            "expected SHM task to be spawned for non-blacklisted stream"
        );
    }

    //todo fix test make sure it works
    // If the (topic,key) is blacklisted due to previous SHM errors, AnnounceShm should not spawn a reader task.
    #[tokio::test]
    async fn shm_reader_is_skipped_when_blacklisted() {
        let (req_tx, mut _req_rx) = tokio::sync::mpsc::channel::<tt_types::wire::Request>(8);
        let bus = ClientMessageBus::new_with_transport(req_tx);

        let mut rt = EngineRuntime::new(Some(100));
        let key = SymbolKey::new(
            Instrument::from_str("TST.Z25").unwrap(),
            ProviderKind::ProjectX(tt_types::providers::ProjectXTenant::Topstep),
        );
        // Pre-mark the stream as SHM-disabled
        rt.shm_blacklist.insert((Topic::MBP10, key.clone()), ());

        rt.start(NopStrategy, true).await.expect("engine start");

        let ann = tt_types::wire::AnnounceShm {
            topic: Topic::MBP10,
            key: key.clone(),
            name: "test.mbp10".to_string(),
            layout_ver: 1,
            size: 4096,
        };
        let _ = bus.broadcast(Response::AnnounceShm(ann));

        // Give the engine loop a short moment; it should NOT spawn a task for blacklisted key.
        sleep(Duration::from_millis(20)).await;

        assert!(
            rt.shm_tasks.get(&(Topic::MBP10, key.clone())).is_none(),
            "SHM task should not be spawned for blacklisted stream"
        );
    }
}*/
