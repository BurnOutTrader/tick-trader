use crate::models::{Command, DataTopic};
use crate::statics::bus::{bus, initialize_accounts};
use crate::statics::consolidators::CONSOLIDATORS;
use crate::statics::core::{LAST_ASK, LAST_BID, LAST_PRICE};
use crate::statics::subscriptions::CMD_Q;
use crate::traits::Strategy;
use dashmap::DashMap;
use smallvec::SmallVec;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tracing::error;
use tracing::info;
use tt_types::consolidators::ConsolidatorKey;
use tt_types::data::core::Candle;
use tt_types::data::core::{Bbo, Tick};
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{BarBatch, Request, Response, TickBatch};
use tt_types::wire::{Bytes, QuoteBatch};

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

    /// Start the engine processing loop and hand an EngineHandle to the strategy.
    ///
    /// Parameters:
    /// - strategy: Your Strategy implementation; on_start will be invoked with an EngineHandle.
    ///
    /// Returns: EngineHandle for issuing subscriptions and orders from your strategy.
    pub async fn start<S: Strategy>(
        &mut self,
        mut strategy: S,
        backtest_mode: bool,
    ) -> anyhow::Result<()> {
        let mut receiver = bus().add_client();
        let shm_tasks = self.shm_tasks.clone();
        let shm_blacklist_for_task = self.shm_blacklist.clone();
        let backtest_notify_for_task = self.backtest_notify.clone();

        // Call on_start before moving strategy
        info!("engine: invoking strategy.on_start");
        strategy.on_start();
        info!("engine: strategy.on_start returned");
        // Flush any pending commands that on_start may have enqueued.
        Self::drain_commands_for_task().await;
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
        let slow_spin_ns = self.slow_spin_ns;
        let handle_task = tokio::spawn(async move {
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
                        // Drive a time-based consolidation first if due
                        _ = ticker.tick() => {
                            if !backtest_mode {
                                // 1) Walk consolidators mutably and collect outputs.
                                if !CONSOLIDATORS.is_empty() {
                                    let mut outs: SmallVec<[(ProviderKind, Candle); 16]> = SmallVec::new();
                                    for mut entry in CONSOLIDATORS.iter_mut() {
                                        let provider = entry.key().provider;        // read key (cheap)
                                        if let Some(tt_types::consolidators::ConsolidatedOut::Candle(c)) =
                                            entry.value_mut().on_time(crate::statics::clock::time_now())
                                        {
                                            outs.push((provider, c));
                                        }
                                    } // all MapRefMut guards dropped here; no locks held
                                    for (prov, c) in outs {
                                        strategy.on_bar(&c, prov);
                                    }
                                }
                            }
                        },
                        resp_opt = receiver.recv() => {
                            if let Ok(resp) = resp_opt {
                            match resp {
                                Response::BacktestTimeUpdated { now } => {
                                    if !backtest_mode {
                                       panic!("Backtest time created in live mode")
                                    }
                                    // Sync global backtest clock facade so all time reads use this logical time
                                    crate::statics::clock::backtest_advance_to(now);
                                    // 1) Drive time-based consolidators using orchestrator-provided logical time
                                    if !CONSOLIDATORS.is_empty() {
                                        let mut outs: SmallVec<[(ProviderKind, Candle); 16]> = SmallVec::new();
                                        for mut entry in CONSOLIDATORS.iter_mut() {
                                            let provider = entry.key().provider;
                                            if let Some(tt_types::consolidators::ConsolidatedOut::Candle(c)) = entry.value_mut().on_time(now) {
                                                outs.push((provider, c));
                                            }
                                        }
                                        for (prov, c) in outs {
                                            strategy.on_bar(&c, prov);
                                        }
                                    }
                                    // 3) Flat-by-close enforcement is handled by backtest risk models; do not enforce in runtime
                                },
                                Response::BacktestCompleted { end: _ } => {
                                    // Graceful shutdown: stop strategy and terminate engine task
                                    strategy.on_stop();
                                    return;
                                },
                                Response::WarmupComplete{ .. } => {
                                   strategy.on_warmup_complete();
                                },
                                Response::TickBatch(TickBatch {
                                    ticks,
                                    provider_kind,
                                    ..
                                }) => {
                                    for t in ticks {
                                        let symbol_key = SymbolKey::new(t.instrument.clone(), provider_kind);
                                        LAST_PRICE.insert(symbol_key, t.price);
                                        crate::statics::portfolio::apply_mark(provider_kind, &t.instrument, t.price, t.time);
                                        strategy.on_tick(&t, provider_kind);
                                        // Drive any consolidators registered for ticks on this key
                                        let key = ConsolidatorKey::new(t.instrument.clone(), provider_kind, Topic::Ticks);
                                        if let Some(mut cons) = CONSOLIDATORS
                                            .get_mut(&key)
                                            && let Some(out) = cons.on_tick(&t)
                                            && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                                        {
                                            strategy.on_bar(c, provider_kind);
                                        }
                                    }
                                }
                                Response::QuoteBatch(QuoteBatch {
                                    quotes,
                                    provider_kind,
                                    ..
                                }) => {
                                    for q in quotes {
                                        let symbol_key = SymbolKey::new(q.instrument.clone(), provider_kind);
                                        LAST_ASK.insert(symbol_key.clone(), q.ask); LAST_BID.insert(symbol_key, q.bid);
                                        strategy.on_quote(&q, provider_kind);
                                        let key = ConsolidatorKey::new(q.instrument.clone(), provider_kind, Topic::Quotes);
                                        if let Some(mut cons) = CONSOLIDATORS
                                            .get_mut(&key)
                                            && let Some(out) = cons.on_bbo(&q)
                                            && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                                        {
                                            strategy.on_bar(c, provider_kind);
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
                                        let symbol_key = SymbolKey::new(b.instrument.clone(), provider_kind);
                                        LAST_PRICE.insert(symbol_key, b.close);
                                        crate::statics::portfolio::apply_mark(provider_kind, &b.instrument, b.close, b.time_end);
                                        strategy.on_bar(&b, provider_kind);
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
                                            CONSOLIDATORS.get_mut(&key)
                                            && let Some(out) = cons.on_candle(&b)
                                            && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                                        {
                                            strategy.on_bar(c, provider_kind);
                                        }
                                    }
                                }
                                Response::MBP10Batch(ob) => {
                                    strategy.on_mbp10(&ob.event, ob.provider_kind);
                                }
                                Response::OrdersBatch(ob) => {
                                    // Update portfolios in backtest using order updates; live updates flow via provider events
                                    if backtest_mode {
                                        use std::collections::HashMap;
                                        let mut groups: HashMap<AccountKey, Vec<tt_types::accounts::events::OrderUpdate>> = HashMap::new();
                                        for o in &ob.orders {
                                            let key = AccountKey::new(o.provider_kind, o.account_name.clone());
                                            groups.entry(key).or_default().push(o.clone());
                                        }
                                        for (key, orders) in groups {
                                            let batch = tt_types::wire::OrdersBatch { topic: ob.topic, seq: ob.seq, orders };
                                            crate::statics::portfolio::apply_orders_batch(key, batch);
                                        }
                                    }
                                    // Forward order updates to the strategy
                                    strategy.on_orders_batch(&ob);
                                }
                                Response::PositionsBatch(pb) => {
                                    // Update per-account portfolios from positions snapshot; not forwarded to strategies
                                    use std::collections::HashMap;
                                    let topic = pb.topic;
                                    let seq = pb.seq;
                                    let mut groups: HashMap<AccountKey, Vec<tt_types::accounts::events::PositionDelta>> = HashMap::new();
                                    for p in pb.positions.into_iter() {
                                        let key = AccountKey::new(p.provider_kind, p.account_name.clone());
                                        groups.entry(key).or_default().push(p);
                                    }
                                    let now = crate::statics::clock::time_now();
                                    for (key, positions) in groups {
                                        let batch = tt_types::wire::PositionsBatch { topic, seq, positions };
                                        crate::statics::portfolio::apply_positions_batch(key, batch, now);
                                    }
                                }
                                Response::AccountDeltaBatch(ab) => {
                                    // Record account deltas into portfolios
                                    crate::statics::portfolio::apply_account_delta_batch(ab.clone());
                                }
                                Response::ClosedTrades(_t) => {

                                }
                                Response::Tick {
                                    tick,
                                    provider_kind,
                                } => {
                                    let symbol_key = SymbolKey::new(tick.instrument.clone(), provider_kind);
                                    LAST_PRICE.insert(symbol_key, tick.price);
                                    crate::statics::portfolio::apply_mark(provider_kind, &tick.instrument, tick.price, tick.time);
                                    strategy.on_tick(&tick, provider_kind);
                                }
                                Response::Quote { bbo, provider_kind } => {
                                    let symbol_key = SymbolKey::new(bbo.instrument.clone(), provider_kind);
                                    LAST_BID.insert(symbol_key.clone(), bbo.bid);
                                    LAST_ASK.insert(symbol_key.clone(), bbo.ask);
                                    strategy.on_quote(&bbo, provider_kind);
                                }
                                Response::Bar {
                                    candle,
                                    provider_kind,
                                } => {
                                    let symbol_key = SymbolKey::new(candle.instrument.clone(), provider_kind);
                                    LAST_PRICE.insert(symbol_key, candle.close);
                                    crate::statics::portfolio::apply_mark(provider_kind, &candle.instrument, candle.close, candle.time_end);
                                    strategy.on_bar(&candle, provider_kind);
                                }
                                Response::Mbp10 {
                                    mbp10,
                                    provider_kind,
                                } => {
                                    //let symbol_key = SymbolKey::new(mbp10.instrument.clone(), provider_kind);
                                    //LAST_PRICE.insert(symbol_key, mbp10.price); //todo
                                    strategy.on_mbp10(&mbp10, provider_kind);
                                }
                                Response::AnnounceShm(ann) => {
                                    if backtest_mode { continue; }

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
                                                                    error!("tx shm broadcast failed: {e}; terminating SHM task");
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
                                                                        .handle_request(
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
                                Response::DbUpdateComplete { .. } => {}
                            }
                            // Flush any pending commands emitted by strategy callbacks.
                            Self::drain_commands_for_task().await;
                            // After processing a message in backtest mode, notify feeder so it can proceed to the next one.
                            if backtest_mode
                                && let Some(notify) = &backtest_notify_for_task
                            {
                                notify.notify_one();
                            }
                        }
                    }
                }
            }
        });

        self.task = Some(handle_task);
        Ok(())
    }

    async fn drain_commands_for_task() {
        use tt_types::wire::{SubscribeKey, UnsubscribeKey};
        let bus = bus();
        while let Some(cmd) = CMD_Q.pop() {
            match cmd {
                Command::Subscribe { topic, key } => {
                    let _ = bus
                        .handle_request(Request::SubscribeKey(SubscribeKey {
                            topic,
                            key,
                            latest_only: false,
                            from_seq: 0,
                        }))
                        .await;
                }
                Command::Unsubscribe { topic, key } => {
                    let _ = bus
                        .handle_request(Request::UnsubscribeKey(UnsubscribeKey { topic, key }))
                        .await;
                }
                Command::Place(spec) => {
                    let _ = bus.handle_request(Request::PlaceOrder(spec)).await;
                }
                Command::Cancel(spec) => {
                    let _ = bus.handle_request(Request::CancelOrder(spec)).await;
                }
                Command::Replace(spec) => {
                    let _ = bus.handle_request(Request::ReplaceOrder(spec)).await;
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
