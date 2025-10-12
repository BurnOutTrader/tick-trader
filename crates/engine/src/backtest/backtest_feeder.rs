use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use tt_bus::ClientMessageBus;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{BarBatch, Response};

use crate::backtest::backtest_clock::BacktestClock;

/// Windowed DB feeder that simulates a provider by emitting Responses over an in-process bus.
/// It listens for SubscribeKey/UnsubscribeKey requests on a request channel you provide when
/// constructing the bus via ClientMessageBus::new_with_transport(req_tx).
#[derive(Clone)]
pub struct BacktestFeederConfig {
    /// Size of prefetch window for each key (e.g., 30 minutes)
    pub window: ChronoDuration,
    /// Size of lookahead buffer beyond the prefetch window (e.g., 5 minutes)
    pub lookahead: ChronoDuration,
    /// Optional warmup period prior to the start timestamp; events in warmup are emitted first
    /// (useful for consolidators to stabilize). If zero, no warmup prefeed occurs.
    pub warmup: ChronoDuration,
}
impl Default for BacktestFeederConfig {
    fn default() -> Self {
        // Default to fetching one month at a time to reduce query fan-out and round-trips
        // for historical backtests. Lookahead provides a small buffer to avoid tight refills.
        Self {
            window: ChronoDuration::days(30),
            lookahead: ChronoDuration::days(1),
            warmup: ChronoDuration::zero(),
        }
    }
}

/// A key subscription with its DB cursor and buffered data.
struct KeyState {
    provider: ProviderKind,
    instrument: Instrument,
    topic: Topic,
    /// Next fetch start time
    cursor: DateTime<Utc>,
    /// End of current fetched window
    window_end: DateTime<Utc>,
    /// Buffered events from DB for this key
    buf: BTreeMap<DateTime<Utc>, Vec<tt_database::queries::TopicDataEnum>>, // ordered by time
    /// Emit info log once on first data emission to confirm flow
    first_emitted: bool,
}

/// Min-heap entry used to merge across keys by next event time
struct HeapEntry {
    t: DateTime<Utc>,
    key: (Topic, SymbolKey),
}
impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.t.eq(&other.t)
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        other.t.cmp(&self.t)
    }
}

pub struct BacktestFeederHandle {
    pub bus: Arc<ClientMessageBus>,
    join: JoinHandle<()>,
}
impl BacktestFeederHandle {
    pub async fn stop(self) {
        self.join.abort();
    }
}

pub struct BacktestFeeder;

impl BacktestFeeder {
    /// Construct an in-process bus and spawn a feeder bound to the provided DB connection and config.
    /// Returns a bus suitable for EngineRuntime::new_backtest and a handle to stop the feeder.
    pub fn start_with_db(
        conn: tt_database::init::Connection,
        cfg: BacktestFeederConfig,
        clock: Option<Arc<BacktestClock>>,
    ) -> BacktestFeederHandle {
        // Create a request channel the bus will use for outbound requests
        let (req_tx, mut req_rx) = mpsc::channel::<tt_types::wire::Request>(1024);
        let bus = ClientMessageBus::new_with_transport(req_tx);
        let bus_clone = bus.clone();

        let join = tokio::spawn(async move {
            // Active subscriptions
            let mut keys: HashMap<(Topic, SymbolKey), KeyState> = HashMap::new();
            // Merge heap of next timestamps per key
            let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
            // Pending outgoing events (after normalization) awaiting emission order
            let mut _out_q: VecDeque<Response> = VecDeque::new();

            // helper: ensure a key has data loaded up to cursor+window
            async fn ensure_window(
                ks: &mut KeyState,
                conn: &tt_database::init::Connection,
                cfg: &BacktestFeederConfig,
            ) {
                let want_end = ks.cursor + cfg.window;
                if want_end <= ks.window_end {
                    return;
                }
                let start = ks.window_end;
                let end = want_end + cfg.lookahead;
                match tt_database::queries::get_time_indexed(
                    conn,
                    ks.provider,
                    &ks.instrument,
                    ks.topic,
                    start,
                    end,
                )
                .await
                {
                    Ok(map) => {
                        let mut rows = 0usize;
                        for (t, v) in map.into_iter() {
                            rows += v.len();
                            ks.buf.entry(t).or_default().extend(v);
                        }
                        info!(topic=?ks.topic, inst=%ks.instrument, start=%start, end=%end, rows, "backtest_feeder: fetched window");
                        ks.window_end = end;
                    }
                    Err(e) => {
                        warn!("feeder: db get_time_indexed error: {:?}", e);
                        ks.window_end = end; // avoid refetch loop
                    }
                }
            }

            // helper: push next event time for a key into heap
            fn push_next_for_key(
                heap: &mut BinaryHeap<HeapEntry>,
                topic: Topic,
                sk: &SymbolKey,
                ks: &KeyState,
            ) {
                if let Some((&t, _)) = ks.buf.iter().next() {
                    heap.push(HeapEntry {
                        t,
                        key: (topic, sk.clone()),
                    });
                }
            }

            // helper: normalize and emit a single TopicDataEnum as Response
            // Map candle resolution to a Topic variant for batching
            fn topic_for_candle(c: &tt_types::data::core::Candle) -> Topic {
                use tt_types::data::models::Resolution;
                match c.resolution {
                    Resolution::Seconds(1) => Topic::Candles1s,
                    Resolution::Minutes(1) => Topic::Candles1m,
                    Resolution::Hours(1) => Topic::Candles1h,
                    Resolution::Daily => Topic::Candles1d,
                    _ => panic!("unexpected resolution"),
                }
            }

            async fn emit_one(
                bus: &Arc<ClientMessageBus>,
                tde: &tt_database::queries::TopicDataEnum,
                provider: ProviderKind,
            ) {
                match tde {
                    tt_database::queries::TopicDataEnum::Tick(t) => {
                        let _ = bus
                            .route_response(Response::Tick {
                                tick: t.clone(),
                                provider_kind: provider,
                            })
                            .await;
                    }
                    tt_database::queries::TopicDataEnum::Bbo(b) => {
                        let _ = bus
                            .route_response(Response::Quote {
                                bbo: b.clone(),
                                provider_kind: provider,
                            })
                            .await;
                    }
                    tt_database::queries::TopicDataEnum::Mbp10(m) => {
                        let _ = bus
                            .route_response(Response::Mbp10 {
                                mbp10: m.clone(),
                                provider_kind: provider,
                            })
                            .await;
                    }
                    tt_database::queries::TopicDataEnum::Candle(c) => {
                        // Emit as a BarBatch (even for a single candle) to carry a topic for routing
                        let batch = BarBatch {
                            topic: topic_for_candle(c),
                            seq: 0,
                            bars: vec![c.clone()],
                            provider_kind: provider,
                        };
                        let _ = bus.route_response(Response::BarBatch(batch)).await;
                    }
                }
            }

            // Main loop: interleave handling of requests with emitting events in time order
            loop {
                // 1) Drain any pending requests without blocking
                while let Ok(req) = req_rx.try_recv() {
                    use tt_types::wire::Request;
                    match req {
                        Request::SubscribeKey(skreq) => {
                            // Acknowledge subscribe immediately
                            let instr = skreq.key.instrument.clone();
                            let topic = skreq.topic;
                            let provider = skreq.key.provider;
                            let _ = bus_clone
                                .route_response(Response::SubscribeResponse {
                                    topic,
                                    instrument: instr.clone(),
                                    success: true,
                                })
                                .await;

                            // Determine start time from DB extent (earliest available); fallback to epoch if none
                            let (earliest_opt, _latest_opt) =
                                match tt_database::queries::get_extent(
                                    &conn, provider, &instr, topic,
                                )
                                .await
                                {
                                    Ok(e) => {
                                        info!("{:?}", e);
                                        e
                                    }
                                    Err(e) => {
                                        warn!("feeder: get_extent error: {:?}", e);
                                        (None, None)
                                    }
                                };
                            let start =
                                earliest_opt.unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());

                            // Initialize KeyState at start
                            let mut ks = KeyState {
                                provider,
                                instrument: instr.clone(),
                                topic,
                                cursor: start,
                                window_end: start,
                                buf: BTreeMap::new(),
                                first_emitted: false,
                            };

                            // Warmup prefetch and emit if configured (from start - warmup up to start)
                            if !cfg.warmup.is_zero() {
                                let warm_start =
                                    start.checked_sub_signed(cfg.warmup).unwrap_or(start);
                                match tt_database::queries::get_time_indexed(
                                    &conn, provider, &instr, topic, warm_start, start,
                                )
                                .await
                                {
                                    Ok(map) => {
                                        for (_t, vec) in map.iter() {
                                            for item in vec {
                                                emit_one(&bus_clone, item, provider).await;
                                            }
                                        }
                                    }
                                    Err(e) => warn!("feeder warmup error: {:?}", e),
                                }
                            }
                            // Prime first window after start
                            ks.cursor = start;
                            ensure_window(&mut ks, &conn, &cfg).await;
                            push_next_for_key(&mut heap, topic, &skreq.key, &ks);
                            keys.insert((topic, skreq.key.clone()), ks);
                        }
                        Request::UnsubscribeKey(ureq) => {
                            keys.remove(&(ureq.topic, ureq.key.clone()));
                            // No specific unsubscribe response in wire; engine will see UnsubscribeResponse only from server in live.
                            let _ = bus_clone
                                .route_response(Response::UnsubscribeResponse {
                                    topic: ureq.topic,
                                    instrument: ureq.key.instrument.clone(),
                                })
                                .await;
                        }
                        // Ignore others in backtest feeder
                        _ => {}
                    }
                }

                // 2) Emit the next earliest event across all keys if available
                if let Some(HeapEntry {
                    t,
                    key: (topic, sk),
                }) = heap.pop()
                {
                    if let Some(ks) = keys.get_mut(&(topic, sk.clone())) {
                        if let Some(mut vec) = ks.buf.remove(&t) {
                            // First emission log for this key
                            if !ks.first_emitted {
                                info!(topic=?topic, inst=%ks.instrument, ts=%t, count=%vec.len(), "backtest_feeder: emitting first data batch for key");
                                ks.first_emitted = true;
                            }
                            // Advance clock deterministically to event time
                            if let Some(ref clock) = clock {
                                let ns = t.timestamp_nanos_opt().unwrap_or(0) as u64; // negative times clamp to 0
                                clock.advance_to_at_least(ns);
                                clock.bump_ns(1);
                            }
                            // Emit all items at this timestamp in recorded order
                            for item in vec.drain(..) {
                                emit_one(&bus_clone, &item, ks.provider).await;
                            }
                            // Move cursor up to at least this t
                            if t > ks.cursor {
                                ks.cursor = t;
                            }
                            // Refill window if close to end
                            if ks.cursor + ChronoDuration::seconds(1) >= ks.window_end {
                                ensure_window(ks, &conn, &cfg).await;
                            }
                        }
                        // Push next time for this key, if any
                        push_next_for_key(&mut heap, topic, &sk, ks);
                    }
                } else {
                    // idle yield
                    tokio::task::yield_now().await;
                }
            }
        });

        BacktestFeederHandle { bus, join }
    }
}
