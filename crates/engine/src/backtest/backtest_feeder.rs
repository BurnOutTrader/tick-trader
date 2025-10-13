use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use rust_decimal::Decimal;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use tt_bus::ClientMessageBus;
use tt_types::accounts::events::{OrderUpdate, PositionDelta, PositionSide, ProviderOrderId};
use tt_types::accounts::order::OrderState;
use tt_types::engine_id::EngineUuid;
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
    /// Optional absolute start time (UTC) to clamp backtest range.
    pub range_start: Option<DateTime<Utc>>,
    /// Optional absolute end time (UTC) to clamp backtest range.
    pub range_end: Option<DateTime<Utc>>,
}
impl Default for BacktestFeederConfig {
    fn default() -> Self {
        // Default to fetching one month at a time to reduce query fan-out and round-trips
        // for historical backtests. Lookahead provides a small buffer to avoid tight refills.
        Self {
            window: ChronoDuration::days(2),
            lookahead: ChronoDuration::days(1),
            warmup: ChronoDuration::zero(),
            range_start: None,
            range_end: None,
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
    /// Optional hard stop time; after this, no more data will be fetched or emitted.
    hard_stop: Option<DateTime<Utc>>,
    /// Whether this key has reached hard stop and is completed
    done: bool,
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
            // Last known mark price per (provider,instrument) to price synthetic fills
            let mut marks: HashMap<(ProviderKind, Instrument), Decimal> = HashMap::new();

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
                let mut end = want_end + cfg.lookahead;
                // Respect hard stop if configured
                if let Some(hs) = ks.hard_stop
                    && end > hs
                {
                    end = hs;
                }
                if start >= end {
                    // Nothing to fetch
                    return;
                }
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
                if ks.done {
                    return;
                }
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
                            let epoch = Utc.timestamp_opt(0, 0).unwrap();
                            let mut start = earliest_opt.unwrap_or(epoch);
                            // Respect configured range start if provided
                            if let Some(rs) = cfg.range_start
                                && rs > start
                            {
                                start = rs;
                            }
                            // Initialize KeyState at the start
                            let mut ks = KeyState {
                                provider,
                                instrument: instr.clone(),
                                topic,
                                cursor: start,
                                window_end: start,
                                buf: BTreeMap::new(),
                                first_emitted: false,
                                hard_stop: cfg.range_end,
                                done: false,
                            };

                            // Warmup prefetch and emit if configured (from start - warmup up to start)
                            if !cfg.warmup.is_zero() {
                                let lb = start.min(cfg.range_start.unwrap_or(start));
                                let mut warm_start =
                                    start.checked_sub_signed(cfg.warmup).unwrap_or(start);
                                if warm_start < lb {
                                    warm_start = lb;
                                }
                                if warm_start < start {
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
                            }
                            // Signal warmup complete to the engine/strategy (even if warmup==0)
                            let _ = bus_clone
                                .route_response(Response::WarmupComplete {
                                    topic,
                                    instrument: instr.clone(),
                                })
                                .await;
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
                        Request::SubscribeAccount(_sa) => {
                            // Backtest: no backend; portfolio updates will be driven by our synthetic orders
                            // We could emit an initial empty snapshot in the future.
                        }
                        Request::PlaceOrder(spec) => {
                            // Synthesize an immediate fill at last known mark
                            let prov = spec.account_key.provider;
                            let instr = spec.instrument.clone();
                            let now = Utc::now();
                            let px = marks
                                .get(&(prov, instr.clone()))
                                .cloned()
                                .unwrap_or(Decimal::ZERO);
                            // Try to extract engine order id from custom tag; otherwise generate one
                            let engine_order_id = if let Some(tag) = &spec.custom_tag {
                                if let Some((eng, _)) =
                                    EngineUuid::extract_and_remove_engine_tag(tag)
                                {
                                    eng
                                } else {
                                    EngineUuid::new()
                                }
                            } else {
                                EngineUuid::new()
                            };
                            let leaves = 0i64;
                            let cum_qty = spec.qty;
                            let ou = OrderUpdate {
                                name: spec.account_key.account_name.clone(),
                                instrument: instr.clone(),
                                provider_kind: prov,
                                provider_order_id: Some(ProviderOrderId(format!(
                                    "bt-{}",
                                    engine_order_id
                                ))),
                                order_id: engine_order_id,
                                state: OrderState::Filled,
                                leaves,
                                cum_qty,
                                avg_fill_px: px,
                                tag: spec.custom_tag.clone(),
                                time: now,
                            };
                            let ob = tt_types::wire::OrdersBatch {
                                topic: Topic::Orders,
                                seq: 0,
                                orders: vec![ou],
                            };
                            let side = if spec.side == tt_types::accounts::events::Side::Buy {
                                PositionSide::Long
                            } else {
                                PositionSide::Short
                            };
                            let pd = PositionDelta {
                                instrument: instr.clone(),
                                account_name: spec.account_key.account_name.clone(),
                                provider_kind: prov,
                                net_qty: Decimal::from(cum_qty),
                                average_price: px,
                                open_pnl: Decimal::ZERO,
                                time: now,
                                side,
                            };
                            let pb = tt_types::wire::PositionsBatch {
                                topic: Topic::Positions,
                                seq: 0,
                                positions: vec![pd],
                            };
                            let _ = bus_clone.route_response(Response::OrdersBatch(ob)).await;
                            let _ = bus_clone.route_response(Response::PositionsBatch(pb)).await;
                        }
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
                            // If we have a hard stop and this timestamp is beyond it, complete this key
                            if let Some(hs) = ks.hard_stop
                                && t > hs
                            {
                                ks.done = true;
                                info!(topic=?topic, inst=%ks.instrument, stop=%hs, "backtest_feeder: reached hard stop; completing key");
                                // Do not reinsert; drop key below
                                keys.remove(&(topic, sk.clone()));
                                continue;
                            }
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
                                // Update last marks for synthetic fills
                                match &item {
                                    tt_database::queries::TopicDataEnum::Tick(tk) => {
                                        marks
                                            .insert((ks.provider, tk.instrument.clone()), tk.price);
                                    }
                                    tt_database::queries::TopicDataEnum::Bbo(bbo) => {
                                        // Use bid as a conservative mark for simplicity
                                        marks
                                            .insert((ks.provider, bbo.instrument.clone()), bbo.bid);
                                    }
                                    tt_database::queries::TopicDataEnum::Candle(c) => {
                                        marks.insert((ks.provider, c.instrument.clone()), c.close);
                                    }
                                    tt_database::queries::TopicDataEnum::Mbp10(_m) => {}
                                }
                                emit_one(&bus_clone, &item, ks.provider).await;
                            }
                            // Move cursor up to at least this t
                            if t > ks.cursor {
                                ks.cursor = t;
                            }
                            // Refill window if close to end and not past hard stop
                            let can_refill = ks.hard_stop.map(|hs| ks.cursor < hs).unwrap_or(true);
                            if can_refill && ks.cursor + ChronoDuration::seconds(1) >= ks.window_end
                            {
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
