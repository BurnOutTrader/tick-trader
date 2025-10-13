use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use tt_bus::ClientMessageBus;
use tt_types::accounts::events::{OrderUpdate, ProviderOrderId};
use tt_types::accounts::order::OrderState;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{BarBatch, Response};

use crate::backtest::backtest_clock::BacktestClock;
use crate::backtest::realism_models::project_x::calander::HoursCalendar;
use crate::backtest::realism_models::project_x::fill::{CmeFillModel, FillConfig};
use crate::backtest::realism_models::project_x::latency::PxLatency;
use crate::backtest::realism_models::project_x::slippage::NoSlippage;
use crate::backtest::realism_models::traits::{FillModel, LatencyModel};

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
        backtest_notify: Option<Arc<Notify>>,
    ) -> BacktestFeederHandle {
        // Create a request channel the bus will use for outbound requests
        let (req_tx, mut req_rx) = mpsc::channel::<tt_types::wire::Request>(1024);
        let bus = ClientMessageBus::new_with_transport(req_tx);
        let bus_clone = bus.clone();

        let join = tokio::spawn(async move {
            let notify = backtest_notify;
            async fn await_ack(notify: &Option<Arc<Notify>>) {
                if let Some(n) = notify {
                    n.notified().await;
                }
            }
            // --- Simple fill engine state (model-driven order lifecycle) ---
            #[derive(Debug, Clone)]
            struct SimOrder {
                spec: tt_types::wire::PlaceOrder,
                provider: ProviderKind,
                #[allow(dead_code)]
                engine_order_id: EngineUuid,
                provider_order_id: ProviderOrderId,
                ack_at: DateTime<Utc>,
                fill_at: DateTime<Utc>,
                ack_emitted: bool,
                user_tag: Option<String>,
                done: bool,
            }
            fn to_chrono(d: std::time::Duration) -> ChronoDuration {
                ChronoDuration::from_std(d)
                    .unwrap_or_else(|_| ChronoDuration::milliseconds(d.as_millis() as i64))
            }
            // Instantiate shared realism models (defaults for now)
            let _lat_model = PxLatency::default();
            let mut fill_model: CmeFillModel = CmeFillModel {
                cfg: FillConfig::default(),
            };
            let mut slip_model = NoSlippage::new();
            let cal_model = HoursCalendar::default();
            // Simulated orders
            let mut sim_orders: HashMap<EngineUuid, SimOrder> = HashMap::new();

            // Active subscriptions
            let mut keys: HashMap<(Topic, SymbolKey), KeyState> = HashMap::new();
            // Merge heap of next timestamps per key
            let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
            // Pending outgoing events (after normalization) awaiting emission order
            let mut _out_q: VecDeque<Response> = VecDeque::new();
            // Last known mark price per (provider,instrument) to price synthetic fills
            let mut marks: HashMap<(ProviderKind, Instrument), Decimal> = HashMap::new();
            // Orchestrator-controlled logical time watermark; only emit events <= this time.
            let mut watermark: Option<DateTime<Utc>> = None;
            // Ensure we emit BacktestCompleted exactly once when end is reached
            let mut completed_emitted: bool = false;

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
                notify: &Option<Arc<Notify>>,
            ) {
                match tde {
                    tt_database::queries::TopicDataEnum::Tick(t) => {
                        let _ = bus
                            .route_response(Response::Tick {
                                tick: t.clone(),
                                provider_kind: provider,
                            })
                            .await;
                        await_ack(notify).await;
                    }
                    tt_database::queries::TopicDataEnum::Bbo(b) => {
                        let _ = bus
                            .route_response(Response::Quote {
                                bbo: b.clone(),
                                provider_kind: provider,
                            })
                            .await;
                        await_ack(notify).await;
                    }
                    tt_database::queries::TopicDataEnum::Mbp10(m) => {
                        let _ = bus
                            .route_response(Response::Mbp10 {
                                mbp10: m.clone(),
                                provider_kind: provider,
                            })
                            .await;
                        await_ack(notify).await;
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
                        await_ack(notify).await;
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
                            await_ack(&notify).await;

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
                                                    emit_one(&bus_clone, item, provider, &notify)
                                                        .await;
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
                            await_ack(&notify).await;
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
                            await_ack(&notify).await;
                        }
                        Request::SubscribeAccount(_sa) => {
                            // Backtest: no backend; portfolio updates will be driven by our synthetic orders
                            // We could emit an initial empty snapshot in the future.
                        }
                        Request::BacktestAdvanceTo(bta) => {
                            // Update watermark and drain events up to this time
                            watermark = Some(bta.to);
                            // Drain in-order up to watermark
                            'drain: loop {
                                if let Some(next) = heap.peek() {
                                    if let Some(wm) = watermark {
                                        if next.t > wm {
                                            break 'drain;
                                        }
                                    } else {
                                        break 'drain;
                                    }
                                } else {
                                    break 'drain;
                                }
                                // Safe to unwrap due to peek above
                                if let Some(HeapEntry {
                                    t,
                                    key: (topic, sk),
                                }) = heap.pop()
                                    && let Some(ks) = keys.get_mut(&(topic, sk.clone()))
                                {
                                    if let Some(mut vec) = ks.buf.remove(&t) {
                                        if let Some(hs) = ks.hard_stop
                                            && t > hs
                                        {
                                            ks.done = true;
                                            info!(topic=?topic, inst=%ks.instrument, stop=%hs, "backtest_feeder: reached hard stop; completing key");
                                            keys.remove(&(topic, sk.clone()));
                                            continue;
                                        }
                                        if !ks.first_emitted {
                                            info!(topic=?topic, inst=%ks.instrument, ts=%t, count=%vec.len(), "backtest_feeder: emitting first data batch for key");
                                            ks.first_emitted = true;
                                        }
                                        if let Some(ref clock) = clock {
                                            let ns = t.timestamp_nanos_opt().unwrap_or(0) as u64;
                                            clock.advance_to_at_least(ns);
                                            clock.bump_ns(1);
                                        }
                                        for item in vec.drain(..) {
                                            match &item {
                                                tt_database::queries::TopicDataEnum::Tick(tk) => {
                                                    marks.insert(
                                                        (ks.provider, tk.instrument.clone()),
                                                        tk.price,
                                                    );
                                                }
                                                tt_database::queries::TopicDataEnum::Bbo(bbo) => {
                                                    marks.insert(
                                                        (ks.provider, bbo.instrument.clone()),
                                                        bbo.bid,
                                                    );
                                                }
                                                tt_database::queries::TopicDataEnum::Candle(c) => {
                                                    marks.insert(
                                                        (ks.provider, c.instrument.clone()),
                                                        c.close,
                                                    );
                                                }
                                                tt_database::queries::TopicDataEnum::Mbp10(_m) => {}
                                            }
                                            emit_one(&bus_clone, &item, ks.provider, &notify).await;
                                        }
                                        if t > ks.cursor {
                                            ks.cursor = t;
                                        }
                                        let can_refill =
                                            ks.hard_stop.map(|hs| ks.cursor < hs).unwrap_or(true);
                                        if can_refill
                                            && ks.cursor + ChronoDuration::seconds(1)
                                                >= ks.window_end
                                        {
                                            ensure_window(ks, &conn, &cfg).await;
                                        }
                                    }
                                    // Push next
                                    push_next_for_key(&mut heap, topic, &sk, ks);
                                }
                            }
                            // After draining market data up to watermark, process simulated orders
                            let now = bta.to;
                            let mut due: Vec<OrderUpdate> = Vec::new();
                            let mut finished: Vec<EngineUuid> = Vec::new();
                            for (oid, so) in sim_orders.iter_mut() {
                                if !so.ack_emitted && now >= so.ack_at {
                                    due.push(OrderUpdate {
                                        name: so.spec.account_key.account_name.clone(),
                                        instrument: so.spec.instrument.clone(),
                                        provider_kind: so.provider,
                                        provider_order_id: Some(so.provider_order_id.clone()),
                                        order_id: *oid,
                                        state: OrderState::Acknowledged,
                                        leaves: so.spec.qty,
                                        cum_qty: 0,
                                        avg_fill_px: Decimal::ZERO,
                                        tag: so.user_tag.clone(),
                                        time: so.ack_at,
                                    });
                                    so.ack_emitted = true;
                                }
                                if !so.done && now >= so.fill_at {
                                    // Price at last mark; use fill model to allow overrides
                                    let last_px = marks
                                        .get(&(so.provider, so.spec.instrument.clone()))
                                        .cloned()
                                        .unwrap_or(Decimal::ZERO);
                                    // Try model matching (no book view for now)
                                    let fills = fill_model.match_book(
                                        now,
                                        None,
                                        last_px,
                                        &mut so.spec,
                                        &mut slip_model,
                                        &cal_model,
                                    );
                                    let mut total_qty: i64 = 0;
                                    let mut vwap_num = Decimal::ZERO;
                                    for f in fills.iter() {
                                        total_qty += f.qty;
                                        vwap_num += f.price * Decimal::from(f.qty);
                                    }
                                    if total_qty == 0 {
                                        total_qty = so.spec.qty;
                                        vwap_num = last_px * Decimal::from(total_qty);
                                    }
                                    let avg = if total_qty != 0 {
                                        vwap_num / Decimal::from(total_qty)
                                    } else {
                                        Decimal::ZERO
                                    };
                                    due.push(OrderUpdate {
                                        name: so.spec.account_key.account_name.clone(),
                                        instrument: so.spec.instrument.clone(),
                                        provider_kind: so.provider,
                                        provider_order_id: Some(so.provider_order_id.clone()),
                                        order_id: *oid,
                                        state: OrderState::Filled,
                                        leaves: 0,
                                        cum_qty: total_qty,
                                        avg_fill_px: avg,
                                        tag: so.user_tag.clone(),
                                        time: so.fill_at,
                                    });
                                    so.done = true;
                                    finished.push(*oid);
                                }
                            }
                            if !due.is_empty() {
                                let ob = tt_types::wire::OrdersBatch {
                                    topic: Topic::Orders,
                                    seq: 0,
                                    orders: due,
                                };
                                let _ = bus_clone.route_response(Response::OrdersBatch(ob)).await;
                                await_ack(&notify).await;
                            }
                            if !finished.is_empty() {
                                for id in finished {
                                    sim_orders.remove(&id);
                                }
                            }
                            // After draining up to watermark (and emitting due orders), notify runtime of logical time
                            let _ = bus_clone
                                .route_response(Response::BacktestTimeUpdated { now })
                                .await;
                            await_ack(&notify).await;
                            // If we have an end range and reached/passed it, emit BacktestCompleted once
                            if !completed_emitted
                                && let Some(end) = cfg.range_end
                                && now >= end
                            {
                                let _ = bus_clone
                                    .route_response(Response::BacktestCompleted { end })
                                    .await;
                                await_ack(&notify).await;
                                completed_emitted = true;
                            }
                        }
                        Request::PlaceOrder(mut spec) => {
                            // Enqueue into simulation engine; do not emit immediately
                            let prov = spec.account_key.provider;
                            // Logical base time from orchestrator
                            let base = watermark.unwrap_or_else(Utc::now);
                            // Ensure an engine order id exists (prefer embedded tag)
                            let (engine_order_id, user_tag) = if let Some(tag) = &spec.custom_tag {
                                if let Some((eng, user_tag)) =
                                    EngineUuid::extract_and_remove_engine_tag(tag)
                                {
                                    (eng, user_tag)
                                } else {
                                    (EngineUuid::new(), None)
                                }
                            } else {
                                (EngineUuid::new(), None)
                            };
                            let provider_order_id =
                                ProviderOrderId(format!("bt-{}", engine_order_id));

                            // Basic validation: quantity sign must match side and be non-zero
                            let mut reject = false;
                            if spec.qty <= 0 {
                                reject = true;
                            } else {
                                // Normalize to positive quantity to keep fills/leaves non-negative across the engine
                                if spec.qty < 0 {
                                    spec.qty = -spec.qty;
                                }
                            }

                            // Logic validation for order types (when we have a mark)
                            let last_mark = marks
                                .get(&(prov, spec.instrument.clone()))
                                .and_then(|d| d.to_f64());

                            match spec.order_type {
                                tt_types::wire::OrderType::Stop => {
                                    // Must have a stop price
                                    if spec.stop_price.is_none() {
                                        reject = true;
                                    } else if let Some(m) = last_mark {
                                        let sp = spec.stop_price.unwrap();
                                        match spec.side {
                                            tt_types::accounts::events::Side::Buy => {
                                                if sp <= m {
                                                    reject = true;
                                                }
                                            }
                                            tt_types::accounts::events::Side::Sell => {
                                                if sp >= m {
                                                    reject = true;
                                                }
                                            }
                                        }
                                    }
                                }
                                tt_types::wire::OrderType::StopLimit => {
                                    if spec.stop_price.is_none() || spec.limit_price.is_none() {
                                        reject = true;
                                    } else if let Some(m) = last_mark {
                                        let sp = spec.stop_price.unwrap();
                                        match spec.side {
                                            tt_types::accounts::events::Side::Buy => {
                                                if sp <= m {
                                                    reject = true;
                                                }
                                            }
                                            tt_types::accounts::events::Side::Sell => {
                                                if sp >= m {
                                                    reject = true;
                                                }
                                            }
                                        }
                                    }
                                }
                                tt_types::wire::OrderType::TrailingStop => {
                                    if spec.trail_price.map(|p| p <= 0.0).unwrap_or(true) {
                                        reject = true;
                                    }
                                }
                                _ => {}
                            }

                            if reject {
                                // Immediately emit a rejected order update with user's original tag
                                let upd = OrderUpdate {
                                    name: spec.account_key.account_name.clone(),
                                    instrument: spec.instrument.clone(),
                                    provider_kind: prov,
                                    provider_order_id: Some(provider_order_id.clone()),
                                    order_id: engine_order_id,
                                    state: OrderState::Rejected,
                                    leaves: 0,
                                    cum_qty: 0,
                                    avg_fill_px: Decimal::ZERO,
                                    tag: user_tag.clone(),
                                    time: base,
                                };
                                let ob = tt_types::wire::OrdersBatch {
                                    topic: Topic::Orders,
                                    seq: 0,
                                    orders: vec![upd],
                                };
                                let _ = bus_clone.route_response(Response::OrdersBatch(ob)).await;
                                await_ack(&notify).await;
                                continue;
                            }

                            // Initialize models (defaults for now)
                            let mut lat = PxLatency::default();
                            let mut fill_model = CmeFillModel {
                                cfg: FillConfig::default(),
                            };
                            let _slip = NoSlippage::new();
                            let _cal = HoursCalendar::default();
                            // Normalize order on submit
                            fill_model.on_submit(base, &mut spec);

                            // Schedule ack and (first) fill per latency
                            let ack_at = base + to_chrono(lat.submit_to_ack());
                            let fill_at = ack_at + to_chrono(lat.ack_to_first_fill());

                            // Stash order
                            let sim = SimOrder {
                                spec,
                                provider: prov,
                                engine_order_id,
                                provider_order_id,
                                ack_at,
                                fill_at,
                                ack_emitted: false,
                                done: false,
                                user_tag,
                            };
                            sim_orders.entry(engine_order_id).or_insert(sim);
                        }
                        _ => {}
                    }
                }

                // 2) Emit the next earliest event across all keys if available AND <= watermark
                let mut did_work = false;
                if let Some(peek) = heap.peek()
                    && let Some(wm) = watermark
                    && peek.t <= wm
                {
                    let HeapEntry {
                        t,
                        key: (topic, sk),
                    } = heap.pop().unwrap();
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
                                emit_one(&bus_clone, &item, ks.provider, &notify).await;
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
                        did_work = true;
                    }
                }
                if !did_work {
                    // idle yield
                    tokio::task::yield_now().await;
                }
            }
        });

        BacktestFeederHandle { bus, join }
    }
}
