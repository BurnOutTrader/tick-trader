use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use rust_decimal::Decimal;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tracing::{info, warn};
use tt_bus::ClientMessageBus;
use tt_types::accounts::events::{AccountDelta, OrderUpdate, ProviderOrderId};
use tt_types::accounts::order::OrderState;
use tt_types::data::mbp10::BookLevels;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{AccountDeltaBatch, BarBatch, Response};

use crate::backtest::backtest_clock::BacktestClock;
use crate::backtest::realism_models::project_x::calander::HoursCalendar;
use crate::backtest::realism_models::project_x::fee::PxFlatFee;
use crate::backtest::realism_models::project_x::fill::{CmeFillModel, FillConfig};
use crate::backtest::realism_models::project_x::latency::PxLatency;
use crate::backtest::realism_models::project_x::slippage::NoSlippage;
use crate::backtest::realism_models::traits::{
    FeeModel, FillModel, LatencyModel, SessionCalendar, SlippageModel,
};

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
    /// Factory for latency model instances used by the feeder (per-order)
    pub make_latency: Arc<dyn Fn() -> Box<dyn LatencyModel> + Send + Sync>,
    /// Factory for per-order fill model instances
    pub make_fill: Arc<dyn Fn() -> Box<dyn FillModel> + Send + Sync>,
    /// Factory for per-order slippage model instances
    pub make_slippage: Arc<dyn Fn() -> Box<dyn SlippageModel> + Send + Sync>,
    /// Factory for per-order fee model instances
    pub make_fee: Arc<dyn Fn() -> Box<dyn FeeModel> + Send + Sync>,
    /// Shared session calendar
    pub calendar: Arc<dyn SessionCalendar>,
}

impl Default for BacktestFeederConfig {
    fn default() -> BacktestFeederConfig {
        Self {
            window: ChronoDuration::days(2),
            lookahead: ChronoDuration::days(1),
            warmup: ChronoDuration::zero(),
            range_start: None,
            range_end: None,
            make_latency: Arc::new(|| Box::new(PxLatency::default())),
            make_fill: Arc::new(|| {
                Box::new(CmeFillModel {
                    cfg: FillConfig::default(),
                })
            }),
            make_slippage: Arc::new(|| Box::new(NoSlippage::new())),
            make_fee: Arc::new(|| Box::new(PxFlatFee::new())),
            calendar: Arc::new(HoursCalendar::default()),
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

/// Last known quote/last marks per symbol for realistic pricing
#[derive(Clone, Copy, Default)]
struct Mark {
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    last: Option<Decimal>,
}
impl Mark {
    #[allow(dead_code)]
    #[inline]
    fn with_last(mut self, px: Decimal) -> Self {
        self.last = Some(px);
        self
    }
    #[allow(dead_code)]
    #[inline]
    fn with_bid(mut self, px: Decimal) -> Self {
        self.bid = Some(px);
        self
    }
    #[allow(dead_code)]
    #[inline]
    fn with_ask(mut self, px: Decimal) -> Self {
        self.ask = Some(px);
        self
    }
    #[inline]
    fn mid(&self) -> Option<Decimal> {
        match (self.bid, self.ask) {
            (Some(b), Some(a)) => Some((b + a) / Decimal::from(2i32)),
            _ => None,
        }
    }
    #[inline]
    fn spread(&self) -> Option<Decimal> {
        match (self.bid, self.ask) {
            (Some(b), Some(a)) => Some(a - b),
            _ => None,
        }
    }
    #[inline]
    fn ref_px(&self) -> Decimal {
        if let Some(m) = self.mid() {
            m
        } else if let Some(l) = self.last {
            l
        } else {
            Decimal::ZERO
        }
    }

    // Timestamped helpers for mark update
    fn update_tick(&mut self, px: Decimal, _t: chrono::DateTime<chrono::Utc>) {
        self.last = Some(px);
    }
    fn update_bbo(&mut self, bid: Decimal, ask: Decimal, _t: chrono::DateTime<chrono::Utc>) {
        self.bid = Some(bid);
        self.ask = Some(ask);
    }
    fn update_candle_close(&mut self, close: Decimal, _t: chrono::DateTime<chrono::Utc>) {
        self.last = Some(close);
    }
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
                // Order lifecycle tracking
                orig_qty: i64,
                cum_qty: i64,
                cum_vwap_num: Decimal,
                // Cancel state
                cancel_at: Option<DateTime<Utc>>,
                cancel_pending: bool,
                // Replace state
                replace_at: Option<DateTime<Utc>>,
                replace_pending: bool,
                replace_req: Option<tt_types::wire::ReplaceOrder>,
                // Per-order models
                fill_model: Box<dyn FillModel>,
                slip_model: Box<dyn SlippageModel>,
                fee_model: Box<dyn FeeModel>,
                latency_model: Box<dyn LatencyModel>,
                fees: Decimal,
                // Resting state for maker attribution
                resting: bool,
            }
            fn to_chrono(d: std::time::Duration) -> ChronoDuration {
                ChronoDuration::from_std(d)
                    .unwrap_or_else(|_| ChronoDuration::milliseconds(d.as_millis() as i64))
            }
            // Instantiate shared realism models from config
            // Note: Fill and Slippage models are per-order factories; instantiate on order placement
            let cal_model: Arc<dyn SessionCalendar> = cfg.calendar.clone();
            // Simulated orders
            let mut sim_orders: HashMap<EngineUuid, SimOrder> = HashMap::new();
            // Per-account ledger for AccountDelta snapshots
            let mut accounts_ledger: HashMap<
                (ProviderKind, tt_types::accounts::account::AccountName),
                AccountDelta,
            > = HashMap::new();
            // Per-account/instrument positions ledger for PositionsBatch snapshots
            let mut positions_ledger: HashMap<
                (
                    ProviderKind,
                    tt_types::accounts::account::AccountName,
                    Instrument,
                ),
                tt_types::accounts::events::PositionDelta,
            > = HashMap::new();

            // Active subscriptions
            let mut keys: HashMap<(Topic, SymbolKey), KeyState> = HashMap::new();
            // Merge heap of next timestamps per key
            let mut heap: BinaryHeap<HeapEntry> = BinaryHeap::new();
            // Pending outgoing events (after normalization) awaiting emission order
            let mut _out_q: VecDeque<Response> = VecDeque::new();
            // Last known quote/last marks per symbol to price synthetic fills realistically
            let mut marks: HashMap<SymbolKey, Mark> = HashMap::new();
            // Latest MBP10 books per symbol for realistic matching
            let mut books: HashMap<SymbolKey, BookLevels> = HashMap::new();
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
                                                    let key = SymbolKey::new(
                                                        tk.instrument.clone(),
                                                        ks.provider,
                                                    );
                                                    let entry = marks.entry(key).or_default();
                                                    entry.update_tick(tk.price, t);
                                                }
                                                tt_database::queries::TopicDataEnum::Bbo(bbo) => {
                                                    let key = SymbolKey::new(
                                                        bbo.instrument.clone(),
                                                        ks.provider,
                                                    );
                                                    let entry = marks.entry(key).or_default();
                                                    entry.update_bbo(bbo.bid, bbo.ask, t);
                                                }
                                                tt_database::queries::TopicDataEnum::Candle(c) => {
                                                    let key = SymbolKey::new(
                                                        c.instrument.clone(),
                                                        ks.provider,
                                                    );
                                                    let entry = marks.entry(key).or_default();
                                                    entry.update_candle_close(c.close, t);
                                                }
                                                tt_database::queries::TopicDataEnum::Mbp10(m) => {
                                                    let key = SymbolKey::new(
                                                        m.instrument.clone(),
                                                        ks.provider,
                                                    );
                                                    if let Some(ref b) = m.book {
                                                        books.insert(key, b.clone());
                                                    }
                                                }
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
                            let mut updated_accounts: Vec<(
                                ProviderKind,
                                tt_types::accounts::account::AccountName,
                            )> = Vec::new();
                            // Track touched positions (provider, account, instrument) for emission later
                            let mut touched_positions: Vec<(
                                ProviderKind,
                                tt_types::accounts::account::AccountName,
                                Instrument,
                            )> = Vec::new();
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
                                    let (last_px, _spread_opt) = {
                                        let key =
                                            SymbolKey::new(so.spec.instrument.clone(), so.provider);
                                        if let Some(m) = marks.get(&key) {
                                            (m.ref_px(), m.spread())
                                        } else {
                                            (Decimal::ZERO, None)
                                        }
                                    };
                                    // Try model matching with latest book snapshot if available
                                    let book_opt = {
                                        let key =
                                            SymbolKey::new(so.spec.instrument.clone(), so.provider);
                                        books.get(&key)
                                    };
                                    let mut fills = so.fill_model.match_book(
                                        now,
                                        book_opt,
                                        last_px,
                                        &mut so.spec,
                                        &mut *so.slip_model,
                                        &*cal_model,
                                        &*so.fee_model,
                                    );
                                    // Respect session calendar: if closed/halting at this logical time, defer the fill to next open
                                    if !cal_model.is_open(&so.spec.instrument, now)
                                        || cal_model.is_halt(&so.spec.instrument, now)
                                    {
                                        // Reschedule fill to the next open (or push forward by 1s if unknown)
                                        if let Some(next_open) =
                                            cal_model.next_open_after(&so.spec.instrument, now)
                                        {
                                            so.fill_at = next_open;
                                        } else {
                                            so.fill_at = now + ChronoDuration::seconds(1);
                                        }
                                        continue;
                                    }
                                    let mut total_qty: i64 = 0;
                                    let mut vwap_num = Decimal::ZERO;
                                    for f in fills.iter() {
                                        total_qty += f.qty;
                                        vwap_num += f.price * Decimal::from(f.qty);
                                    }
                                    // If no fills occurred, leave order working: reschedule next attempt a bit later
                                    if total_qty == 0 {
                                        so.resting = true; // mark as resting for maker attribution
                                        so.fill_at = now + ChronoDuration::milliseconds(5);
                                        continue;
                                    }
                                    // If we were resting, attribute these fills as maker
                                    if so.resting {
                                        for f in &mut fills {
                                            f.maker = true;
                                        }
                                        so.resting = false;
                                    }
                                    // Update positions ledger per fill and collect touched positions
                                    for f in &fills {
                                        let acct_name = so.spec.account_key.account_name.clone();
                                        let key =
                                            (so.provider, acct_name.clone(), f.instrument.clone());
                                        let entry = positions_ledger.entry(key.clone()).or_insert(
                                            tt_types::accounts::events::PositionDelta {
                                                instrument: f.instrument.clone(),
                                                account_name: acct_name.clone(),
                                                provider_kind: so.provider,
                                                net_qty: Decimal::ZERO,
                                                average_price: Decimal::ZERO,
                                                open_pnl: Decimal::ZERO,
                                                time: now,
                                                side:
                                                    tt_types::accounts::events::PositionSide::Flat,
                                            },
                                        );
                                        let side_sign: i64 = match so.spec.side {
                                            tt_types::accounts::events::Side::Buy => 1,
                                            tt_types::accounts::events::Side::Sell => -1,
                                        };
                                        let delta_qty = Decimal::from(f.qty * side_sign);
                                        let old_qty = entry.net_qty;
                                        let new_qty = old_qty + delta_qty;
                                        // Update average price
                                        if old_qty.is_zero()
                                            || (old_qty > Decimal::ZERO && new_qty < Decimal::ZERO)
                                            || (old_qty < Decimal::ZERO && new_qty > Decimal::ZERO)
                                        {
                                            entry.average_price = f.price;
                                        } else if new_qty != Decimal::ZERO {
                                            let pq =
                                                entry.average_price * old_qty + f.price * delta_qty;
                                            entry.average_price = pq / new_qty;
                                        } else {
                                            entry.average_price = Decimal::ZERO;
                                        }
                                        entry.net_qty = new_qty;
                                        entry.time = now;
                                        entry.side = if new_qty > Decimal::ZERO {
                                            tt_types::accounts::events::PositionSide::Long
                                        } else if new_qty < Decimal::ZERO {
                                            tt_types::accounts::events::PositionSide::Short
                                        } else {
                                            tt_types::accounts::events::PositionSide::Flat
                                        };
                                        touched_positions.push(key);
                                    }

                                    // Apply fees per fill
                                    let fee_ctx = crate::backtest::models::FeeCtx {
                                        sim_time: now,
                                        instrument: so.spec.instrument.clone(),
                                    };
                                    let mut fee_delta = Decimal::ZERO;
                                    for f in &fills {
                                        let money = so.fee_model.on_fill(&fee_ctx, f);
                                        so.fees += money.amount;
                                        fee_delta += money.amount;
                                    }
                                    if !fee_delta.is_zero() {
                                        let acct_key =
                                            (so.provider, so.spec.account_key.account_name.clone());
                                        let entry = accounts_ledger
                                            .entry(acct_key.clone())
                                            .or_insert(AccountDelta {
                                                provider_kind: so.provider,
                                                name: so.spec.account_key.account_name.clone(),
                                                equity: Decimal::ZERO,
                                                day_realized_pnl: Decimal::ZERO,
                                                open_pnl: Decimal::ZERO,
                                                time: now,
                                                can_trade: true,
                                            });
                                        // Update snapshot fields
                                        entry.day_realized_pnl += fee_delta;
                                        entry.equity += fee_delta;
                                        entry.time = now;
                                        updated_accounts.push(acct_key);
                                    }
                                    // Update cumulative fill stats
                                    so.cum_qty += total_qty;
                                    so.cum_vwap_num += vwap_num;
                                    let cum_avg = if so.cum_qty != 0 {
                                        so.cum_vwap_num / Decimal::from(so.cum_qty)
                                    } else {
                                        Decimal::ZERO
                                    };
                                    let leaves = so.orig_qty - so.cum_qty;
                                    if leaves > 0 {
                                        // Emit partial and keep working the remainder
                                        due.push(OrderUpdate {
                                            name: so.spec.account_key.account_name.clone(),
                                            instrument: so.spec.instrument.clone(),
                                            provider_kind: so.provider,
                                            provider_order_id: Some(so.provider_order_id.clone()),
                                            order_id: *oid,
                                            state: OrderState::PartiallyFilled,
                                            leaves,
                                            cum_qty: so.cum_qty,
                                            avg_fill_px: cum_avg,
                                            tag: so.user_tag.clone(),
                                            time: so.fill_at,
                                        });
                                        // Update working quantity to the leaves and reschedule next attempt
                                        so.spec.qty = leaves;
                                        so.fill_at = now + so.latency_model.replace_rtt();
                                    } else {
                                        // Fully filled
                                        due.push(OrderUpdate {
                                            name: so.spec.account_key.account_name.clone(),
                                            instrument: so.spec.instrument.clone(),
                                            provider_kind: so.provider,
                                            provider_order_id: Some(so.provider_order_id.clone()),
                                            order_id: *oid,
                                            state: OrderState::Filled,
                                            leaves: 0,
                                            cum_qty: so.cum_qty,
                                            avg_fill_px: cum_avg,
                                            tag: so.user_tag.clone(),
                                            time: so.fill_at,
                                        });
                                        so.done = true;
                                        finished.push(*oid);
                                    }
                                }
                                // Apply pending cancel/replace effects after matching (fills-first ordering)
                                if !so.done
                                    && so.cancel_pending
                                    && let Some(at) = so.cancel_at
                                    && now >= at
                                {
                                    due.push(OrderUpdate {
                                        name: so.spec.account_key.account_name.clone(),
                                        instrument: so.spec.instrument.clone(),
                                        provider_kind: so.provider,
                                        provider_order_id: Some(so.provider_order_id.clone()),
                                        order_id: *oid,
                                        state: OrderState::Canceled,
                                        leaves: so.spec.qty,
                                        cum_qty: so.cum_qty,
                                        avg_fill_px: if so.cum_qty > 0 {
                                            so.cum_vwap_num / Decimal::from(so.cum_qty)
                                        } else {
                                            Decimal::ZERO
                                        },
                                        tag: so.user_tag.clone(),
                                        time: at,
                                    });
                                    so.done = true;
                                    finished.push(*oid);
                                    so.cancel_pending = false;
                                    so.cancel_at = None;
                                }
                                if !so.done
                                    && so.replace_pending
                                    && let Some(at) = so.replace_at
                                    && now >= at
                                {
                                    if let Some(req) = so.replace_req.clone() {
                                        // Apply qty change if present
                                        if let Some(new_qty_raw) = req.new_qty {
                                            let mut new_qty = new_qty_raw.abs();
                                            if new_qty < so.cum_qty {
                                                new_qty = so.cum_qty;
                                            }
                                            so.orig_qty = new_qty;
                                            let leaves = so.orig_qty - so.cum_qty;
                                            so.spec.qty = leaves.max(0);
                                        }
                                        // Apply price fields if present
                                        if let Some(p) = req.new_limit_price {
                                            so.spec.limit_price = Some(p);
                                        }
                                        if let Some(p) = req.new_stop_price {
                                            so.spec.stop_price = Some(p);
                                        }
                                        if let Some(p) = req.new_trail_price {
                                            so.spec.trail_price = Some(p);
                                        }
                                        // After replace, schedule next fill attempt slightly later
                                        so.fill_at = now + ChronoDuration::milliseconds(5);
                                    }
                                    so.replace_pending = false;
                                    so.replace_at = None;
                                    so.replace_req = None;
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
                            // Emit account snapshots for any accounts updated by fees this tick
                            if !updated_accounts.is_empty() {
                                // Deduplicate keys without requiring Ord
                                let mut set: std::collections::HashSet<(
                                    ProviderKind,
                                    tt_types::accounts::account::AccountName,
                                )> = std::collections::HashSet::new();
                                let mut accounts_vec: Vec<AccountDelta> = Vec::new();
                                for key in updated_accounts.into_iter() {
                                    if set.insert(key.clone())
                                        && let Some(snap) = accounts_ledger.get(&key)
                                    {
                                        accounts_vec.push(snap.clone());
                                    }
                                }
                                if !accounts_vec.is_empty() {
                                    let ab = AccountDeltaBatch {
                                        topic: Topic::AccountEvt,
                                        seq: 0,
                                        accounts: accounts_vec,
                                    };
                                    let _ = bus_clone
                                        .route_response(Response::AccountDeltaBatch(ab))
                                        .await;
                                    await_ack(&notify).await;
                                }
                            }
                            // Emit positions snapshots for touched positions this tick
                            if !touched_positions.is_empty() {
                                use std::collections::HashSet;
                                let mut seen: HashSet<(
                                    ProviderKind,
                                    tt_types::accounts::account::AccountName,
                                    Instrument,
                                )> = HashSet::new();
                                let mut positions_vec: Vec<
                                    tt_types::accounts::events::PositionDelta,
                                > = Vec::new();
                                for key in touched_positions.drain(..) {
                                    if seen.insert(key.clone())
                                        && let Some(pd) = positions_ledger.get(&key)
                                    {
                                        let mut snap = pd.clone();
                                        snap.time = now;
                                        positions_vec.push(snap);
                                    }
                                }
                                if !positions_vec.is_empty() {
                                    let pb = tt_types::wire::PositionsBatch {
                                        topic: Topic::Positions,
                                        seq: 0,
                                        positions: positions_vec,
                                    };
                                    let _ = bus_clone
                                        .route_response(Response::PositionsBatch(pb))
                                        .await;
                                    await_ack(&notify).await;
                                }
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
                            let acct_name = spec.account_key.account_name.clone();
                            // Logical base time from orchestrator
                            let base = watermark.expect(
                                "no watermark set; backtest must drive time via BacktestAdvanceTo",
                            );
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

                            // Normalize quantity sign to platform standard and validate non-zero
                            let mut reject = false;
                            let normalized_qty = spec.side.normalize_qty(spec.qty);
                            if normalized_qty == 0 {
                                reject = true;
                            } else {
                                spec.qty = normalized_qty;
                            }

                            match spec.order_type {
                                tt_types::wire::OrderType::Stop
                                | tt_types::wire::OrderType::TrailingStop => {
                                    if spec.stop_price.is_none() {
                                        reject = true;
                                    }
                                }
                                tt_types::wire::OrderType::StopLimit => {
                                    if spec.stop_price.is_none() || spec.limit_price.is_none() {
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

                            // Initialize per-order latency model; use shared calendar
                            let mut lat = (cfg.make_latency)();
                            // Instantiate per-order fill/slippage/fee models and normalize on submit
                            let mut fill_model = (cfg.make_fill)();
                            fill_model.on_submit(base, &mut spec);
                            let slip_model = (cfg.make_slippage)();
                            let fee_model = (cfg.make_fee)();
                            let latency_model = (cfg.make_latency)();

                            // Schedule ack and (first) fill per latency
                            let ack_at = base + to_chrono(lat.submit_to_ack());
                            let fill_at = ack_at + to_chrono(lat.ack_to_first_fill());

                            // Stash order
                            let orig_qty = spec.qty;
                            let sim = SimOrder {
                                spec,
                                provider: prov,
                                engine_order_id,
                                provider_order_id,
                                ack_at,
                                fill_at,
                                ack_emitted: false,
                                user_tag,
                                done: false,
                                // lifecycle
                                orig_qty,
                                cum_qty: 0,
                                cum_vwap_num: Decimal::ZERO,
                                // cancel state
                                cancel_at: None,
                                cancel_pending: false,
                                // replace state
                                replace_at: None,
                                replace_pending: false,
                                replace_req: None,
                                // models
                                fill_model,
                                slip_model,
                                fee_model,
                                latency_model,
                                fees: Decimal::ZERO,
                                resting: false,
                            };
                            // Ensure ledger entry exists for this account
                            let acct_key = (prov, acct_name.clone());
                            accounts_ledger
                                .entry(acct_key.clone())
                                .or_insert(AccountDelta {
                                    provider_kind: prov,
                                    name: acct_name.clone(),
                                    equity: Decimal::ZERO,
                                    day_realized_pnl: Decimal::ZERO,
                                    open_pnl: Decimal::ZERO,
                                    time: base,
                                    can_trade: true,
                                });
                            sim_orders.entry(engine_order_id).or_insert(sim);
                        }
                        Request::CancelOrder(cxl) => {
                            // Schedule a cancel using provider_order_id; ignore if not found
                            let base = watermark.expect(
                                "no watermark set; backtest must drive time via BacktestAdvanceTo",
                            );
                            // Create a latency model instance for timing
                            let mut lat = (cfg.make_latency)();
                            let when = base + to_chrono(lat.cancel_rtt());
                            // Find matching order by provider_order_id and account name
                            let target_id = cxl.provider_order_id;
                            let acct = cxl.account_name;
                            for (_eid, so) in sim_orders.iter_mut() {
                                if so.provider_order_id.0 == target_id
                                    && so.spec.account_key.account_name == acct
                                {
                                    so.cancel_at = Some(when);
                                    so.cancel_pending = true;
                                    break;
                                }
                            }
                        }
                        Request::ReplaceOrder(rpl) => {
                            // Schedule a replace using provider_order_id; ignore if not found
                            let base = watermark.expect(
                                "no watermark set; backtest must drive time via BacktestAdvanceTo",
                            );
                            let mut lat = (cfg.make_latency)();
                            let when = base + to_chrono(lat.replace_rtt());
                            let target_id = rpl.provider_order_id.clone();
                            let acct = rpl.account_name.clone();
                            for (_eid, so) in sim_orders.iter_mut() {
                                if so.provider_order_id.0 == target_id
                                    && so.spec.account_key.account_name == acct
                                {
                                    so.replace_at = Some(when);
                                    so.replace_pending = true;
                                    so.replace_req = Some(rpl.clone());
                                    // Do NOT block fills; order remains live until replace takes effect
                                    break;
                                }
                            }
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
                                // Update last marks for synthetic fills with timestamps
                                match &item {
                                    tt_database::queries::TopicDataEnum::Tick(tk) => {
                                        let key =
                                            SymbolKey::new(tk.instrument.clone(), ks.provider);
                                        let entry = marks.entry(key).or_default();
                                        entry.update_tick(tk.price, t);
                                    }
                                    tt_database::queries::TopicDataEnum::Bbo(bbo) => {
                                        let key =
                                            SymbolKey::new(bbo.instrument.clone(), ks.provider);
                                        let entry = marks.entry(key).or_default();
                                        entry.update_bbo(bbo.bid, bbo.ask, t);
                                    }
                                    tt_database::queries::TopicDataEnum::Candle(c) => {
                                        let key = SymbolKey::new(c.instrument.clone(), ks.provider);
                                        let entry = marks.entry(key).or_default();
                                        entry.update_candle_close(c.close, t);
                                    }
                                    tt_database::queries::TopicDataEnum::Mbp10(m) => {
                                        let key = SymbolKey::new(m.instrument.clone(), ks.provider);
                                        if let Some(ref b) = m.book {
                                            books.insert(key, b.clone());
                                        }
                                    }
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
