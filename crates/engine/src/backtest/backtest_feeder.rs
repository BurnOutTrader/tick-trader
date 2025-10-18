use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use rust_decimal::Decimal;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc};
use tracing::{info, warn};
use tt_types::accounts::events::{AccountDelta, OrderUpdate, ProviderOrderId};
use tt_types::accounts::order::OrderState;
use tt_types::data::mbp10::BookLevels;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{AccountDeltaBatch, BarsBatch, Request, Response};

use crate::backtest::backtest_clock::BacktestClock;
use crate::backtest::realism_models::project_x::calander::HoursCalendar;
use crate::backtest::realism_models::project_x::fee::PxFlatFee;
use crate::backtest::realism_models::project_x::fill::{CmeFillModel, FillConfig};
use crate::backtest::realism_models::project_x::latency::PxLatency;
use crate::backtest::realism_models::project_x::slippage::NoSlippage;
use crate::backtest::realism_models::traits::{
    FeeModel, FillModel, LatencyModel, SessionCalendar, SlippageModel,
};
use crate::client::ClientMessageBus;
use crate::statics::bus::BUS_CLIENT;
use crate::statics::clock::time_now;
use crate::statics::portfolio::{PORTFOLIOS, Portfolio};

#[derive(Clone)]
pub enum MatchingPolicy {
    FIFO,
    LIFO,
}

#[derive(Clone, Copy)]
pub enum OrderAckDuringClosedPolicy {
    /// Reject orders submitted while the session is closed or halted
    Reject,
    /// Defer the acknowledgement until the next session open; order is not working until acked
    DeferAckUntilOpen,
    /// Acknowledge immediately but defer any fills until the session is open (current behavior)
    AckButDeferFills,
}

/// Windowed DB feeder that simulates a provider by emitting Responses over an in-process bus.
/// It listens for SubscribeKey/UnsubscribeKey requests on a request channel you provide when
/// constructing the bus via ClientMessageBus::new_with_transport(req_tx).
#[derive(Clone)]
pub struct BacktestFeederConfig {
    /// Optional liquidation window: flatten open positions within this duration before session close
    pub flatten_before_close: Option<ChronoDuration>,
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
    /// Policy for acknowledging orders submitted during session close/halts
    pub order_ack_closed_policy: OrderAckDuringClosedPolicy,
    /// Trade matching policy for realizing PnL on position reductions
    pub matching_policy: MatchingPolicy,
}

impl Default for BacktestFeederConfig {
    fn default() -> BacktestFeederConfig {
        Self {
            flatten_before_close: None,
            window: ChronoDuration::days(2),
            lookahead: ChronoDuration::days(1),
            warmup: ChronoDuration::days(1),
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
            order_ack_closed_policy: OrderAckDuringClosedPolicy::Reject,
            matching_policy: MatchingPolicy::FIFO,
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

// Helper types and functions extracted from start_with_db for readability
#[derive(Clone, Debug)]
struct Lot {
    side: tt_types::accounts::events::Side,
    qty: i64, // unsigned logical quantity per exchange conventions (positive)
    price: Decimal,
    _time: DateTime<Utc>,
}

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

async fn ensure_window(
    ks: &mut KeyState,
    conn: &tt_database::init::Connection,
    cfg: &BacktestFeederConfig,
    watermark: Option<DateTime<Utc>>,
) {
    // Drive extension by the furthest of cursor or watermark to avoid stalls when buffer is empty
    let driver = watermark
        .map(|wm| if wm > ks.cursor { wm } else { ks.cursor })
        .unwrap_or(ks.cursor);
    // Target end should jump ahead sufficiently to avoid tiny incremental refills.
    // Advance by at least one full window from current window_end, and also beyond the driver.
    let candidate_a = ks.window_end + cfg.window;
    let candidate_b = driver + cfg.window + cfg.lookahead;
    let want_end = if candidate_a > candidate_b {
        candidate_a
    } else {
        candidate_b
    };
    if want_end <= ks.window_end {
        return;
    }
    let start = ks.window_end;
    let mut end = want_end;
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

fn topic_for_candle(c: &tt_types::data::core::Candle) -> Topic {
    if let Some(t) = crate::models::DataTopic::topic_for_resolution(&c.resolution) {
        t
    } else {
        // Fallback to a safe default; diagnostics elsewhere will log mismatches
        Topic::Candles1d
    }
}

async fn emit_one(
    bus: &ClientMessageBus,
    tde: &tt_database::queries::TopicDataEnum,
    provider: ProviderKind,
    _notify: &Option<Arc<Notify>>,
) {
    match tde {
        tt_database::queries::TopicDataEnum::Tick(t) => {
            let _ = bus.broadcast(Response::Tick {
                tick: t.clone(),
                provider_kind: provider,
            });
        }
        tt_database::queries::TopicDataEnum::Bbo(b) => {
            let _ = bus.broadcast(Response::Quote {
                bbo: b.clone(),
                provider_kind: provider,
            });
        }
        tt_database::queries::TopicDataEnum::Mbp10(m) => {
            let _ = bus.broadcast(Response::Mbp10 {
                mbp10: m.clone(),
                provider_kind: provider,
            });
        }
        tt_database::queries::TopicDataEnum::Candle(c) => {
            // Emit as a BarBatch (even for a single candle) to carry a topic for routing
            let batch = BarsBatch {
                topic: topic_for_candle(c),
                seq: 0,
                bars: vec![c.clone()],
                provider_kind: provider,
            };
            let _ = bus.broadcast(Response::BarBatch(batch));
        }
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
        account_balance: Decimal,
    ) -> anyhow::Result<()> {
        // Create internal request channel for the write loop
        let (req_tx, mut req_rx) = mpsc::channel::<Request>(1024);
        let bus = ClientMessageBus::new_with_transport(req_tx.clone());
        BUS_CLIENT
            .set(bus)
            .expect("DANGER: bus client already initialized in backtest");

        std::mem::drop(tokio::spawn(async move {
            let notify = backtest_notify;
            // --- Simple fill engine state (model-driven order lifecycle) ---
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
            // Per-account/instrument open lots book for realizing PnL
            let mut lots_ledger: HashMap<
                (
                    ProviderKind,
                    tt_types::accounts::account::AccountName,
                    Instrument,
                ),
                VecDeque<Lot>,
            > = HashMap::new();
            // Contracts cache for tick conversion
            let mut contracts_cache: HashMap<
                ProviderKind,
                ahash::AHashMap<Instrument, FuturesContract>,
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
            // Queue of requests issued before the first watermark is set. These are buffered
            // so strategies can place orders on WarmupComplete without being rejected.
            let mut pre_wm_queue: Vec<tt_types::wire::Request> = Vec::new();
            // Ensure we emit BacktestCompleted exactly once when end is reached
            let mut completed_emitted: bool = false;
            // Track per-day forced liquidations to avoid duplicate actions within the same trading day
            let mut forced_liq_today: HashSet<(
                ProviderKind,
                tt_types::accounts::account::AccountName,
                Instrument,
                chrono::NaiveDate,
            )> = HashSet::new();

            // Main loop: interleave handling of requests with emitting events in time order
            loop {
                let bus = BUS_CLIENT.get().unwrap();
                // 1) Drain any pending requests without blocking
                while let Ok(req) = req_rx.try_recv() {
                    use tt_types::wire::Request;
                    match req {
                        Request::SubscribeAccount(req) => {
                            if !PORTFOLIOS.contains_key(&req.key) {
                                let delta = AccountDelta {
                                    provider_kind: req.key.provider,
                                    name: req.key.account_name.clone(),
                                    key: req.key.clone(),
                                    equity: account_balance,
                                    day_realized_pnl: Decimal::ZERO,
                                    open_pnl: Decimal::ZERO,
                                    time: time_now(),
                                    can_trade: true,
                                };
                                let account = Portfolio::new(req.key.clone(), delta);
                                PORTFOLIOS.insert(req.key.clone(), account);

                                // On account initialization, load contracts for this provider from DB and
                                // initialize the global CONTRACTS map used by portfolio/open PnL, and also
                                // prime the local contracts cache for tick conversion.
                                let provider = req.key.provider;
                                // Determine cutoff time as in InstrumentsMapRequest to avoid lookahead bias
                                let cutoff_dt = if let Some(dt) = cfg.range_start {
                                    dt
                                } else if let Some(ref clk) = clock {
                                    clk.now_dt()
                                } else {
                                    Utc.timestamp_opt(0, 0).unwrap()
                                };
                                let _cutoff_naive = cutoff_dt.date_naive();
                                #[allow(clippy::collapsible_if)]
                                if let Ok(map) =
                                    tt_database::queries::get_contracts_map(&conn, provider).await
                                {
                                    // Filter out contracts that would not yet be known at cutoff time
                                    let cutoff_naive = cutoff_dt.date_naive();
                                    let mut filtered: Vec<FuturesContract> = map
                                        .into_values()
                                        .filter(|fc| fc.activation_date <= cutoff_naive)
                                        .collect();
                                    // Initialize global CONTRACTS map used by Portfolio/open PnL
                                    crate::statics::portfolio::initialize_contracts(
                                        provider,
                                        filtered.clone(),
                                    );
                                    // Also prime the local contracts cache for tick conversion
                                    let mut amap: ahash::AHashMap<Instrument, FuturesContract> =
                                        ahash::AHashMap::new();
                                    for c in filtered.drain(..) {
                                        amap.insert(c.instrument.clone(), c);
                                    }
                                    contracts_cache.insert(provider, amap);
                                }
                            }
                        }
                        Request::InstrumentsMapRequest(req) => {
                            // Fetch contracts map from DB and filter to avoid lookahead bias
                            let provider = req.provider;
                            let corr_id = req.corr_id;

                            // Determine cutoff time for lookahead-bias: use configured range_start if set,
                            // otherwise use the backtest clock if available; fall back to epoch.
                            let cutoff_dt = if let Some(dt) = cfg.range_start {
                                dt
                            } else if let Some(ref clk) = clock {
                                clk.now_dt()
                            } else {
                                // default to epoch (include nothing that activates after this)
                                Utc.timestamp_opt(0, 0).unwrap()
                            };
                            let cutoff_naive = cutoff_dt.date_naive();

                            match tt_database::queries::get_contracts_map(&conn, provider).await {
                                Ok(map) => {
                                    // Filter out contracts that would not yet be known at cutoff time
                                    let mut contracts: Vec<
                                        tt_types::securities::security::FuturesContract,
                                    > = map
                                        .into_values()
                                        .filter(|fc| fc.activation_date <= cutoff_naive)
                                        .collect();
                                    // Sort for deterministic order (by root then instrument string)
                                    contracts.sort_by(|a, b| {
                                        (a.root.clone(), a.instrument.to_string())
                                            .cmp(&(b.root.clone(), b.instrument.to_string()))
                                    });

                                    let resp = tt_types::wire::ContractsResponse {
                                        provider: format!("{:?}", provider),
                                        contracts,
                                        corr_id,
                                    };
                                    bus.route_response(
                                        Response::InstrumentsMapResponse(resp),
                                        corr_id,
                                    );
                                }
                                Err(e) => {
                                    warn!("feeder: get_contracts_map error: {:?}", e);
                                    let resp = tt_types::wire::ContractsResponse {
                                        provider: format!("{:?}", provider),
                                        contracts: Vec::new(),
                                        corr_id,
                                    };
                                    bus.route_response(
                                        Response::InstrumentsMapResponse(resp),
                                        corr_id,
                                    );
                                }
                            }
                        }
                        Request::SubscribeKey(skreq) => {
                            // Acknowledge subscribe immediately
                            let instr = skreq.key.instrument.clone();
                            let topic = skreq.topic;
                            let provider = skreq.key.provider;
                            let _ = bus.broadcast(Response::SubscribeResponse {
                                topic,
                                instrument: instr.clone(),
                                success: true,
                            });

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

                            // Warmup prefetch and emit if configured (from regular backtest start minus warmup up to start)
                            if !cfg.warmup.is_zero() {
                                // Determine the earliest time we are allowed to fetch for warmup:
                                // Use the DB's earliest extent (or epoch) only; do NOT clamp to range_start so warmup can precede it.
                                let lower_bound = earliest_opt.unwrap_or(epoch);
                                let mut warm_start =
                                    start.checked_sub_signed(cfg.warmup).unwrap_or(start);
                                if warm_start < lower_bound {
                                    warm_start = lower_bound;
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
                                                    emit_one(bus, item, provider, &notify).await;
                                                }
                                            }
                                        }
                                        Err(e) => warn!("feeder warmup error: {:?}", e),
                                    }
                                }
                            }
                            // Signal warmup complete to the engine/strategy (even if warmup==0)
                            let _ = bus.broadcast(Response::WarmupComplete {
                                topic,
                                instrument: instr.clone(),
                            });

                            // Prime first window after start
                            ks.cursor = start;
                            ensure_window(&mut ks, &conn, &cfg, None).await;
                            push_next_for_key(&mut heap, topic, &skreq.key, &ks);
                            keys.insert((topic, skreq.key.clone()), ks);
                        }
                        Request::UnsubscribeKey(ureq) => {
                            keys.remove(&(ureq.topic, ureq.key.clone()));
                            // No specific unsubscribe response in wire; engine will see UnsubscribeResponse only from server in live.
                            let _ = bus.broadcast(Response::UnsubscribeResponse {
                                topic: ureq.topic,
                                instrument: ureq.key.instrument.clone(),
                            });
                        }
                        Request::BacktestAdvanceTo(bta) => {
                            // Update watermark and drain events up to this time
                            let was_none = watermark.is_none();
                            watermark = Some(bta.to);
                            // If this is the first watermark, process any buffered order/cxl/replace requests now
                            if was_none && !pre_wm_queue.is_empty() {
                                use tt_types::wire::Request as _Req;
                                for pending in pre_wm_queue.drain(..) {
                                    match pending {
                                        _Req::PlaceOrder(mut spec) => {
                                            // Enqueue into simulation engine; do not emit immediately
                                            let prov = spec.account_key.provider;
                                            let acct_name = spec.account_key.account_name.clone();
                                            // Logical base time from orchestrator
                                            let base = watermark.expect(
                                                "no watermark set; backtest must drive time via BacktestAdvanceTo",
                                            );
                                            // Ensure an engine order id exists (prefer embedded tag)
                                            let (engine_order_id, user_tag) = if let Some(tag) =
                                                &spec.custom_tag
                                            {
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
                                            let mut reject_reason: Option<String> = None;
                                            let normalized_qty = spec.side.normalize_qty(spec.qty);
                                            if normalized_qty == 0 {
                                                reject_reason =
                                                    Some("quantity must be non-zero".to_string());
                                            } else {
                                                spec.qty = normalized_qty;
                                            }

                                            match spec.order_type {
                                                tt_types::wire::OrderType::Stop => {
                                                    if spec.stop_price.is_none() {
                                                        reject_reason =
                                                            Some("stop price required".to_string());
                                                    }
                                                }
                                                tt_types::wire::OrderType::TrailingStop => {
                                                    // Trailing stops require a trail distance; initial stop_price is derived/ratcheted by the fill model
                                                    if spec.trail_price.is_none() {
                                                        reject_reason = Some(
                                                            "trail distance required".to_string(),
                                                        );
                                                    }
                                                }
                                                tt_types::wire::OrderType::StopLimit => {
                                                    if spec.stop_price.is_none()
                                                        || spec.limit_price.is_none()
                                                    {
                                                        reject_reason = Some(
                                                            "stop and limit prices required"
                                                                .to_string(),
                                                        );
                                                    }
                                                }
                                                _ => {}
                                            }

                                            if let Some(reason) = reject_reason {
                                                // Immediately emit a rejected order update with user's original tag
                                                let upd = OrderUpdate {
                                                    account_name: spec
                                                        .account_key
                                                        .account_name
                                                        .clone(),
                                                    instrument: spec.instrument.clone(),
                                                    provider_kind: prov,
                                                    provider_order_id: Some(
                                                        provider_order_id.clone(),
                                                    ),
                                                    order_id: engine_order_id,
                                                    state: OrderState::Rejected,
                                                    leaves: 0,
                                                    cum_qty: 0,
                                                    avg_fill_px: Decimal::ZERO,
                                                    tag: user_tag.clone(),
                                                    time: base,
                                                    side: spec.side,
                                                    msg: Some(reason),
                                                };
                                                let ob = tt_types::wire::OrdersBatch {
                                                    topic: Topic::Orders,
                                                    seq: 0,
                                                    orders: vec![upd],
                                                };
                                                let _ = bus.broadcast(Response::OrdersBatch(ob));
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

                                            // Determine effective ack base time according to session state and policy
                                            let is_closed_or_halt = !cal_model
                                                .is_open(&spec.instrument, base)
                                                || cal_model.is_halt(&spec.instrument, base);
                                            // Early rejection if policy is Reject during closed/halts
                                            if is_closed_or_halt
                                                && matches!(
                                                    cfg.order_ack_closed_policy,
                                                    OrderAckDuringClosedPolicy::Reject
                                                )
                                            {
                                                let upd = OrderUpdate {
                                                    account_name: spec
                                                        .account_key
                                                        .account_name
                                                        .clone(),
                                                    instrument: spec.instrument.clone(),
                                                    provider_kind: prov,
                                                    provider_order_id: Some(
                                                        provider_order_id.clone(),
                                                    ),
                                                    order_id: engine_order_id,
                                                    state: OrderState::Rejected,
                                                    leaves: 0,
                                                    cum_qty: 0,
                                                    avg_fill_px: Decimal::ZERO,
                                                    tag: user_tag.clone(),
                                                    time: base,
                                                    side: spec.side,
                                                    msg: Some(
                                                        "market closed or halted".to_string(),
                                                    ),
                                                };
                                                let ob = tt_types::wire::OrdersBatch {
                                                    topic: Topic::Orders,
                                                    seq: 0,
                                                    orders: vec![upd],
                                                };
                                                let _ = bus.broadcast(Response::OrdersBatch(ob));
                                                continue;
                                            }
                                            let ack_base = if is_closed_or_halt
                                                && matches!(
                                                    cfg.order_ack_closed_policy,
                                                    OrderAckDuringClosedPolicy::DeferAckUntilOpen
                                                ) {
                                                if let Some(next_open) = cal_model
                                                    .next_open_after(&spec.instrument, base)
                                                {
                                                    next_open
                                                } else {
                                                    base + ChronoDuration::seconds(1)
                                                }
                                            } else {
                                                base
                                            };

                                            // Schedule ack and (first) fill per latency
                                            let ack_at = ack_base + to_chrono(lat.submit_to_ack());
                                            let fill_at =
                                                ack_at + to_chrono(lat.ack_to_first_fill());

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
                                            accounts_ledger.entry(acct_key.clone()).or_insert(
                                                AccountDelta {
                                                    provider_kind: prov,
                                                    name: acct_name.clone(),
                                                    key: AccountKey::new(prov, acct_name.clone()),
                                                    equity: account_balance,
                                                    day_realized_pnl: Decimal::ZERO,
                                                    open_pnl: Decimal::ZERO,
                                                    time: base,
                                                    can_trade: true,
                                                },
                                            );
                                            sim_orders.entry(engine_order_id).or_insert(sim);
                                        }
                                        _Req::CancelOrder(cxl) => {
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
                                        _Req::ReplaceOrder(rpl) => {
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
                            }
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
                                            emit_one(bus, &item, ks.provider, &notify).await;
                                        }
                                        if t > ks.cursor {
                                            ks.cursor = t;
                                        }
                                        let can_refill =
                                            ks.hard_stop.map(|hs| ks.cursor < hs).unwrap_or(true);
                                        if can_refill
                                            && (ks.cursor + ChronoDuration::seconds(1)
                                                >= ks.window_end
                                                || ks.buf.is_empty())
                                        {
                                            ensure_window(ks, &conn, &cfg, watermark).await;
                                        }
                                    }
                                    // Push next
                                    push_next_for_key(&mut heap, topic, &sk, ks);
                                }
                            }
                            // After draining, if buffer ran dry ahead of watermark, proactively extend windows and seed heap
                            if let Some(wm) = watermark {
                                for ((topic, sk), ks) in keys.iter_mut() {
                                    if !ks.done && ks.buf.is_empty() {
                                        ensure_window(ks, &conn, &cfg, Some(wm)).await;
                                        push_next_for_key(&mut heap, *topic, sk, ks);
                                    }
                                }
                            }
                            // After draining market data up to watermark, process simulated orders
                            // Also, after refills, mark keys as done if they have reached hard_stop with no data buffered
                            let mut to_remove: Vec<(Topic, SymbolKey)> = Vec::new();
                            for ((topic, sk), ks) in keys.iter_mut() {
                                if ks.hard_stop.map(|hs| ks.window_end >= hs).unwrap_or(false)
                                    && ks.buf.is_empty()
                                {
                                    ks.done = true;
                                    to_remove.push((*topic, sk.clone()));
                                }
                            }
                            if !to_remove.is_empty() {
                                for (topic, sk) in to_remove.drain(..) {
                                    keys.remove(&(topic, sk));
                                }
                            }

                            let now = bta.to;
                            let mut due: Vec<OrderUpdate> = Vec::new();
                            let mut closed_trades: Vec<tt_types::wire::Trade> = Vec::new();
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
                                        account_name: so.spec.account_key.account_name.clone(),
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
                                        side: so.spec.side,
                                        msg: None,
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
                                    // Update positions via lots matching and realize PnL when reducing
                                    for f in &fills {
                                        let acct_name = so.spec.account_key.account_name.clone();
                                        let key =
                                            (so.provider, acct_name.clone(), f.instrument.clone());

                                        // Ensure a positions entry exists for snapshots
                                        let _pos_entry = positions_ledger
                                            .entry(key.clone())
                                            .or_insert(tt_types::accounts::events::PositionDelta {
                                                instrument: f.instrument.clone(),
                                                account_name: acct_name.clone(),
                                                provider_kind: so.provider,
                                                net_qty: Decimal::ZERO,
                                                average_price: Decimal::ZERO,
                                                open_pnl: Decimal::ZERO,
                                                time: now,
                                                side:
                                                    tt_types::accounts::events::PositionSide::Flat,
                                            });

                                        // Ensure contracts cache for this provider (for tick conversion)
                                        if let std::collections::hash_map::Entry::Vacant(e) =
                                            contracts_cache.entry(so.provider)
                                            && let Ok(map) =
                                                tt_database::queries::get_contracts_map(
                                                    &conn,
                                                    so.provider,
                                                )
                                                .await
                                        {
                                            e.insert(map);
                                        }

                                        let (tick_size, value_per_tick) = if let Some(imap) =
                                            contracts_cache.get(&so.provider)
                                            && let Some(fc) = imap.get(&f.instrument)
                                        {
                                            (fc.tick_size, fc.value_per_tick)
                                        } else {
                                            (Decimal::ONE, Decimal::ONE)
                                        };

                                        // Open lots deque for this account/instrument
                                        let deque = lots_ledger.entry(key.clone()).or_default();

                                        let fill_side = so.spec.side; // buy adds long lots, sell adds short lots
                                        // Determine if this fill reduces existing exposure (opposite sign)
                                        let reduces = if let Some(front) = deque.front() {
                                            front.side != fill_side
                                        } else {
                                            false
                                        };

                                        let mut remaining = f.qty;
                                        let mut realized_this_fill = Decimal::ZERO;

                                        if reduces {
                                            while remaining > 0 && !deque.is_empty() {
                                                // choose lot per policy
                                                let lot_side;
                                                let lot_px;
                                                let available;
                                                {
                                                    // Borrow mutably per end
                                                    match cfg.matching_policy {
                                                        MatchingPolicy::FIFO => {
                                                            let lot = deque.front_mut().unwrap();
                                                            lot_side = lot.side;
                                                            lot_px = lot.price;
                                                            available = lot.qty;
                                                        }
                                                        MatchingPolicy::LIFO => {
                                                            let lot = deque.back_mut().unwrap();
                                                            lot_side = lot.side;
                                                            lot_px = lot.price;
                                                            available = lot.qty;
                                                        }
                                                    }
                                                }
                                                let matched = if remaining < available {
                                                    remaining
                                                } else {
                                                    available
                                                };

                                                // Compute realized pnl in contract currency
                                                let matched_dec = Decimal::from(matched);
                                                let ticks = match lot_side {
                                                    tt_types::accounts::events::Side::Buy => {
                                                        (f.price - lot_px) / tick_size
                                                    }
                                                    tt_types::accounts::events::Side::Sell => {
                                                        (lot_px - f.price) / tick_size
                                                    }
                                                };
                                                let pnl = ticks * value_per_tick * matched_dec;
                                                realized_this_fill += pnl;

                                                // Record closed trade segment
                                                closed_trades.push(tt_types::wire::Trade {
                                                    id: EngineUuid::new(),
                                                    provider: so.provider,
                                                    account_name: acct_name.clone(),
                                                    instrument: f.instrument.clone(),
                                                    creation_time: now,
                                                    price: f.price,
                                                    profit_and_loss: pnl,
                                                    fees: Decimal::ZERO,
                                                    side: lot_side,
                                                    size: matched_dec,
                                                    voided: false,
                                                    order_id: so.provider_order_id.0.clone(),
                                                });

                                                // Reduce the lot
                                                match cfg.matching_policy {
                                                    MatchingPolicy::FIFO => {
                                                        if let Some(lot) = deque.front_mut() {
                                                            lot.qty -= matched;
                                                            if lot.qty == 0 {
                                                                deque.pop_front();
                                                            }
                                                        }
                                                    }
                                                    MatchingPolicy::LIFO => {
                                                        if let Some(lot) = deque.back_mut() {
                                                            lot.qty -= matched;
                                                            if lot.qty == 0 {
                                                                deque.pop_back();
                                                            }
                                                        }
                                                    }
                                                }
                                                remaining -= matched;
                                            }
                                            // Any residual becomes new exposure on fill side
                                            if remaining > 0 {
                                                deque.push_back(Lot {
                                                    side: fill_side,
                                                    qty: remaining,
                                                    price: f.price,
                                                    _time: now,
                                                });
                                            }
                                        } else {
                                            // Pure increase in exposure — append a new lot
                                            deque.push_back(Lot {
                                                side: fill_side,
                                                qty: remaining,
                                                price: f.price,
                                                _time: now,
                                            });
                                        }

                                        // Update account ledger with realized pnl if any
                                        if !realized_this_fill.is_zero() {
                                            let acct_key = (
                                                so.provider,
                                                so.spec.account_key.account_name.clone(),
                                            );
                                            let entry = accounts_ledger
                                                .entry(acct_key.clone())
                                                .or_insert(AccountDelta {
                                                    provider_kind: so.provider,
                                                    name: so.spec.account_key.account_name.clone(),
                                                    key: so.spec.account_key.clone(),
                                                    equity: account_balance,
                                                    day_realized_pnl: Decimal::ZERO,
                                                    open_pnl: Decimal::ZERO,
                                                    time: now,
                                                    can_trade: true,
                                                });
                                            entry.day_realized_pnl += realized_this_fill;
                                            entry.equity += realized_this_fill;
                                            entry.time = now;
                                            updated_accounts.push(acct_key);
                                        }

                                        // Recompute remaining net position and avg price from lots
                                        let pd = positions_ledger.get_mut(&key).unwrap();
                                        if deque.is_empty() {
                                            pd.net_qty = Decimal::ZERO;
                                            pd.average_price = Decimal::ZERO;
                                            pd.side =
                                                tt_types::accounts::events::PositionSide::Flat;
                                        } else {
                                            let current_side =
                                                deque.front().map(|l| l.side).unwrap_or(fill_side);
                                            let mut sum_qty: i64 = 0;
                                            let mut vwap_num = Decimal::ZERO;
                                            for l in deque.iter() {
                                                sum_qty += l.qty;
                                                vwap_num += l.price * Decimal::from(l.qty);
                                            }
                                            let avg = if sum_qty > 0 {
                                                vwap_num / Decimal::from(sum_qty)
                                            } else {
                                                Decimal::ZERO
                                            };
                                            pd.average_price = avg;
                                            pd.net_qty = match current_side {
                                                tt_types::accounts::events::Side::Buy => {
                                                    Decimal::from(sum_qty)
                                                }
                                                tt_types::accounts::events::Side::Sell => {
                                                    Decimal::from(-sum_qty)
                                                }
                                            };
                                            pd.side = if pd.net_qty > Decimal::ZERO {
                                                tt_types::accounts::events::PositionSide::Long
                                            } else if pd.net_qty < Decimal::ZERO {
                                                tt_types::accounts::events::PositionSide::Short
                                            } else {
                                                tt_types::accounts::events::PositionSide::Flat
                                            };
                                        }
                                        positions_ledger.get_mut(&key).unwrap().time = now;
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
                                                key: so.spec.account_key.clone(),
                                                equity: account_balance,
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
                                            account_name: so.spec.account_key.account_name.clone(),
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
                                            side: so.spec.side,
                                            msg: None,
                                        });
                                        // Update working quantity to the leaves and reschedule next attempt
                                        so.spec.qty = leaves;
                                        so.fill_at = now + so.latency_model.replace_rtt();
                                    } else {
                                        // Fully filled
                                        due.push(OrderUpdate {
                                            account_name: so.spec.account_key.account_name.clone(),
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
                                            side: so.spec.side,
                                            msg: None,
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
                                        account_name: so.spec.account_key.account_name.clone(),
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
                                        side: so.spec.side,
                                        msg: None,
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
                                let _ = bus.broadcast(Response::OrdersBatch(ob));
                            }

                            // Risk: auto-liquidate positions within X minutes before session close (ProjectX policy)
                            if let Some(liq_window) = cfg.flatten_before_close {
                                // Collect keys to avoid borrow issues
                                let keys: Vec<(
                                    ProviderKind,
                                    tt_types::accounts::account::AccountName,
                                    Instrument,
                                )> = positions_ledger.keys().cloned().collect();
                                for key in keys {
                                    if let Some(pd) = positions_ledger.get(&key) {
                                        if pd.net_qty.is_zero() {
                                            continue;
                                        }
                                        let instr = pd.instrument.clone();
                                        // Only enforce while session is open
                                        if !cal_model.is_open(&instr, now) {
                                            continue;
                                        }
                                        if let Some(close) = cal_model.next_close_after(&instr, now)
                                        {
                                            let until_close = close - now;
                                            if until_close <= liq_window {
                                                // Avoid double-liquidation on the same trading day
                                                let day = cal_model.trading_day(&instr, now);
                                                let tag =
                                                    (key.0, key.1.clone(), instr.clone(), day);
                                                if !forced_liq_today.insert(tag) {
                                                    continue;
                                                }

                                                // Determine reference price: last known mark
                                                let sym_key = SymbolKey::new(instr.clone(), key.0);
                                                let ref_px = if let Some(m) = marks.get(&sym_key) {
                                                    m.ref_px()
                                                } else {
                                                    Decimal::ZERO
                                                };
                                                if ref_px.is_zero() {
                                                    continue;
                                                }

                                                // Tick conversion
                                                let (tick_size, value_per_tick) = if let Some(imap) =
                                                    contracts_cache.get(&key.0)
                                                    && let Some(fc) = imap.get(&instr)
                                                {
                                                    (fc.tick_size, fc.value_per_tick)
                                                } else {
                                                    (Decimal::ONE, Decimal::ONE)
                                                };

                                                // Consume all open lots to flat
                                                let deque = lots_ledger
                                                    .entry((key.0, key.1.clone(), instr.clone()))
                                                    .or_default();
                                                while let Some(lot) = match cfg.matching_policy {
                                                    MatchingPolicy::FIFO => deque.front().cloned(),
                                                    MatchingPolicy::LIFO => deque.back().cloned(),
                                                } {
                                                    let matched = lot.qty;
                                                    let matched_dec = Decimal::from(matched);
                                                    // Realized PnL vs liquidation price
                                                    let ticks = match lot.side {
                                                        tt_types::accounts::events::Side::Buy => {
                                                            (ref_px - lot.price) / tick_size
                                                        }
                                                        tt_types::accounts::events::Side::Sell => {
                                                            (lot.price - ref_px) / tick_size
                                                        }
                                                    };
                                                    let pnl = ticks * value_per_tick * matched_dec;
                                                    // Record trade
                                                    closed_trades.push(tt_types::wire::Trade {
                                                        id: EngineUuid::new(),
                                                        provider: key.0,
                                                        account_name: key.1.clone(),
                                                        instrument: instr.clone(),
                                                        creation_time: now,
                                                        price: ref_px,
                                                        profit_and_loss: pnl,
                                                        fees: Decimal::ZERO,
                                                        side: lot.side,
                                                        size: matched_dec,
                                                        voided: false,
                                                        order_id: "forced-liquidation".to_string(),
                                                    });
                                                    // Remove from deque
                                                    match cfg.matching_policy {
                                                        MatchingPolicy::FIFO => {
                                                            if let Some(front) = deque.front_mut() {
                                                                front.qty -= matched;
                                                                if front.qty == 0 {
                                                                    deque.pop_front();
                                                                }
                                                            }
                                                        }
                                                        MatchingPolicy::LIFO => {
                                                            if let Some(back) = deque.back_mut() {
                                                                back.qty -= matched;
                                                                if back.qty == 0 {
                                                                    deque.pop_back();
                                                                }
                                                            }
                                                        }
                                                    }
                                                    // Update account ledgers
                                                    let acct_key = (key.0, key.1.clone());
                                                    let entry = accounts_ledger
                                                        .entry(acct_key.clone())
                                                        .or_insert(AccountDelta {
                                                            provider_kind: key.0,
                                                            name: key.1.clone(),
                                                            key: AccountKey::new(
                                                                key.0,
                                                                key.1.clone(),
                                                            ),
                                                            equity: account_balance,
                                                            day_realized_pnl: Decimal::ZERO,
                                                            open_pnl: Decimal::ZERO,
                                                            time: now,
                                                            can_trade: true,
                                                        });
                                                    entry.day_realized_pnl += pnl;
                                                    entry.equity += pnl;
                                                    entry.time = now;
                                                    updated_accounts.push((key.0, key.1.clone()));
                                                }
                                                // Update position to flat
                                                if let Some(pdm) = positions_ledger.get_mut(&key) {
                                                    pdm.net_qty = Decimal::ZERO;
                                                    pdm.average_price = Decimal::ZERO;
                                                    pdm.side = tt_types::accounts::events::PositionSide::Flat;
                                                    pdm.time = now;
                                                }
                                                touched_positions.push((
                                                    key.0,
                                                    key.1.clone(),
                                                    instr.clone(),
                                                ));
                                            }
                                        }
                                    }
                                }
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
                                    let _ = bus.broadcast(Response::AccountDeltaBatch(ab));
                                }
                            }
                            // Emit closed trades for realized PnL this tick
                            if !closed_trades.is_empty() {
                                let _ = bus.broadcast(Response::ClosedTrades(closed_trades));
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
                                    let _ = bus.broadcast(Response::PositionsBatch(pb));
                                }
                            }
                            if !finished.is_empty() {
                                for id in finished {
                                    sim_orders.remove(&id);
                                }
                            }
                            // After draining up to watermark (and emitting due orders), notify runtime of logical time
                            // Provide a next-event hint so the orchestrator can fast-forward when using very small steps
                            let next_hint = if let Some(peek) = heap.peek() {
                                Some(peek.t)
                            } else if keys.is_empty() {
                                cfg.range_end
                            } else {
                                None
                            };
                            let _ = bus.broadcast(Response::BacktestTimeUpdated {
                                now,
                                latest_time: next_hint,
                            });
                            // Yield to allow orchestrator/runtime to process time update immediately, avoiding scheduling stalls that were previously masked by logging
                            tokio::task::yield_now().await;

                            // If we have an end range and reached/passed it, emit BacktestCompleted once
                            if !completed_emitted
                                && let Some(end) = cfg.range_end
                                && now >= end
                            {
                                let _ = bus.broadcast(Response::BacktestCompleted { end });

                                completed_emitted = true;
                            }
                        }
                        Request::PlaceOrder(mut spec) => {
                            // If watermark has not been established yet, buffer this order until the first advance
                            if watermark.is_none() {
                                pre_wm_queue.push(Request::PlaceOrder(spec));
                                continue;
                            }
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

                            let mut msg = None;
                            match spec.order_type {
                                tt_types::wire::OrderType::Stop => {
                                    if spec.stop_price.is_none() {
                                        reject = true;
                                        msg = Some("Stop price is none".to_string())
                                    }
                                }
                                tt_types::wire::OrderType::TrailingStop => {
                                    // Trailing stops require a trail distance; initial stop_price is derived/ratcheted by the fill model
                                    if spec.trail_price.is_none() {
                                        reject = true;
                                        msg = Some("Trail price is none".to_string())
                                    }
                                }
                                tt_types::wire::OrderType::StopLimit => {
                                    if spec.stop_price.is_none() || spec.limit_price.is_none() {
                                        reject = true;
                                        msg = Some(
                                            "Stop price is none or Limit price is none".to_string(),
                                        )
                                    }
                                }
                                _ => {}
                            }

                            if reject {
                                // Immediately emit a rejected order update with user's original tag
                                let upd = OrderUpdate {
                                    account_name: spec.account_key.account_name.clone(),
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
                                    side: spec.side,
                                    msg,
                                };
                                let ob = tt_types::wire::OrdersBatch {
                                    topic: Topic::Orders,
                                    seq: 0,
                                    orders: vec![upd],
                                };
                                let _ = bus.broadcast(Response::OrdersBatch(ob));

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

                            // Determine effective ack base time according to session state and policy
                            let is_closed_or_halt = !cal_model.is_open(&spec.instrument, base)
                                || cal_model.is_halt(&spec.instrument, base);
                            // Early rejection if policy is Reject during closed/halts
                            if is_closed_or_halt
                                && matches!(
                                    cfg.order_ack_closed_policy,
                                    OrderAckDuringClosedPolicy::Reject
                                )
                            {
                                let upd = OrderUpdate {
                                    account_name: spec.account_key.account_name.clone(),
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
                                    side: spec.side,
                                    msg: Some("Is closed or halt".to_string()),
                                };
                                let ob = tt_types::wire::OrdersBatch {
                                    topic: Topic::Orders,
                                    seq: 0,
                                    orders: vec![upd],
                                };
                                let _ = bus.broadcast(Response::OrdersBatch(ob));

                                continue;
                            }
                            let ack_base = if is_closed_or_halt
                                && matches!(
                                    cfg.order_ack_closed_policy,
                                    OrderAckDuringClosedPolicy::DeferAckUntilOpen
                                ) {
                                if let Some(next_open) =
                                    cal_model.next_open_after(&spec.instrument, base)
                                {
                                    next_open
                                } else {
                                    base + ChronoDuration::seconds(1)
                                }
                            } else {
                                base
                            };

                            // Schedule ack and (first) fill per latency
                            let ack_at = ack_base + to_chrono(lat.submit_to_ack());
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
                                    key: AccountKey::new(prov, acct_name.clone()),
                                    equity: account_balance,
                                    day_realized_pnl: Decimal::ZERO,
                                    open_pnl: Decimal::ZERO,
                                    time: base,
                                    can_trade: true,
                                });
                            sim_orders.entry(engine_order_id).or_insert(sim);
                        }
                        Request::CancelOrder(cxl) => {
                            // If no watermark yet, buffer cancel until first advance
                            if watermark.is_none() {
                                pre_wm_queue.push(Request::CancelOrder(cxl));
                                continue;
                            }
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
                            // If no watermark yet, buffer replace until first advance
                            if watermark.is_none() {
                                pre_wm_queue.push(Request::ReplaceOrder(rpl));
                                continue;
                            }
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
                                emit_one(bus, &item, ks.provider, &notify).await;
                            }
                            // Move cursor up to at least this t
                            if t > ks.cursor {
                                ks.cursor = t;
                            }
                            // Refill window if close to end and not past hard stop
                            let can_refill = ks.hard_stop.map(|hs| ks.cursor < hs).unwrap_or(true);
                            if can_refill && ks.cursor + ChronoDuration::seconds(1) >= ks.window_end
                            {
                                ensure_window(ks, &conn, &cfg, watermark).await;
                            }
                        }
                        // Push next time for this key, if any
                        push_next_for_key(&mut heap, topic, &sk, ks);
                        did_work = true;
                    }
                }
                if !did_work {
                    // Proactively extend windows when idle and watermark has advanced beyond current window
                    if let Some(wm) = watermark {
                        let mut refilled = false;
                        for ((topic, sk), ks) in keys.iter_mut() {
                            if !ks.done && ks.buf.is_empty() {
                                ensure_window(ks, &conn, &cfg, Some(wm)).await;
                                // If this key has reached its hard stop with no data, mark it done and remove it
                                if ks.hard_stop.map(|hs| ks.window_end >= hs).unwrap_or(false)
                                    && ks.buf.is_empty()
                                {
                                    ks.done = true;
                                    // defer removal until after loop to avoid borrow issues; mark by pushing a sentinel next time
                                } else {
                                    push_next_for_key(&mut heap, *topic, sk, ks);
                                    refilled = true;
                                }
                            }
                        }
                        if refilled {
                            // We seeded more work; skip yielding so next loop can pop immediately
                            continue;
                        }
                        // Clean up any keys that are done and have no buffered data
                        let mut to_remove: Vec<(Topic, SymbolKey)> = Vec::new();
                        for ((topic, sk), ks) in keys.iter_mut() {
                            if ks.done && ks.buf.is_empty() {
                                to_remove.push((*topic, sk.clone()));
                            }
                        }
                        for (topic, sk) in to_remove.drain(..) {
                            keys.remove(&(topic, sk));
                        }
                    }
                    // idle yield
                    tokio::task::yield_now().await;
                }
            }
        }));

        Ok(())
    }
}
