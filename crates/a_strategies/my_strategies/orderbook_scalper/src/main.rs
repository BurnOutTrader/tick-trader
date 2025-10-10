use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tt_bus::ClientMessageBus;
use tt_engine::engine::{DataTopic, EngineHandle, EngineRuntime, Strategy};
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::AccountDelta;
use tt_types::data::mbp10::{Action as MbpAction, BookLevels, BookSide as MbpSide, Mbp10};
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire;
use tt_types::wire::{OrderType, Trade};
use tt_types::engine_id::EngineUuid; // NEW: engine-side order id tracking

#[allow(dead_code)]
#[derive(Default, Debug, Clone)]
struct LastTrade {
    price: Option<Decimal>,
    size: Option<Decimal>,
}
#[allow(dead_code)]
#[derive(Default, Debug, Clone)]
struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_trade: LastTrade,
}
#[allow(dead_code)]
impl OrderBook {
    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    fn seed_from_snapshot(&mut self, book: &BookLevels) {
        self.clear();
        for (i, px) in book.bid_px.iter().enumerate() {
            let px = *px;
            let sz = *book.bid_sz.get(i).unwrap_or(&Decimal::zero());
            if sz > Decimal::zero() {
                self.bids.insert(px, sz);
            }
        }
        for (i, px) in book.ask_px.iter().enumerate() {
            let px = *px;
            let sz = *book.ask_sz.get(i).unwrap_or(&Decimal::zero());
            if sz > Decimal::zero() {
                self.asks.insert(px, sz);
            }
        }
    }

    fn apply_modify(&mut self, side: MbpSide, price: Decimal, size: Decimal) {
        match side {
            MbpSide::Bid => {
                if size.is_zero() {
                    self.bids.remove(&price);
                } else {
                    self.bids.insert(price, size);
                }
            }
            MbpSide::Ask => {
                if size.is_zero() {
                    self.asks.remove(&price);
                } else {
                    self.asks.insert(price, size);
                }
            }
            MbpSide::None => {}
        }
    }

    fn note_trade(&mut self, price: Decimal, size: Decimal) {
        self.last_trade.price = Some(price);
        self.last_trade.size = Some(size);
    }

    fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(p, s)| (p.clone(), *s))
    }
    fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(p, s)| (p.clone(), *s))
    }

    fn print_top_n(&self, n: usize) {
        let mut bids: Vec<(Decimal, Decimal)> = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(p, s)| (p.clone(), *s))
            .collect();
        let asks: Vec<(Decimal, Decimal)> = self
            .asks
            .iter()
            .take(n)
            .map(|(p, s)| (p.clone(), *s))
            .collect();

        println!("-- ORDER BOOK (top {}) --", n);
        if let (Some((bb_px, bb_sz)), Some((ba_px, ba_sz))) = (self.best_bid(), self.best_ask()) {
            println!("BBO: bid {} x {} | ask {} x {}", bb_sz, bb_px, ba_px, ba_sz);
        } else {
            println!("BBO: unavailable");
        }
        if let Some(p) = self.last_trade.price.clone() {
            println!("Last trade: {} @ {:?}", p, self.last_trade.size);
        }
        println!("BIDS:");
        for (px, sz) in bids.drain(..) {
            println!("  {} x {}", sz, px);
        }
        println!("ASKS:");
        for (px, sz) in asks {
            println!("  {} x {}", sz, px);
        }
    }
}

#[derive(Clone)]
// New: lightweight config and state for the strategy
struct StrategyConfig {
    key: SymbolKey,
    instrument: Instrument,
    provider: ProviderKind,
    account_name: AccountName,
    max_pos_abs: Decimal,
}

// Dev plan
// 1. consider strategy having internal receivers to remove locks from the engine.
// have a loop for position, order, account
// have one for each data type, make a new channel between engine and server for each base data type.

struct OrderBookStrategy {
    account_name: AccountName,
    engine: Option<EngineHandle>,
    book: OrderBook,
    cfg: Option<StrategyConfig>,
    net_pos: Decimal,
    last_manage: Option<Instant>,
    manage_interval: Duration,
    // New: adaptive management state
    last_desired: Option<(bool, bool)>,
    last_bbo: Option<(Decimal, Decimal)>,
    last_cancel_at: Option<Instant>,
    min_cancel_interval: Duration,
    // New: periodic refresh to clean stale orders
    last_refresh_at: Option<Instant>,
    refresh_interval: Duration,
    // Anchor BBO at last placement to detect far-from-market stale orders
    anchor_bbo: Option<(Decimal, Decimal)>,
    anchor_set_at: Option<Instant>,
    stale_spread_mult: u32,
    max_anchor_age: Duration,
    // Trend tracking (EMA of midprice delta)
    trend_last_mid: Option<Decimal>,
    trend_mom: Decimal,
    trend_alpha: Decimal,
    sell_count: u64,
    buy_count: u64,
    // New: per-side placement cooldowns to reduce spam
    last_place_bid_at: Option<Instant>,
    last_place_ask_at: Option<Instant>,
    place_cooldown: Duration,
    can_trade: bool,
    // NEW: track working orders by engine id
    working_bid: Option<EngineUuid>,
    working_ask: Option<EngineUuid>,
    // Edge: avoid quoting micro spreads
    min_spread_ratio: Decimal,
    // Debug: force place both sides regardless of edge
    debug_force_place: bool,
}
#[allow(dead_code)]
impl OrderBookStrategy {
    fn new(cfg: StrategyConfig, account_name: AccountName) -> Self {
        Self {
            account_name,
            engine: None,
            book: OrderBook::default(),
            cfg: Some(cfg),
            net_pos: Decimal::ZERO,
            last_manage: None,
            manage_interval: Duration::from_millis(100),
            last_desired: None,
            last_bbo: None,
            last_cancel_at: None,
            min_cancel_interval: Duration::from_millis(1000),
            last_refresh_at: None,
            refresh_interval: Duration::from_secs(5),
            anchor_bbo: None,
            anchor_set_at: None,
            stale_spread_mult: 8,
            max_anchor_age: Duration::from_secs(20),
            trend_last_mid: None,
            trend_mom: Decimal::ZERO,
            trend_alpha: Decimal::new(2, 1),
            sell_count: 0,
            buy_count: 0,
            last_place_bid_at: None,
            last_place_ask_at: None,
            place_cooldown: Duration::from_millis(400),
            can_trade: true,
            working_bid: None,
            working_ask: None,
            // Default very permissive to ensure activity; tune via env MIN_SPREAD_BPS
            // 0 means disabled; MIN_SPREAD_BPS env can override at runtime
            min_spread_ratio: Decimal::ZERO,
            debug_force_place: false,
        }
    }

    fn apply_env_overrides(&mut self) {
        // MIN_SPREAD_BPS: integer bps; 1 = 0.01%; 0 disables
        if let Ok(v) = std::env::var("MIN_SPREAD_BPS") {
            if let Ok(bps) = v.parse::<i64>() {
                self.min_spread_ratio = Decimal::from(bps) / Decimal::from(10_000i64);
                println!("CFG: MIN_SPREAD_BPS={} -> ratio {}", bps, self.min_spread_ratio);
            }
        }
        if let Ok(v) = std::env::var("STALE_MULT") {
            if let Ok(m) = v.parse::<u32>() {
                self.stale_spread_mult = m;
                println!("CFG: STALE_MULT={}", m);
            }
        }
        if let Ok(v) = std::env::var("PLACE_COOLDOWN_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.place_cooldown = Duration::from_millis(ms);
                println!("CFG: PLACE_COOLDOWN_MS={}", ms);
            }
        }
        if let Ok(v) = std::env::var("CANCEL_THROTTLE_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.min_cancel_interval = Duration::from_millis(ms);
                println!("CFG: CANCEL_THROTTLE_MS={}", ms);
            }
        }
        if let Ok(v) = std::env::var("MANAGE_INTERVAL_MS") {
            if let Ok(ms) = v.parse::<u64>() {
                self.manage_interval = Duration::from_millis(ms);
                println!("CFG: MANAGE_INTERVAL_MS={}", ms);
            }
        }
        if let Ok(v) = std::env::var("FORCE_PLACE_BOTH") {
            self.debug_force_place = v == "1" || v.eq_ignore_ascii_case("true");
            println!("CFG: FORCE_PLACE_BOTH={}", self.debug_force_place);
        }
    }

    fn throttle_ok(&mut self) -> bool {
        let now = Instant::now();
        if let Some(prev) = self.last_manage {
            if now.duration_since(prev) < self.manage_interval {
                println!("SKIP: throttle window ({:?} < {:?})", now.duration_since(prev), self.manage_interval);
                return false;
            }
        }
        self.last_manage = Some(now);
        true
    }

    fn cancel_all_for_instrument(&mut self) -> bool {
        if let (Some(h), Some(cfg)) = (&self.engine, &self.cfg) {
            // Rate-limit cancels aggressively
            if let Some(prev) = self.last_cancel_at {
                if prev.elapsed() < self.min_cancel_interval {
                    println!("CANCEL-ALL: throttled (elapsed {:?} < {:?})", prev.elapsed(), self.min_cancel_interval);
                    return false;
                }
            }
            let open = h.open_orders_for_instrument(&cfg.instrument);
            let mut did_cancel = false;
            for ou in open {
                println!("CANCEL-ALL: canceling order_id={} provider={:?}", ou.order_id, ou.provider_kind);
                {
                    // Use engine order_id with provider to cancel via engine's mapping
                    let _ = h.cancel_order(ou.provider_kind, cfg.account_name.clone(), ou.order_id);
                    did_cancel = true;
                }
            }
            if did_cancel {
                self.last_cancel_at = Some(Instant::now());
            }
            return did_cancel;
        }
        false
    }

    // Force-cancel open orders for the instrument, ignoring cancel throttle.
    fn cancel_all_for_instrument_force(&mut self) {
        if let (Some(h), Some(cfg)) = (&self.engine, &self.cfg) {
            let open = h.open_orders_for_instrument(&cfg.instrument);
            for ou in open {
                println!("CANCEL-ALL-FORCE: canceling order_id={} provider={:?}", ou.order_id, ou.provider_kind);
                // Force-cancel using engine order_id mapping
                let _ = h.cancel_order(ou.provider_kind, cfg.account_name.clone(), ou.order_id);
            }
            self.last_cancel_at = Some(Instant::now());
        }
    }

    // Cancel a single working side if present (respects throttle unless force)
    fn cancel_side(&mut self, is_bid: bool, force: bool) {
        if self.engine.is_none() || self.cfg.is_none() { return; }
        if is_bid {
            if let Some(order_id) = self.working_bid {
                if !force {
                    if let Some(prev) = self.last_cancel_at {
                        if prev.elapsed() < self.min_cancel_interval {
                            println!("CANCEL bid throttled (elapsed {:?} < {:?})", prev.elapsed(), self.min_cancel_interval);
                            return;
                        }
                    }
                }
                let h = self.engine.as_ref().unwrap();
                let cfg = self.cfg.as_ref().unwrap();
                println!("CANCEL bid order_id={}", order_id);
                let _ = h.cancel_order(cfg.provider, cfg.account_name.clone(), order_id);
                self.last_cancel_at = Some(Instant::now());
                self.working_bid = None;
            }
        } else {
            if let Some(order_id) = self.working_ask {
                if !force {
                    if let Some(prev) = self.last_cancel_at {
                        if prev.elapsed() < self.min_cancel_interval {
                            println!("CANCEL ask throttled (elapsed {:?} < {:?})", prev.elapsed(), self.min_cancel_interval);
                            return;
                        }
                    }
                }
                let h = self.engine.as_ref().unwrap();
                let cfg = self.cfg.as_ref().unwrap();
                println!("CANCEL ask order_id={}", order_id);
                let _ = h.cancel_order(cfg.provider, cfg.account_name.clone(), order_id);
                self.last_cancel_at = Some(Instant::now());
                self.working_ask = None;
            }
        }
    }

    // Determine if current BBO moved far from the anchor (or anchor too old)
    fn bbo_moved_far(&self, bb_px: Decimal, ba_px: Decimal) -> bool {
        if let Some((abid, aask)) = self.anchor_bbo {
            let spread = ba_px - bb_px;
            if spread <= Decimal::ZERO {
                println!("STALE-CHECK: invalid spread {} (<=0)", spread);
                return false;
            }
            let move_bid = (bb_px - abid).abs();
            let move_ask = (ba_px - aask).abs();
            let mv = if move_bid > move_ask { move_bid } else { move_ask };
            let mult = Decimal::from(self.stale_spread_mult as i64);
            if mv >= spread * mult {
                println!("STALE-CHECK: moved_far mv={} >= spread({})*mult({})", mv, spread, mult);
                return true;
            }
        }
        if let Some(at) = self.anchor_set_at {
            if at.elapsed() >= self.max_anchor_age {
                println!("STALE-CHECK: anchor_age {:?} >= {:?}", at.elapsed(), self.max_anchor_age);
                return true;
            }
        }
        false
    }

    // Periodically check and clean only truly stale (far-from-market) working orders
    fn cleanup_stale_orders(&mut self) {
        if self.engine.is_none() || self.cfg.is_none() {
            return;
        }
        let now = Instant::now();
        if let Some(prev) = self.last_refresh_at {
            if now.duration_since(prev) < self.refresh_interval {
                println!("STALE: skip refresh (elapsed {:?} < {:?})", now.duration_since(prev), self.refresh_interval);
                return;
            }
        }
        let h = self.engine.as_ref().unwrap().clone();
        let cfg = self.cfg.as_ref().unwrap();
        let open = h.open_orders_for_instrument(&cfg.instrument);
        println!("STALE: open_orders={} for {}", open.len(), cfg.instrument);
        if open.is_empty() {
            self.last_refresh_at = Some(now);
            return;
        }
        // If we have a valid BBO and it moved far from the placement anchor, force cancel regardless of cancel throttle.
        if let (Some((bb_px, _bb_sz)), Some((ba_px, _ba_sz))) =
            (self.book.best_bid(), self.book.best_ask())
        {
            if ba_px > bb_px && self.bbo_moved_far(bb_px, ba_px) {
                self.cancel_all_for_instrument_force();
                // Drop local tracking too
                self.working_bid = None;
                self.working_ask = None;
            }
        } else {
            // Without BBO context, be conservative: do nothing except tick the refresh timer.
            println!("STALE: no valid BBO to evaluate staleness");
        }
        self.last_refresh_at = Some(now);
    }

    fn ensure_quotes(&mut self) {
        println!("ENSURE_QUOTES: begin");
        if self.engine.is_none() || self.cfg.is_none() {
            println!("ENSURE_QUOTES: no engine or no cfg");
            return;
        }
        if !self.throttle_ok() {
            return;
        }
        let h = self.engine.as_ref().unwrap().clone();
        let max_pos_abs = self.cfg.as_ref().unwrap().max_pos_abs;
        // Only quote when we have a valid BBO (with sizes)
        let ((bb_px, bb_sz), (ba_px, ba_sz)) = match (self.book.best_bid(), self.book.best_ask()) {
            (Some((bb_px, bb_sz)), Some((ba_px, ba_sz))) if ba_px > bb_px => {
                ((bb_px, bb_sz), (ba_px, ba_sz))
            }
            other => {
                println!("ENSURE_QUOTES: missing/invalid BBO = {:?}", other);
                return;
            }
        };

        // Basic spread edge: require at least X bps of mid
        let mid = (bb_px + ba_px) / Decimal::from(2i64);
        let spread = ba_px - bb_px;
        if mid > Decimal::ZERO {
            let min_spread = mid * self.min_spread_ratio; // e.g., X bps of mid
            println!("BBO mid={} bid={} ask={} spread={} min_spread={} ratio={}", mid, bb_px, ba_px, spread, min_spread, self.min_spread_ratio);
            if spread < min_spread {
                // too tight to have an edge
                println!("ENSURE_QUOTES: skip due to min_spread gate");
                return;
            }
        }

        // Decide which sides to quote based on current net position and simple microstructure signals
        let mut want_bid = true;
        let mut want_ask = true;
        // Inventory and max position guards
        if self.net_pos >= max_pos_abs { want_bid = false; }
        if self.net_pos <= -max_pos_abs { want_ask = false; }
        if self.net_pos > Decimal::ZERO { want_bid = false; }
        if self.net_pos < Decimal::ZERO { want_ask = false; }

        // Top-of-book imbalance skew: favor the side with deeper interest; avoid quoting the likely adverse side
        let total_sz = bb_sz + ba_sz;
        if total_sz > Decimal::ZERO {
            let imb = bb_sz / total_sz; // 0..1, >0.5 means bid-heavy (upward pressure)
            let upper = Decimal::new(65, 2); // 0.65
            let lower = Decimal::new(35, 2); // 0.35
            println!("IMBALANCE: bb_sz={} ba_sz={} imb={}", bb_sz, ba_sz, imb);
            if imb >= upper {
                // bid heavy → avoid selling into strength unless offloading inventory
                if self.net_pos <= Decimal::ZERO { want_ask = false; }
            } else if imb <= lower {
                // ask heavy → avoid buying into weakness unless covering short
                if self.net_pos >= Decimal::ZERO { want_bid = false; }
            }
        }

        // Momentum handling:
        let mom_up = self.trend_mom > Decimal::ZERO;
        let mom_down = self.trend_mom < Decimal::ZERO;
        let offer_threshold: Decimal = Decimal::from(100);
        println!("MOMENTUM: trend_mom={} up={} down={} net_pos={}", self.trend_mom, mom_up, mom_down, self.net_pos);
        if self.net_pos > offer_threshold && mom_up {
            // Long and momentum up: place offers to reduce risk into strength
            want_ask = true;
        } else if self.net_pos < -offer_threshold && mom_down {
            // Short and momentum down: place bids to cover into weakness
            want_bid = true;
        } else {
            // Default behavior: let winners run (avoid placing exits with the trend)
            if mom_up && self.net_pos > Decimal::ZERO { want_ask = false; }
            else if mom_down && self.net_pos < Decimal::ZERO { want_bid = false; }
        }

        // Edge gating via microprice tilt + momentum alignment
        if spread > Decimal::ZERO && total_sz > Decimal::ZERO {
            let micro = (ba_px * bb_sz + bb_px * ba_sz) / total_sz;
            let tilt = micro - mid;
            let tilt_thresh = spread * Decimal::new(2, 1); // 0.2 * spread
            let edge_up = tilt > tilt_thresh && mom_up;
            let edge_down = tilt < -tilt_thresh && mom_down;
            let exit_override = (self.net_pos > offer_threshold && mom_up)
                || (self.net_pos < -offer_threshold && mom_down);
            println!("EDGE: micro={} tilt={} tilt_thresh={} up_edge={} down_edge={} exit_override={} desired_pre=({}, {})", micro, tilt, tilt_thresh, edge_up, edge_down, exit_override, want_bid, want_ask);
            if want_bid && !exit_override && !edge_down {
                // require micro tilt down to add bids
                want_bid = false;
            }
            if want_ask && !exit_override && !edge_up {
                // require micro tilt up to add asks
                want_ask = false;
            }
        }

        // If both sides disabled, fall back to the side of least inventory exposure
        if !want_bid && !want_ask {
            if self.net_pos >= Decimal::ZERO { want_bid = true; } else { want_ask = true; }
        }

        let desired = (want_bid, want_ask);
        let desired = if self.debug_force_place { (true, true) } else { desired };
        println!("DESIRED: bid={} ask={} working_bid={:?} working_ask={:?}", desired.0, desired.1, self.working_bid, self.working_ask);

        // Skip if desired state and BBO didn't change
        if let Some(prev_desired) = self.last_desired {
            if prev_desired == desired {
                if let Some(prev_bbo) = self.last_bbo.clone() {
                    if prev_bbo == (bb_px, ba_px) {
                        println!("ENSURE_QUOTES: no change in desired/BBO; skip");
                        return;
                    }
                }
            }
        }
        self.last_desired = Some(desired);
        self.last_bbo = Some((bb_px, ba_px));

        // Detect staleness relative to anchor and reset sides if needed
        let moved_far = self.bbo_moved_far(bb_px, ba_px);
        if moved_far {
            // Cancel both sides forcefully if market moved far
            println!("ENSURE_QUOTES: market moved far; force-cancel both sides");
            self.cancel_side(true, true);
            self.cancel_side(false, true);
            self.anchor_bbo = Some((bb_px, ba_px));
            self.anchor_set_at = Some(Instant::now());
        }

        // Side-specific management: cancel undesired sides, place desired if not working and not in cooldown
        let now = Instant::now();
        // Cancel ask if not desired
        if !desired.1 { self.cancel_side(false, false); }
        // Cancel bid if not desired
        if !desired.0 { self.cancel_side(true, false); }

        if self.can_trade {
            // Set anchor at placement time
            if desired.0 || desired.1 {
                self.anchor_bbo = Some((bb_px, ba_px));
                self.anchor_set_at = Some(Instant::now());
            }

            let key = SymbolKey {
                instrument: self.cfg.clone().unwrap().instrument,
                provider: self.cfg.clone().unwrap().provider,
            };
            // BID placement
            if desired.0 && self.working_bid.is_none() {
                let can_place = match self.last_place_bid_at { Some(t) => now.duration_since(t) >= self.place_cooldown, None => true };
                if can_place {
                    self.buy_count += 1;
                    self.last_place_bid_at = Some(now);
                    println!("PLACE BID: JoinBid qty=1 at bbo={} (count={})", bb_px, self.buy_count);
                    if let Ok(order_id) = h.place_order(
                        self.account_name.clone(),
                        key.clone(),
                        tt_types::accounts::events::Side::Buy,
                        1,
                        OrderType::JoinBid,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ) {
                        println!("PLACE BID: order_id={}", order_id);
                        self.working_bid = Some(order_id);
                    }
                } else {
                    println!("PLACE BID: cooldown; skip");
                }
            }
            // ASK placement (FIX: JoinAsk)
            if desired.1 && self.working_ask.is_none() {
                let can_place = match self.last_place_ask_at { Some(t) => now.duration_since(t) >= self.place_cooldown, None => true };
                if can_place {
                    self.sell_count += 1;
                    self.last_place_ask_at = Some(now);
                    println!("PLACE ASK: JoinAsk qty=1 at bbo={} (count={})", ba_px, self.sell_count);
                    if let Ok(order_id) = h.place_order(
                        self.account_name.clone(),
                        key,
                        tt_types::accounts::events::Side::Sell,
                        1,
                        OrderType::JoinAsk,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    ) {
                        println!("PLACE ASK: order_id={}", order_id);
                        self.working_ask = Some(order_id);
                    }
                } else {
                    println!("PLACE ASK: cooldown; skip");
                }
            }
        } else {
            println!("ENSURE_QUOTES: can_trade=false; skip placements");
        }
    }

    // Sync our working ids against the latest OrdersBatch snapshot
    fn sync_working_from_orders(&mut self, ob: &wire::OrdersBatch) {
        use tt_types::accounts::order::OrderState;
        if self.cfg.is_none() { return; }
        let instr = &self.cfg.as_ref().unwrap().instrument;
        println!("ORDERS_BATCH: {} orders (instrument filter={})", ob.orders.len(), instr);
        // Build a quick set of still-open orders for our instrument
        for ou in ob.orders.iter().filter(|o| &o.instrument == instr) {
            println!("ORDER: id={} state={:?} leaves={} avg_px={}", ou.order_id, ou.state, ou.leaves, ou.avg_fill_px);
            if let Some(wb) = self.working_bid {
                if ou.order_id == wb {
                    // Clear tracking when no longer working
                    if matches!(ou.state, OrderState::Canceled | OrderState::Rejected | OrderState::Filled) || ou.leaves == 0 {
                        println!("TRACK: clearing working_bid due to state/leaves");
                        self.working_bid = None;
                    }
                }
            }
            if let Some(wa) = self.working_ask {
                if ou.order_id == wa {
                    if matches!(ou.state, OrderState::Canceled | OrderState::Rejected | OrderState::Filled) || ou.leaves == 0 {
                        println!("TRACK: clearing working_ask due to state/leaves");
                        self.working_ask = None;
                    }
                }
            }
        }

        // If we track an id that no longer appears for our instrument at all, drop it.
        let ids: Vec<_> = ob.orders.iter().filter(|o| &o.instrument == instr).map(|o| o.order_id).collect();
        if let Some(wb) = self.working_bid { if !ids.contains(&wb) { println!("TRACK: working_bid {:?} not in snapshot; clear", wb); self.working_bid = None; } }
        if let Some(wa) = self.working_ask { if !ids.contains(&wa) { println!("TRACK: working_ask {:?} not in snapshot; clear", wa); self.working_ask = None; } }
    }
}

impl Strategy for OrderBookStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        info!("strategy start");
        println!("CALLBACK on_start");
        self.engine = Some(h.clone());
        // Apply env overrides early
        self.apply_env_overrides();

        if self.cfg.is_none() {
            let instrument = Instrument::from_str("MNQ.Z25").unwrap();
            let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);
            let key = SymbolKey::new(instrument.clone(), provider);
            self.cfg = Some(StrategyConfig {
                key,
                instrument,
                provider,
                account_name: AccountName::from_str("UNKNOWN").unwrap(),
                max_pos_abs: Decimal::from(150),
            });
        }

        let cfg = self.cfg.as_ref().unwrap();
        // Non-blocking subscriptions via command queue
        let _ = h.subscribe_now(DataTopic::MBP10, cfg.key.clone());
        //let _ = h.subscribe_now(DataTopic::Ticks, cfg.key.clone());
    }

    fn on_stop(&mut self) {
        info!("strategy stop");
        println!("CALLBACK on_stop");
    }

    fn on_tick(&mut self, t: &tt_types::data::core::Tick, _provider_kind: ProviderKind) {
        println!("CALLBACK on_tick: {:?}", t)
    }

    fn on_quote(&mut self, q: &tt_types::data::core::Bbo, _provider_kind: ProviderKind) {
        println!("CALLBACK on_quote: {:?}", q);
    }

    fn on_bar(&mut self, _b: &tt_types::data::core::Candle, _provider_kind: ProviderKind) {}

    fn on_mbp10(&mut self, d: &Mbp10, _provider_kind: ProviderKind) {
        println!("CALLBACK on_mbp10: action={:?} side={:?} price={} size={} book_present={}", d.action, d.side, d.price, d.size, d.book.is_some());
        let ob = &mut self.book;
        if let Some(ref book) = d.book {
            ob.seed_from_snapshot(book);
        }
        match d.action {
            MbpAction::Clear => ob.clear(),
            MbpAction::Modify | MbpAction::Add | MbpAction::Cancel => {
                ob.apply_modify(d.side, d.price, d.size)
            }
            MbpAction::Trade | MbpAction::Fill => ob.note_trade(d.price, d.size),
            MbpAction::None => {}
        }

        // Update trend (EMA of midprice delta)
        if let (Some((bb_px, _)), Some((ba_px, _))) = (self.book.best_bid(), self.book.best_ask()) {
            if ba_px > bb_px {
                let mid = (bb_px + ba_px) / Decimal::from(2i64);
                if let Some(prev) = self.trend_last_mid {
                    let delta = mid - prev;
                    let alpha = self.trend_alpha;
                    self.trend_mom = alpha * delta + (Decimal::new(1, 0) - alpha) * self.trend_mom;
                }
                self.trend_last_mid = Some(mid);
            }
        }

        // Opportunistic maintenance
        self.cleanup_stale_orders();
        self.ensure_quotes();
    }

    fn on_orders_batch(&mut self, b: &wire::OrdersBatch) {
        println!("CALLBACK on_orders_batch: {} orders", b.orders.len());
        // Sync state of working order ids, then print
        self.sync_working_from_orders(b);
        for order in &b.orders {
            println!("ORDER UPDATE: {:?}", order);
        }
    }

    fn on_positions_batch(&mut self, b: &wire::PositionsBatch) {
        println!("CALLBACK on_positions_batch: {} positions", b.positions.len());
        if let Some(cfg) = &self.cfg {
            if let Some(p) = b.positions.iter().find(|p| p.instrument == cfg.instrument) {
                self.net_pos = p.net_qty;
                println!("NET POS UPDATE: {} on {}", self.net_pos, cfg.instrument);
            }
        }
        for pos in &b.positions {
            println!("POSITION: {:?}", pos)
        }
    }

    fn on_account_delta(&mut self, accounts: &[AccountDelta]) {
        println!("CALLBACK on_account_delta: {} accounts", accounts.len());
        for account_delta in accounts {
            println!("ACCOUNT: {:?}", account_delta);
            if account_delta.can_trade == false {
                if account_delta.name == self.account_name {
                    self.can_trade = false
                }
            } else if self.can_trade == false {
                self.can_trade = true;
            }
        }
    }

    fn on_trades_closed(&mut self, trades: Vec<Trade>) {
        println!("CALLBACK on_trades_closed: {} trades", trades.len());
        for trade in trades {
            println!("TRADE CLOSED: {:?}", trade);
        }
    }

    fn on_subscribe(&mut self, instrument: Instrument, data_topic: DataTopic, success: bool) {
        println!(
            "Subscribed to {} on topic {:?}: Success: {}",
            instrument, data_topic, success
        );
        if !success {
            eprintln!(
                "Warning: subscribe failed for {:?} on {} — upstream may be unavailable; engine will call on_stop if connection closes.",
                data_topic, instrument
            );
        }
    }

    fn on_unsubscribe(&mut self, _instrument: Instrument, data_topic: DataTopic) {
        println!("CALLBACK on_unsubscribe: {:?}", data_topic);
    }

    fn accounts(&self) -> Vec<AccountKey> {
        if let Some(cfg) = &self.cfg {
            vec![AccountKey::new(cfg.provider, cfg.account_name.clone())]
        } else {
            vec![]
        }
    }
}
#[allow(dead_code)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tt_bus=info,tt_engine=info,projectx.ws=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;

    let mut engine = EngineRuntime::new(bus.clone());

    let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);
    let instrument = Instrument::from_str("MNQ.Z25").unwrap();
    let key = SymbolKey::new(instrument.clone(), provider);

    let account_name = AccountName::from_str("PRAC-V2-64413-98419885").unwrap();

    let strategy = OrderBookStrategy::new(
        StrategyConfig {
            key: key.clone(),
            instrument: instrument.clone(),
            provider,
            account_name: account_name.clone(),
            max_pos_abs: Decimal::from(150),
        },
        account_name.clone(),
    );

    let _handle = engine.start(strategy).await?;

    sleep(Duration::from_secs(10000)).await;

    let _ = engine.stop().await?;
    Ok(())
}

