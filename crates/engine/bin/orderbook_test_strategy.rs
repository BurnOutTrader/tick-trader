use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
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
use tt_types::wire::Trade;

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

// New: lightweight config and state for the strategy
struct StrategyConfig {
    key: SymbolKey,
    instrument: Instrument,
    provider: ProviderKind,
    account_name: AccountName,
    max_pos_abs: Decimal,
}

#[derive(Default)]
struct OrderBookStrategy {
    engine: Option<EngineHandle>,
    book: OrderBook,
    cfg: Option<StrategyConfig>,
    net_pos: Decimal,
    // Track our average entry price when we have an open position
    avg_entry: Option<Decimal>,
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
    last_place_bid_px: Option<(Decimal, Instant)>,
    last_place_ask_px: Option<(Decimal, Instant)>,
    place_cooldown: Duration,
    // New: skip quoting when spread tighter than N points
    min_spread_points: Decimal,
    // Edge gating: require strong signal and persistence
    min_tilt_ratio: Decimal,         // e.g., 0.35 of spread
    min_mom_points: Decimal,         // e.g., 1.0 points EMA delta magnitude
    edge_dwell: Duration,            // require edge to persist before acting
    edge_dir: Option<(i8, Instant)>, // 1=up, -1=down, time started
    // Avoid buying and selling at the exact same price within a window
    avoid_same_price_window: Duration,
}
#[allow(dead_code)]
impl OrderBookStrategy {
    fn new(cfg: StrategyConfig) -> Self {
        Self {
            engine: None,
            book: OrderBook::default(),
            cfg: Some(cfg),
            net_pos: Decimal::ZERO,
            avg_entry: None,
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
            last_place_bid_px: None,
            last_place_ask_px: None,
            place_cooldown: Duration::from_millis(2000),
            min_spread_points: Decimal::from(3),
            min_tilt_ratio: Decimal::new(35, 2),    // 0.35
            min_mom_points: Decimal::from(1),       // 1.0 point
            edge_dwell: Duration::from_millis(500), // 500ms persistence
            edge_dir: None,
            avoid_same_price_window: Duration::from_secs(60),
        }
    }

    fn throttle_ok(&mut self) -> bool {
        let now = Instant::now();
        if let Some(prev) = self.last_manage {
            if now.duration_since(prev) < self.manage_interval {
                return false;
            }
        }
        self.last_manage = Some(now);
        true
    }

    async fn cancel_all_for_instrument(&mut self) -> bool {
        if let (Some(h), Some(cfg)) = (&self.engine, &self.cfg) {
            // Rate-limit cancels aggressively
            if let Some(prev) = self.last_cancel_at {
                if prev.elapsed() < self.min_cancel_interval {
                    return false;
                }
            }
            let open = h.orders_for_instrument(&cfg.instrument).await;
            let mut did_cancel = false;
            for ou in open {
                if let Some(poid) = ou.provider_order_id {
                    let _ = h
                        .cancel_order(wire::CancelOrder {
                            account_name: cfg.account_name.clone(),
                            provider_order_id: Some(poid.0),
                            client_order_id: None,
                        })
                        .await;
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
    async fn cancel_all_for_instrument_force(&mut self) {
        if let (Some(h), Some(cfg)) = (&self.engine, &self.cfg) {
            let open = h.orders_for_instrument(&cfg.instrument).await;
            for ou in open {
                if let Some(poid) = ou.provider_order_id {
                    let _ = h
                        .cancel_order(wire::CancelOrder {
                            account_name: cfg.account_name.clone(),
                            provider_order_id: Some(poid.0),
                            client_order_id: None,
                        })
                        .await;
                }
            }
            self.last_cancel_at = Some(Instant::now());
        }
    }

    // Determine if current BBO moved far from the anchor (or anchor too old)
    fn bbo_moved_far(&self, bb_px: Decimal, ba_px: Decimal) -> bool {
        if let Some((abid, aask)) = self.anchor_bbo {
            let spread = ba_px - bb_px;
            if spread <= Decimal::ZERO {
                return false;
            }
            let move_bid = (bb_px - abid).abs();
            let move_ask = (ba_px - aask).abs();
            let mv = if move_bid > move_ask {
                move_bid
            } else {
                move_ask
            };
            let mult = Decimal::from(self.stale_spread_mult as i64);
            if mv >= spread * mult {
                return true;
            }
        }
        if let Some(at) = self.anchor_set_at {
            if at.elapsed() >= self.max_anchor_age {
                return true;
            }
        }
        false
    }

    // Periodically check and clean only truly stale (far-from-market) working orders
    async fn cleanup_stale_orders(&mut self) {
        if self.engine.is_none() || self.cfg.is_none() {
            return;
        }
        let now = Instant::now();
        if let Some(prev) = self.last_refresh_at {
            if now.duration_since(prev) < self.refresh_interval {
                return;
            }
        }
        let h = self.engine.as_ref().unwrap().clone();
        let cfg = self.cfg.as_ref().unwrap();
        let open = h.orders_for_instrument(&cfg.instrument).await;
        if open.is_empty() {
            self.last_refresh_at = Some(now);
            return;
        }
        // If we have a valid BBO and it moved far from the placement anchor, force cancel regardless of cancel throttle.
        if let (Some((bb_px, _bb_sz)), Some((ba_px, _ba_sz))) =
            (self.book.best_bid(), self.book.best_ask())
        {
            if ba_px > bb_px && self.bbo_moved_far(bb_px, ba_px) {
                self.cancel_all_for_instrument_force().await;
            }
        } else {
            // Without BBO context, be conservative: do nothing except tick the refresh timer.
        }
        self.last_refresh_at = Some(now);
    }

    async fn ensure_quotes(&mut self) {
        if self.engine.is_none() || self.cfg.is_none() {
            return;
        }
        if !self.throttle_ok() {
            return;
        }
        let h = self.engine.as_ref().unwrap().clone();
        let (key_clone, account_name_clone, max_pos_abs, instrument_clone) = {
            let cfg = self.cfg.as_ref().unwrap();
            (
                cfg.key.clone(),
                cfg.account_name.clone(),
                cfg.max_pos_abs,
                cfg.instrument.clone(),
            )
        };
        // Only quote when we have a valid BBO (with sizes)
        let ((bb_px, bb_sz), (ba_px, ba_sz)) = match (self.book.best_bid(), self.book.best_ask()) {
            (Some((bb_px, bb_sz)), Some((ba_px, ba_sz))) if ba_px > bb_px => {
                ((bb_px, bb_sz), (ba_px, ba_sz))
            }
            _ => return,
        };

        // Decide which sides to quote
        let mut want_bid = true;
        let mut want_ask = true;
        // Inventory and max position guards
        if self.net_pos >= max_pos_abs {
            want_bid = false;
        }
        if self.net_pos <= -max_pos_abs {
            want_ask = false;
        }
        if self.net_pos > Decimal::ZERO {
            want_bid = false;
        }
        if self.net_pos < Decimal::ZERO {
            want_ask = false;
        }

        // Top-of-book imbalance skew: favor the side with deeper interest; avoid quoting the likely adverse side
        let total_sz = bb_sz + ba_sz;
        if total_sz > Decimal::ZERO {
            let imb = bb_sz / total_sz; // 0..1, >0.5 means bid-heavy (upward pressure)
            let upper = Decimal::new(65, 2); // 0.65
            let lower = Decimal::new(35, 2); // 0.35
            if imb >= upper {
                want_ask = false;
            }
            // avoid selling into strength
            else if imb <= lower {
                want_bid = false;
            } // avoid buying into weakness
        }

        // Momentum handling:
        // - If position is over 100 in the direction of momentum, offer out into momentum.
        // - Otherwise, let winners run by not placing exit orders with the prevailing trend.
        let mom_up = self.trend_mom > Decimal::ZERO;
        let mom_down = self.trend_mom < Decimal::ZERO;
        let mom_abs = self.trend_mom.abs();
        let offer_threshold: Decimal = Decimal::from(100);
        if self.net_pos > offer_threshold && mom_up {
            // Long and momentum up: place offers to reduce risk into strength
            want_ask = true;
        } else if self.net_pos < -offer_threshold && mom_down {
            // Short and momentum down: place bids to cover into weakness
            want_bid = true;
        } else {
            // Default behavior: let winners run (avoid placing exits with the trend)
            if mom_up && self.net_pos > Decimal::ZERO {
                want_ask = false;
            } else if mom_down && self.net_pos < Decimal::ZERO {
                want_bid = false;
            }
        }

        // Edge gating via microprice tilt + momentum alignment
        let spread = ba_px - bb_px;
        if spread > Decimal::ZERO && total_sz > Decimal::ZERO {
            let mid = (bb_px + ba_px) / Decimal::from(2i64);
            let micro = (ba_px * bb_sz + bb_px * ba_sz) / total_sz;
            let tilt = micro - mid;
            // Require stronger tilt (as ratio of spread) and minimum momentum magnitude
            let tilt_ratio = if spread > Decimal::ZERO {
                tilt / spread
            } else {
                Decimal::ZERO
            };
            let edge_up =
                tilt_ratio >= self.min_tilt_ratio && mom_up && mom_abs >= self.min_mom_points;
            let edge_down =
                tilt_ratio <= -self.min_tilt_ratio && mom_down && mom_abs >= self.min_mom_points;
            let exit_override = (self.net_pos > offer_threshold && mom_up)
                || (self.net_pos < -offer_threshold && mom_down);
            // If no strong edge present and no exit override, do not quote
            if !exit_override && !(edge_up || edge_down) {
                return;
            }

            // Enforce dwell: edge direction must persist for edge_dwell before acting
            let now = Instant::now();
            let dir = if edge_up {
                1
            } else if edge_down {
                -1
            } else {
                0
            };
            if dir != 0 {
                match self.edge_dir {
                    Some((d, started)) if d == dir => {
                        if now.duration_since(started) < self.edge_dwell {
                            return;
                        }
                    }
                    _ => {
                        self.edge_dir = Some((dir, now));
                        return;
                    }
                }
            }

            // Restrict to quoting only in the edge direction (single-sided), unless exit_override applies
            if !exit_override {
                if edge_up {
                    want_ask = false; // only bid with up-edge
                    want_bid = true;
                } else if edge_down {
                    want_bid = false; // only ask with down-edge
                    want_ask = true;
                }
            }
        }

        // New gating: avoid placing our spread < N points, and avoid placing within N points of our avg entry.
        // Determine candidate prices for desired sides
        let mut want_bid_px = if want_bid { Some(bb_px) } else { None };
        let mut want_ask_px = if want_ask { Some(ba_px) } else { None };

        // Avoid placing opposite side at the exact same price as our last placement within a window
        if let Some((last_apx, when)) = self.last_place_ask_px {
            if want_bid
                && want_bid_px == Some(last_apx)
                && when.elapsed() < self.avoid_same_price_window
            {
                want_bid = false;
                want_bid_px = None;
            }
        }
        if let Some((last_bpx, when)) = self.last_place_bid_px {
            if want_ask
                && want_ask_px == Some(last_bpx)
                && when.elapsed() < self.avoid_same_price_window
            {
                want_ask = false;
                want_ask_px = None;
            }
        }

        // If both sides are desired but our two quotes would be too close, keep only the side farther from avg_entry (if available)
        if let (Some(bpx), Some(apx)) = (want_bid_px, want_ask_px) {
            if apx - bpx < self.min_spread_points {
                if let Some(avg) = self.avg_entry {
                    let dist_bid = (bpx - avg).abs();
                    let dist_ask = (apx - avg).abs();
                    if dist_bid >= dist_ask {
                        // keep bid, drop ask
                        want_ask = false;
                        want_ask_px = None;
                    } else {
                        // keep ask, drop bid
                        want_bid = false;
                        want_bid_px = None;
                    }
                } else {
                    // Without avg, keep only one side based on inventory: reduce exposure
                    if self.net_pos >= Decimal::ZERO {
                        // long/flat: prefer bid further from market impact
                        want_ask = false;
                        want_ask_px = None;
                    } else {
                        want_bid = false;
                        want_bid_px = None;
                    }
                }
            }
        }

        // Enforce distance from average entry for each side independently
        if let Some(avg) = self.avg_entry {
            if let Some(bpx) = want_bid_px {
                if (bpx - avg).abs() < self.min_spread_points {
                    want_bid = false;
                    want_bid_px = None;
                }
            }
            if let Some(apx) = want_ask_px {
                if (apx - avg).abs() < self.min_spread_points {
                    want_ask = false;
                    want_ask_px = None;
                }
            }
        }

        // If both sides ended up disabled, stop here
        if !want_bid && !want_ask {
            return;
        }

        // If both sides were disabled by filters, fall back to the side of least inventory exposure
        if !want_bid && !want_ask {
            if self.net_pos >= Decimal::ZERO {
                want_bid = true;
            } else {
                want_ask = true;
            }
        }

        let desired = (want_bid, want_ask);

        // Skip if desired state and BBO didn't change
        if let Some(prev_desired) = self.last_desired {
            if prev_desired == desired {
                if let Some(prev_bbo) = self.last_bbo.clone() {
                    if prev_bbo == (bb_px, ba_px) {
                        return;
                    }
                }
            }
        }
        self.last_desired = Some(desired);
        self.last_bbo = Some((bb_px, ba_px));

        // Clear existing working orders to avoid stale or wrong-side resting orders (rate-limited)
        let did_cancel = self.cancel_all_for_instrument().await;
        if !did_cancel {
            // If we couldn't cancel due to throttle and we still have open orders, avoid stacking new ones.
            let open = h.orders_for_instrument(&instrument_clone).await;
            if !open.is_empty() {
                return;
            }
        }

        // Apply per-side placement cooldowns
        let now = Instant::now();
        if want_bid {
            if let Some(t) = self.last_place_bid_at {
                if now.duration_since(t) < self.place_cooldown {
                    // suppress bid placement due to cooldown
                    want_bid = false;
                }
            }
        }
        if want_ask {
            if let Some(t) = self.last_place_ask_at {
                if now.duration_since(t) < self.place_cooldown {
                    // suppress ask placement for this cycle
                    want_ask = false;
                }
            }
        }

        // Set anchor BBO at (re)placement time for stale-distance tracking
        if desired.0 || desired.1 {
            self.anchor_bbo = Some((bb_px, ba_px));
            self.anchor_set_at = Some(Instant::now());
        }

        // Re-place desired sides using join orders to stick to BBO
        if want_bid {
            self.buy_count += 1;
            self.last_place_bid_at = Some(now);
            self.last_place_bid_px = Some((bb_px, now));
            let order = wire::PlaceOrder {
                account_name: account_name_clone.clone(),
                key: key_clone.clone(),
                side: tt_types::accounts::events::Side::Buy,
                qty: 1,
                r#type: wire::OrderType::JoinBid,
                limit_price: None,
                stop_price: None,
                trail_price: None,
                custom_tag: None,
                stop_loss: None,
                take_profit: None,
            };
            info!("Placing Order: {:?}", order);
            //let _ = h.place_order(order).await;
        }
        if want_ask {
            self.sell_count += 1;
            self.last_place_ask_at = Some(now);
            self.last_place_ask_px = Some((ba_px, now));
            let order = wire::PlaceOrder {
                account_name: account_name_clone.clone(),
                key: key_clone.clone(),
                side: tt_types::accounts::events::Side::Sell,
                qty: 1,
                r#type: wire::OrderType::JoinAsk,
                limit_price: None,
                stop_price: None,
                trail_price: None,
                custom_tag: None,
                stop_loss: None,
                take_profit: None,
            };
            info!("Placing Order: {:?}", order);
            //let _ = h.place_order(order).await;
        }
    }
}

#[async_trait::async_trait]
impl Strategy for OrderBookStrategy {
    async fn on_start(&mut self, h: EngineHandle) {
        info!("strategy start");
        self.engine = Some(h.clone());

        // If cfg is not yet set (should be set by main), build a default for MNQ.Z25 on Topstep
        if self.cfg.is_none() {
            let instrument = Instrument::from_str("MNQ.Z25").unwrap();
            let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);
            let key = SymbolKey::new(instrument.clone(), provider);
            self.cfg = Some(StrategyConfig {
                key,
                instrument,
                provider,
                account_name: AccountName::from_str("UNKNOWN").unwrap(),
                max_pos_abs: Decimal::from(100),
            });
        }

        let cfg = self.cfg.as_ref().unwrap();
        h.subscribe_key(DataTopic::MBP10, cfg.key.clone())
            .await
            .unwrap();
        h.subscribe_key(DataTopic::Ticks, cfg.key.clone())
            .await
            .unwrap();
    }
    async fn on_stop(&mut self) {
        info!("strategy stop");
    }
    async fn on_tick(&mut self, t: tt_types::data::core::Tick, _provider_kind: ProviderKind) {
        println!("{:?}", t)
    }
    async fn on_quote(&mut self, q: tt_types::data::core::Bbo, _provider_kind: ProviderKind) {
        println!("{:?}", q);
    }
    async fn on_bar(&mut self, _b: tt_types::data::core::Candle, _provider_kind: ProviderKind) {}

    async fn on_mbp10(&mut self, d: Mbp10, _provider_kind: ProviderKind) {
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
        /* println!(
            "MBP10 evt: action={:?} side={:?} px={} sz={} flags={:?} ts_event={} ts_recv={}",
            d.action, d.side, d.price, d.size, d.flags, d.ts_event, d.ts_recv
        );*/
        //ob.print_top_n(5);

        // Update trend (EMA of midprice delta)
        if let (Some((bb_px, _)), Some((ba_px, _))) = (self.book.best_bid(), self.book.best_ask()) {
            if ba_px > bb_px {
                let mid = (bb_px + ba_px) / Decimal::from(2i64);
                if let Some(prev) = self.trend_last_mid {
                    let delta = mid - prev;
                    let alpha = self.trend_alpha;
                    // EMA of delta
                    self.trend_mom = alpha * delta + (Decimal::new(1, 0) - alpha) * self.trend_mom;
                }
                self.trend_last_mid = Some(mid);
            }
        }

        // Opportunistically clean up stale and then (throttled) manage our quotes
        self.cleanup_stale_orders().await;
        self.ensure_quotes().await;
    }

    async fn on_orders_batch(&mut self, b: wire::OrdersBatch) {
        // For visibility; could also reconcile here if desired
        println!("orders batch: {} updates", b.orders.len());
    }
    async fn on_positions_batch(&mut self, b: wire::PositionsBatch) {
        if let Some(cfg) = &self.cfg {
            if let Some(p) = b.positions.iter().find(|p| p.instrument == cfg.instrument) {
                self.net_pos = p.net_qty;
                // Track average entry when we have an open position; clear when flat
                if p.net_qty != Decimal::ZERO {
                    self.avg_entry = Some(p.average_price);
                } else {
                    self.avg_entry = None;
                }
            }
        }
        println!(
            "positions batch (net_pos={:.2}) : {} entries",
            self.net_pos,
            b.positions.len()
        );
    }
    async fn on_account_delta(&mut self, accounts: Vec<AccountDelta>) {
        for account_delta in accounts {
            println!("{:?}", account_delta);
        }
    }

    async fn on_trades_closed(&mut self, _trades: Vec<Trade>) {
        todo!()
    }

    async fn on_subscribe(&mut self, instrument: Instrument, data_topic: DataTopic, success: bool) {
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
    async fn on_unsubscribe(&mut self, _instrument: Instrument, data_topic: DataTopic) {
        println!("{:?}", data_topic);
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
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;

    let mut engine = EngineRuntime::new(bus.clone());

    // Provider and instrument
    let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);
    let instrument = Instrument::from_str("MNQ.Z25").unwrap();
    let key = SymbolKey::new(instrument.clone(), provider);

    // Target account name we will use for placing orders
    let account_name =
        tt_types::accounts::account::AccountName::from_str("PRAC-V2-64413-98419885").unwrap();
    engine.initialize_account_names(provider, vec![]).await?;
    // Create strategy with placeholder account_id and known account_name; we'll set id after engine start
    let strategy = Arc::new(Mutex::new(OrderBookStrategy::new(StrategyConfig {
        key: key.clone(),
        instrument: instrument.clone(),
        provider,
        account_name,
        max_pos_abs: Decimal::from(150),
    })));

    // Start engine to obtain a sub_id and begin processing responses
    let _handle = engine.start(strategy.clone()).await?;

    // Resolve account id for the provided account name on ProjectX Topstep and subscribe to exec streams
    let target_account_name = "PRAC-V2-64413-98419885";

    // Initialize account interest so we receive orders/positions/account deltas
    engine
        .initialize_account_names(
            provider,
            vec![tt_types::accounts::account::AccountName::from_str(target_account_name).unwrap()],
        )
        .await?;

    // Run for a while; adjust as needed
    sleep(Duration::from_secs(10000)).await;

    let _ = engine.stop().await?;
    Ok(())
}
