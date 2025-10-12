use crate::backtest::backtest_execution::{
    FeeCtx, Fill, LimitPolicy, MarketPolicy, Money, PostFillAction, ProposedPortfolioChange,
    RiskCtx, RiskDecision, StopTrigger,
};
use crate::traits::{FeeModel, FillModel, LatencyModel, RiskModel, SessionCalendar, SlippageModel};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use std::time::Duration;
use tt_types::accounts::events::{PositionDelta, Side};
use tt_types::data::core::Exchange;
use tt_types::data::mbp10::BookLevels;
use tt_types::securities::hours::market_hours::{
    MarketHours, hours_for_exchange, next_session_after, session_bounds,
};
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{OrderType, PlaceOrder, ReplaceOrder};

// =========================
// Default CME-oriented models
// =========================
/// CME-like latency profile (tight but non-zero).
#[derive(Debug, Clone, Copy)]
pub struct CmeLatency {
    pub submit_ack_ms: u64,
    pub ack_fill_ms: u64,
    pub cancel_rtt_ms: u64,
    pub replace_rtt_ms: u64,
}
impl CmeLatency {
    pub fn new(
        submit_ack_ms: u64,
        ack_fill_ms: u64,
        cancel_rtt_ms: u64,
        replace_rtt_ms: u64,
    ) -> Self {
        Self {
            submit_ack_ms,
            ack_fill_ms,
            cancel_rtt_ms,
            replace_rtt_ms,
        }
    }
}
impl Default for CmeLatency {
    fn default() -> Self {
        Self::new(3, 6, 12, 12)
    }
}
impl LatencyModel for CmeLatency {
    fn submit_to_ack(&mut self) -> Duration {
        Duration::from_millis(self.submit_ack_ms)
    }
    fn ack_to_first_fill(&mut self) -> Duration {
        Duration::from_millis(self.ack_fill_ms)
    }
    fn cancel_rtt(&mut self) -> Duration {
        Duration::from_millis(self.cancel_rtt_ms)
    }
    fn replace_rtt(&mut self) -> Duration {
        Duration::from_millis(self.replace_rtt_ms)
    }
}

/// No additional slippage; execute at touch/reference.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSlippage;
impl SlippageModel for NoSlippage {
    fn adjust(
        &mut self,
        side: Side,
        ref_price: Decimal,
        _spread: Option<Decimal>,
        _qty: i64,
    ) -> Decimal {
        let _ = side;
        ref_price
    }
}

/// Simple spread-based slippage: market orders pay 0.5 * spread.
#[derive(Debug, Default, Clone, Copy)]
pub struct HalfSpreadSlippage;
impl SlippageModel for HalfSpreadSlippage {
    fn adjust(
        &mut self,
        side: Side,
        ref_price: Decimal,
        spread: Option<Decimal>,
        _qty: i64,
    ) -> Decimal {
        if let Some(sp) = spread {
            let half = sp / Decimal::from(2);
            match side {
                Side::Buy => ref_price + half,
                Side::Sell => ref_price - half,
            }
        } else {
            ref_price
        }
    }
}

/// Flat per-contract fees typical for CME+clearing+broker (approximate; configurable).
#[derive(Debug, Clone, Copy)]
pub struct CmeFlatFee {
    pub per_contract: Decimal, // e.g., 2.50 USD per contract per side
}

impl Default for CmeFlatFee {
    fn default() -> Self {
        Self {
            per_contract: Decimal::new(250, 2),
        }
    } // 2.50
}

impl FeeModel for CmeFlatFee {
    fn on_new_order(&self, _ctx: &FeeCtx, _o: &PlaceOrder) -> Money {
        Money::ZERO
    }
    fn on_fill(&self, _ctx: &FeeCtx, f: &Fill) -> Money {
        // Charge per contract filled (absolute qty).
        let contracts = Decimal::from(f.qty.abs());
        Money {
            amount: contracts * self.per_contract,
        }
    }
    fn on_cancel(&self, _ctx: &FeeCtx, _o: &PlaceOrder, _canceled_qty: i64) -> Money {
        let _ = _canceled_qty;
        Money::ZERO
    }
    fn settle(&self, _ctx: &FeeCtx, _pos: &PositionDelta) -> Money {
        Money::ZERO
    }
}

impl SessionCalendar for HoursCalendar {
    fn is_open(&self, _instr: &Instrument, t: DateTime<Utc>) -> bool {
        self.hours.is_open(t)
    }

    fn session_bounds(
        &self,
        _instr: &Instrument,
        t: DateTime<Utc>,
    ) -> (DateTime<Utc>, DateTime<Utc>) {
        session_bounds(&self.hours, t)
    }

    fn next_open_after(&self, _instr: &Instrument, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let (open, _close) = next_session_after(&self.hours, t);
        Some(open)
    }

    fn next_close_after(&self, _instr: &Instrument, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let (_open, close) = session_bounds(&self.hours, t);
        Some(close)
    }

    fn trading_day(&self, _instr: &Instrument, t: DateTime<Utc>) -> NaiveDate {
        // Define trading day by the *session close* local date.
        let (_open, close) = session_bounds(&self.hours, t);
        close.with_timezone(&self.hours.tz).date_naive()
    }

    fn is_halt(&self, _instr: &Instrument, t: DateTime<Utc>) -> bool {
        // Delegate: a venue-level "maintenance" window counts as a halt for matching
        self.hours.is_maintenance(t)
    }
}

impl RiskModel for SimpleCmeRisk {
    fn pre_place(&self, _ctx: &RiskCtx, _o: &PlaceOrder) -> RiskDecision {
        RiskDecision::Allow
    }
    fn pre_replace(
        &self,
        _ctx: &RiskCtx,
        _r: &ReplaceOrder,
        _current: &PlaceOrder,
    ) -> RiskDecision {
        RiskDecision::Allow
    }
    fn pre_cancel(&self, _ctx: &RiskCtx, _current: &PlaceOrder) -> RiskDecision {
        RiskDecision::Allow
    }
    fn on_fill(&self, ctx: &RiskCtx, _f: &Fill) -> PostFillAction {
        if ctx.day_realized_pnl + ctx.open_pnl <= -self.max_daily_loss {
            PostFillAction::FlattenAll {
                reason: "daily loss limit breached",
            }
        } else {
            PostFillAction::None
        }
    }
    fn margin_required(&self, _ctx: &RiskCtx, _after: &ProposedPortfolioChange) -> Money {
        Money::ZERO
    }
    fn can_trade(&self, ctx: &RiskCtx) -> bool {
        ctx.day_realized_pnl + ctx.open_pnl > -self.max_daily_loss
    }
}

impl FillModel for CmeFillModel {
    fn on_submit(&mut self, _now: DateTime<Utc>, _order: &mut PlaceOrder) {
        // Basic normalization: IOC and FOK cannot both be true.
        // if order.tif_ioc && order.tif_fok { order.tif_fok = false; }
    }

    fn match_book(
        &mut self,
        _now: DateTime<Utc>,
        book: Option<&BookLevels>,
        last_price: Decimal,
        order: &mut PlaceOrder,
        slip: &mut dyn SlippageModel,
        cal: &dyn SessionCalendar,
    ) -> Vec<Fill> {
        let mut out = Vec::new();
        if !cal.is_open(&order.key.instrument, Utc::now()) {
            return out;
        }
        if order.qty == 0 {
            return out;
        }

        // Helper: compute spread from best levels if present
        let (best_bid, best_ask, spread) = if let Some(b) = book {
            let bb = b.bid_px.first().cloned();
            let ba = b.ask_px.first().cloned();
            let sp = match (bb, ba) {
                (Some(bid), Some(ask)) if ask > bid => Some(ask - bid),
                _ => None,
            };
            (bb, ba, sp)
        } else {
            (None, None, None)
        };

        // Walks the opposite side of the book accumulating fills up to qty_remaining.
        let mut walk_depth = |prices: &[Decimal],
                              sizes: &[Decimal],
                              price_ok: &mut dyn FnMut(Decimal) -> bool,
                              side: Side| {
            let mut qty_remaining = order.qty;
            let levels = prices.len().min(sizes.len());
            for i in 0..levels {
                let px = prices[i];
                if !price_ok(px) {
                    break;
                }
                let avail_i64 = sizes[i].to_i64().unwrap_or(i64::MAX); // assume decimal size is whole contracts
                if avail_i64 <= 0 {
                    continue;
                }
                let take = qty_remaining.min(avail_i64);
                if take <= 0 {
                    break;
                }
                let px_adj = slip.adjust(side, px, spread, take);
                out.push(Fill {
                    instrument: order.key.instrument.clone(),
                    qty: take,
                    price: px_adj,
                    maker: false,
                });
                qty_remaining -= take;
                if qty_remaining == 0 {
                    break;
                }
            }
            qty_remaining
        };

        // Routing per type
        match order.order_type {
            OrderType::Market => {
                match order.side {
                    Side::Buy => {
                        if let Some(b) = book {
                            let mut always_ok = |_p: Decimal| true;
                            let rem = walk_depth(&b.ask_px, &b.ask_sz, &mut always_ok, Side::Buy);
                            if rem > 0 && out.is_empty() {
                                // No visible book or zero sizes: fall back to last price for a single fill
                                let px_adj = slip.adjust(Side::Buy, last_price, spread, rem);
                                out.push(Fill {
                                    instrument: order.key.instrument.clone(),
                                    qty: rem,
                                    price: px_adj,
                                    maker: false,
                                });
                            }
                        } else {
                            let px_adj = slip.adjust(Side::Buy, last_price, spread, order.qty);
                            out.push(Fill {
                                instrument: order.key.instrument.clone(),
                                qty: order.qty,
                                price: px_adj,
                                maker: false,
                            });
                        }
                    }
                    Side::Sell => {
                        if let Some(b) = book {
                            let mut always_ok = |_p: Decimal| true;
                            let rem = walk_depth(&b.bid_px, &b.bid_sz, &mut always_ok, Side::Sell);
                            if rem > 0 && out.is_empty() {
                                let px_adj = slip.adjust(Side::Sell, last_price, spread, rem);
                                out.push(Fill {
                                    instrument: order.key.instrument.clone(),
                                    qty: rem,
                                    price: px_adj,
                                    maker: false,
                                });
                            }
                        } else {
                            let px_adj = slip.adjust(Side::Sell, last_price, spread, order.qty);
                            out.push(Fill {
                                instrument: order.key.instrument.clone(),
                                qty: order.qty,
                                price: px_adj,
                                maker: false,
                            });
                        }
                    }
                }
            }
            OrderType::Limit => {
                if let Some(lim) = order.limit_price.and_then(Decimal::from_f64) {
                    match order.side {
                        Side::Buy => {
                            if let Some(b) = book {
                                let mut ok = |px: Decimal| px <= lim;
                                let _rem = walk_depth(&b.ask_px, &b.ask_sz, &mut ok, Side::Buy);
                            } else if lim >= last_price {
                                let px_adj = slip.adjust(Side::Buy, last_price, spread, order.qty);
                                out.push(Fill {
                                    instrument: order.key.instrument.clone(),
                                    qty: order.qty,
                                    price: px_adj,
                                    maker: false,
                                });
                            }
                        }
                        Side::Sell => {
                            if let Some(b) = book {
                                let mut ok = |px: Decimal| px >= lim;
                                let _rem = walk_depth(&b.bid_px, &b.bid_sz, &mut ok, Side::Sell);
                            } else if lim <= last_price {
                                let px_adj = slip.adjust(Side::Sell, last_price, spread, order.qty);
                                out.push(Fill {
                                    instrument: order.key.instrument.clone(),
                                    qty: order.qty,
                                    price: px_adj,
                                    maker: false,
                                });
                            }
                        }
                    }
                }
            }
            OrderType::JoinBid => {
                match order.side {
                    // Buy joining the bid = maker at bid; only executes if market is crossed.
                    Side::Buy => {
                        if let Some(b) = book
                            && let (Some(bb), Some(ba)) = (best_bid, best_ask)
                            && ba <= bb
                        {
                            // crossed; we can take offers up to our join price
                            let mut ok = |px: Decimal| px <= bb;
                            let _rem = walk_depth(&b.ask_px, &b.ask_sz, &mut ok, Side::Buy);
                        }
                        // otherwise rest; no immediate fill
                    }
                    // Sell joining the bid = take from bids up to best bid
                    Side::Sell => {
                        if let Some(b) = book
                            && let Some(bb) = best_bid
                        {
                            let mut ok = |px: Decimal| px >= bb;
                            let _rem = walk_depth(&b.bid_px, &b.bid_sz, &mut ok, Side::Sell);
                        }
                    }
                }
            }
            OrderType::JoinAsk => {
                match order.side {
                    // Buy joining the ask = take from asks up to best ask
                    Side::Buy => {
                        if let Some(b) = book
                            && let Some(ba) = best_ask
                        {
                            let mut ok = |px: Decimal| px <= ba;
                            let _rem = walk_depth(&b.ask_px, &b.ask_sz, &mut ok, Side::Buy);
                        }
                    }
                    // Sell joining the ask = maker at ask; only executes if crossed.
                    Side::Sell => {
                        if let Some(b) = book
                            && let (Some(bb), Some(ba)) = (best_bid, best_ask)
                            && bb >= ba
                        {
                            let mut ok = |px: Decimal| px >= ba;
                            let _rem = walk_depth(&b.bid_px, &b.bid_sz, &mut ok, Side::Sell);
                        }
                        // otherwise rest; no immediate fill
                    }
                }
            }
            OrderType::Stop | OrderType::StopLimit => {
                if let Some(stop) = order.stop_price.and_then(Decimal::from_f64) {
                    // Trigger source: bid/ask vs last; fallback to last
                    let trigger_px = match self.cfg.stop_trigger {
                        StopTrigger::Trade | StopTrigger::Last => Some(last_price),
                        StopTrigger::BidAsk => match order.side {
                            Side::Buy => best_ask,
                            Side::Sell => best_bid,
                        },
                    }
                    .or(Some(last_price));

                    if let Some(tp) = trigger_px {
                        let triggered = match order.side {
                            Side::Buy => tp >= stop,
                            Side::Sell => tp <= stop,
                        };
                        if triggered {
                            if let OrderType::Stop = order.order_type {
                                // Treat as market after trigger
                                match order.side {
                                    Side::Buy => {
                                        if let Some(b) = book {
                                            let mut always_ok = |_p: Decimal| true;
                                            let rem = walk_depth(
                                                &b.ask_px,
                                                &b.ask_sz,
                                                &mut always_ok,
                                                Side::Buy,
                                            );
                                            if rem > 0 && out.is_empty() {
                                                let px_adj =
                                                    slip.adjust(Side::Buy, last_price, spread, rem);
                                                out.push(Fill {
                                                    instrument: order.key.instrument.clone(),
                                                    qty: rem,
                                                    price: px_adj,
                                                    maker: false,
                                                });
                                            }
                                        } else {
                                            let px_adj = slip.adjust(
                                                Side::Buy,
                                                last_price,
                                                spread,
                                                order.qty,
                                            );
                                            out.push(Fill {
                                                instrument: order.key.instrument.clone(),
                                                qty: order.qty,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    }
                                    Side::Sell => {
                                        if let Some(b) = book {
                                            let mut always_ok = |_p: Decimal| true;
                                            let rem = walk_depth(
                                                &b.bid_px,
                                                &b.bid_sz,
                                                &mut always_ok,
                                                Side::Sell,
                                            );
                                            if rem > 0 && out.is_empty() {
                                                let px_adj = slip.adjust(
                                                    Side::Sell,
                                                    last_price,
                                                    spread,
                                                    rem,
                                                );
                                                out.push(Fill {
                                                    instrument: order.key.instrument.clone(),
                                                    qty: rem,
                                                    price: px_adj,
                                                    maker: false,
                                                });
                                            }
                                        } else {
                                            let px_adj = slip.adjust(
                                                Side::Sell,
                                                last_price,
                                                spread,
                                                order.qty,
                                            );
                                            out.push(Fill {
                                                instrument: order.key.instrument.clone(),
                                                qty: order.qty,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    }
                                }
                            } else {
                                // StopLimit
                                let eff_lim = order
                                    .limit_price
                                    .and_then(Decimal::from_f64)
                                    .unwrap_or(stop);
                                match order.side {
                                    Side::Buy => {
                                        if let Some(b) = book {
                                            let mut ok = |px: Decimal| px <= eff_lim;
                                            let _rem = walk_depth(
                                                &b.ask_px,
                                                &b.ask_sz,
                                                &mut ok,
                                                Side::Buy,
                                            );
                                        } else if eff_lim >= last_price {
                                            let px_adj = slip.adjust(
                                                Side::Buy,
                                                last_price,
                                                spread,
                                                order.qty,
                                            );
                                            out.push(Fill {
                                                instrument: order.key.instrument.clone(),
                                                qty: order.qty,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    }
                                    Side::Sell => {
                                        if let Some(b) = book {
                                            let mut ok = |px: Decimal| px >= eff_lim;
                                            let _rem = walk_depth(
                                                &b.bid_px,
                                                &b.bid_sz,
                                                &mut ok,
                                                Side::Sell,
                                            );
                                        } else if eff_lim <= last_price {
                                            let px_adj = slip.adjust(
                                                Side::Sell,
                                                last_price,
                                                spread,
                                                order.qty,
                                            );
                                            out.push(Fill {
                                                instrument: order.key.instrument.clone(),
                                                qty: order.qty,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            OrderType::TrailingStop => {
                if self.cfg.trailing_enabled {
                    // Determine trigger reference price per config
                    let trigger_px = match self.cfg.stop_trigger {
                        StopTrigger::Trade | StopTrigger::Last => Some(last_price),
                        StopTrigger::BidAsk => match order.side {
                            Side::Buy => best_ask,
                            Side::Sell => best_bid,
                        },
                    }
                    .or(Some(last_price));

                    if let Some(tp) = trigger_px
                        && let Some(trail) = order.trail_price.and_then(Decimal::from_f64)
                    {
                        // Initialize or ratchet the internal stop stored in order.stop_price
                        let mut cur_stop = order.stop_price.and_then(Decimal::from_f64);
                        match order.side {
                            // Trailing buy (to cover a short): stop follows price downward; triggers on break above
                            Side::Buy => {
                                let desired = tp + trail;
                                cur_stop = Some(match cur_stop {
                                    Some(s) => s.min(desired),
                                    None => desired,
                                });
                                order.stop_price = cur_stop.and_then(|d| d.to_f64());
                                if let Some(stop_now) = cur_stop
                                    && tp >= stop_now
                                    && let Some(b) = book
                                {
                                    let mut always_ok = |_p: Decimal| true;
                                    let rem =
                                        walk_depth(&b.ask_px, &b.ask_sz, &mut always_ok, Side::Buy);
                                    if rem > 0 && out.is_empty() {
                                        let px_adj =
                                            slip.adjust(Side::Buy, last_price, spread, rem);
                                        out.push(Fill {
                                            instrument: order.key.instrument.clone(),
                                            qty: rem,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    }
                                } else {
                                    let px_adj =
                                        slip.adjust(Side::Buy, last_price, spread, order.qty);
                                    out.push(Fill {
                                        instrument: order.key.instrument.clone(),
                                        qty: order.qty,
                                        price: px_adj,
                                        maker: false,
                                    });
                                }
                            }
                            // Trailing sell (to protect a long): stop follows price upward; triggers on break below
                            Side::Sell => {
                                let desired = tp - trail;
                                cur_stop = Some(match cur_stop {
                                    Some(s) => s.max(desired),
                                    None => desired,
                                });
                                order.stop_price = cur_stop.and_then(|d| d.to_f64());
                                if let Some(stop_now) = cur_stop
                                    && tp <= stop_now
                                {
                                    if let Some(b) = book {
                                        let mut always_ok = |_p: Decimal| true;
                                        let rem = walk_depth(
                                            &b.bid_px,
                                            &b.bid_sz,
                                            &mut always_ok,
                                            Side::Sell,
                                        );
                                        if rem > 0 && out.is_empty() {
                                            let px_adj =
                                                slip.adjust(Side::Sell, last_price, spread, rem);
                                            out.push(Fill {
                                                instrument: order.key.instrument.clone(),
                                                qty: rem,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    } else {
                                        let px_adj =
                                            slip.adjust(Side::Sell, last_price, spread, order.qty);
                                        out.push(Fill {
                                            instrument: order.key.instrument.clone(),
                                            qty: order.qty,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        out
    }
}

/// Fill configuration knobs
#[derive(Debug, Clone, Copy)]
pub struct FillConfig {
    pub limit_policy: LimitPolicy,
    pub market_policy: MarketPolicy,
    pub stop_trigger: StopTrigger,
    pub trailing_enabled: bool,
}

impl Default for FillConfig {
    fn default() -> Self {
        Self {
            limit_policy: LimitPolicy::TouchOrCross,
            market_policy: MarketPolicy::AtTouch,
            stop_trigger: StopTrigger::Trade,
            trailing_enabled: true,
        }
    }
}

/// Generic adapter: use the engine's MarketHours model as a SessionCalendar.
#[derive(Debug, Clone)]
pub struct HoursCalendar {
    pub hours: MarketHours,
}

impl HoursCalendar {
    /// Convenience constructor for a given exchange using your hours table.
    pub fn for_exchange(ex: Exchange) -> Self {
        Self {
            hours: hours_for_exchange(ex),
        }
    }
}

impl Default for HoursCalendar {
    fn default() -> Self {
        // Default to CME hours to preserve previous behavior
        Self::for_exchange(Exchange::CME)
    }
}

/// Simple CME risk: enforce daily loss stop; allow everything else.
#[derive(Debug, Clone, Copy)]
pub struct SimpleCmeRisk {
    pub max_daily_loss: Decimal, // in currency units
}

impl SimpleCmeRisk {
    pub fn new(max_daily_loss: Decimal) -> SimpleCmeRisk {
        Self { max_daily_loss }
    }
}

/// CME-ish fill model using touch-or-cross logic with optional stop triggers.
#[derive(Debug, Clone, Copy, Default)]
pub struct CmeFillModel {
    pub cfg: FillConfig,
}
