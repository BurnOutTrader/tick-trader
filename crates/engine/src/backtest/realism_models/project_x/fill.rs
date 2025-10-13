use crate::backtest::models::{FeeCtx, Fill, LimitPolicy, MarketPolicy, StopTrigger};
use crate::backtest::realism_models::traits::{
    FeeModel, FillModel, SessionCalendar, SlippageModel,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use tt_types::accounts::events::Side;
use tt_types::data::mbp10::BookLevels;
use tt_types::wire::{OrderType, PlaceOrder};
/// Fill configuration knobs
#[derive(Debug, Clone, Copy)]
pub struct FillConfig {
    pub limit_policy: LimitPolicy,
    pub market_policy: MarketPolicy,
    pub stop_trigger: StopTrigger,
    pub trailing_enabled: bool,
}

/// CME-ish fill model using touch-or-cross logic with optional stop triggers.
#[derive(Debug, Clone, Copy, Default)]
pub struct CmeFillModel {
    pub cfg: FillConfig,
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
impl FillModel for CmeFillModel {
    fn on_submit(&mut self, _now: DateTime<Utc>, _order: &mut PlaceOrder) {
        // Basic normalization: IOC and FOK cannot both be true.
        // if order.tif_ioc && order.tif_fok { order.tif_fok = false; }
    }

    fn match_book(
        &mut self,
        now: DateTime<Utc>,
        book: Option<&BookLevels>,
        last_price: Decimal,
        order: &mut PlaceOrder,
        slip: &mut dyn SlippageModel,
        cal: &dyn SessionCalendar,
        _fee_model: &dyn FeeModel,
    ) -> Vec<Fill> {
        let mut out = Vec::new();
        if !cal.is_open(&order.instrument, now) {
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
        let _fee_ctx = FeeCtx {
            sim_time: now,
            instrument: order.instrument.clone(),
        };
        // Limit-lock guard helper: no fills if venue is price-limit locked at candidate px
        let price_limit_ok =
            |px: Decimal| -> bool { !cal.is_limit_locked(&order.instrument, px, now) };
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
                if !price_limit_ok(px) {
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
                    instrument: order.instrument.clone(),
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
                            match self.cfg.market_policy {
                                MarketPolicy::BookWalk => {
                                    let mut always_ok = |_p: Decimal| true;
                                    let rem =
                                        walk_depth(&b.ask_px, &b.ask_sz, &mut always_ok, Side::Buy);
                                    if rem > 0 && out.is_empty() && price_limit_ok(last_price) {
                                        // No visible book or zero sizes: fall back to last price for a single fill
                                        let px_adj =
                                            slip.adjust(Side::Buy, last_price, spread, rem);
                                        out.push(Fill {
                                            instrument: order.instrument.clone(),
                                            qty: rem,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    }
                                }
                                MarketPolicy::AtTouch => {
                                    // Only consume level-1 ask, leave remainder (partial fill)
                                    if let (Some(px0), Some(sz0)) =
                                        (b.ask_px.first().cloned(), b.ask_sz.first().cloned())
                                        && price_limit_ok(px0)
                                    {
                                        let take = order.qty.min(sz0.to_i64().unwrap_or(0).max(0));
                                        if take > 0 {
                                            let px_adj = slip.adjust(Side::Buy, px0, spread, take);
                                            out.push(Fill {
                                                instrument: order.instrument.clone(),
                                                qty: take,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    }
                                }
                                MarketPolicy::AtMid => {
                                    if let (Some(bb), Some(ba)) = (best_bid, best_ask) {
                                        let mid = (bb + ba) / Decimal::from(2i32);
                                        let px_adj = slip.adjust(Side::Buy, mid, spread, order.qty);
                                        out.push(Fill {
                                            instrument: order.instrument.clone(),
                                            qty: order.qty,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    } else {
                                        // Fallback to last price
                                        let px_adj =
                                            slip.adjust(Side::Buy, last_price, spread, order.qty);
                                        out.push(Fill {
                                            instrument: order.instrument.clone(),
                                            qty: order.qty,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    }
                                }
                            }
                        } else if price_limit_ok(last_price) {
                            let px_adj = slip.adjust(Side::Buy, last_price, spread, order.qty);
                            out.push(Fill {
                                instrument: order.instrument.clone(),
                                qty: order.qty,
                                price: px_adj,
                                maker: false,
                            });
                        }
                    }
                    Side::Sell => {
                        if let Some(b) = book {
                            match self.cfg.market_policy {
                                MarketPolicy::BookWalk => {
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
                                            instrument: order.instrument.clone(),
                                            qty: rem,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    }
                                }
                                MarketPolicy::AtTouch => {
                                    if let (Some(px0), Some(sz0)) =
                                        (b.bid_px.first().cloned(), b.bid_sz.first().cloned())
                                        && price_limit_ok(px0)
                                    {
                                        let take = order.qty.min(sz0.to_i64().unwrap_or(0).max(0));
                                        if take > 0 {
                                            let px_adj = slip.adjust(Side::Sell, px0, spread, take);
                                            out.push(Fill {
                                                instrument: order.instrument.clone(),
                                                qty: take,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    }
                                }
                                MarketPolicy::AtMid => {
                                    if let (Some(bb), Some(ba)) = (best_bid, best_ask) {
                                        let mid = (bb + ba) / Decimal::from(2i32);
                                        let px_adj =
                                            slip.adjust(Side::Sell, mid, spread, order.qty);
                                        out.push(Fill {
                                            instrument: order.instrument.clone(),
                                            qty: order.qty,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    } else {
                                        let px_adj =
                                            slip.adjust(Side::Sell, last_price, spread, order.qty);
                                        out.push(Fill {
                                            instrument: order.instrument.clone(),
                                            qty: order.qty,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    }
                                }
                            }
                        } else if price_limit_ok(last_price) {
                            let px_adj = slip.adjust(Side::Sell, last_price, spread, order.qty);
                            out.push(Fill {
                                instrument: order.instrument.clone(),
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
                                match self.cfg.limit_policy {
                                    LimitPolicy::TouchOrCross => {
                                        let mut ok = |px: Decimal| px <= lim;
                                        let _rem =
                                            walk_depth(&b.ask_px, &b.ask_sz, &mut ok, Side::Buy);
                                    }
                                    LimitPolicy::AtOrBetter => {
                                        // Same price guard as TouchOrCross; explicit branch for clarity
                                        let mut ok = |px: Decimal| px <= lim;
                                        let _rem =
                                            walk_depth(&b.ask_px, &b.ask_sz, &mut ok, Side::Buy);
                                    }
                                    LimitPolicy::NextTickOnly => {
                                        // Only consume the touch if at or better; leave remainder working
                                        if let (Some(px0), Some(sz0)) =
                                            (b.ask_px.first().cloned(), b.ask_sz.first().cloned())
                                            && px0 <= lim
                                        {
                                            let take =
                                                order.qty.min(sz0.to_i64().unwrap_or(0).max(0));
                                            if take > 0 {
                                                let px_adj =
                                                    slip.adjust(Side::Buy, px0, spread, take);
                                                out.push(Fill {
                                                    instrument: order.instrument.clone(),
                                                    qty: take,
                                                    price: px_adj,
                                                    maker: false,
                                                });
                                            }
                                        }
                                    }
                                }
                            } else if lim >= last_price && price_limit_ok(last_price) {
                                let px_adj = slip.adjust(Side::Buy, last_price, spread, order.qty);
                                out.push(Fill {
                                    instrument: order.instrument.clone(),
                                    qty: order.qty,
                                    price: px_adj,
                                    maker: false,
                                });
                            }
                        }
                        Side::Sell => {
                            if let Some(b) = book {
                                match self.cfg.limit_policy {
                                    LimitPolicy::TouchOrCross => {
                                        let mut ok = |px: Decimal| px >= lim;
                                        let _rem =
                                            walk_depth(&b.bid_px, &b.bid_sz, &mut ok, Side::Sell);
                                    }
                                    LimitPolicy::AtOrBetter => {
                                        let mut ok = |px: Decimal| px >= lim;
                                        let _rem =
                                            walk_depth(&b.bid_px, &b.bid_sz, &mut ok, Side::Sell);
                                    }
                                    LimitPolicy::NextTickOnly => {
                                        if let (Some(px0), Some(sz0)) =
                                            (b.bid_px.first().cloned(), b.bid_sz.first().cloned())
                                            && px0 >= lim
                                        {
                                            let take =
                                                order.qty.min(sz0.to_i64().unwrap_or(0).max(0));
                                            if take > 0 {
                                                let px_adj =
                                                    slip.adjust(Side::Sell, px0, spread, take);
                                                out.push(Fill {
                                                    instrument: order.instrument.clone(),
                                                    qty: take,
                                                    price: px_adj,
                                                    maker: false,
                                                });
                                            }
                                        }
                                    }
                                }
                            } else if lim <= last_price && price_limit_ok(last_price) {
                                let px_adj = slip.adjust(Side::Sell, last_price, spread, order.qty);
                                out.push(Fill {
                                    instrument: order.instrument.clone(),
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
                                                    instrument: order.instrument.clone(),
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
                                                instrument: order.instrument.clone(),
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
                                                    instrument: order.instrument.clone(),
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
                                                instrument: order.instrument.clone(),
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
                                                instrument: order.instrument.clone(),
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
                                                instrument: order.instrument.clone(),
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
                                {
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
                                                instrument: order.instrument.clone(),
                                                qty: rem,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    } else {
                                        let px_adj =
                                            slip.adjust(Side::Buy, last_price, spread, order.qty);
                                        out.push(Fill {
                                            instrument: order.instrument.clone(),
                                            qty: order.qty,
                                            price: px_adj,
                                            maker: false,
                                        });
                                    }
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
                                                instrument: order.instrument.clone(),
                                                qty: rem,
                                                price: px_adj,
                                                maker: false,
                                            });
                                        }
                                    } else {
                                        let px_adj =
                                            slip.adjust(Side::Sell, last_price, spread, order.qty);
                                        out.push(Fill {
                                            instrument: order.instrument.clone(),
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
