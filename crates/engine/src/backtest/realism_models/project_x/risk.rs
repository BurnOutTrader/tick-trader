use crate::backtest::models::{
    Fill, Money, PostFillAction, ProposedPortfolioChange, RiskCtx, RiskDecision,
};
use crate::backtest::realism_models::traits::RiskModel;
use rust_decimal::Decimal;
use tt_types::wire::{PlaceOrder, ReplaceOrder};

/// Simple CME risk: enforce daily loss stop; allow everything else.
#[derive(Debug, Clone, Copy)]
pub struct SimpleRisk {
    pub max_daily_loss: Decimal, // in currency units
}

impl SimpleRisk {
    pub fn new(max_daily_loss: Decimal) -> SimpleRisk {
        Self { max_daily_loss }
    }
}
impl RiskModel for SimpleRisk {
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
