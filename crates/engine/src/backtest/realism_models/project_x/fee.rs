use crate::backtest::models::{FeeCtx, Fill, Money};
use crate::backtest::realism_models::traits::FeeModel;
use rust_decimal::Decimal;
use tt_types::accounts::events::PositionDelta;
use tt_types::wire::PlaceOrder;

/// Flat per-contract fees typical for CME+clearing+broker (approximate; configurable).
#[derive(Debug, Clone, Copy)]
pub struct PxFlatFee {
    pub per_contract: Decimal, // e.g., 2.50 USD per contract per side
}

impl Default for PxFlatFee {
    fn default() -> Self {
        Self {
            per_contract: Decimal::new(250, 2),
        }
    } // 2.50
}

impl FeeModel for PxFlatFee {
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
