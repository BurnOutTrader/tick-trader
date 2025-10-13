use crate::backtest::realism_models::traits::SlippageModel;
use rust_decimal::Decimal;
use tt_types::accounts::events::Side;

/// No additional slippage; execute at touch/reference.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoSlippage;
impl NoSlippage {
    pub fn new() -> Self {
        Self
    }
}

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
