use crate::backtest::models::{FeeCtx, Fill, Money};
use crate::backtest::realism_models::traits::FeeModel;
use rust_decimal::Decimal;
use tt_types::accounts::events::PositionDelta;
use tt_types::securities::futures_helpers::extract_root;
use tt_types::wire::PlaceOrder;

/// Flat per-contract fees typical for CME+clearing+broker (approximate; configurable).
#[derive(Debug, Clone, Copy, Default)]
pub struct PxFlatFee;

impl PxFlatFee {
    pub fn new() -> Self {
        Self {}
    }
}

impl PxFlatFee {
    // Return a per-contract fee for a symbol string (USD). If the symbol is not
    // known we return None so the caller can fall back to the configured default.
    fn lookup_per_contract(sym: &str) -> Option<Decimal> {
        use Decimal as D;
        match sym {
            // CME Equity Futures
            "ES" => Some(D::new(280, 2)),
            "MES" => Some(D::new(74, 2)),
            "NQ" => Some(D::new(280, 2)),
            "MNQ" => Some(D::new(74, 2)),
            "RTY" => Some(D::new(280, 2)),
            "M2K" => Some(D::new(74, 2)),
            "NKD" => Some(D::new(434, 2)),
            "MBT" => Some(D::new(234, 2)),
            "MET" => Some(D::new(24, 2)),

            // CME NYMEX Futures
            "CL" => Some(D::new(304, 2)),
            "MCL" => Some(D::new(104, 2)),
            "QM" => Some(D::new(244, 2)),
            "PL" => Some(D::new(324, 2)),
            "QG" => Some(D::new(104, 2)),
            "RB" => Some(D::new(304, 2)),
            "HO" => Some(D::new(304, 2)),
            "NG" => Some(D::new(320, 2)),
            "MNG" => Some(D::new(124, 2)),

            // CME CBOT Equity Futures
            "YM" => Some(D::new(280, 2)),
            "MYM" => Some(D::new(74, 2)),

            // CME Foreign Exchange Futures
            "6A" => Some(D::new(324, 2)),
            "M6A" => Some(D::new(52, 2)),
            "6B" => Some(D::new(324, 2)),
            "6C" => Some(D::new(324, 2)),
            "6E" => Some(D::new(324, 2)),
            "M6E" => Some(D::new(52, 2)),
            "6J" => Some(D::new(324, 2)),
            "6S" => Some(D::new(324, 2)),
            "E7" => Some(D::new(174, 2)),
            "6M" => Some(D::new(324, 2)),
            "6N" => Some(D::new(324, 2)),
            "M6B" => Some(D::new(52, 2)),

            // CME CBOT Financial/Interest Rate Futures
            "ZT" => Some(D::new(134, 2)),
            "ZF" => Some(D::new(134, 2)),
            "ZN" => Some(D::new(160, 2)),
            "ZB" => Some(D::new(178, 2)),
            "UB" => Some(D::new(194, 2)),
            "TN" => Some(D::new(164, 2)),

            // CME COMEX Futures
            "GC" => Some(D::new(324, 2)),
            "MGC" => Some(D::new(124, 2)),
            "SI" => Some(D::new(324, 2)),
            "SIL" => Some(D::new(204, 2)),
            "HG" => Some(D::new(324, 2)),
            "MHG" => Some(D::new(124, 2)),

            // CME Agricultural Futures
            "HE" => Some(D::new(424, 2)),
            "LE" => Some(D::new(424, 2)),

            // CME CBOT Commodity Futures
            "ZC" => Some(D::new(430, 2)),
            "ZW" => Some(D::new(430, 2)),
            "ZS" => Some(D::new(430, 2)),
            "ZM" => Some(D::new(430, 2)),
            "ZL" => Some(D::new(430, 2)),

            _ => None,
        }
    }
}

impl FeeModel for PxFlatFee {
    fn on_new_order(&self, _ctx: &FeeCtx, _o: &PlaceOrder) -> Money {
        Money::ZERO
    }
    fn on_fill(&self, _ctx: &FeeCtx, f: &Fill) -> Money {
        let root = extract_root(&f.instrument);
        let per_contract = match PxFlatFee::lookup_per_contract(&root) {
            Some(d) => d,
            None => return Money::ZERO,
        };
        let contracts = Decimal::from(f.qty.abs());
        Money {
            amount: contracts * per_contract,
        }
    }
    fn on_cancel(&self, _ctx: &FeeCtx, _o: &PlaceOrder, _canceled_qty: i64) -> Money {
        Money::ZERO
    }
    fn settle(&self, _ctx: &FeeCtx, _pos: &PositionDelta) -> Money {
        Money::ZERO
    }
}
