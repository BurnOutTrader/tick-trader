use crate::securities::symbols::Currency;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use crate::securities::security::FuturesContract;
// Somewhere common (e.g., engine_multi or a small models.rs)

pub trait FeeModel: Send + Sync {
    fn estimate(&self, root: &str, price: Decimal) -> Decimal;
}
pub trait SlippageModel: Send + Sync {
    fn slip(&self, price: Decimal, qty: Decimal) -> Decimal;
}
pub trait FillModel: Send + Sync {
    fn fill_price(&self, mkt_px: Decimal, order_px: Option<Decimal>, qty: Decimal) -> Decimal;
}
pub trait BuyingPowerModel: Send + Sync {
    fn initial_margin(&self, px: Decimal, qty: Decimal, security: &FuturesContract) -> Decimal;
    fn buying_power(&self, portfolio_ccy: Currency, symbol_ccy: Currency) -> Decimal;
}
pub trait VolatilityModel: Send + Sync {
    fn update(&self, last: Decimal, t: DateTime<Utc>);
}
pub trait SettlementModel: Send + Sync {
    fn settles_intraday(&self) -> bool;
}
