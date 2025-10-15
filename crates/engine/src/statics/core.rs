use dashmap::DashMap;
use rust_decimal::Decimal;
use std::sync::LazyLock;
use tt_types::keys::SymbolKey;

pub static LAST_PRICE: LazyLock<DashMap<SymbolKey, Decimal>> = LazyLock::new(Default::default);
pub static LAST_BID: LazyLock<DashMap<SymbolKey, Decimal>> = LazyLock::new(Default::default);
pub static LAST_ASK: LazyLock<DashMap<SymbolKey, Decimal>> = LazyLock::new(Default::default);
