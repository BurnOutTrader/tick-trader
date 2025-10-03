use crate::securities::market_hours::{hours_for_exchange, MarketHours};
use crate::securities::symbols::{Currency, Exchange, Instrument, SecurityType};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use ahash::AHashMap;
use lazy_static::lazy_static;
use crate::base_data::Price;
use crate::securities::futures_helpers::{activation_ns_default, extract_root, parse_expiry_from_instrument};
use rust_decimal::dec;

/// Handle that composes facts (props), calendar (hours), models, and small runtime cache.
#[derive(Clone)]
pub struct FuturesContract {
    pub root: String,
    pub instrument: Instrument,
    pub security_type: SecurityType,
    pub exchange: Exchange,

    /// Trading calendar for this instrument
    pub hours: MarketHours,

/*    /// Pluggable models (always present; defaulted by an initializer/factory)
    pub fee_model: Arc<dyn FeeModel>,
    pub slippage_model: Arc<dyn SlippageModel>,
    pub fill_model: Arc<dyn FillModel>,
    pub bp_model: Arc<dyn BuyingPowerModel>,
    pub vol_model: Arc<dyn VolatilityModel>,
    pub settlement_model: Arc<dyn SettlementModel>,*/

    pub tick_size: Decimal,
    pub value_per_tick: Decimal,
    pub decimal_accuracy: u32,
    pub quote_ccy: Currency,
    pub activation_date: NaiveDate,
    pub expiration_date: NaiveDate,
    pub is_continuous: bool,
}

impl FuturesContract {
    pub fn from_root_with_default_models(instrument: &Instrument, exchange: Exchange, security_type: SecurityType) -> Option<Self> {
        let root = extract_root(instrument);
        let market_hours = hours_for_exchange(exchange);
        let is_continuous = root == instrument.to_string();
        let symbol_info = SYMBOL_INFO_MAP.get(root.as_str())?;
        let expiry = parse_expiry_from_instrument(&instrument)?;
        let activation_ns  = activation_ns_default(&root, instrument)?;
        let activation = DateTime::<Utc>::from_timestamp_nanos(activation_ns as i64);
        Some(Self {
            root,
            instrument: instrument.clone(),
            security_type,
            exchange,
            hours: market_hours,
            tick_size: symbol_info.tick_size,
            value_per_tick: symbol_info.value_per_tick,
            decimal_accuracy: symbol_info.decimal_accuracy,
            quote_ccy: Currency::USD,
            activation_date: activation.date_naive(),
            expiration_date: expiry,
            is_continuous,
        })
    }
}


#[derive(Clone, Debug, PartialEq, PartialOrd,)]
pub struct SymbolInfo {
    pub symbol_name: String,
    pub value_per_tick: Price,
    pub tick_size: Price,
    pub decimal_accuracy: u32
}

impl SymbolInfo {
    pub fn new(
        symbol_name: String,
        value_per_tick: Price,
        tick_size: Price,
        decimal_accuracy: u32
    ) -> Self {
        Self {
            symbol_name,
            value_per_tick,
            tick_size,
            decimal_accuracy,
        }
    }
}

lazy_static! {
    static ref SYMBOL_INFO_MAP: AHashMap<&'static str, SymbolInfo> = {
        let mut map = AHashMap::new();

        macro_rules! add_symbol {
            ($symbol:expr, $value_per_tick:expr, $tick_size:expr, $accuracy:expr) => {
                map.insert($symbol, SymbolInfo {
                    symbol_name: $symbol.to_string(),
                    value_per_tick: dec!($value_per_tick),
                    tick_size: dec!($tick_size),
                    decimal_accuracy: $accuracy,
                });
            };
        }

        // Grains
        add_symbol!("XC", 1.25, 0.25, 2);  // 5.0/4
        add_symbol!("XK", 1.25, 0.25, 2);  // 5.0/4
        add_symbol!("XW", 1.25, 0.25, 2);  // 5.0/4
        add_symbol!("YM", 5.0, 1.0, 0);    // Already correct as 1 tick = 1 point
        add_symbol!("ZB", 1.953125, 0.0625, 4);  // 31.25/16
        add_symbol!("ZC", 12.5, 0.25, 2);  // 50.0/4
        add_symbol!("ZF", 0.244140625, 0.0078125, 6);  // 31.25/128
        add_symbol!("ZL", 6.0, 0.01, 4);   // 600.0/100
        add_symbol!("ZM", 10.0, 0.1, 1);   // 100.0/10
        add_symbol!("ZN", 0.48828125, 0.015625, 5);  // 31.25/64
        add_symbol!("ZO", 12.5, 0.25, 2);  // 50.0/4
        add_symbol!("ZR", 0.25, 0.005, 4); // 50.0/200
        add_symbol!("ZS", 12.5, 0.25, 2);  // 50.0/4
        add_symbol!("ZT", 0.244140625, 0.0078125, 6);  // 31.25/128
        add_symbol!("ZW", 12.5, 0.25, 2);  // 50.0/4

        // CME Futures
        add_symbol!("6A", 1.0, 0.0001, 4);   // 10.0/10
        add_symbol!("6B", 0.625, 0.0001, 4); // 6.25/10
        add_symbol!("6C", 1.0, 0.0001, 4);   // 10.0/10
        add_symbol!("6E", 1.25, 0.0001, 4);  // 12.5/10
        add_symbol!("6J", 0.0125, 0.000001, 6);  // 12.5/1000
        add_symbol!("6M", 0.1, 0.00001, 5);  // 10.0/100
        add_symbol!("6N", 1.0, 0.0001, 4);   // 10.0/10
        add_symbol!("6S", 1.25, 0.0001, 4);  // 12.5/10
        add_symbol!("E7", 0.625, 0.0001, 4); // 6.25/10
        add_symbol!("EMD", 2.5, 0.05, 2);    // 50.0/20
        add_symbol!("ES", 12.5, 0.25, 2);    // 50.0/4
        add_symbol!("GE", 0.0625, 0.0025, 4); // 25.0/400
        add_symbol!("GF", 1.25, 0.025, 3);   // 50.0/40
        add_symbol!("HE", 0.1, 0.0025, 4);   // 40.0/400
        add_symbol!("J7", 0.00625, 0.000001, 6); // 6.25/1000
        add_symbol!("LE", 1.0, 0.025, 3);    // 40.0/40
        add_symbol!("NQ", 5.0, 0.25, 2);     // 20.0/4
        add_symbol!("RF", 1.25, 0.0001, 4);  // 12.5/10
        add_symbol!("SP", 25.0, 0.1, 2);     // 250.0/10

        // COMEX Futures
        add_symbol!("GC", 10.0, 0.1, 2);     // 100.0/10
        add_symbol!("HG", 0.0125, 0.0005, 4); // 25.0/2000
        add_symbol!("QI", 0.03125, 0.0025, 4); // 12.5/400
        add_symbol!("SI", 0.125, 0.005, 3);  // 25.0/200

        // NYMEX Futures
        add_symbol!("CL", 10.0, 0.01, 2);    // 1000.0/100
        add_symbol!("HO", 4.2, 0.0001, 4);   // 42000.0/10000
        add_symbol!("NG", 10.0, 0.001, 3);   // 10000.0/1000
        add_symbol!("PA", 5.0, 0.05, 2);     // 100.0/20
        add_symbol!("PL", 5.0, 0.1, 2);      // 50.0/10
        add_symbol!("QM", 5.0, 0.01, 2);     // 500.0/100
        add_symbol!("RB", 4.2, 0.0001, 4);   // 42000.0/10000

        // Micro Futures
        add_symbol!("MES", 1.25, 0.25, 2);   // 5.0/4
        add_symbol!("MNQ", 0.50, 0.25, 2);   // 2.0/4
        add_symbol!("M2K", 0.50, 0.1, 2);    // 5.0/10
        add_symbol!("MYM", 0.50, 1.0, 0);    // Already correct
        add_symbol!("MGC", 1.0, 0.1, 2);     // 10.0/10
        add_symbol!("SIL", 0.0125, 0.005, 3); // 2.5/200
        add_symbol!("MCL", 1.0, 0.01, 2);    // 100.0/100
        add_symbol!("MBT", 1.25, 0.25, 2);   // 5.0/4
        add_symbol!("M6A", 0.1, 0.0001, 4);  // 1.0/10
        add_symbol!("M6B", 0.0625, 0.0001, 4); // 0.625/10
        add_symbol!("M6E", 0.125, 0.0001, 4);  // 1.25/10
        add_symbol!("MJY", 0.00125, 0.000001, 6);  // 1.25/1000

        map
    };
}

