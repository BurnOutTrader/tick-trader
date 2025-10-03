use ahash::AHashMap;
use crate::reality_models::models::ModelSet;
use crate::reality_models::traits::{
    BuyingPowerModel, FeeModel, FillModel, SettlementModel, SlippageModel, VolatilityModel,
};
use crate::securities::symbols::Currency;
use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use rust_decimal::{dec, Decimal};
use crate::securities::security::FuturesContract;

// One global default you can reference (or build all entries explicitly)
lazy_static::lazy_static! {
    static ref DEFAULT_FUTURES: ModelSet = ModelSet::default();
}

//Default models
pub struct DefaultFeeModel;

impl FeeModel for DefaultFeeModel {
    fn estimate(&self, root: &str, quantity: Decimal) -> Decimal {
        if let Some(info) = COMMISSION_PER_CONTRACT.get(root) {
            return info.per_side * quantity
        }
        dec!(0.00)
    }
}

#[derive(Clone, Debug)]
pub struct CommissionInfo {
    pub per_side: Decimal,
    #[allow(dead_code)]
    pub currency: String,
}

lazy_static! {
    static ref COMMISSION_PER_CONTRACT: AHashMap<&'static str, CommissionInfo> = {
        let mut map = AHashMap::new();

        // Stock Index Futures (Exchange Fee: 0.50 USD)
        map.insert("YM",  CommissionInfo { per_side: dec!(1.90) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZDJ", CommissionInfo { per_side: dec!(1.54) + dec!(0.50), currency: "USD".to_string() });
        map.insert("M2K", CommissionInfo { per_side: dec!(0.50) + dec!(0.20), currency: "USD".to_string() });
        map.insert("MES", CommissionInfo { per_side: dec!(0.50) + dec!(0.20), currency: "USD".to_string() });
        map.insert("FDXS", CommissionInfo { per_side: dec!(0.27) + dec!(0.10), currency: "EUR".to_string() });
        map.insert("MYM", CommissionInfo { per_side: dec!(0.50) + dec!(0.20), currency: "USD".to_string() });
        map.insert("FSXE", CommissionInfo { per_side: dec!(0.23) + dec!(0.10), currency: "EUR".to_string() });
        map.insert("ES",  CommissionInfo { per_side: dec!(1.90) + dec!(0.60), currency: "USD".to_string() });
        map.insert("MJNK", CommissionInfo { per_side: dec!(39.90) + dec!(1.00), currency: "JPY".to_string() });
        map.insert("MNQ", CommissionInfo { per_side: dec!(0.50) + dec!(0.20), currency: "USD".to_string() });
        map.insert("NQ",  CommissionInfo { per_side: dec!(1.90) + dec!(0.60), currency: "USD".to_string() });
        map.insert("EMD", CommissionInfo { per_side: dec!(1.85) + dec!(0.55), currency: "USD".to_string() });
        map.insert("NKD", CommissionInfo { per_side: dec!(2.88) + dec!(0.50), currency: "USD".to_string() });
        map.insert("SP",  CommissionInfo { per_side: dec!(2.88) + dec!(0.60), currency: "USD".to_string() });
        map.insert("ZND", CommissionInfo { per_side: dec!(2.88) + dec!(0.60), currency: "USD".to_string() });
        map.insert("FXXP", CommissionInfo { per_side: dec!(0.90) + dec!(0.10), currency: "EUR".to_string() });
        map.insert("FDAX", CommissionInfo { per_side: dec!(1.77) + dec!(0.20), currency: "EUR".to_string() });
        map.insert("FESB", CommissionInfo { per_side: dec!(0.80) + dec!(0.10), currency: "EUR".to_string() });
        map.insert("FESX", CommissionInfo { per_side: dec!(0.90) + dec!(0.10), currency: "EUR".to_string() });
        map.insert("FDXM", CommissionInfo { per_side: dec!(0.76) + dec!(0.15), currency: "EUR".to_string() });
        map.insert("RTY", CommissionInfo { per_side: dec!(1.90) + dec!(0.60), currency: "USD".to_string() });
        map.insert("VX",  CommissionInfo { per_side: dec!(2.27) + dec!(0.75), currency: "USD".to_string() });
        map.insert("FVS", CommissionInfo { per_side: dec!(0.72) + dec!(0.10), currency: "EUR".to_string() });
        map.insert("VXM", CommissionInfo { per_side: dec!(0.35) + dec!(0.20), currency: "USD".to_string() });

        // Currency Futures (Exchange Fee: 0.30 USD)
        map.insert("6Z", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("RMB", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6M", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("TRE", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6L", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6N", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("PLN", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("SEK", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("TRY", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6A", CommissionInfo { per_side: dec!(2.12) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6B", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6C", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6E", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6J", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("6S", CommissionInfo { per_side: dec!(2.13) + dec!(0.30), currency: "USD".to_string() });
        map.insert("E7", CommissionInfo { per_side: dec!(1.38) + dec!(0.20), currency: "USD".to_string() });
        map.insert("J7", CommissionInfo { per_side: dec!(1.38) + dec!(0.20), currency: "USD".to_string() });
        map.insert("M6A", CommissionInfo { per_side: dec!(0.39) + dec!(0.15), currency: "USD".to_string() });
        map.insert("M6B", CommissionInfo { per_side: dec!(0.39) + dec!(0.15), currency: "USD".to_string() });

        // Energy Futures (Exchange Fee: 0.80 USD)
        map.insert("CL",  CommissionInfo { per_side: dec!(2.13) + dec!(0.80), currency: "USD".to_string() });
        map.insert("MCL", CommissionInfo { per_side: dec!(0.65) + dec!(0.30), currency: "USD".to_string() });
        map.insert("MNG", CommissionInfo { per_side: dec!(0.75) + dec!(0.30), currency: "USD".to_string() });

        // Metals Futures (Exchange Fee: 0.40 USD)
        map.insert("GC",  CommissionInfo { per_side: dec!(2.12) + dec!(0.40), currency: "USD".to_string() });
        map.insert("MGC", CommissionInfo { per_side: dec!(0.65) + dec!(0.20), currency: "USD".to_string() });

        // Metals Futures
        map.insert("QC",  CommissionInfo { per_side: dec!(1.54) + dec!(0.40), currency: "USD".to_string() });
        map.insert("QI",  CommissionInfo { per_side: dec!(1.54) + dec!(0.40), currency: "USD".to_string() });
        map.insert("QO",  CommissionInfo { per_side: dec!(1.54) + dec!(0.40), currency: "USD".to_string() });
        map.insert("SI",  CommissionInfo { per_side: dec!(2.12) + dec!(0.50), currency: "USD".to_string() });
        map.insert("SIL", CommissionInfo { per_side: dec!(1.15) + dec!(0.35), currency: "USD".to_string() });
        map.insert("PA",  CommissionInfo { per_side: dec!(2.07) + dec!(0.50), currency: "USD".to_string() });
        map.insert("PL",  CommissionInfo { per_side: dec!(2.12) + dec!(0.50), currency: "USD".to_string() });

        // Financial Futures
        map.insert("SR",   CommissionInfo { per_side: dec!(1.13) + dec!(0.25), currency: "USD".to_string() });
        map.insert("10YY", CommissionInfo { per_side: dec!(0.45) + dec!(0.10), currency: "USD".to_string() });
        map.insert("30YY", CommissionInfo { per_side: dec!(0.45) + dec!(0.10), currency: "USD".to_string() });
        map.insert("2YY",  CommissionInfo { per_side: dec!(0.45) + dec!(0.10), currency: "USD".to_string() });
        map.insert("5YY",  CommissionInfo { per_side: dec!(0.45) + dec!(0.10), currency: "USD".to_string() });
        map.insert("UB",   CommissionInfo { per_side: dec!(1.47) + dec!(0.30), currency: "USD".to_string() });
        map.insert("MWN",  CommissionInfo { per_side: dec!(0.45) + dec!(0.10), currency: "USD".to_string() });
        map.insert("JGB",  CommissionInfo { per_side: dec!(499.90) + dec!(10.00), currency: "JPY".to_string() });
        map.insert("MTN",  CommissionInfo { per_side: dec!(0.45) + dec!(0.10), currency: "USD".to_string() });
        map.insert("Z3N",  CommissionInfo { per_side: dec!(1.17) + dec!(0.25), currency: "USD".to_string() });
        map.insert("ZB",   CommissionInfo { per_side: dec!(1.39) + dec!(0.30), currency: "USD".to_string() });
        map.insert("ZF",   CommissionInfo { per_side: dec!(1.17) + dec!(0.25), currency: "USD".to_string() });
        map.insert("ZN",   CommissionInfo { per_side: dec!(1.32) + dec!(0.30), currency: "USD".to_string() });
        map.insert("TN",   CommissionInfo { per_side: dec!(1.32) + dec!(0.30), currency: "USD".to_string() });
        map.insert("ZQ",   CommissionInfo { per_side: dec!(1.49) + dec!(0.35), currency: "USD".to_string() });
        map.insert("ZT",   CommissionInfo { per_side: dec!(1.17) + dec!(0.25), currency: "USD".to_string() });
        map.insert("GE",   CommissionInfo { per_side: dec!(1.72) + dec!(0.35), currency: "USD".to_string() });
        map.insert("GLB",  CommissionInfo { per_side: dec!(1.72) + dec!(0.35), currency: "USD".to_string() });
        map.insert("FGBL", CommissionInfo { per_side: dec!(0.77) + dec!(0.15), currency: "EUR".to_string() });
        map.insert("FGBM", CommissionInfo { per_side: dec!(0.77) + dec!(0.15), currency: "EUR".to_string() });
        map.insert("FGBS", CommissionInfo { per_side: dec!(0.77) + dec!(0.15), currency: "EUR".to_string() });
        map.insert("FBTP", CommissionInfo { per_side: dec!(0.74) + dec!(0.15), currency: "EUR".to_string() });
        map.insert("FOAT", CommissionInfo { per_side: dec!(0.77) + dec!(0.15), currency: "EUR".to_string() });
        map.insert("FGBX", CommissionInfo { per_side: dec!(0.77) + dec!(0.15), currency: "EUR".to_string() });
        map.insert("FBTS", CommissionInfo { per_side: dec!(0.77) + dec!(0.15), currency: "EUR".to_string() });

        // Grains Futures (Exchange Fee: 0.50 USD)
        map.insert("XC", CommissionInfo { per_side: dec!(1.55) + dec!(0.50), currency: "USD".to_string() });
        map.insert("XK", CommissionInfo { per_side: dec!(1.56) + dec!(0.50), currency: "USD".to_string() });
        map.insert("XW", CommissionInfo { per_side: dec!(1.56) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZC", CommissionInfo { per_side: dec!(2.62) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZE", CommissionInfo { per_side: dec!(2.62) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZL", CommissionInfo { per_side: dec!(2.62) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZM", CommissionInfo { per_side: dec!(2.62) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZO", CommissionInfo { per_side: dec!(2.62) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZR", CommissionInfo { per_side: dec!(2.62) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZS", CommissionInfo { per_side: dec!(2.62) + dec!(0.50), currency: "USD".to_string() });
        map.insert("ZW", CommissionInfo { per_side: dec!(2.62) + dec!(0.50), currency: "USD".to_string() });

        // Softs Futures (Exchange Fee: 0.60 USD)
        map.insert("DA",  CommissionInfo { per_side: dec!(2.42) + dec!(0.60), currency: "USD".to_string() });
        map.insert("LBS", CommissionInfo { per_side: dec!(2.42) + dec!(0.60), currency: "USD".to_string() });
        map.insert("CC",  CommissionInfo { per_side: dec!(2.63) + dec!(0.60), currency: "USD".to_string() });
        map.insert("CT",  CommissionInfo { per_side: dec!(2.63) + dec!(0.60), currency: "USD".to_string() });
        map.insert("KC",  CommissionInfo { per_side: dec!(2.62) + dec!(0.60), currency: "USD".to_string() });
        map.insert("OJ",  CommissionInfo { per_side: dec!(2.63) + dec!(0.60), currency: "USD".to_string() });
        map.insert("SB",  CommissionInfo { per_side: dec!(2.63) + dec!(0.60), currency: "USD".to_string() });

        // Meats Futures (Exchange Fee: 0.55 USD)
        map.insert("GF", CommissionInfo { per_side: dec!(2.62) + dec!(0.55), currency: "USD".to_string() });
        map.insert("HE", CommissionInfo { per_side: dec!(2.62) + dec!(0.55), currency: "USD".to_string() });
        map.insert("LE", CommissionInfo { per_side: dec!(2.62) + dec!(0.55), currency: "USD".to_string() });

        map
    };
}

pub struct DefaultSlippage;

impl SlippageModel for DefaultSlippage {
    fn slip(&self, p: Decimal, _: Decimal) -> Decimal {
        p
    }
}

pub struct DefaultFill;

impl FillModel for DefaultFill {
    fn fill_price(&self, m: Decimal, o: Option<Decimal>, _: Decimal) -> Decimal {
        o.unwrap_or(m)
    }
}

pub struct DefaultFuturesBP;

impl BuyingPowerModel for DefaultFuturesBP {
    fn initial_margin(&self, px: Decimal, qty: Decimal, _: &FuturesContract) -> Decimal {
        (px * qty).abs() * dec!(0.05)
    }
    fn buying_power(&self, _p: Currency, _s: Currency) -> Decimal {
        dec!(1_000_000)
    }
}

pub struct DefaultVol;

impl VolatilityModel for DefaultVol {
    fn update(&self, _: Decimal, _: DateTime<Utc>) {}
}

pub struct DefaultSettle;

impl SettlementModel for DefaultSettle {
    fn settles_intraday(&self) -> bool {
        true
    }
}


lazy_static! {
    pub(crate) static ref INTRADAY_MARGINS: AHashMap<&'static str, Decimal> = {
        let mut map = AHashMap::new();
        map.insert("MES", dec!(40.00));
        map.insert("MNQ", dec!(100.00));
        map.insert("MYM", dec!(50.00));
        map.insert("M2K", dec!(50.00));
        map.insert("ES", dec!(400.00));
        map.insert("NQ", dec!(1000.00));
        map.insert("YM", dec!(500.00));
        map.insert("RTY", dec!(500.00));
        map.insert("EMD", dec!(3775.00));
        map.insert("NKD", dec!(2250.00));
        map.insert("6A", dec!(362.50));
        map.insert("6B", dec!(475.00));
        map.insert("6C", dec!(250.00));
        map.insert("6E", dec!(525.00));
        map.insert("6J", dec!(700.00));
        map.insert("6N", dec!(350.00));
        map.insert("6S", dec!(925.00));
        map.insert("E7", dec!(262.50));
        map.insert("J7", dec!(350.00));
        map.insert("M6A", dec!(36.25));
        map.insert("M6B", dec!(47.50));
        map.insert("M6E", dec!(52.50));
        map.insert("MJY", dec!(70.00));
        map.insert("CL", dec!(1650.00));
        map.insert("QM", dec!(825.00));
        map.insert("MCL", dec!(165.00));
        map.insert("NG", dec!(5500.00));
        map.insert("QG", dec!(1460.00));
        map.insert("RB", dec!(7900.00));
        map.insert("HO", dec!(8600.00));
        map.insert("GC", dec!(2075.00));
        map.insert("QO", dec!(1037.50));
        map.insert("MGC", dec!(207.50));
        map.insert("HG", dec!(1525.00));
        map.insert("QC", dec!(762.50));
        map.insert("SI", dec!(11000.00));
        map.insert("QI", dec!(5500.00));
        map.insert("SIL", dec!(2200.00));
        map.insert("PL", dec!(2800.00));
        map.insert("ZB", dec!(925.00));
        map.insert("ZF", dec!(350.00));
        map.insert("ZN", dec!(500.00));
        map.insert("ZT", dec!(262.50));
        map.insert("ZC", dec!(1300.00));
        map.insert("ZW", dec!(2000.00));
        map.insert("ZS", dec!(2400.00));
        map.insert("ZL", dec!(3150.00));
        map.insert("ZM", dec!(3100.00));
        map.insert("ZO", dec!(1400.00));
        map.insert("ZR", dec!(1575.00));
        map.insert("XC", dec!(260.00));
        map.insert("XW", dec!(400.00));
        map.insert("XK", dec!(480.00));
        map
    };

    static ref OVERNIGHT_MARGINS: AHashMap<&'static str, Decimal> = {
        let mut map = AHashMap::new();
        map.insert("MES", dec!(1460.00));
        map.insert("MNQ", dec!(2220.00));
        map.insert("MYM", dec!(1040.00));
        map.insert("M2K", dec!(760.00));
        map.insert("ES", dec!(14600.00));
        map.insert("NQ", dec!(22200.00));
        map.insert("YM", dec!(10400.00));
        map.insert("RTY", dec!(7600.00));
        map.insert("EMD", dec!(15100.00));
        map.insert("NKD", dec!(12000.00));
        map.insert("6A", dec!(1450.00));
        map.insert("6B", dec!(1900.00));
        map.insert("6C", dec!(1000.00));
        map.insert("6E", dec!(2100.00));
        map.insert("6J", dec!(2800.00));
        map.insert("6N", dec!(1450.00));
        map.insert("6S", dec!(3700.00));
        map.insert("E7", dec!(1050.00));
        map.insert("J7", dec!(1400.00));
        map.insert("M6A", dec!(145.00));
        map.insert("M6B", dec!(190.00));
        map.insert("M6E", dec!(210.00));
        map.insert("MJY", dec!(280.00));
        map.insert("CL", dec!(6600.00));
        map.insert("QM", dec!(3300.00));
        map.insert("MCL", dec!(660.00));
        map.insert("NG", dec!(5500.00));
        map.insert("QG", dec!(1460.00));
        map.insert("RB", dec!(7900.00));
        map.insert("HO", dec!(8600.00));
        map.insert("GC", dec!(10000.00));
        map.insert("QO", dec!(5000.00));
        map.insert("MGC", dec!(1000.00));
        map.insert("HG", dec!(6100.00));
        map.insert("QC", dec!(3050.00));
        map.insert("SI", dec!(11000.00));
        map.insert("QI", dec!(5500.00));
        map.insert("SIL", dec!(2200.00));
        map.insert("PL", dec!(2800.00));
        map.insert("ZB", dec!(3700.00));
        map.insert("ZF", dec!(1400.00));
        map.insert("ZN", dec!(2000.00));
        map.insert("ZT", dec!(1050.00));
        map.insert("ZC", dec!(1300.00));
        map.insert("ZW", dec!(2000.00));
        map.insert("ZS", dec!(2400.00));
        map.insert("ZL", dec!(3150.00));
        map.insert("ZM", dec!(3100.00));
        map.insert("ZO", dec!(1400.00));
        map.insert("ZR", dec!(1575.00));
        map.insert("XC", dec!(260.00));
        map.insert("XW", dec!(400.00));
        map.insert("XK", dec!(480.00));
        map
    };
}
