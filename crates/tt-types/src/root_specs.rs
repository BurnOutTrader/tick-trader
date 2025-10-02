// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use ahash::AHashMap;
use nautilus_model::{
    enums::AssetClass,
    identifiers::InstrumentId,
    types::{Currency, Price, Quantity},
};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use ustr::{Ustr, ustr};

/// Make a Quantity from a numeric value using the value's natural decimal scale.
/// - integers => precision 0
/// - 0.5     => precision 1
/// - 12.500  => precision 3 (but we trim trailing zeros below)
#[inline]
pub fn qty_from_value(v: f64) -> Quantity {
    // Convert via Decimal to avoid fp artifacts, then trim trailing zeros.
    let mut d = Decimal::from_f64(v).expect("finite multiplier");
    d = d.normalize(); // strip trailing zeros in the scale
    let p = d.scale() as u8;
    Quantity::from_decimal(d, p).expect("valid Quantity")
}

/// Compute the contract point-value (multiplier) from tick specs safely:
/// multiplier = tick_value / tick_size
#[inline]
pub fn multiplier_from_tick(tick_value: f64, tick_size: f64) -> Quantity {
    let mut d_tv = Decimal::from_f64(tick_value).expect("finite tick_value");
    let mut d_ts = Decimal::from_f64(tick_size).expect("finite tick_size");

    // Normalize both so the division result has the minimal necessary scale.
    d_tv = d_tv.normalize();
    d_ts = d_ts.normalize();

    let mut m = (d_tv / d_ts).normalize(); // exact for terminating decimals
    let p = m.scale() as u8;

    Quantity::from_decimal(m, p).expect("valid multiplier")
}

#[inline]
pub fn multiplier_from_price(price_increment: Price, tick_value: f64) -> Quantity {
    // multiplier = tick_value / price_increment.value()
    let mut m = Decimal::from_f64(tick_value).unwrap()
        / Decimal::from_f64(price_increment.as_f64()).unwrap();
    m = m.normalize();
    let p = m.scale() as u8;
    Quantity::from_decimal(m, p).unwrap()
}

// RootSpec used to build a FuturesContract
#[derive(Clone, Debug)]
pub struct RootSpec {
    pub asset_class: AssetClass,
    pub currency: Currency,
    pub exchange: Option<Ustr>, // ISO 10383 MIC
    pub underlying: Ustr,       // e.g., "EUR/USD", "WTI Crude", "S&P 500"
    pub price_precision: u8,
    pub price_increment: Price, // tick size with matching precision
    #[allow(unused_variables)]
    pub size_precision: u8,
    pub size_increment: Quantity, // typically 1 contract
    pub multiplier: Quantity,     // $ per 1.0 price move (tick_value / tick_size)
    #[allow(unused_variables)]
    pub lot_size: Quantity, // usually 1 contract
    // reference (handy for checks/logs)
    #[allow(unused_variables)]
    pub tick_size: f64,
    #[allow(unused_variables)]
    pub tick_value: f64,
}

pub fn hardcoded_roots() -> AHashMap<&'static str, RootSpec> {
    let mut m = AHashMap::new();

    // ===== FX (CME / XCME) =====
    m.insert(
        "6B",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("GBP/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.0001,
            tick_value: 6.25,
            price_increment: Price::new(0.0001, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(6.25 / 0.0001),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "6C",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("CAD/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.00005,
            tick_value: 5.0,
            price_increment: Price::new(0.00005, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 0.00005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "6A",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("AUD/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.00005,
            tick_value: 5.0,
            price_increment: Price::new(0.00005, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 0.00005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "6E",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("EUR/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.00005,
            tick_value: 6.25,
            price_increment: Price::new(0.00005, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(6.25 / 0.00005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "6J",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("JPY/USD"),
            price_precision: 7,
            size_precision: 0,
            tick_size: 0.0000005,
            tick_value: 6.25,
            price_increment: Price::new(0.0000005, 7),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(6.25 / 0.0000005), // 12_500_000
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "6M",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("MXN/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.00001,
            tick_value: 5.0,
            price_increment: Price::new(0.00001, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 0.00001),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "6N",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("NZD/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.00005,
            tick_value: 5.0,
            price_increment: Price::new(0.00005, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 0.00005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "6S",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("CHF/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.00005,
            tick_value: 6.25,
            price_increment: Price::new(0.00005, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(6.25 / 0.00005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "E7",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("EUR/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.0001,
            tick_value: 6.25,
            price_increment: Price::new(0.0001, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(6.25 / 0.0001),
            lot_size: Quantity::from(1u32),
        },
    );

    // FX micros
    m.insert(
        "M6A",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("AUD/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.0001,
            tick_value: 1.0,
            price_increment: Price::new(0.0001, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(1.0 / 0.0001),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "M6B",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("GBP/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.0001,
            tick_value: 0.625,
            price_increment: Price::new(0.0001, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(0.625 / 0.0001),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "M6E",
        RootSpec {
            asset_class: AssetClass::FX,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("EUR/USD"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.0001,
            tick_value: 1.25,
            price_increment: Price::new(0.0001, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(1.25 / 0.0001),
            lot_size: Quantity::from(1u32),
        },
    );

    // ===== Equity Indices =====
    m.insert(
        "ES",
        RootSpec {
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("S&P 500"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.25,
            tick_value: 12.5,
            price_increment: Price::new(0.25, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(12.5 / 0.25),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "NQ",
        RootSpec {
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("NASDAQ-100"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.25,
            tick_value: 5.0,
            price_increment: Price::new(0.25, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 0.25),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "RTY",
        RootSpec {
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("Russell 2000"),
            price_precision: 1,
            size_precision: 0,
            tick_size: 0.1,
            tick_value: 5.0,
            price_increment: Price::new(0.1, 1),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 0.1),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "YM",
        RootSpec {
            // NOTE: fractional point value -> needs qty_from_value to avoid rounding
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("Dow Jones Industrial Average"),
            price_precision: 0,
            size_precision: 0,
            tick_size: 1.0,
            tick_value: 5.0,
            price_increment: Price::new(1.0, 0),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 1.0),
            lot_size: Quantity::from(1u32),
        },
    );

    // Equity index micros
    m.insert(
        "MNQ",
        RootSpec {
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("NASDAQ-100"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.25,
            tick_value: 0.5,
            price_increment: Price::new(0.25, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(0.5 / 0.25),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "MES",
        RootSpec {
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("S&P 500"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.25,
            tick_value: 1.25,
            price_increment: Price::new(0.25, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(1.25 / 0.25),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "MYM",
        RootSpec {
            // NOTE: fractional 0.5 point value
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("Dow Jones Industrial Average"),
            price_precision: 0,
            size_precision: 0,
            tick_size: 1.0,
            tick_value: 0.5,
            price_increment: Price::new(1.0, 0),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(0.5 / 1.0),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "M2K",
        RootSpec {
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("Russell 2000"),
            price_precision: 1,
            size_precision: 0,
            tick_size: 0.1,
            tick_value: 0.5,
            price_increment: Price::new(0.1, 1),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(0.5 / 0.1),
            lot_size: Quantity::from(1u32),
        },
    );

    // ===== Metals =====
    m.insert(
        "GC",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCEC")),
            underlying: ustr("Gold"),
            price_precision: 1,
            size_precision: 0,
            tick_size: 0.1,
            tick_value: 10.0,
            price_increment: Price::new(0.1, 1),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(10.0 / 0.1),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "MGC",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCEC")),
            underlying: ustr("Gold"),
            price_precision: 1,
            size_precision: 0,
            tick_size: 0.1,
            tick_value: 1.0,
            price_increment: Price::new(0.1, 1),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(1.0 / 0.1),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "SI",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCEC")),
            underlying: ustr("Silver"),
            price_precision: 3,
            size_precision: 0,
            tick_size: 0.005,
            tick_value: 25.0,
            price_increment: Price::new(0.005, 3),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(25.0 / 0.005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "SIL",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCEC")),
            underlying: ustr("Silver"),
            price_precision: 3,
            size_precision: 0,
            tick_size: 0.005,
            tick_value: 5.0,
            price_increment: Price::new(0.005, 3),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 0.005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "HG",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCEC")),
            underlying: ustr("Copper"),
            price_precision: 4,
            size_precision: 0,
            tick_size: 0.0005,
            tick_value: 12.5,
            price_increment: Price::new(0.0005, 4),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(12.5 / 0.0005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "MHG",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCEC")),
            underlying: ustr("Copper"),
            price_precision: 4,
            size_precision: 0,
            tick_size: 0.0005,
            tick_value: 1.25,
            price_increment: Price::new(0.0005, 4),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(1.25 / 0.0005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "PL",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("Platinum"),
            price_precision: 1,
            size_precision: 0,
            tick_size: 0.1,
            tick_value: 5.0,
            price_increment: Price::new(0.1, 1),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(5.0 / 0.1),
            lot_size: Quantity::from(1u32),
        },
    );

    // ===== Energies (NYMEX / XNYM) =====
    m.insert(
        "CL",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("WTI Crude Oil"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.01,
            tick_value: 10.0,
            price_increment: Price::new(0.01, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(10.0 / 0.01),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "QM",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("Crude Oil (E-mini)"),
            price_precision: 3,
            size_precision: 0,
            tick_size: 0.025,
            tick_value: 12.5,
            price_increment: Price::new(0.025, 3),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(12.5 / 0.025),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "MCL",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("WTI Crude Oil"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.01,
            tick_value: 1.0,
            price_increment: Price::new(0.01, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(1.0 / 0.01),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "NG",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("Henry Hub Natural Gas"),
            price_precision: 3,
            size_precision: 0,
            tick_size: 0.001,
            tick_value: 10.0,
            price_increment: Price::new(0.001, 3),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(10.0 / 0.001),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "QG",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("Natural Gas (E-mini)"),
            price_precision: 3,
            size_precision: 0,
            tick_size: 0.005,
            tick_value: 12.5,
            price_increment: Price::new(0.005, 3),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(12.5 / 0.005),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "MNG",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("Henry Hub Natural Gas"),
            price_precision: 3,
            size_precision: 0,
            tick_size: 0.001,
            tick_value: 1.0,
            price_increment: Price::new(0.001, 3),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(1.0 / 0.001),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "RB",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("RBOB Gasoline"),
            price_precision: 4,
            size_precision: 0,
            tick_size: 0.0001,
            tick_value: 4.2,
            price_increment: Price::new(0.0001, 4),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(4.2 / 0.0001),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "HO",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XNYM")),
            underlying: ustr("NY Harbor ULSD"),
            price_precision: 4,
            size_precision: 0,
            tick_size: 0.0001,
            tick_value: 4.2,
            price_increment: Price::new(0.0001, 4),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(4.2 / 0.0001),
            lot_size: Quantity::from(1u32),
        },
    );

    // ===== Treasuries (CBOT / XCBT) =====
    // ZF: 1/128 = 0.0078125 (needs 7 dp to avoid rounding)
    m.insert(
        "ZF",
        RootSpec {
            asset_class: AssetClass::Debt,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("US 5-Year Treasury Notes"),
            price_precision: 7,
            size_precision: 0,
            tick_size: 0.0078125,
            tick_value: 7.8125,
            price_increment: Price::new(0.0078125, 7),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(7.8125 / 0.0078125),
            lot_size: Quantity::from(1u32),
        },
    );

    // ZT: 1/256 = 0.00390625 (needs 8 dp to avoid rounding)
    m.insert(
        "ZT",
        RootSpec {
            asset_class: AssetClass::Debt,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("US 2-Year Treasury Notes"),
            price_precision: 8,
            size_precision: 0,
            tick_size: 0.00390625,
            tick_value: 7.8125,
            price_increment: Price::new(0.00390625, 8),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(7.8125 / 0.00390625),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "ZN",
        RootSpec {
            asset_class: AssetClass::Debt,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("US 10-Year Treasury Notes"),
            price_precision: 6,
            size_precision: 0,
            tick_size: 0.015625,
            tick_value: 15.625,
            price_increment: Price::new(0.015625, 6),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(15.625 / 0.015625),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "UB",
        RootSpec {
            asset_class: AssetClass::Debt,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("US Ultra Treasury Bond"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.03125,
            tick_value: 31.25,
            price_increment: Price::new(0.03125, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(31.25 / 0.03125),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "ZB",
        RootSpec {
            asset_class: AssetClass::Debt,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("US 30-Year Treasury Bonds"),
            price_precision: 5,
            size_precision: 0,
            tick_size: 0.03125,
            tick_value: 31.25,
            price_increment: Price::new(0.03125, 5),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(31.25 / 0.03125),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "TN",
        RootSpec {
            asset_class: AssetClass::Debt,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("US Ultra 10-Year Treasury Note"),
            price_precision: 6,
            size_precision: 0,
            tick_size: 0.015625,
            tick_value: 15.625,
            price_increment: Price::new(0.015625, 6),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(15.625 / 0.015625),
            lot_size: Quantity::from(1u32),
        },
    );

    // ===== Agricultural (CBOT / XCBT) =====
    m.insert(
        "ZC",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("Corn"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.25,
            tick_value: 12.5,
            price_increment: Price::new(0.25, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(12.5 / 0.25),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "ZS",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("Soybeans"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.25,
            tick_value: 12.5,
            price_increment: Price::new(0.25, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(12.5 / 0.25),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "ZW",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("Wheat"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.25,
            tick_value: 12.5,
            price_increment: Price::new(0.25, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(12.5 / 0.25),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "ZL",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("Soybean Oil"),
            price_precision: 2,
            size_precision: 0,
            tick_size: 0.01,
            tick_value: 6.0,
            price_increment: Price::new(0.01, 2),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(6.0 / 0.01),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "ZM",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCBT")),
            underlying: ustr("Soybean Meal"),
            price_precision: 1,
            size_precision: 0,
            tick_size: 0.1,
            tick_value: 10.0,
            price_increment: Price::new(0.1, 1),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(10.0 / 0.1),
            lot_size: Quantity::from(1u32),
        },
    );

    // ===== Livestock (CME / XCME) =====
    m.insert(
        "LE",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("Live Cattle"),
            price_precision: 3,
            size_precision: 0,
            tick_size: 0.025,
            tick_value: 10.0,
            price_increment: Price::new(0.025, 3),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(10.0 / 0.025),
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "HE",
        RootSpec {
            asset_class: AssetClass::Commodity,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("Lean Hogs"),
            price_precision: 3,
            size_precision: 0,
            tick_size: 0.025,
            tick_value: 10.0,
            price_increment: Price::new(0.025, 3),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(10.0 / 0.025),
            lot_size: Quantity::from(1u32),
        },
    );

    // ===== Crypto (CME / XCME) — fractional multipliers =====
    m.insert(
        "MBT",
        RootSpec {
            asset_class: AssetClass::Cryptocurrency,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("Bitcoin"),
            price_precision: 0,
            size_precision: 0,
            tick_size: 5.0,
            tick_value: 0.5,
            price_increment: Price::new(5.0, 0),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(0.5 / 5.0), // 0.1 with precision 1
            lot_size: Quantity::from(1u32),
        },
    );
    m.insert(
        "MET",
        RootSpec {
            asset_class: AssetClass::Cryptocurrency,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("Ether"),
            price_precision: 1,
            size_precision: 0,
            tick_size: 0.5,
            tick_value: 0.05,
            price_increment: Price::new(0.5, 1),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(0.05 / 0.5), // 0.1 with precision 1
            lot_size: Quantity::from(1u32),
        },
    );

    // ===== International Index (CME / XCME) =====
    m.insert(
        "NKD",
        RootSpec {
            asset_class: AssetClass::Index,
            currency: Currency::USD(),
            exchange: Some(ustr("XCME")),
            underlying: ustr("Nikkei 225"),
            price_precision: 1,
            size_precision: 0,
            tick_size: 5.0,
            tick_value: 25.0,
            price_increment: Price::new(5.0, 1),
            size_increment: Quantity::from(1u32),
            multiplier: qty_from_value(25.0 / 5.0),
            lot_size: Quantity::from(1u32),
        },
    );

    m
}

pub fn get_precision(root: &str) -> Option<u8> {
    hardcoded_roots().get(root).map(|r| r.price_precision)
}
