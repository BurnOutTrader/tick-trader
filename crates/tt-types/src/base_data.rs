pub use crate::securities::symbols::Exchange;
use crate::securities::symbols::Instrument;
pub use chrono::{DateTime, Utc};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
pub use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use strum_macros::Display;

#[derive(Clone, PartialEq, Eq, Debug, Hash, Display)]
pub enum Feed {
    Bbo {
        symbol: String,
        exchange: Exchange,
    },
    Ticks {
        symbol: String,
        exchange: Exchange,
    },
    Candles {
        symbol: String,
        exchange: Exchange,
        resolution: Resolution,
    },
    TickBars {
        symbol: String,
        exchange: Exchange,
        ticks: u32,
    },
    OrderBookL2 {
        symbol: String,
        exchange: Exchange,
        depth: u8,
    },
    OrderBookL3 {
        symbol: String,
        exchange: Exchange,
    },
}

pub type Price = Decimal;
pub type Volume = Decimal;

/// Resolution for time- or tick-based aggregation.
///
/// Used mainly for candles and bar consolidators.
///
/// - [`Ticks`] – number of ticks per bar or 1 tick.
/// - [`Seconds(u8)`] – N-second bars (e.g. 1-second, 5-second).
/// - [`Minutes(u8)`] – N-minute bars.
/// - [`Hours(u8)`] – N-hour bars.
/// - [`TickBars(u32)`] – Bars built from a fixed number of ticks.
/// - [`Daily`] – One bar per trading day.
/// - [`Weekly`] – One bar per trading week.
#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize, Deserialize)]
pub enum Resolution {
    Seconds(u8),
    Minutes(u8),
    Hours(u8),
    Daily,
    Weekly,
}

impl Resolution {
    /// Returns a short key string for file paths or topic suffixes when applicable.
    /// Returns None for TickBars which are generally parameterized elsewhere.
    pub fn as_key(&self) -> Option<&'static str> {
        match self {
            Resolution::Seconds(n) => Some(Box::leak(format!("sec{}", n).into_boxed_str())),
            Resolution::Minutes(n) => Some(Box::leak(format!("min{}", n).into_boxed_str())),
            Resolution::Hours(n) => Some(Box::leak(format!("hr{}", n).into_boxed_str())),
            Resolution::Daily => Some("daily"),
            Resolution::Weekly => Some("weekly"),
        }
    }

    pub fn is_intraday(&self) -> bool {
        !matches!(self, Resolution::Daily | Resolution::Weekly)
    }

    pub fn to_os_string(&self) -> String {
        match self {
            Resolution::Seconds(n) => format!("sec{}", n),
            Resolution::Minutes(n) => format!("min{}", n),
            Resolution::Hours(n) => format!("hr{}", n),
            Resolution::Daily => "daily".to_string(),
            Resolution::Weekly => "weekly".to_string(),
        }
    }

    pub fn as_duration(&self) -> chrono::Duration {
        match self {
            Resolution::Seconds(n) => chrono::Duration::seconds(*n as i64),
            Resolution::Minutes(n) => chrono::Duration::minutes(*n as i64),
            Resolution::Hours(n) => chrono::Duration::hours(*n as i64),
            Resolution::Daily => chrono::Duration::days(1),
            Resolution::Weekly => chrono::Duration::weeks(1),
        }
    }
}

/// Trade direction.
///
/// Indicates whether a trade or order was executed on the buy or sell side.
#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq), derive(Debug))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize, Deserialize)]
pub enum Side {
    /// Buyer-initiated trade.
    Buy,
    /// Seller-initiated trade.
    Sell,
    /// Unknown direction.
    None,
}

/// How the bar closed
#[derive(Archive, RkyvDeserialize, RkyvSerialize)]
#[rkyv(compare(PartialEq))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize, Deserialize)]
pub enum BarClose {
    /// Close > Open
    Bullish,
    /// Close < Open
    Bearish,
    /// Close == Open
    Flat,
}

/// A single executed trade (tick).
///
/// Represents the smallest atomic piece of trade data.
/// Often used as input for tick charts or indicators.
#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct Tick {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQZ5"`).
    pub instrument: Instrument,
    /// Trade price.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub price: Price,
    /// Trade size (quantity).
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub volume: Volume,
    /// UTC timestamp of the trade.
    #[rkyv(with = crate::rkyv_types::DateTimeUtcDef)]
    pub time: DateTime<Utc>,
    /// Whether the trade was buyer- or seller-initiated.
    pub side: Side,

    pub venue_seq: Option<u32>,
}

/// A candlestick / bar of aggregated trades.
///
/// Produced by consolidating ticks or vendor-provided candles.
#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct Candle {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQZ5"`).
    pub instrument: Instrument,
    /// Start time of the candle (inclusive).
    #[rkyv(with = crate::rkyv_types::DateTimeUtcDef)]
    pub time_start: DateTime<Utc>,
    /// End time of the candle (exclusive).
    #[rkyv(with = crate::rkyv_types::DateTimeUtcDef)]
    pub time_end: DateTime<Utc>,
    /// Open price.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub open: Price,
    /// High price.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub high: Price,
    /// Low price.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub low: Price,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub close: Price,
    /// Total traded volume.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub volume: Volume,
    /// Volume executed at the ask.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub ask_volume: Volume,
    /// Volume executed at the bid.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub bid_volume: Volume,
    /// Resolution used to build this candle.
    pub resolution: Resolution,
}

impl Candle {
    /// How the bar closed, bullish, bearish or flat.
    pub fn bar_close(&self) -> BarClose {
        if self.close > self.open {
            BarClose::Bullish
        } else if self.close < self.open {
            BarClose::Bearish
        } else {
            BarClose::Flat
        }
    }

    /// The range of the bar
    pub fn range(&self) -> Decimal {
        self.high - self.low
    }
}

#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct TickBar {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQZ5"`).
    pub instrument: Instrument,
    /// Start time of the candle (inclusive).
    #[rkyv(with = crate::rkyv_types::DateTimeUtcDef)]
    pub time_start: DateTime<Utc>,
    /// End time of the candle (exclusive).
    #[rkyv(with = crate::rkyv_types::DateTimeUtcDef)]
    pub time_end: DateTime<Utc>,
    /// Open price.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub open: Price,
    /// High price.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub high: Price,
    /// Low price.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub low: Price,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub close: Price,
    /// Total traded volume.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub volume: Volume,
    /// Volume executed at the ask.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub ask_volume: Volume,
    /// Volume executed at the bid.
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub bid_volume: Volume,
}

/// Best bid and offer (BBO).
///
/// A lightweight snapshot of the top of the order book.
#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct Bbo {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQZ5"`).
    pub instrument: Instrument,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub bid: Price,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub bid_size: Volume,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub ask: Price,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub ask_size: Volume,
    #[rkyv(with = crate::rkyv_types::DateTimeUtcDef)]
    pub time: DateTime<Utc>, // normalized event time

    // ---- Optional cross-vendor metadata ----
    /// Order counts at top level (Rithmic: *_orders, Databento: bid_ct_00/ask_ct_00).
    pub bid_orders: Option<u32>,
    pub ask_orders: Option<u32>,

    /// Venue/publisher sequence number (Databento: sequence, other feeds often have one).
    pub venue_seq: Option<u32>,

    /// Whether this record is a snapshot/seed vs. incremental (many feeds set this).
    pub is_snapshot: Option<bool>,
}

#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct BookLevel {
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub price: Price,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub volume: Volume,
    pub level: u32, // number of orders at this price level, if available
}

/// Full order book snapshot.
///
/// Contains bid and ask ladders up to the requested depth.
/// Depth levels are sorted: index 0 = best price level.
#[derive(
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct OrderBook {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQZ5"`).
    pub instrument: Instrument,
    /// descending iteration = best to worst
    pub bids: Vec<BookLevel>,
    /// descending iteration = best to worst
    pub asks: Vec<BookLevel>,
    /// UTC timestamp of the snapshot.
    #[rkyv(with = crate::rkyv_types::DateTimeUtcDef)]
    pub time: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn candle_bar_close_and_range() {
        let c = Candle {
            symbol: "ES".into(),
            instrument: Instrument::from_str("ESZ5").unwrap(),
            time_start: Utc::now(),
            time_end: Utc::now(),
            open: Price::from(10),
            high: Price::from(15),
            low: Price::from(9),
            close: Price::from(12),
            volume: Volume::from(1000),
            ask_volume: Volume::from(500),
            bid_volume: Volume::from(500),
            resolution: Resolution::Seconds(1),
        };
        assert_eq!(c.bar_close(), BarClose::Bullish);
        assert_eq!(c.range(), Decimal::from(6));
    }

    #[test]
    fn resolution_helpers() {
        assert_eq!(Resolution::Seconds(1).as_key(), Some("sec1"));
        assert_eq!(Resolution::Minutes(5).to_os_string(), "min5");
        assert!(Resolution::Daily.is_intraday() == false);
        assert!(Resolution::Seconds(2).is_intraday());
    }

    #[test]
    fn exchange_from_str_parses_case_insensitively() {
        assert_eq!(Exchange::from_str("CME").unwrap(), Exchange::CME);
        assert_eq!(Exchange::from_str("globex").unwrap(), Exchange::GLOBEX);
        assert!(Exchange::from_str("unknown").is_none());
    }
}
