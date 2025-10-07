pub use crate::securities::symbols::Exchange;
use crate::securities::symbols::Instrument;
pub use chrono::{DateTime, Utc};
use chrono::TimeZone;
pub use rust_decimal::Decimal;
use crate::data::models::{BarClose, Price, Resolution, Side, Volume};
use rkyv::{Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

/// A single executed trade (tick).
///
/// Represents the smallest atomic piece of trade data.
/// Often used as input for tick charts or indicators.
#[derive(rkyv::Archive, RkyvDeserialize, RkyvSerialize, Debug, PartialEq, Clone)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Tick {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQ.Z25"`).
    pub instrument: Instrument,
    /// Trade price.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub price: Price,
    /// Trade size (quantity).
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub volume: Volume,
    /// UTC timestamp of the trade.
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub time: DateTime<Utc>,
    /// Whether the trade was buyer- or seller-initiated.
    pub side: Side,

    pub venue_seq: Option<u32>,
}
/// Convert an archived Tick's time to chrono::DateTime<Utc> without deserializing the full Tick.
/*pub fn archived_tick_time(archived: &ArchivedTick) -> DateTime<Utc> {
    // The archived field type depends on your remote adapter, so just convert manually
    let secs = archived.time.secs();
    let nanos = archived.time.nanos();
    Utc.timestamp_opt(secs, nanos).single().expect("invalid timestamp")
}*/

/// A candlestick / bar of aggregated trades.
///
/// Produced by consolidating ticks or vendor-provided candles.
#[derive(rkyv::Archive, RkyvDeserialize, RkyvSerialize, Debug, PartialEq, Clone)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Candle {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQ.Z25"`).
    pub instrument: Instrument,
    /// Start time of the candle (inclusive).
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub time_start: DateTime<Utc>,
    /// End time of the candle (exclusive).
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub time_end: DateTime<Utc>,
    /// Open price.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub open: Price,
    /// High price.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub high: Price,
    /// Low price.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub low: Price,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub close: Price,
    /// Total traded volume.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub volume: Volume,
    /// Volume executed at the ask.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub ask_volume: Volume,
    /// Volume executed at the bid.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
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

    pub fn is_closed(&self, time: DateTime<Utc>) -> bool {
        time > self.time_end
    }
}

#[derive(rkyv::Archive, RkyvDeserialize, RkyvSerialize, Debug, PartialEq, Clone)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct TickBar {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQ.Z25"`).
    pub instrument: Instrument,
    /// Start time of the candle (inclusive).
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub time_start: DateTime<Utc>,
    /// End time of the candle (exclusive).
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub time_end: DateTime<Utc>,
    /// Open price.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub open: Price,
    /// High price.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub high: Price,
    /// Low price.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub low: Price,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub close: Price,
    /// Total traded volume.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub volume: Volume,
    /// Volume executed at the ask.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub ask_volume: Volume,
    /// Volume executed at the bid.
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub bid_volume: Volume,
}

/// Best bid and offer (BBO).
///
/// A lightweight snapshot of the top of the order book.
#[derive(rkyv::Archive, RkyvDeserialize, RkyvSerialize, Debug, PartialEq, Clone)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Bbo {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQ.Z25"`).
    pub instrument: Instrument,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub bid: Price,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub bid_size: Volume,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub ask: Price,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub ask_size: Volume,
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
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
