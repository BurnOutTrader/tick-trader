use crate::data::models::{BarClose, Price, Resolution, TradeSide, Volume};
pub use crate::securities::symbols::Exchange;
use crate::securities::symbols::Instrument;
use crate::wire::Bytes;
pub use chrono::{DateTime, Utc};
use rkyv::{AlignedVec, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
pub use rust_decimal::Decimal;

/// A single executed trade (tick).
///
/// Represents the smallest atomic piece of trade data.
/// Often used as input for tick charts or indicators.
#[derive(rkyv::Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct Tick {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQ.Z25"`).
    pub instrument: Instrument,
    /// Trade price.
    pub price: Price,
    /// Trade size (quantity).
    pub volume: Volume,
    /// UTC timestamp of the trade.
    pub time: DateTime<Utc>,
    /// Whether the trade was buyer- or seller-initiated.
    pub side: TradeSide,

    pub venue_seq: Option<u32>,
}

impl Bytes<Self> for Tick {
    fn from_bytes(archived: &[u8]) -> anyhow::Result<Tick> {
        match rkyv::from_bytes::<Tick>(archived) {
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }

    fn to_aligned_bytes(&self) -> AlignedVec {
        // Serialize directly into an AlignedVec for maximum compatibility with rkyv
        rkyv::to_bytes::<_, 256>(self).expect("rkyv::to_bytes failed")
    }
}

/// A candlestick / bar of aggregated trades.
///
/// Produced by consolidating ticks or vendor-provided candles.
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct Candle {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQ.Z25"`).
    pub instrument: Instrument,
    /// Start time of the candle (inclusive).
    pub time_start: DateTime<Utc>,
    /// End time of the candle (exclusive).
    pub time_end: DateTime<Utc>,
    /// Open price.
    pub open: Price,
    /// High price.
    pub high: Price,
    /// Low price.
    pub low: Price,

    pub close: Price,
    /// Total traded volume.
    pub volume: Volume,
    /// Volume executed at the ask.
    pub ask_volume: Volume,
    /// Volume executed at the bid.
    pub bid_volume: Volume,
    /// Resolution used to build this candle.
    pub resolution: Resolution,
}

impl Bytes<Self> for Candle {
    fn from_bytes(archived: &[u8]) -> anyhow::Result<Candle> {
        // Ensure alignment for rkyv by copying into an AlignedVec first
        match rkyv::from_bytes::<Candle>(&archived) {
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }

    fn to_aligned_bytes(&self) -> AlignedVec {
        // Serialize directly into an AlignedVec for maximum compatibility with rkyv
        rkyv::to_bytes::<_, 256>(self).expect("rkyv::to_bytes failed")
    }
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

#[derive(rkyv::Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct TickBar {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQ.Z25"`).
    pub instrument: Instrument,
    /// Start time of the candle (inclusive).
    pub time_start: DateTime<Utc>,
    /// End time of the candle (exclusive).
    pub time_end: DateTime<Utc>,
    /// Open price.
    pub open: Price,
    /// High price.
    pub high: Price,
    /// Low price.
    pub low: Price,

    pub close: Price,
    /// Total traded volume.
    pub volume: Volume,
    /// Volume executed at the ask.
    pub ask_volume: Volume,
    /// Volume executed at the bid.
    pub bid_volume: Volume,
}

impl Bytes<Self> for TickBar {
    fn from_bytes(archived: &[u8]) -> anyhow::Result<TickBar> {
        // If the archived bytes do not end with the delimiter, proceed as before
        match rkyv::from_bytes::<TickBar>(archived) {
            //Ignore this warning: Trait `Deserialize<ResponseType, SharedDeserializeMap>` is not implemented for `ArchivedRequestType` [E0277]
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }

    fn to_aligned_bytes(&self) -> AlignedVec {
        // Serialize directly into an AlignedVec for maximum compatibility with rkyv
        rkyv::to_bytes::<_, 256>(self).expect("rkyv::to_bytes failed")
    }
}

/// Best bid and offer (BBO).
///
/// A lightweight snapshot of the top of the order book.
#[derive(rkyv::Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct Bbo {
    /// Symbol identifier (e.g. `"MNQ"`, `"AAPL"`).
    pub symbol: String,
    /// Symbol identifier (e.g. `"MNQ.Z25"`).
    pub instrument: Instrument,

    pub bid: Price,

    pub bid_size: Volume,

    pub ask: Price,

    pub ask_size: Volume,

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

impl Bytes<Self> for Bbo {
    fn from_bytes(archived: &[u8]) -> anyhow::Result<Bbo> {
        match rkyv::from_bytes::<Bbo>(&archived) {
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }

    fn to_aligned_bytes(&self) -> AlignedVec {
        // Serialize directly into an AlignedVec for maximum compatibility with rkyv
        rkyv::to_bytes::<_, 256>(self).expect("rkyv::to_bytes failed")
    }
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
