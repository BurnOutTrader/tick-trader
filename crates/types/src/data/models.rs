use strum_macros::Display;
use serde::{Deserialize, Serialize};
use rust_decimal::Decimal;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Display, Serialize, Deserialize)]
pub enum BarClose {
    /// Close > Open
    Bullish,
    /// Close < Open
    Bearish,
    /// Close == Open
    Flat,
}