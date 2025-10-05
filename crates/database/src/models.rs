use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use strum_macros::Display;

/// What’s stored (determines folder layout + schema columns)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Display)]
pub enum DataKind {
    /// Executed trades (ticks). One file per **day**.
    Tick,
    /// Best bid/offer (BBO quotes). One file per **day**.
    Bbo,
    /// Aggregated bars/candles. Daily=per year, Weekly=one file, intraday=per day.
    Candle,

    BookL2,
}

// ---------- Disambiguation key for same-timestamp events ----------
// Store time in microseconds to align with Parquet/Arrow, plus a tie-breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventKey {
    pub ts_utc_us: i64, // UTC micros since epoch
    pub tie: u32,       // sequence within same ts (venue_seq or synthetic)
}

// ---------- Core records (storage-friendly shapes) ----------
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TickRow {
    pub provider: String,
    pub symbol_id: String,
    pub exchange: String,
    pub price: f64,
    pub size: f64,
    pub side: u8,           // 0 None, 1 Buy, 2 Sell
    pub key_ts_utc_ns: i64, // <-- ns (replaces *_us)
    pub key_tie: u32,
    pub venue_seq: Option<u32>,
    pub exec_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CandleRow {
    pub provider: String,
    pub symbol_id: String,
    pub exchange: String,
    pub res: String,        // "S1","M5","D",...
    pub time_start_ns: i64, // <-- ns
    pub time_end_ns: i64,   // <-- ns (we key candles by END time in replay)
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub ask_volume: f64,
    pub bid_volume: f64,
    pub num_trades: u64,
}

impl CandleRow {
    pub fn from_candle(provider: String, candle: Candle) -> Self {
        Self {
            provider,
            symbol_id: candle.symbol,
            exchange: format!("{:?}", candle.exchange),
            res: candle.resolution.to_os_string(),
            time_start_ns: candle.time_start.timestamp_nanos_opt().unwrap_or(0),
            time_end_ns: candle.time_end.timestamp_nanos_opt().unwrap_or(0),
            open: candle.open.to_f64().unwrap_or(0.0),
            high: candle.high.to_f64().unwrap_or(0.0),
            low: candle.low.to_f64().unwrap_or(0.0),
            close: candle.close.to_f64().unwrap_or(0.0),
            volume: candle.volume.to_f64().unwrap_or(0.0),
            ask_volume: candle.ask_volume.to_f64().unwrap_or(0.0),
            bid_volume: candle.bid_volume.to_f64().unwrap_or(0.0),
            num_trades: candle.num_of_trades.to_u64().unwrap_or(0),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BboRow {
    pub provider: String,
    pub symbol_id: String,
    pub exchange: String,
    pub key_ts_utc_ns: i64, // <-- ns
    pub bid: f64,
    pub bid_size: f64,
    pub ask: f64,
    pub ask_size: f64,
    pub bid_orders: Option<u32>,
    pub ask_orders: Option<u32>,
    pub venue_seq: Option<u32>,
    pub is_snapshot: Option<bool>,
}
// ---------- Catalog entities ----------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Provider {
    pub provider: String,
    pub version: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SymbolMeta {
    pub symbol_id: String,
    pub security: String,
    pub exchange: String,
    pub currency: String,
    pub root: Option<String>,          // for futures
    pub continuous_of: Option<String>, // if this is a continuous symbol
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UniverseMember {
    pub universe: String, // e.g. "CME_MICROS"
    pub symbol_id: String,
    pub provider: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SeqBound {
    pub ts: chrono::DateTime<chrono::Utc>,
    pub seq: Option<i64>,
}
