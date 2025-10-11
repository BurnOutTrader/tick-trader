use dotenvy::dotenv;
use std::time::Instant;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::server_side::traits::ProviderParams;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubState {
    Unsubscribed,
    Subscribing,
    Subscribed,
    Unsubscribing,
    Error,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum StrategyMode {
    Backtest,
    LivePaper,
    Live,
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    // health thresholds (defaults; env override)
    pub ticks_warn_ms: u64,
    pub ticks_alert_ms: u64,
    pub depth_warn_ms: u64,
    pub depth_alert_ms: u64,
    pub bars1s_warn_ms: u64,
    pub bars1s_alert_ms: u64,
    pub bars1m_warn_ms: u64,
    pub bars1m_alert_ms: u64,
    pub orders_warn_ms: u64,
    pub orders_alert_ms: u64,
    // replay windows
    pub ticks_replay_secs: u64,
    pub bars1s_window_secs: u64,
    pub bars1m_window_secs: u64,
    // depth/mbo ring duration (approx)
    pub depth_ring_secs: u64,
    pub db_path: String,
}

impl Default for EngineConfig {
    fn default() -> Self {
        dotenv().ok();
        Self {
            ticks_warn_ms: crate::helpers::env_u64("TT_TICKS_WARN_MS", 100),
            ticks_alert_ms: crate::helpers::env_u64("TT_TICKS_ALERT_MS", 500),
            depth_warn_ms: crate::helpers::env_u64("TT_DEPTH_WARN_MS", 100),
            depth_alert_ms: crate::helpers::env_u64("TT_DEPTH_ALERT_MS", 500),
            bars1s_warn_ms: crate::helpers::env_u64("TT_BARS1S_WARN_MS", 2000),
            bars1s_alert_ms: crate::helpers::env_u64("TT_BARS1S_ALERT_MS", 5000),
            bars1m_warn_ms: crate::helpers::env_u64("TT_BARS1M_WARN_MS", 120_000),
            bars1m_alert_ms: crate::helpers::env_u64("TT_BARS1M_ALERT_MS", 300_000),
            orders_warn_ms: crate::helpers::env_u64("TT_ORDERS_WARN_MS", 5000),
            orders_alert_ms: crate::helpers::env_u64("TT_ORDERS_ALERT_MS", 10_000),
            ticks_replay_secs: crate::helpers::env_u64("TT_TICKS_REPLAY_SECS", 60),
            bars1s_window_secs: crate::helpers::env_u64("TT_BARS1S_WINDOW_SECS", 600),
            bars1m_window_secs: crate::helpers::env_u64("TT_BARS1M_WINDOW_SECS", 3600),
            depth_ring_secs: crate::helpers::env_u64("TT_DEPTH_RING_SECS", 5),
            db_path: crate::helpers::env_string("DB_PATH", "./storage"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamKey {
    pub topic: Topic,
    pub key: SymbolKey,
}

#[derive(Debug, Clone)]
pub struct InterestEntry {
    pub downstream_count: u32,
    pub state: SubState,
    pub params: Option<ProviderParams>,
    pub last_upstream_data_at: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
pub struct StreamMetrics {
    pub frames: u64,
    pub bytes: u64,
    pub drops: u64,
    pub credit_stalls: u64,
    pub last_upstream_data_at: Option<Instant>,
    pub last_downstream_send_at: Option<Instant>,
}

// Replay cache skeletons
#[derive(Debug, Clone)]
pub struct TickRec {
    pub ts_ns: i64,
    pub bytes: usize,
}

#[derive(Debug, Clone)]
pub struct BarRec {
    pub ts_ns: i64,
    pub bytes: usize,
}

#[derive(Debug, Clone)]
pub struct DepthDeltaRec {
    pub ts_ns: i64,
    pub bytes: usize,
}

#[derive(Clone, Debug, Copy, Hash, PartialEq, Eq)]
pub enum DataTopic {
    Ticks,
    Quotes,
    MBP10,
    Candles1s,
    Candles1m,
    Candles1h,
    Candles1d,
}

impl DataTopic {
    pub(crate) fn to_topic_or_err(self) -> anyhow::Result<Topic> {
        match self {
            DataTopic::Ticks => Ok(Topic::Ticks),
            DataTopic::Quotes => Ok(Topic::Quotes),
            DataTopic::MBP10 => Ok(Topic::MBP10),
            DataTopic::Candles1s => Ok(Topic::Candles1s),
            DataTopic::Candles1m => Ok(Topic::Candles1m),
            DataTopic::Candles1h => Ok(Topic::Candles1h),
            DataTopic::Candles1d => Ok(Topic::Candles1d),
        }
    }

    pub fn from(topic: Topic) -> Self {
        match topic {
            Topic::Ticks => DataTopic::Ticks,
            Topic::Quotes => DataTopic::Quotes,
            Topic::MBP10 => DataTopic::MBP10,
            Topic::Candles1s => DataTopic::Candles1s,
            Topic::Candles1m => DataTopic::Candles1m,
            Topic::Candles1h => DataTopic::Candles1h,
            Topic::Candles1d => DataTopic::Candles1d,
            _ => unimplemented!("Topic: {} not added to engine handling", topic),
        }
    }
}

// Non-blocking commands enqueued by the strategy/handle and drained by the engine task
pub enum Command {
    Subscribe { topic: Topic, key: SymbolKey },
    Unsubscribe { topic: Topic, key: SymbolKey },
    Place(tt_types::wire::PlaceOrder),
    Cancel(tt_types::wire::CancelOrder),
    Replace(tt_types::wire::ReplaceOrder),
}
