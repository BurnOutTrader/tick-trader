use crate::data::core::{Bbo, Candle, Tick};
use crate::keys::Topic;
use crate::providers::ProviderKind;
use crate::securities::symbols::Instrument;
use chrono::{DateTime, Utc};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use uuid::Uuid;

/// What a backtest wants to pull.
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct HistoricalRangeRequest {
    pub provider_kind: ProviderKind,
    pub topic: Topic,
    pub instrument: Instrument,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// Streamed payloads from a provider.
#[derive(Debug)]
pub enum HistoryEvent {
    Tick(Tick),
    Candle(Candle),
    // Optional future:
    Bbo(Bbo),
    EndOfStream,          // provider finished successfully
    Error(anyhow::Error), // provider aborted
}

/// Provider-facing opaque handle (used by the router for cancellation).
pub trait HistoryHandle: Send + Sync {
    fn cancel(&self); // idempotent
    fn id(&self) -> Uuid; // stable id for logs
}
