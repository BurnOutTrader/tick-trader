use crate::base_data::{Bbo, Candle, Exchange, OrderBookSnapShot, Tick};
use crate::keys::Topic;
use crate::providers::ProviderKind;
use crate::securities::symbols::Instrument;
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// What a backtest wants to pull.
#[derive(Clone, Debug)]
pub struct HistoricalRequest {
    pub provider_kind: ProviderKind,
    pub topic: Topic,
    pub instrument: Instrument,
    pub exchange: Exchange,
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
    OrderBook(OrderBookSnapShot),
    EndOfStream,          // provider finished successfully
    Error(anyhow::Error), // provider aborted
}

/// Provider-facing opaque handle (used by the router for cancellation).
pub trait HistoryHandle: Send + Sync {
    fn cancel(&self); // idempotent
    fn id(&self) -> Uuid; // stable id for logs
}
