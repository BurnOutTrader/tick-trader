use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::base_data::{Bbo, Candle, OrderBook, Tick};
use crate::keys::SymbolKey;
use crate::providers::ProviderKind;

/// What a backtest wants to pull.
#[derive(Clone, Debug)]
pub struct HistoricalRequest {
    pub feed: SymbolKey,
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
    OrderBookL2(OrderBook),
    EndOfStream,          // provider finished successfully
    Error(anyhow::Error), // provider aborted
}

/// Provider-facing opaque handle (used by the router for cancellation).
pub trait HistoryHandle: Send + Sync {
    fn cancel(&self); // idempotent
    fn id(&self) -> Uuid; // stable id for logs
}
