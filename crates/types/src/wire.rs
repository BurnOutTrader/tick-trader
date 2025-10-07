use crate::data::core::{Bbo, Candle, Exchange, Tick};
use crate::keys::{SymbolKey, Topic};
use crate::providers::ProviderKind;
use crate::securities::security::FuturesContract;
use crate::securities::symbols::Instrument;
use rust_decimal::Decimal;
use serde::{Serialize, Deserialize};

/// Request to subscribe to a topic (coarse, topic-level interest)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscribe {
    /// Topic to subscribe to
    pub topic: Topic,
    /// If true, only deliver the latest item (no backlog)
    pub latest_only: bool,
    /// Sequence number to start from (if supported)
    pub from_seq: u64,
}
impl Subscribe {
    pub fn topic(&self) -> Topic {
        self.topic
    }
}

/// Request to subscribe to a specific (topic, key) pair (precise, key-based)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscribeKey {
    /// Topic to subscribe to
    pub topic: Topic,
    /// Symbol key (instrument/provider)
    pub key: SymbolKey,
    /// If true, only deliver the latest item (no backlog)
    pub latest_only: bool,
    /// Sequence number to start from (if supported)
    pub from_seq: u64,
}
impl SubscribeKey {
    pub fn topic(&self) -> Topic {
        self.topic
    }
    pub fn key(&self) -> &SymbolKey {
        &self.key
    }
}

/// Ping message (heartbeat)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ping {
    /// Nanosecond timestamp
    pub ts_ns: i64,
}
/// Pong reply (heartbeat)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pong {
    /// Nanosecond timestamp
    pub ts_ns: i64,
}

/// Batch of ticks for a topic/sequence
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TickBatch {
    /// Topic (e.g. Ticks)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Ticks in this batch
    pub ticks: Vec<Tick>,
}

/// Batch of quotes (BBOs) for a topic/sequence
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuoteBatch {
    /// Topic (e.g. Quotes)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Quotes in this batch
    pub quotes: Vec<Bbo>,
}

/// Batch of OHLC bars for a topic/sequence
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BarBatch {
    /// Topic (e.g. Bars1m)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Bars in this batch
    pub bars: Vec<Candle>,
}

/// Batch of order book snapshots/updates
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MBP10Batch {
    /// Topic (e.g. Depth)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    pub event: crate::data::mbp10::Mbp10
}

/// Vendor-specific binary data batch
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VendorData {
    /// Topic
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Opaque vendor data
    pub data: Vec<u8>,
}

/// Batch of order updates (lossless)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrdersBatch {
    /// Topic (e.g. Orders)
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Orders in this batch
    pub orders: Vec<crate::accounts::events::OrderUpdate>,
}

/// Batch of position updates (lossless)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionsBatch {
    /// Topic (e.g. Positions)
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Positions in this batch
    pub positions: Vec<crate::accounts::events::PositionDelta>,
}

/// Batch of account delta updates (lossless)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountDeltaBatch {
    /// Topic (e.g. AccountEvt)
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Account deltas in this batch
    pub accounts: Vec<crate::accounts::events::AccountDelta>,
}

/// Request for available instruments from a provider
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentsRequest {
    /// Provider to query
    pub provider: ProviderKind,
    /// Optional pattern to filter instruments
    pub pattern: Option<String>,
    /// Correlation ID for matching response
    pub corr_id: u64,
}

/// Response with available instruments from a provider
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentsResponse {
    /// Provider
    pub provider: ProviderKind,
    /// List of instruments
    pub instruments: Vec<Instrument>,
    /// Correlation ID
    pub corr_id: u64,
}

/// Response with a map of instruments and their contract details
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentsMapResponse {
    /// Provider name
    pub provider: String,
    /// Pairs of (Instrument, FuturesContractWire)
    pub instruments: Vec<(Instrument, FuturesContract)>,
    /// Correlation ID
    pub corr_id: u64,
}

/// Request for a full map of futures contracts from a provider
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstrumentsMapRequest {
    /// Provider to query
    pub provider: ProviderKind,
    /// Correlation ID for matching response
    pub corr_id: u64,
}

/// Authentication credentials for a provider
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthCredentials {
    /// Provider
    pub provider: ProviderKind,
    /// Username (if required)
    pub username: Option<String>,
    /// Password (if required)
    pub password: Option<String>,
    /// API key (if required)
    pub api_key: Option<String>,
    /// Secret (if required)
    pub secret: Option<String>,
    /// For provider-specific fields
    pub extra: Vec<(String, String)>,
}

/// Unsubscribe from all topics/keys (clean detach)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnsubscribeAll {
    /// Optional reason for unsubscribe
    pub reason: Option<String>,
}

/// Client-initiated disconnect request (ask the router to kick this connection)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Kick {
    /// Optional reason for disconnect
    pub reason: Option<String>,
}

/// Unsubscribe from a specific (topic, key) pair
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsubscribeKey {
    /// Topic
    pub topic: Topic,
    /// Symbol key
    pub key: SymbolKey,
}

/// Announce a shared memory (SHM) snapshot stream
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AnnounceShm {
    /// Topic
    pub topic: Topic,
    /// Symbol key
    pub key: SymbolKey,
    /// SHM segment name
    pub name: String,
    /// Layout version
    pub layout_ver: u32,
    /// Size in bytes
    pub size: u64,
}

/// Order type for wire protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderTypeWire {
    /// Market order
    Market,
    /// Limit order
    Limit,
    /// Stop order
    Stop,
    /// Stop-limit order
    StopLimit,
    /// Trailing stop order
    TrailingStop,
    /// Join bid order
    JoinBid,
    /// Join ask order
    JoinAsk,
}

/// Bracket order details for stop loss/take profit
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BracketWire {
    /// Number of ticks for bracket
    pub ticks: i32,
    /// Order type
    pub r#type: OrderTypeWire,
}

/// Place a new order
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaceOrder {
    /// Account ID
    pub account_id: i64,
    /// Symbol key
    pub key: SymbolKey,
    /// Side (buy/sell)
    pub side: crate::accounts::events::Side,
    /// Quantity
    pub qty: i64,
    /// Order type
    pub r#type: OrderTypeWire,
    /// Limit price (if applicable)
    pub limit_price: Option<f64>,
    /// Stop price (if applicable)
    pub stop_price: Option<f64>,
    /// Trailing price (if applicable)
    pub trail_price: Option<f64>,
    /// Custom tag for order
    pub custom_tag: Option<String>,
    /// Optional stop loss bracket
    pub stop_loss: Option<BracketWire>,
    /// Optional take profit bracket
    pub take_profit: Option<BracketWire>,
}

/// Cancel an existing order
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CancelOrder {
    /// Account ID
    pub account_id: i64,
    /// Provider order ID (if known)
    pub provider_order_id: Option<String>,
    /// Client order ID (if known)
    pub client_order_id: Option<crate::accounts::events::ClientOrderId>,
}

/// Replace (modify) an existing order
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplaceOrder {
    /// Account ID
    pub account_id: i64,
    /// Provider order ID (if known)
    pub provider_order_id: Option<String>,
    /// Client order ID (if known)
    pub client_order_id: Option<crate::accounts::events::ClientOrderId>,
    /// New quantity (if modifying)
    pub new_qty: Option<i64>,
    /// New limit price (if modifying)
    pub new_limit_price: Option<f64>,
    /// New stop price (if modifying)
    pub new_stop_price: Option<f64>,
    /// New trailing price (if modifying)
    pub new_trail_price: Option<f64>,
}

/// Subscribe to all execution streams for an account
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SubscribeAccount {
    /// Account key
    pub key: crate::keys::AccountKey,
}

/// Unsubscribe from all execution streams for an account
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnsubscribeAccount {
    /// Account key
    pub key: crate::keys::AccountKey,
}

/// Request account info for a provider
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountInfoRequest {
    /// Provider
    pub provider: ProviderKind,
    /// Correlation ID
    pub corr_id: u64,
}

/// Summary of an account (for info response)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountSummaryWire {
    /// Account ID
    pub account_id: i64,
    /// Account name
    pub account_name: String,
    /// Provider
    pub provider: ProviderKind,
}

/// Response with account info for a provider
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountInfoResponse {
    /// Provider
    pub provider: ProviderKind,
    /// Correlation ID
    pub corr_id: u64,
    /// Accounts
    pub accounts: Vec<AccountSummaryWire>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Request {
    // Control from clients to server (coarse)
    Subscribe(Subscribe),
    // New key-based control
    SubscribeKey(SubscribeKey),
    UnsubscribeKey(UnsubscribeKey),
    // Account-level execution streams control
    SubscribeAccount(SubscribeAccount),
    UnsubscribeAccount(UnsubscribeAccount),
    // Startup account info
    AccountInfoRequest(AccountInfoRequest),
    Ping(Ping),
    UnsubscribeAll(UnsubscribeAll),
    // Client-initiated disconnect (request to be kicked)
    Kick(Kick),
    InstrumentsRequest(InstrumentsRequest),
    InstrumentsMapRequest(InstrumentsMapRequest),
    // Execution
    PlaceOrder(PlaceOrder),
    CancelOrder(CancelOrder),
    ReplaceOrder(ReplaceOrder),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Response {
    // Control replies
    Pong(Pong),
    // Router → Client announcements
    AnnounceShm(AnnounceShm),
    // Instruments
    InstrumentsResponse(InstrumentsResponse),
    InstrumentsMapResponse(InstrumentsMapResponse),
    // Account info (startup)
    AccountInfoResponse(AccountInfoResponse),
    // Data (batches first)
    TickBatch(TickBatch),
    QuoteBatch(QuoteBatch),
    BarBatch(BarBatch),
    MBP10(MBP10Batch),
    VendorData(VendorData),
    OrdersBatch(OrdersBatch),
    PositionsBatch(PositionsBatch),
    AccountDeltaBatch(AccountDeltaBatch),
    SubscribeResponse {
        topic: Topic,
        instrument: Instrument,
        success: bool,
    },
    UnsubscribeResponse {
        topic: Topic,
        instrument: Instrument,
    },
    // Single items (for completeness; bus generally batches)
    Tick(Tick),
    Quote(Bbo),
    Bar(Candle),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WireMessage {
    Request(Request),
    Response(Response),
}

/// Encode any serializable message to bytes
pub fn encode<T: Serialize>(msg: &T) -> anyhow::Result<Vec<u8>> {
    let buf = bincode::serialize(msg)?;
    Ok(buf)
}

/// Decode bytes into a message
pub fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> anyhow::Result<T> {
    let msg = bincode::deserialize(bytes)?;
    Ok(msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn instruments_map_response_bincode_roundtrip() {
        // Build a minimal FuturesContract with sane Decimals
        let fc = FuturesContract {
            root: "MNQ".to_string(),
            instrument: Instrument::from_str("MNQ.Z25").unwrap(),
            security_type: crate::securities::symbols::SecurityType::Future,
            exchange: crate::securities::symbols::Exchange::GLOBEX,
            provider_contract_name: "MNQZ5".to_string(),
            provider_id: ProviderKind::ProjectX(crate::providers::ProjectXTenant::Topstep),
            tick_size: rust_decimal::Decimal::new(25, 2),
            value_per_tick: rust_decimal::Decimal::new(50, 0),
            decimal_accuracy: 2,
            quote_ccy: crate::securities::symbols::Currency::USD,
            activation_date: None,
            expiration_date: None,
            is_continuous: false,
        };
        let resp = InstrumentsMapResponse {
            provider: "ProjectX::Topstep".to_string(),
            instruments: vec![(Instrument::from_str("MNQ.Z25").unwrap(), fc)],
            corr_id: 42,
        };
        let wm = WireMessage::Response(Response::InstrumentsMapResponse(resp.clone()));
        let bytes = encode(&wm).expect("encode");
        let decoded: WireMessage = decode(&bytes).expect("decode");
        match decoded {
            WireMessage::Response(Response::InstrumentsMapResponse(r2)) => {
                assert_eq!(r2.corr_id, resp.corr_id);
                assert_eq!(r2.provider, resp.provider);
                assert_eq!(r2.instruments.len(), 1);
                let (_ins, fc2) = &r2.instruments[0];
                assert_eq!(fc2.tick_size, rust_decimal::Decimal::new(25, 2));
                assert_eq!(fc2.value_per_tick, rust_decimal::Decimal::new(50, 0));
            }
            other => panic!("unexpected decoded: {:?}", other),
        }
    }
}