use crate::base_data::{Bbo, Candle, Exchange, Tick};
use crate::keys::{SymbolKey, Topic};
use crate::providers::ProviderKind;
use crate::securities::security::FuturesContract;
use crate::securities::symbols::Instrument;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;

/// Request to subscribe to a topic (coarse, topic-level interest)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Ping {
    /// Nanosecond timestamp
    pub ts_ns: i64,
}
/// Pong reply (heartbeat)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Pong {
    /// Nanosecond timestamp
    pub ts_ns: i64,
}

/// Batch of ticks for a topic/sequence
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct TickBatch {
    /// Topic (e.g. Ticks)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Ticks in this batch
    pub ticks: Vec<Tick>,
}

/// Batch of quotes (BBOs) for a topic/sequence
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct QuoteBatch {
    /// Topic (e.g. Quotes)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Quotes in this batch
    pub quotes: Vec<Bbo>,
}

/// Batch of OHLC bars for a topic/sequence
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct BarBatch {
    /// Topic (e.g. Bars1m)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Bars in this batch
    pub bars: Vec<Candle>,
}

/// Batch of order book snapshots/updates
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct OrderBookBatch {
    /// Topic (e.g. Depth)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Order books in this batch
    pub books: Vec<crate::base_data::OrderBookSnapShot>,
}

/// Vendor-specific binary data batch
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct VendorData {
    /// Topic
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Opaque vendor data
    pub data: Vec<u8>,
}

/// Batch of order updates (lossless)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct OrdersBatch {
    /// Topic (e.g. Orders)
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Orders in this batch
    pub orders: Vec<crate::accounts::events::OrderUpdate>,
}

/// Batch of position updates (lossless)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct PositionsBatch {
    /// Topic (e.g. Positions)
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Positions in this batch
    pub positions: Vec<crate::accounts::events::PositionDelta>,
}

/// Batch of account delta updates (lossless)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct AccountDeltaBatch {
    /// Topic (e.g. AccountEvt)
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Account deltas in this batch
    pub accounts: Vec<crate::accounts::events::AccountDelta>,
}

/// Request for available instruments from a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct InstrumentsRequest {
    /// Provider to query
    pub provider: ProviderKind,
    /// Optional pattern to filter instruments
    pub pattern: Option<String>,
    /// Correlation ID for matching response
    pub corr_id: u64,
}

/// Response with available instruments from a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct InstrumentsResponse {
    /// Provider
    pub provider: ProviderKind,
    /// List of instruments
    pub instruments: Vec<Instrument>,
    /// Correlation ID
    pub corr_id: u64,
}

/// Minimal wire representation of a FuturesContract
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct FuturesContractWire {
    /// Instrument identifier
    pub instrument: Instrument,
    /// Provider identifier
    pub provider_id: ProviderKind,
    /// Exchange
    pub exchange: Exchange,
    /// Tick size
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub tick_size: Decimal,
    /// Value per tick
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub value_per_tick: Decimal,
    /// Number of decimal places
    pub decimal_accuracy: u32,
    /// True if this is a continuous contract
    pub is_continuous: bool,
}

impl FuturesContractWire {
    pub fn from_contract(fc: &FuturesContract) -> Self {
        FuturesContractWire {
            instrument: fc.instrument.clone(),
            provider_id: fc.provider_id.clone(),
            exchange: fc.exchange.clone(),
            tick_size: fc.tick_size.clone(),
            value_per_tick: fc.value_per_tick.clone(),
            decimal_accuracy: fc.decimal_accuracy,
            is_continuous: fc.is_continuous,
        }
    }
}

/// Response with a map of instruments and their contract details
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct InstrumentsMapResponse {
    /// Provider name
    pub provider: String,
    /// Pairs of (Instrument, FuturesContractWire)
    pub instruments: Vec<(Instrument, FuturesContractWire)>,
    /// Correlation ID
    pub corr_id: u64,
}

/// Request for a full map of futures contracts from a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct InstrumentsMapRequest {
    /// Provider to query
    pub provider: ProviderKind,
    /// Correlation ID for matching response
    pub corr_id: u64,
}

/// Authentication credentials for a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct UnsubscribeAll {
    /// Optional reason for unsubscribe
    pub reason: Option<String>,
}

/// Client-initiated disconnect request (ask the router to kick this connection)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct Kick {
    /// Optional reason for disconnect
    pub reason: Option<String>,
}

/// Unsubscribe from a specific (topic, key) pair
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeKey {
    /// Topic
    pub topic: Topic,
    /// Symbol key
    pub key: SymbolKey,
}

/// Announce a shared memory (SHM) snapshot stream
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct BracketWire {
    /// Number of ticks for bracket
    pub ticks: i32,
    /// Order type
    pub r#type: OrderTypeWire,
}

/// Place a new order
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct CancelOrder {
    /// Account ID
    pub account_id: i64,
    /// Provider order ID (if known)
    pub provider_order_id: Option<String>,
    /// Client order ID (if known)
    pub client_order_id: Option<crate::accounts::events::ClientOrderId>,
}

/// Replace (modify) an existing order
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct SubscribeAccount {
    /// Account key
    pub key: crate::keys::AccountKey,
}

/// Unsubscribe from all execution streams for an account
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct UnsubscribeAccount {
    /// Account key
    pub key: crate::keys::AccountKey,
}

/// Request account info for a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct AccountInfoRequest {
    /// Provider
    pub provider: ProviderKind,
    /// Correlation ID
    pub corr_id: u64,
}

/// Summary of an account (for info response)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct AccountSummaryWire {
    /// Account ID
    pub account_id: i64,
    /// Account name
    pub account_name: String,
    /// Provider
    pub provider: ProviderKind,
}

/// Response with account info for a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct AccountInfoResponse {
    /// Provider
    pub provider: ProviderKind,
    /// Correlation ID
    pub corr_id: u64,
    /// Accounts
    pub accounts: Vec<AccountSummaryWire>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
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

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
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
    OrderBookBatch(OrderBookBatch),
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

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub enum WireMessage {
    Request(Request),
    Response(Response),
}

pub mod codec {
    use super::{ArchivedWireMessage, WireMessage};
    use rkyv::rancor::Error;

    pub fn encode(env: &WireMessage) -> Vec<u8> {
        rkyv::to_bytes::<Error>(env).expect("serialize").to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<WireMessage, String> {
        // Ensure alignment by copying into an AlignedVec before accessing
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        let arch =
            rkyv::access::<ArchivedWireMessage, Error>(&aligned[..]).map_err(|e| e.to_string())?;
        rkyv::deserialize::<WireMessage, Error>(arch).map_err(|e| e.to_string())
    }
}
