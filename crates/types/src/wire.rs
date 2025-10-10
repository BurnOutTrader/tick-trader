use crate::accounts::account::AccountName;
use crate::accounts::events::{ProviderOrderId, Side};
use crate::data::core::{Bbo, Candle, Tick};
use crate::engine_id::EngineUuid;
use crate::keys::{SymbolKey, Topic};
use crate::providers::ProviderKind;
use crate::securities::security::FuturesContract;
use crate::securities::symbols::Instrument;
use ahash::HashMap;
use chrono::{DateTime, Utc};
use rkyv::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;
use crate::data::mbp10::Mbp10;

pub const ENGINE_TAG_PREFIX: &str = "+eId:";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct Trade {
    pub id: EngineUuid,
    pub provider: ProviderKind,
    pub account_name: AccountName,
    pub instrument: Instrument,
    pub creation_time: DateTime<Utc>,
    pub price: Decimal,
    pub profit_and_loss: Decimal,
    pub fees: Decimal,
    pub side: Side,
    pub size: Decimal,
    pub voided: bool,
    pub order_id: String,
}

/// Request to subscribe to a topic (coarse, topic-level interest)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct Ping {
    /// Nanosecond timestamp
    pub ts_ns: DateTime<Utc>,
}
/// Pong reply (heartbeat)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct Pong {
    /// Nanosecond timestamp
    pub ts_ns: DateTime<Utc>,
}

/// Batch of ticks for a topic/sequence
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct TickBatch {
    /// Topic (e.g. Ticks)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Ticks in this batch
    pub ticks: Vec<Tick>,
    ///The provider of the data
    pub provider_kind: ProviderKind,
}

/// Batch of quotes (BBOs) for a topic/sequence
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct QuoteBatch {
    /// Topic (e.g. Quotes)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Quotes in this batch
    pub quotes: Vec<Bbo>,
    ///The provider of the data
    pub provider_kind: ProviderKind,
}

/// Batch of OHLC bars for a topic/sequence
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct BarBatch {
    /// Topic (e.g. Bars1m)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Bars in this batch
    pub bars: Vec<Candle>,
    ///The provider of the data
    pub provider_kind: ProviderKind,
}

/// Batch of order book snapshots/updates
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct MBP10Batch {
    /// Topic (e.g. Depth)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    pub event: crate::data::mbp10::Mbp10,
    ///The provider of the data
    pub provider_kind: ProviderKind,
}

/// Vendor-specific binary data batch
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct VendorData {
    /// Topic
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Opaque vendor data
    pub data: Vec<u8>,
}

/// Batch of order updates (lossless)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct OrdersBatch {
    /// Topic (e.g. Orders)
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Orders in this batch
    pub orders: Vec<crate::accounts::events::OrderUpdate>,
}

impl OrdersBatch {
    pub fn print(&self) {
        for order in self.orders.iter() {
            println!(
                "OrderUpdate: {:?}, Provider: {:?}, Price: {}, Qty: {} ",
                order.instrument, order.state, order.avg_fill_px, order.cum_qty
            );
        }
    }
}

/// Batch of position updates (lossless)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct PositionsBatch {
    /// Topic (e.g. Positions)
    pub topic: Topic,
    /// Sequence number (monotonic per topic)
    pub seq: u64,
    /// Positions in this batch
    pub positions: Vec<crate::accounts::events::PositionDelta>,
}

/// Batch of account delta updates (lossless)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct AccountDeltaBatch {
    /// Topic (e.g. AccountEvt)
    pub topic: Topic,
    /// Sequence number
    pub seq: u64,
    /// Account deltas in this batch
    pub accounts: Vec<crate::accounts::events::AccountDelta>,
}

/// Request for available instruments from a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct InstrumentsRequest {
    /// Provider to query
    pub provider: ProviderKind,
    /// Optional pattern to filter instruments
    pub pattern: Option<String>,
    /// Correlation ID for matching response
    pub corr_id: u64,
}

/// Response with available instruments from a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct InstrumentsResponse {
    /// Provider
    pub provider: ProviderKind,
    /// List of instruments
    pub instruments: Vec<Instrument>,
    /// Correlation ID
    pub corr_id: u64,
}

/// Response with a map of instruments and their contract details
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct ContractsResponse {
    /// Provider name
    pub provider: String,
    /// Pairs of (Instrument, FuturesContractWire)
    pub instruments: Vec<FuturesContract>,
    /// Correlation ID
    pub corr_id: u64,
}

/// Request for a full map of futures contracts from a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct InstrumentsMapRequest {
    /// Provider to query
    pub provider: ProviderKind,
    /// Correlation ID for matching response
    pub corr_id: u64,
}

/// Authentication credentials for a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
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
    pub extra: HashMap<String, String>,
}

/// Unsubscribe from all topics/keys (clean detach)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct UnsubscribeAll {
    /// Optional reason for unsubscribe
    pub reason: Option<String>,
}

/// Client-initiated disconnect request (ask the router to kick this connection)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct Kick {
    /// Optional reason for disconnect
    pub reason: Option<String>,
}

/// Unsubscribe from a specific (topic, key) pair
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct UnsubscribeKey {
    /// Topic
    pub topic: Topic,
    /// Symbol key
    pub key: SymbolKey,
}

/// Announce a shared memory (SHM) snapshot stream
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub enum OrderType {
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct BracketWire {
    /// Number of ticks for bracket
    pub ticks: i32,
    /// Order type
    pub r#type: OrderType,
}

/// Place a new order
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct PlaceOrder {
    /// Account name
    pub account_name: crate::accounts::account::AccountName,
    /// Symbol key
    pub key: SymbolKey,
    /// Side (buy/sell)
    pub side: crate::accounts::events::Side,
    /// Quantity
    pub qty: i64,
    /// Order type
    pub r#type: OrderType,
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct CancelOrder {
    /// Account ID
    pub account_name: AccountName,
    /// Provider order ID (if known)
    pub provider_order_id: String,
}

/// Replace (modify) an existing order
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct ReplaceOrder {
    /// Account ID
    pub account_name: AccountName,
    /// Provider order ID (if known)
    pub provider_order_id: String,
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
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct SubscribeAccount {
    /// Account key
    pub key: crate::keys::AccountKey,
}

/// Unsubscribe from all execution streams for an account
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct UnsubscribeAccount {
    /// Account key
    pub key: crate::keys::AccountKey,
}

/// Request account info for a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct AccountInfoRequest {
    /// Provider
    pub provider: ProviderKind,
    /// Correlation ID
    pub corr_id: u64,
}

/// Summary of an account (for info response)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct AccountSummaryWire {
    /// Account ID
    pub account_id: i64,
    /// Account name
    pub account_name: AccountName,
    /// Provider
    pub provider: ProviderKind,
}

/// Response with account info for a provider
#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub struct AccountInfoResponse {
    /// Provider
    pub provider: ProviderKind,
    /// Correlation ID
    pub corr_id: u64,
    /// Accounts
    pub accounts: Vec<AccountSummaryWire>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
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

#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub enum Response {
    // Control replies
    Pong(Pong),
    // Router → Client announcements
    AnnounceShm(AnnounceShm),
    // Instruments
    InstrumentsResponse(InstrumentsResponse),
    InstrumentsMapResponse(ContractsResponse),
    // Account info (startup)
    AccountInfoResponse(AccountInfoResponse),
    // Data (batches first)
    TickBatch(TickBatch),
    QuoteBatch(QuoteBatch),
    BarBatch(BarBatch),
    MBP10Batch(MBP10Batch),
    VendorData(VendorData),
    OrdersBatch(OrdersBatch),
    PositionsBatch(PositionsBatch),
    AccountDeltaBatch(AccountDeltaBatch),
    ClosedTrades(Vec<Trade>),
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
    Tick {
        tick: Tick,
        provider_kind: ProviderKind,
    },
    Quote {
        bbo: Bbo,
        provider_kind: ProviderKind,
    },
    Bar {
        candle: Candle,
        provider_kind: ProviderKind,
    },
    Mbp10 {
        mbp10: Mbp10,
        provider_kind: ProviderKind,
    }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, PartialEq, Clone, Debug)]
#[archive(check_bytes)]
pub enum WireMessage {
    Request(Request),
    Response(Response),
}
impl Bytes<Self> for WireMessage {
    fn from_bytes(data: &[u8]) -> anyhow::Result<WireMessage> {
        // Ensure rkyv-required alignment by copying into an AlignedVec before deserializing.
        // Network/UDS transports don't preserve alignment of the destination buffer, and
        // tokio/bytes may yield slices with only 4-byte alignment.
        let mut aligned = AlignedVec::with_capacity(data.len());
        aligned.extend_from_slice(data);
        match rkyv::from_bytes::<WireMessage>(&aligned) {
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }

    fn to_aligned_bytes(&self) -> AlignedVec {
        // Serialize directly into an AlignedVec for maximum compatibility with rkyv
        rkyv::to_bytes::<_, 1024>(self).expect("rkyv::to_bytes failed")
    }
}

pub trait Bytes<T> {
    fn from_bytes(data: &[u8]) -> anyhow::Result<T>;

    // Prefer this when you need rkyv-compatible alignment in the produced buffer.
    // Note: network transports don't preserve alignment guarantees across processes;
    // the receiver should still ensure alignment before deserializing.
    fn to_aligned_bytes(&self) -> AlignedVec
    where
        Self: Sized,
    {
        unreachable!("default impl should be overridden")
    }
}

pub trait VecBytes<T> {
    fn vec_to_aligned(data: &Vec<T>) -> AlignedVec;

    fn from_array_bytes(data: &Vec<u8>) -> anyhow::Result<Vec<T>>;
}
