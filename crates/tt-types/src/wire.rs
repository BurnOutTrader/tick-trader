use crate::base_data::{Bbo, Candle, Exchange, Tick};
use crate::keys::{SymbolKey, Topic};
use crate::providers::ProviderKind;
use crate::securities::security::FuturesContract;
use crate::securities::symbols::Instrument;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Subscribe {
    pub topic: Topic,
    pub latest_only: bool,
    pub from_seq: u64,
}
impl Subscribe {
    pub fn topic(&self) -> Topic {
        self.topic
    }
}

// Key-based subscribe (new control plane message)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
pub struct SubscribeKey {
    pub topic: Topic,
    pub key: SymbolKey,
    pub latest_only: bool,
    pub from_seq: u64,
}
impl SubscribeKey {
    pub fn topic(&self) -> Topic { self.topic }
    pub fn key(&self) -> &SymbolKey { &self.key }
}


#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Ping {
    pub ts_ns: i64,
}
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Pong {
    pub ts_ns: i64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct TickBatch {
    pub topic: Topic,
    pub seq: u64,
    pub ticks: Vec<Tick>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct QuoteBatch {
    pub topic: Topic,
    pub seq: u64,
    pub quotes: Vec<Bbo>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct BarBatch {
    pub topic: Topic,
    pub seq: u64,
    pub bars: Vec<Candle>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct OrderBookBatch {
    pub topic: Topic,
    pub seq: u64,
    pub books: Vec<crate::base_data::OrderBook>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct VendorData {
    pub topic: Topic,
    pub seq: u64,
    pub data: Vec<u8>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct OrdersBatch {
    pub topic: Topic,
    pub seq: u64,
    pub orders: Vec<crate::accounts::events::OrderUpdate>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct PositionsBatch {
    pub topic: Topic,
    pub seq: u64,
    pub positions: Vec<crate::accounts::events::PositionDelta>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct AccountDeltaBatch {
    pub topic: Topic,
    pub seq: u64,
    pub accounts: Vec<crate::accounts::events::AccountDelta>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct MdSubscribeCmd {
    pub provider: ProviderKind,
    pub topic: Topic,
    pub key: SymbolKey,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct MdUnsubscribeCmd {
    pub provider: ProviderKind,
    pub topic: Topic,
    pub key: SymbolKey,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct InstrumentsRequest {
    pub provider: ProviderKind,
    pub pattern: Option<String>,
    pub corr_id: u64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct InstrumentsResponse {
    pub provider: ProviderKind,
    pub instruments: Vec<Instrument>,
    pub corr_id: u64,
}

// Minimal wire representation of a FuturesContract
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct FuturesContractWire {
    pub instrument: Instrument,
    pub provider_id: ProviderKind,
    pub exchange: Exchange,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub tick_size: Decimal,
    #[rkyv(with = crate::rkyv_types::DecimalDef)]
    pub value_per_tick: Decimal,
    pub decimal_accuracy: u32,
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

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct InstrumentsMapResponse {
    pub provider: String,
    /// Pairs of (Instrument string, FuturesContractWire)
    pub instruments: Vec<(Instrument, FuturesContractWire)>,
    pub corr_id: u64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct AuthCredentials {
    pub provider: ProviderKind,
    pub username: Option<String>,
    pub password: Option<String>,
    pub api_key: Option<String>,
    pub secret: Option<String>,
    /// For provider-specific fields
    pub extra: Vec<(String, String)>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct UnsubscribeAll {
    pub reason: Option<String>,
}

// Client-initiated disconnect request (ask the router to kick this connection)
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct Kick {
    pub reason: Option<String>,
}

// Key-based unsubscribe and flow credit
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
pub struct UnsubscribeKey {
    pub topic: Topic,
    pub key: SymbolKey,
}


#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub struct AnnounceShm {
    pub topic: Topic,
    pub key: SymbolKey,
    pub name: String,
    pub layout_ver: u32,
    pub size: u64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub enum Request {
    // Control from clients to server (coarse)
    Subscribe(Subscribe),
    // New key-based control
    SubscribeKey(SubscribeKey),
    UnsubscribeKey(UnsubscribeKey),
    Ping(Ping),
    UnsubscribeAll(UnsubscribeAll),
    // Client-initiated disconnect (request to be kicked)
    Kick(Kick),
    // Provider commands initiated by clients (legacy; to be removed)
    MdSubscribe(MdSubscribeCmd),
    MdUnsubscribe(MdUnsubscribeCmd),
    InstrumentsRequest(InstrumentsRequest),
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
    // Data (batches first)
    TickBatch(TickBatch),
    QuoteBatch(QuoteBatch),
    BarBatch(BarBatch),
    OrderBookBatch(OrderBookBatch),
    VendorData(VendorData),
    OrdersBatch(OrdersBatch),
    PositionsBatch(PositionsBatch),
    AccountDeltaBatch(AccountDeltaBatch),
    SubscribeResponse{topic: Topic, instrument: Instrument, success: bool},
    UnsubscribeResponse{topic: Topic, instrument: Instrument},
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
