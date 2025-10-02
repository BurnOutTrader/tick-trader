use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use crate::base_data::{Bbo, Candle, Tick};
use crate::keys::Topic;

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Subscribe {
    pub topic: Topic,
    pub latest_only: bool,
    pub from_seq: u64,
}
impl Subscribe {
    pub fn topic(&self) -> Topic { self.topic }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct FlowCredit {
    pub topic: Topic,
    pub credits: u32,
}
impl FlowCredit {
    pub fn topic(&self) -> Topic { self.topic }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Ping { pub ts_ns: i64 }
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct Pong { pub ts_ns: i64 }

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

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(compare(PartialEq), derive(Debug))]
pub struct VendorData {
    pub topic: Topic,
    pub seq: u64,
    pub data: Vec<u8>,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq)]
pub enum Envelope {
    // Control
    Subscribe(Subscribe),
    FlowCredit(FlowCredit),
    Ping(Ping),
    Pong(Pong),
    // Data (batches first)
    TickBatch(TickBatch),
    QuoteBatch(QuoteBatch),
    BarBatch(BarBatch),
    VendorData(VendorData),
    // Single items (will be wrapped by bus into batches for fan-out)
    Tick(Tick),
    Quote(Bbo),
    Bar(Candle),
}

pub mod codec {
    use super::{Envelope, ArchivedEnvelope};
    use rkyv::rancor::Error;

    pub fn encode(env: &Envelope) -> Vec<u8> {
        rkyv::to_bytes::<Error>(env).expect("serialize").to_vec()
    }

    pub fn decode(bytes: &[u8]) -> Result<Envelope, String> {
        // Ensure alignment by copying into an AlignedVec before accessing
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        let arch = rkyv::access::<ArchivedEnvelope, Error>(&aligned[..])
            .map_err(|e| e.to_string())?;
        rkyv::deserialize::<Envelope, Error>(arch).map_err(|e| e.to_string())
    }
}
