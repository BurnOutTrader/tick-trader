use crate::securities::symbols::Instrument;
use crate::wire::Bytes;
use chrono::{DateTime, Utc};
use rkyv::{AlignedVec, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct ProviderOrderId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct ClientOrderId(pub crate::Guid);

impl ClientOrderId {
    #[inline]
    pub fn new() -> Self {
        Self(crate::Guid::new_v4())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct ExecId(pub crate::Guid);

impl ExecId {
    #[inline]
    pub fn new() -> Self {
        Self(crate::Guid::new_v4())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    #[inline]
    pub fn sign(self) -> i32 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct OrderEvent {
    pub kind: OrderEventKind,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub provider_seq: Option<u64>,
    pub leaves_qty: Option<i64>,
    pub ts_ns: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum OrderEventKind {
    NewAck {
        instrument: Instrument,
        side: Side,
        qty: i64,
    },
    Replaced {
        new_qty: Option<i64>,
    },
    Canceled,
    Rejected {
        reason: String,
    },
    Expired,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct ExecutionEvent {
    pub exec_id: ExecId,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub side: Side,
    pub qty: i64,
    pub price: Decimal,

    pub fee: Decimal,

    pub ts_ns: DateTime<Utc>,
    pub provider_seq: Option<u64>,
    pub instrument: Instrument,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct CorrectionEvent {
    pub exec_id_ref: ExecId,
    pub delta_qty: i64,

    pub ts_ns: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum AdminEvent {
    SnapshotFromProvider,
    ClockSync,
    RiskUpdate,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct MarkEvent {
    pub instrument: Instrument,

    pub mark_px: Decimal,

    pub ts_ns: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub enum AccountEvent {
    Order(OrderEvent),
    Exec(ExecutionEvent),
    Correction(CorrectionEvent),
    Admin(AdminEvent),
    Mark(MarkEvent),
}
impl Bytes<Self> for AccountEvent {
    fn from_bytes(archived: &[u8]) -> anyhow::Result<AccountEvent> {
        // If the archived bytes do not end with the delimiter, proceed as before
        match rkyv::from_bytes::<AccountEvent>(archived) {
            //Ignore this warning: Trait `Deserialize<ResponseType, SharedDeserializeMap>` is not implemented for `ArchivedRequestType` [E0277]
            Ok(response) => Ok(response),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }

    fn to_aligned_bytes(&self) -> AlignedVec {
        // Serialize directly into an AlignedVec for maximum compatibility with rkyv
        rkyv::to_bytes::<_, 1024>(self).expect("rkyv::to_bytes failed")
    }
}

// Outbound projections
#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct OrderUpdate {
    pub instrument: Instrument,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub state_code: u8,
    pub leaves: i64,
    pub cum_qty: i64,

    pub avg_fill_px: Decimal,

    pub ts_ns: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct PositionDelta {
    pub instrument: Instrument,
    pub net_qty_before: i64,
    pub net_qty_after: i64,

    pub realized_delta: Decimal,

    pub open_pnl: Decimal,

    pub ts_ns: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct AccountDelta {
    pub equity: Decimal,

    pub day_realized_pnl: Decimal,

    pub open_pnl: Decimal,

    pub ts_ns: DateTime<Utc>,
    pub can_trade: bool,
}
