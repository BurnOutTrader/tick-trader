use crate::securities::symbols::Instrument;
use rust_decimal::Decimal;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use chrono::{DateTime, Utc};

use crate::Guid;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
)]
pub struct ProviderOrderId(pub String);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
)]
pub struct ClientOrderId(pub crate::Guid);

impl ClientOrderId {
    #[inline]
    pub fn new() -> Self {
        Self(crate::Guid::new_v4())
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
)]
pub struct ExecId(pub crate::Guid);

impl ExecId {
    #[inline]
    pub fn new() -> Self {
        Self(crate::Guid::new_v4())
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Archive,
    RkyvDeserialize,
    RkyvSerialize,
)]
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

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub struct OrderEvent {
    pub kind: OrderEventKind,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub provider_seq: Option<u64>,
    pub leaves_qty: Option<i64>,
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub ts_ns: DateTime<Utc>,
}

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
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

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub struct ExecutionEvent {
    pub exec_id: ExecId,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub side: Side,
    pub qty: i64,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub price: Decimal,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub fee: Decimal,
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub ts_ns: DateTime<Utc>,
    pub provider_seq: Option<u64>,
    pub instrument: Instrument,
}

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub struct CorrectionEvent {
    pub exec_id_ref: ExecId,
    pub delta_qty: i64,
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub ts_ns: DateTime<Utc>,
}

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub enum AdminEvent {
    SnapshotFromProvider,
    ClockSync,
    RiskUpdate,
}

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub struct MarkEvent {
    pub instrument: Instrument,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub mark_px: Decimal,
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub ts_ns: DateTime<Utc>,
}

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub enum AccountEvent {
    Order(OrderEvent),
    Exec(ExecutionEvent),
    Correction(CorrectionEvent),
    Admin(AdminEvent),
    Mark(MarkEvent),
}

// Outbound projections
#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub struct OrderUpdate {
    pub instrument: Instrument,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub state_code: u8,
    pub leaves: i64,
    pub cum_qty: i64,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub avg_fill_px: Decimal,
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub ts_ns: DateTime<Utc>,
}

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub struct PositionDelta {
    pub instrument: Instrument,
    pub net_qty_before: i64,
    pub net_qty_after: i64,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub realized_delta: Decimal,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub open_pnl: Decimal,
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub ts_ns: DateTime<Utc>,
}

#[derive(
    Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize,
)]
pub struct AccountDelta {
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub equity: Decimal,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub day_realized_pnl: Decimal,
    #[rkyv(with = "crate::rkyv_types::DecimalDef")]
    pub open_pnl: Decimal,
    #[rkyv(with = "crate::rkyv_types::DateTimeUtcDef")]
    pub ts_ns: DateTime<Utc>,
    pub can_trade: bool,
}
