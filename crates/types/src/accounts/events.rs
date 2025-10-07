use crate::securities::symbols::Instrument;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
)]
pub struct ProviderOrderId(pub String);

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
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
    Serialize,
    Deserialize,
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
    Serialize,
    Deserialize,
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
    Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub struct OrderEvent {
    pub kind: OrderEventKind,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub provider_seq: Option<u64>,
    pub leaves_qty: Option<i64>,
    pub ts_ns: i64,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize,
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
    Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub struct ExecutionEvent {
    pub exec_id: ExecId,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub side: Side,
    pub qty: i64,
    pub price: Decimal,
    pub fee: Decimal,
    pub ts_ns: i64,
    pub provider_seq: Option<u64>,
    pub instrument: Instrument,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub struct CorrectionEvent {
    pub exec_id_ref: ExecId,
    pub delta_qty: i64,
    pub ts_ns: i64,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub enum AdminEvent {
    SnapshotFromProvider,
    ClockSync,
    RiskUpdate,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub struct MarkEvent {
    pub instrument: Instrument,
    pub mark_px: Decimal,
    pub ts_ns: i64,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize,
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
    Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub struct OrderUpdate {
    pub instrument: Instrument,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub state_code: u8,
    pub leaves: i64,
    pub cum_qty: i64,
    pub avg_fill_px: Decimal,
    pub ts_ns: i64,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub struct PositionDelta {
    pub instrument: Instrument,
    pub net_qty_before: i64,
    pub net_qty_after: i64,
    pub realized_delta: Decimal,
    pub open_pnl: Decimal,
    pub ts_ns: i64,
}

#[derive(
    Debug, Clone, PartialEq, Serialize, Deserialize,
)]
pub struct AccountDelta {
    pub equity: Decimal,
    pub day_realized_pnl: Decimal,
    pub open_pnl: Decimal,
    pub ts_ns: i64,
    pub can_trade: bool,
}
