use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use crate::rkyv_types::DecimalDef;
use crate::securities::symbols::Instrument;

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ProviderOrderId(pub String);

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ClientOrderId(pub String);

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ExecId(pub String);

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side { Buy, Sell }

impl Side {
    #[inline]
    pub fn sign(self) -> i32 { match self { Side::Buy => 1, Side::Sell => -1 } }
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderEvent {
    pub kind: OrderEventKind,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub provider_seq: Option<u64>,
    pub leaves_qty: Option<i64>,
    pub ts_ns: i64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OrderEventKind {
    NewAck { instrument: Instrument, side: Side, qty: i64 }, 
    Replaced { new_qty: Option<i64> },
    Canceled,
    Rejected { reason: String },
    Expired,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub exec_id: ExecId,
    pub provider_order_id: Option<ProviderOrderId>,
    pub client_order_id: Option<ClientOrderId>,
    pub side: Side,
    pub qty: i64,
    #[rkyv(with = DecimalDef)]
    pub price: Decimal,
    #[rkyv(with = DecimalDef)]
    pub fee: Decimal,
    pub ts_ns: i64,
    pub provider_seq: Option<u64>,
    pub instrument: Instrument,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CorrectionEvent {
    pub exec_id_ref: ExecId,
    pub delta_qty: i64,
    pub ts_ns: i64,
}

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AdminEvent { SnapshotFromProvider, ClockSync, RiskUpdate }

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkEvent { pub instrument: Instrument, #[rkyv(with = DecimalDef)] pub mark_px: Decimal, pub ts_ns: i64 }

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AccountEvent {
    Order(OrderEvent),
    Exec(ExecutionEvent),
    Correction(CorrectionEvent),
    Admin(AdminEvent),
    Mark(MarkEvent),
}

// Outbound projections
#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderUpdate { pub provider_order_id: Option<ProviderOrderId>, pub client_order_id: Option<ClientOrderId>, pub state_code: u8, pub leaves: i64, pub cum_qty: i64, #[rkyv(with = DecimalDef)] pub avg_fill_px: Decimal, pub ts_ns: i64 }

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionDelta { pub instrument: Instrument, pub net_qty_before: i64, pub net_qty_after: i64, #[rkyv(with = DecimalDef)] pub realized_delta: Decimal, #[rkyv(with = DecimalDef)] pub open_pnl: Decimal, pub ts_ns: i64 }

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountDelta { #[rkyv(with = DecimalDef)] pub equity: Decimal, #[rkyv(with = DecimalDef)] pub day_realized_pnl: Decimal, #[rkyv(with = DecimalDef)] pub open_pnl: Decimal, pub ts_ns: i64 }
