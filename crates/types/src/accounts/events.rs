use crate::accounts::account::AccountName;
use crate::accounts::order::OrderState;
use crate::engine_id::EngineUuid;
use crate::keys::AccountKey;
use crate::providers::ProviderKind;
use crate::securities::symbols::Instrument;
use crate::wire::Bytes;
use chrono::{DateTime, Utc};
use rkyv::{AlignedVec, Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;
use strum_macros::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct ProviderOrderId(pub String);

#[derive(
    Debug, Clone, Copy, Display, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize,
)]
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
    /// Normalize a quantity to the platform-wide convention:
    /// - Quantities are always positive magnitudes
    /// - Side conveys direction (Buy vs Sell)
    /// - Zero is invalid and returned as zero for caller to validate
    #[inline]
    pub fn normalize_qty(self, qty: i64) -> i64 {
        qty.abs()
    }
    /// Returns true if the given quantity follows the standard convention (positive, non-zero)
    #[inline]
    pub fn is_qty_sign_valid(self, qty: i64) -> bool {
        qty > 0
    }
    /// Returns the magnitude of the quantity (always non-negative)
    #[inline]
    pub fn unsigned_qty(_self: Self, qty: i64) -> u64 {
        qty.unsigned_abs()
    }
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct OrderEvent {
    pub kind: OrderEventKind,
    pub provider_order_id: Option<ProviderOrderId>,
    pub order_id: EngineUuid,
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
    pub exec_id: EngineUuid,
    pub provider_order_id: Option<ProviderOrderId>,
    pub order_id: EngineUuid,
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
    pub exec_id_ref: EngineUuid,
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
    pub name: AccountName,
    pub instrument: Instrument,
    pub provider_kind: ProviderKind,
    pub provider_order_id: Option<ProviderOrderId>,
    pub order_id: EngineUuid,
    pub state: OrderState,
    pub leaves: i64,
    pub cum_qty: i64,
    pub side: Side,

    pub avg_fill_px: Decimal,
    pub tag: Option<String>,

    pub time: DateTime<Utc>,
}
impl OrderUpdate {
    pub fn to_clean_string(&self) -> String {
        format!(
            "Order Update: {}, {} {}:{} filled:{} remaining:{} @{} ",
            self.provider_kind,
            self.instrument,
            self.side,
            self.state,
            self.cum_qty,
            self.leaves,
            self.time
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Archive, RkyvDeserialize, RkyvSerialize, Display)]
#[archive(check_bytes)]
pub enum PositionSide {
    Long,
    Short,
    Flat,
}
#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct PositionDelta {
    pub instrument: Instrument,
    pub account_name: AccountName,
    pub provider_kind: ProviderKind,
    pub net_qty: Decimal,
    pub average_price: Decimal,
    pub open_pnl: Decimal,
    pub time: DateTime<Utc>,
    pub side: PositionSide,
}
impl PositionDelta {
    pub fn to_clean_string(&self) -> String {
        format!(
            "Position Delta: {}:{}, {}, {}:{}@{} open pnl:{}@{} ",
            self.provider_kind,
            self.account_name,
            self.account_name,
            self.side,
            self.net_qty,
            self.average_price,
            self.open_pnl,
            self.time
        )
    }
}

#[derive(Debug, Clone, PartialEq, Archive, RkyvDeserialize, RkyvSerialize)]
#[archive(check_bytes)]
pub struct AccountDelta {
    pub provider_kind: ProviderKind,

    pub name: AccountName,

    pub key: AccountKey,

    pub equity: Decimal,

    pub day_realized_pnl: Decimal,

    pub open_pnl: Decimal,

    pub time: DateTime<Utc>,

    pub can_trade: bool,
}
impl AccountDelta {
    pub fn to_clean_string(&self) -> String {
        format!(
            "Account Delta: {}:{}, equity:{}, day_pnl:{}, open_pnl:{}, can_trade:{}, @{}",
            self.provider_kind,
            self.name,
            self.equity,
            self.day_realized_pnl,
            self.open_pnl,
            self.can_trade,
            self.time
        )
    }
}
