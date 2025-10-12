use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::PlaceOrder;

/// Limit-order matching policy
#[derive(Debug, Clone, Copy)]
pub enum LimitPolicy {
    AtOrBetter,
    TouchOrCross,
    NextTickOnly,
}

/// Market-order matching policy
#[derive(Debug, Clone, Copy)]
pub enum MarketPolicy {
    AtMid,
    AtTouch,
    BookWalk,
}

/// Stop trigger source
#[derive(Debug, Clone, Copy)]
pub enum StopTrigger {
    Trade,
    BidAsk,
    Last,
}

/// One execution fill.
#[derive(Debug, Clone)]
pub struct Fill {
    pub instrument: Instrument,
    pub qty: i64,
    pub price: Decimal,
    pub maker: bool,
}

/// Context for fees.
#[derive(Debug, Clone)]
pub struct FeeCtx {
    pub sim_time: DateTime<Utc>,
    pub instrument: Instrument,
}

/// Context for risk decisions.
#[derive(Debug, Clone)]
pub struct RiskCtx {
    pub sim_time: DateTime<Utc>,
    pub equity: Decimal,
    pub day_realized_pnl: Decimal,
    pub open_pnl: Decimal,
}

/// Proposed portfolio change for margin checks.
#[derive(Debug, Clone)]
pub struct ProposedPortfolioChange {
    pub instrument: Instrument,
    pub delta_qty: i64,
}

/// Risk engine decision on incoming requests.
#[derive(Debug, Clone)]
pub enum RiskDecision {
    Allow,
    Revise(PlaceOrder),
    Reject { reason: &'static str },
}

/// Post-fill actions after risk evaluation.
#[derive(Debug, Clone)]
pub enum PostFillAction {
    None,
    FlattenAll {
        reason: &'static str,
    },
    FlattenInstr {
        instrument: Instrument,
        reason: &'static str,
    },
}

/// Simple money type for simulator accounting (avoid external coupling here).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Money {
    pub amount: Decimal,
}

impl Money {
    pub const ZERO: Money = Money {
        amount: Decimal::ZERO,
    };
}
