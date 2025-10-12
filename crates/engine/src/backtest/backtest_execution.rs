use crate::backtest::default_models::FillConfig;
use crate::traits::{FeeModel, FillModel, LatencyModel, RiskModel, SessionCalendar, SlippageModel};
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

#[allow(dead_code)]
/// Placeholder provider type; expanded in future iterations.
pub struct FauxExecutionProvider<
    M: FillModel,
    S: SlippageModel,
    L: LatencyModel,
    F: FeeModel,
    C: SessionCalendar,
    R: RiskModel,
> {
    #[allow(dead_code)]
    cfg: FillConfig,
    #[allow(dead_code)]
    fill: M,
    #[allow(dead_code)]
    slip: S,
    #[allow(dead_code)]
    lat: L,
    #[allow(dead_code)]
    fees: F,
    #[allow(dead_code)]
    cal: C,
    #[allow(dead_code)]
    risk: R,
    #[allow(dead_code)]
    seed: u64,
}

impl<M, S, L, F, C, R> FauxExecutionProvider<M, S, L, F, C, R>
where
    M: FillModel,
    S: SlippageModel,
    L: LatencyModel,
    F: FeeModel,
    C: SessionCalendar,
    R: RiskModel,
{
    #[allow(clippy::too_many_arguments, dead_code)]
    pub fn new(
        cfg: FillConfig,
        fill: M,
        slip: S,
        lat: L,
        fees: F,
        cal: C,
        risk: R,
        seed: u64,
    ) -> Self {
        Self {
            cfg,
            fill,
            slip,
            lat,
            fees,
            cal,
            risk,
            seed,
        }
    }
}
