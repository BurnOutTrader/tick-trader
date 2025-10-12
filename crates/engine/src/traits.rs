use crate::backtest::backtest_execution::Money;
use crate::backtest::backtest_execution::{
    FeeCtx, Fill, PostFillAction, ProposedPortfolioChange, RiskCtx, RiskDecision,
};
use crate::handle::EngineHandle;
use crate::models::DataTopic;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use std::time::Duration;
use tt_types::accounts::events::{AccountDelta, PositionDelta, Side};
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::{BookLevels, Mbp10};
use tt_types::keys::AccountKey;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{OrdersBatch, PlaceOrder, PositionsBatch, ReplaceOrder, Trade};

pub trait Strategy: Send + 'static {
    fn on_start(&mut self, _h: EngineHandle) {}
    fn on_stop(&mut self) {}

    fn on_tick(&mut self, _t: &Tick, _provider_kind: ProviderKind) {}
    fn on_quote(&mut self, _q: &Bbo, _provider_kind: ProviderKind) {}
    fn on_bar(&mut self, _b: &Candle, _provider_kind: ProviderKind) {}
    fn on_mbp10(&mut self, _d: &Mbp10, _provider_kind: ProviderKind) {}

    fn on_orders_batch(&mut self, _b: &OrdersBatch) {}
    fn on_positions_batch(&mut self, _b: &PositionsBatch) {}
    fn on_account_delta(&mut self, _accounts: &[AccountDelta]) {}

    fn on_trades_closed(&mut self, _trades: Vec<Trade>) {}

    fn on_subscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic, _success: bool) {}
    fn on_unsubscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic) {}

    fn accounts(&self) -> Vec<AccountKey> {
        Vec::new()
    }
}

/// A minimal FillModel trait; concrete venues can implement richer logic.
pub trait FillModel: Send + Sync {
    /// Called on submit to validate/normalize the order (rounding, IOC/FOK checks, post-only, etc.).
    fn on_submit(&mut self, _now: DateTime<Utc>, _order: &mut PlaceOrder) {}

    /// Core matching step: given a book snapshot and the current order, return fills and mutate leaves.
    fn match_book(
        &mut self,
        now: DateTime<Utc>,
        book: Option<&BookLevels>,
        last_price: Decimal,
        order: &mut PlaceOrder,
        slip: &mut dyn SlippageModel,
        cal: &dyn SessionCalendar,
    ) -> Vec<Fill>;
}

/// Deterministic slippage model
pub trait SlippageModel: Send + Sync {
    /// Compute an effective execution price given a reference price and context.
    fn adjust(
        &mut self,
        side: Side,
        ref_price: Decimal,
        spread: Option<Decimal>,
        qty: i64,
    ) -> Decimal;
}

/// Venue latency profile
pub trait LatencyModel: Send + Sync {
    fn submit_to_ack(&mut self) -> Duration {
        Duration::from_millis(1)
    }
    fn ack_to_first_fill(&mut self) -> Duration {
        Duration::from_millis(1)
    }
    fn cancel_rtt(&mut self) -> Duration {
        Duration::from_millis(1)
    }
    fn replace_rtt(&mut self) -> Duration {
        Duration::from_millis(1)
    }
}

/// Fee model (maker/taker or flat)
pub trait FeeModel: Send + Sync {
    /// Optional venue/new-order assessment fees.
    fn on_new_order(&self, _ctx: &FeeCtx, _o: &PlaceOrder) -> Money {
        Money::ZERO
    }
    /// Exchange/clearing/brokerage on fills. Return negative for fees, positive for rebates.
    fn on_fill(&self, _ctx: &FeeCtx, _f: &Fill) -> Money {
        Money::ZERO
    }
    /// Cancel/replace fee if applicable.
    fn on_cancel(&self, _ctx: &FeeCtx, _o: &PlaceOrder, _canceled_qty: i64) -> Money {
        Money::ZERO
    }
    /// Optional settlement/expiration fee logic.
    fn settle(&self, _ctx: &FeeCtx, _pos: &PositionDelta) -> Money {
        Money::ZERO
    }
}

/// Trading session calendar
pub trait SessionCalendar: Send + Sync {
    /// Is the instrument tradable at time t?
    fn is_open(&self, instr: &Instrument, t: DateTime<Utc>) -> bool;
    /// Returns the (session_start, session_end) enclosing t.
    fn session_bounds(
        &self,
        instr: &Instrument,
        t: DateTime<Utc>,
    ) -> (DateTime<Utc>, DateTime<Utc>);
    /// Next open after t.
    fn next_open_after(&self, instr: &Instrument, t: DateTime<Utc>) -> Option<DateTime<Utc>>;
    /// Next close after t.
    fn next_close_after(&self, instr: &Instrument, t: DateTime<Utc>) -> Option<DateTime<Utc>>;
    /// Trading day key (e.g., CME rolls at 17:00 CT).
    fn trading_day(&self, instr: &Instrument, t: DateTime<Utc>) -> NaiveDate;
    /// True if halted by venue (news/auction/etc).
    fn is_halt(&self, instr: &Instrument, t: DateTime<Utc>) -> bool {
        let _ = (instr, t);
        false
    }
    /// True if price is at limit and locked for trading.
    fn is_limit_locked(&self, instr: &Instrument, _px: Decimal, t: DateTime<Utc>) -> bool {
        let _ = (instr, t);
        false
    }
}

/// Risk model hooks
pub trait RiskModel: Send + Sync {
    fn pre_place(&self, _ctx: &RiskCtx, _o: &PlaceOrder) -> RiskDecision {
        RiskDecision::Allow
    }
    fn pre_replace(
        &self,
        _ctx: &RiskCtx,
        _r: &ReplaceOrder,
        _current: &PlaceOrder,
    ) -> RiskDecision {
        RiskDecision::Allow
    }
    fn pre_cancel(&self, _ctx: &RiskCtx, _current: &PlaceOrder) -> RiskDecision {
        RiskDecision::Allow
    }
    fn on_fill(&self, _ctx: &RiskCtx, _f: &Fill) -> PostFillAction {
        PostFillAction::None
    }
    fn margin_required(&self, _ctx: &RiskCtx, _after: &ProposedPortfolioChange) -> Money {
        Money::ZERO
    }
    fn can_trade(&self, _ctx: &RiskCtx) -> bool {
        true
    }
}
