//! User-facing initialization helpers for backtesting.
//!
//! Provides simple constructors to obtain a BacktestConfig pre-populated with
//! sensible defaults (CME-like) and convenient hooks to customize realism models
//! without having to stitch together factories manually.

use crate::backtest::backtest_feeder::{MatchingPolicy, OrderAckDuringClosedPolicy};
use std::sync::Arc;

use chrono::Duration as ChronoDuration;

use crate::backtest::backtest_feeder::BacktestFeederConfig;
use crate::backtest::orchestrator::BacktestConfig;
use crate::backtest::realism_models::project_x::calander::HoursCalendar;
use crate::backtest::realism_models::project_x::fee::PxFlatFee;
use crate::backtest::realism_models::project_x::fill::{CmeFillModel, FillConfig};
use crate::backtest::realism_models::project_x::latency::PxLatency;
use crate::backtest::realism_models::project_x::slippage::NoSlippage;
use crate::backtest::realism_models::traits::{
    FeeModel, FillModel, LatencyModel, SessionCalendar, SlippageModel,
};

/// Simplified builder for common backtest configurations.
#[derive(Clone)]
pub struct BacktestInit {
    feeder: BacktestFeederConfig,
    step: chrono::Duration,
}

impl Default for BacktestInit {
    fn default() -> Self {
        Self {
            feeder: BacktestFeederConfig::default(),
            step: chrono::Duration::seconds(1),
        }
    }
}

impl BacktestInit {
    /// CME-like defaults: touch-aware fills, flat fees, fixed latencies, no slippage,
    /// hours calendar, 2d window with 1d lookahead.
    pub fn px_defaults() -> Self {
        let feeder = BacktestFeederConfig {
            flatten_before_close: None,
            window: ChronoDuration::days(2),
            lookahead: ChronoDuration::days(1),
            warmup: ChronoDuration::zero(),
            range_start: None,
            range_end: None,
            make_latency: Arc::new(|| Box::new(PxLatency::default())),
            make_fill: Arc::new(|| {
                Box::new(CmeFillModel {
                    cfg: FillConfig::default(),
                })
            }),
            make_slippage: Arc::new(|| Box::new(NoSlippage::new())),
            make_fee: Arc::new(|| Box::new(PxFlatFee::new())),
            calendar: Arc::new(HoursCalendar::default()) as Arc<dyn SessionCalendar>,
            order_ack_closed_policy: OrderAckDuringClosedPolicy::Reject,
            matching_policy: MatchingPolicy::FIFO,
        };
        Self {
            feeder,
            step: chrono::Duration::seconds(1),
        }
    }

    /// Override the fixed orchestrator step duration.
    pub fn with_step(mut self, step: chrono::Duration) -> Self {
        self.step = step;
        self
    }

    /// Override fill configuration policies while keeping other defaults.
    pub fn with_policies(mut self, fill_cfg: FillConfig) -> Self {
        self.feeder.make_fill = Arc::new(move || Box::new(CmeFillModel { cfg: fill_cfg }));
        self
    }

    /// Provide custom model factories for advanced use cases.
    pub fn with_models<FML, FL, FS, FF, CAL>(
        mut self,
        make_latency: FML,
        make_fill: FL,
        make_slippage: FS,
        make_fee: FF,
        calendar: CAL,
    ) -> Self
    where
        FML: Fn() -> Box<dyn LatencyModel> + Send + Sync + 'static,
        FL: Fn() -> Box<dyn FillModel> + Send + Sync + 'static,
        FS: Fn() -> Box<dyn SlippageModel> + Send + Sync + 'static,
        FF: Fn() -> Box<dyn FeeModel> + Send + Sync + 'static,
        CAL: SessionCalendar + 'static,
    {
        self.feeder.make_latency = Arc::new(make_latency);
        self.feeder.make_fill = Arc::new(make_fill);
        self.feeder.make_slippage = Arc::new(make_slippage);
        self.feeder.make_fee = Arc::new(make_fee);
        self.feeder.calendar = Arc::new(calendar);
        self
    }

    /// Override prefetch window/lookahead/warmup.
    pub fn with_windows(
        mut self,
        window: ChronoDuration,
        lookahead: ChronoDuration,
        warmup: ChronoDuration,
    ) -> Self {
        self.feeder.window = window;
        self.feeder.lookahead = lookahead;
        self.feeder.warmup = warmup;
        self
    }

    /// Clamp the backtest absolute date-time range.
    pub fn with_range(
        mut self,
        start: Option<chrono::DateTime<chrono::Utc>>,
        end: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Self {
        self.feeder.range_start = start;
        self.feeder.range_end = end;
        self
    }

    /// Finalize into a BacktestConfig to be passed to start_backtest.
    pub fn build(self) -> BacktestConfig {
        BacktestConfig {
            step: self.step,
            feeder: self.feeder,
            slow_spin: None,
            clock: None,
            start_date: None,
            end_date: None,
        }
    }
}

impl BacktestInit {
    /// Configure the backtest to auto-flatten open positions within the specified window before session close.
    /// Set to Some(duration) to enable, or None (default) to disable.
    pub fn with_flatten_before_close(mut self, dur: ChronoDuration) -> Self {
        self.feeder.flatten_before_close = Some(dur);
        self
    }
}
