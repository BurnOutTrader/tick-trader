use crate::backtest::backtest_clock::BacktestClock;
use crate::backtest::backtest_feeder::{
    BacktestFeeder, BacktestFeederConfig, BacktestFeederHandle,
};
use crate::handle::EngineHandle;
use crate::runtime::EngineRuntime;
use crate::traits::Strategy;
use anyhow::Result;
use chrono::{NaiveDate, TimeZone, Utc};
use std::sync::Arc;

/// Configuration for launching a backtest session.
#[derive(Clone)]
pub struct BacktestConfig {
    /// Feeder (historical DB) parameters.
    pub feeder: BacktestFeederConfig,
    /// Optional slow-spin (in microseconds) to throttle internal loops for debugging.
    pub slow_spin: Option<u64>,
    /// Optional deterministic simulation clock shared with the feeder.
    pub clock: Option<Arc<BacktestClock>>,
    /// Optional start date (UTC, midnight) for clamping the backtest range.
    pub start_date: Option<NaiveDate>,
    /// Optional end date (UTC, end of day) for clamping the backtest range.
    pub end_date: Option<NaiveDate>,
}

impl BacktestConfig {
    pub fn from_to(start: NaiveDate, end: NaiveDate) -> BacktestConfig {
        Self {
            feeder: BacktestFeederConfig::default(),
            slow_spin: None,
            clock: None,
            start_date: Some(start),
            end_date: Some(end),
        }
    }
}

#[allow(clippy::derivable_impls)]
impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            feeder: BacktestFeederConfig::default(),
            slow_spin: None,
            clock: None,
            start_date: None,
            end_date: None,
        }
    }
}

/// Start a backtest by wiring together:
/// - BacktestFeeder (historical DB -> in-process bus)
/// - EngineRuntime in backtest mode (consumes the bus)
/// - User strategy
///
/// Returns the EngineHandle and the feeder handle (so caller can await/stop as needed).
pub async fn start_backtest<S: Strategy>(
    conn: tt_database::init::Connection,
    mut cfg: BacktestConfig,
    strategy: S,
) -> Result<(EngineHandle, BacktestFeederHandle)> {
    // If the caller specified start/end dates, map them into the feeder's date-time range.
    if cfg.start_date.is_some() || cfg.end_date.is_some() {
        if let Some(sd) = cfg.start_date {
            let ndt = sd.and_hms_opt(0, 0, 0).unwrap();
            let dt = Utc.from_utc_datetime(&ndt);
            cfg.feeder.range_start = Some(dt);
        }
        if let Some(ed) = cfg.end_date {
            // Clamp to end-of-day (23:59:59.999999)
            let ndt = ed.and_hms_micro_opt(23, 59, 59, 999_999).unwrap();
            let dt = Utc.from_utc_datetime(&ndt);
            cfg.feeder.range_end = Some(dt);
        }
    }

    // Start the DB-backed feeder which gives us an in-process bus.
    let feeder = BacktestFeeder::start_with_db(conn, cfg.feeder.clone(), cfg.clock.clone());

    // Create an engine runtime bound to the same bus in backtest mode.
    let mut rt = EngineRuntime::new_backtest(feeder.bus.clone(), cfg.slow_spin);

    // Start the strategy.
    let handle = rt.start(strategy).await?;

    Ok((handle, feeder))
}

/// Convenience helper: launch a backtest with explicit start/end dates.
pub async fn start_backtest_with_dates<S: Strategy>(
    conn: tt_database::init::Connection,
    mut cfg: BacktestConfig,
    strategy: S,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<(EngineHandle, BacktestFeederHandle)> {
    cfg.start_date = Some(start_date);
    cfg.end_date = Some(end_date);
    start_backtest(conn, cfg, strategy).await
}
