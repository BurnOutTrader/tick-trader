use std::sync::Arc;

use anyhow::Result;

use crate::backtest::backtest_clock::BacktestClock;
use crate::backtest::backtest_feeder::{
    BacktestFeeder, BacktestFeederConfig, BacktestFeederHandle,
};
use crate::handle::EngineHandle;
use crate::runtime::EngineRuntime;
use crate::traits::Strategy;

/// Configuration for launching a backtest session.
#[derive(Clone)]
pub struct BacktestConfig {
    /// Feeder (historical DB) parameters.
    pub feeder: BacktestFeederConfig,
    /// Optional slow-spin (in microseconds) to throttle internal loops for debugging.
    pub slow_spin: Option<u64>,
    /// Optional deterministic simulation clock shared with the feeder.
    pub clock: Option<Arc<BacktestClock>>,
}

#[allow(clippy::derivable_impls)]
impl Default for BacktestConfig {
    fn default() -> Self {
        Self {
            feeder: BacktestFeederConfig::default(),
            slow_spin: None,
            clock: None,
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
    cfg: BacktestConfig,
    strategy: S,
) -> Result<(EngineHandle, BacktestFeederHandle)> {
    // Start the DB-backed feeder which gives us an in-process bus.
    let feeder = BacktestFeeder::start_with_db(conn, cfg.feeder, cfg.clock.clone());

    // Create an engine runtime bound to the same bus in backtest mode.
    let mut rt = EngineRuntime::new_backtest(feeder.bus.clone(), cfg.slow_spin);

    // Start the strategy.
    let handle = rt.start(strategy).await?;

    Ok((handle, feeder))
}
