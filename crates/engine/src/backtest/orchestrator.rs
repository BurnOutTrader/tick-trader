use crate::backtest::backtest_clock::BacktestClock;
use crate::backtest::backtest_feeder::{BacktestFeeder, BacktestFeederConfig};
use crate::runtime::EngineRuntime;
use crate::statics::bus::bus;
use crate::traits::Strategy;
use anyhow::Result;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use rust_decimal::{Decimal, dec};
use std::sync::Arc;
use tokio::sync::Notify;

/// Configuration for launching a backtest session.
#[derive(Clone)]
pub struct BacktestConfig {
    /// Fixed step duration for orchestrated time advancement. The orchestrator
    /// will advance logical time by this duration, requesting the feeder to emit up to each step.
    pub step: chrono::Duration,
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
    pub fn new(step: chrono::Duration) -> BacktestConfig {
        Self {
            step,
            feeder: BacktestFeederConfig::default(),
            slow_spin: None,
            clock: None,
            start_date: None,
            end_date: None,
        }
    }

    pub fn from_to(step: chrono::Duration, start: NaiveDate, end: NaiveDate) -> BacktestConfig {
        Self {
            step,
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
            step: chrono::Duration::seconds(1),
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
/// Spawns the backtest orchestrator and feeder; returns Ok(()) when initialization completes.
pub async fn start_backtest<S: Strategy>(
    conn: tt_database::init::Connection,
    mut cfg: BacktestConfig,
    strategy: S,
    account_balance: Decimal,
) -> Result<()> {
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

    let notify = Arc::new(Notify::new());

    // Determine start time for clock seeding (default to epoch if not set)
    let start_dt = cfg
        .feeder
        .range_start
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());

    // Ensure we have a shared BacktestClock and set it as the global time source
    let clock: Arc<BacktestClock> = if let Some(c) = cfg.clock.clone() {
        c
    } else {
        let secs = start_dt.timestamp().max(0) as u64;
        let ns = start_dt.timestamp_subsec_nanos() as u64;
        Arc::new(BacktestClock::new(
            secs.saturating_mul(1_000_000_000).saturating_add(ns),
        ))
    };
    crate::statics::clock::set_backtest_clock(clock.clone());
    cfg.clock = Some(clock.clone());

    // Start the DB-backed feeder which gives us an in-process bus.
    let _ = BacktestFeeder::start_with_db(
        conn,
        cfg.feeder.clone(),
        cfg.clock.clone(),
        Some(notify.clone()),
        account_balance,
    );

    // Create an engine runtime bound to the same bus in backtest mode.
    let mut rt = EngineRuntime::new_backtest(cfg.slow_spin, Some(notify.clone()));

    // Start the strategy.
    rt.start(strategy, true).await?;

    // Validate step > 0 and spawn orchestrator loop to advance time in discrete steps.
    if cfg.step <= chrono::Duration::zero() {
        anyhow::bail!("BacktestConfig.step must be positive");
    }
    let step = cfg.step;
    let bus = bus();
    let start = cfg
        .feeder
        .range_start
        .unwrap_or_else(|| Utc.timestamp_opt(0, 0).unwrap());
    let end_opt = cfg.feeder.range_end;

    // Shared hint for the next available event time from the feeder
    use std::sync::Arc;
    use tokio::sync::Mutex;
    let next_hint: Arc<Mutex<Option<DateTime<Utc>>>> = Arc::new(Mutex::new(None));
    // Spawn a listener to capture BacktestTimeUpdated.latest_time as a fast-forward hint
    {
        let bus_for_listener = bus;
        let hint_for_task = next_hint.clone();
        tokio::spawn(async move {
            let mut rx = bus_for_listener.add_client();
            while let Ok(resp) = rx.recv().await {
                if let tt_types::wire::Response::BacktestTimeUpdated {
                    now: _,
                    latest_time,
                } = resp
                    && let Some(t) = latest_time
                {
                    let mut h = hint_for_task.lock().await;
                    *h = Some(t);
                }
            }
        });
    }

    tokio::spawn(async move {
        let mut now = start;
        loop {
            if let Some(end) = end_opt
                && now >= end
            {
                break;
            }
            let mut target = now + step;
            // If we have a hint for a next event time beyond current now, jump to it
            if let Some(hint) = *next_hint.lock().await
                && hint > target
            {
                target = hint;
            }
            now = target;
            // Request feeder to emit up to `now`
            let _ = bus
                .handle_request(tt_types::wire::Request::BacktestAdvanceTo(
                    tt_types::wire::BacktestAdvanceTo { to: now },
                ))
                .await;
            // Yield to allow feeder/engine to process
            tokio::task::yield_now().await;
        }
    });

    Ok(())
}

/// Convenience helper: launch a backtest with explicit start/end dates.
pub async fn start_backtest_with_dates<S: Strategy>(
    conn: tt_database::init::Connection,
    mut cfg: BacktestConfig,
    strategy: S,
    start_date: NaiveDate,
    end_date: NaiveDate,
) -> Result<()> {
    cfg.start_date = Some(start_date);
    cfg.end_date = Some(end_date);
    start_backtest(conn, cfg, strategy, dec!(150_000)).await
}
