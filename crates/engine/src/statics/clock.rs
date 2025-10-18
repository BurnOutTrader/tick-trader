use crate::backtest::backtest_clock::BacktestClock;
use chrono::{DateTime, Utc};
use std::sync::{Arc, LazyLock, RwLock};

/// Global engine clock facade supporting Live and Backtest modes.
enum ClockMode {
    Live,
    Backtest(Arc<BacktestClock>),
}

static CLOCK_MODE: LazyLock<RwLock<ClockMode>> = LazyLock::new(|| RwLock::new(ClockMode::Live));

#[inline]
fn dt_to_ns(dt: DateTime<Utc>) -> u64 {
    let secs = dt.timestamp();
    let nsub = dt.timestamp_subsec_nanos();
    if secs <= 0 {
        return nsub as u64; // clamp
    }
    (secs as u64)
        .saturating_mul(1_000_000_000u64)
        .saturating_add(nsub as u64)
}

/// Set global clock to Live mode (system time).
#[inline]
#[allow(dead_code)]
pub(crate) fn set_live_clock() {
    *CLOCK_MODE.write().expect("poisoned clock mode") = ClockMode::Live;
}

/// Set global clock to Backtest mode using the provided BacktestClock.
#[inline]
pub(crate) fn set_backtest_clock(clock: Arc<BacktestClock>) {
    *CLOCK_MODE.write().expect("poisoned clock mode") = ClockMode::Backtest(clock);
}

/// Advance the backtest clock to at least the provided DateTime. No-op in Live mode.
#[inline]
pub(crate) fn backtest_advance_to(now: DateTime<Utc>) {
    if let ClockMode::Backtest(clk) = &*CLOCK_MODE.read().expect("poisoned clock mode") {
        clk.advance_to_at_least(dt_to_ns(now));
    }
}

/// Returns the current Engine date and time in UTC (from the configured clock mode).
#[inline]
pub fn time_now() -> DateTime<Utc> {
    match &*CLOCK_MODE.read().expect("poisoned clock mode") {
        ClockMode::Live => Utc::now(),
        ClockMode::Backtest(clk) => clk.now_dt(),
    }
}

/// Current engine time in nanoseconds since UNIX_EPOCH (from the configured clock mode).
#[inline]
pub fn time_ns() -> u64 {
    match &*CLOCK_MODE.read().expect("poisoned clock mode") {
        ClockMode::Live => dt_to_ns(Utc::now()),
        ClockMode::Backtest(clk) => clk.now_ns(),
    }
}

#[inline]
pub(crate) fn is_backtest() -> bool {
    matches!(
        &*CLOCK_MODE.read().expect("poisoned clock mode"),
        ClockMode::Backtest(_)
    )
}
