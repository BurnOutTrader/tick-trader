use crate::backtest::backtest_clock::BacktestClock;
use std::sync::LazyLock;

pub(crate) static CLOCK: LazyLock<BacktestClock> = LazyLock::new(|| BacktestClock::new(0));

/// Returns the current Engine date and time in UTC.
///
/// This function utilizes the `CLOCK` object to retrieve the current
/// time as a `chrono::DateTime<chrono::Utc>` instance. It is marked
/// with the `#[inline]` attribute to suggest inlining for performance
/// optimization during compilation.
///
/// # Returns
/// A `chrono::DateTime<chrono::Utc>` object representing the current
#[inline]
pub fn time() -> chrono::DateTime<chrono::Utc> {
    CLOCK.now_dt()
}
/// Current engine time in nanoseconds since UNIX_EPOCH.
#[inline]
pub fn time_ns() -> u64 {
    CLOCK.now_ns()
}
