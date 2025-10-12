use crate::backtest::realism_models::traits::SessionCalendar;
use chrono::{DateTime, NaiveDate, Utc};
use tt_types::data::core::Exchange;
use tt_types::securities::hours::market_hours::{
    MarketHours, hours_for_exchange, next_session_after, session_bounds,
};
use tt_types::securities::symbols::Instrument;

/// Generic adapter: use the engine's MarketHours model as a SessionCalendar.
#[derive(Debug, Clone)]
pub struct HoursCalendar {
    pub hours: MarketHours,
}

impl HoursCalendar {
    /// Convenience constructor for a given exchange using your hours table.
    pub fn for_exchange(ex: Exchange) -> Self {
        Self {
            hours: hours_for_exchange(ex),
        }
    }
}

impl Default for HoursCalendar {
    fn default() -> Self {
        // Default to CME hours to preserve previous behavior
        Self::for_exchange(Exchange::CME)
    }
}
impl SessionCalendar for HoursCalendar {
    fn is_open(&self, _instr: &Instrument, t: DateTime<Utc>) -> bool {
        self.hours.is_open(t)
    }

    fn session_bounds(
        &self,
        _instr: &Instrument,
        t: DateTime<Utc>,
    ) -> (DateTime<Utc>, DateTime<Utc>) {
        session_bounds(&self.hours, t)
    }

    fn next_open_after(&self, _instr: &Instrument, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let (open, _close) = next_session_after(&self.hours, t);
        Some(open)
    }

    fn next_close_after(&self, _instr: &Instrument, t: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let (_open, close) = session_bounds(&self.hours, t);
        Some(close)
    }

    fn trading_day(&self, _instr: &Instrument, t: DateTime<Utc>) -> NaiveDate {
        // Define trading day by the *session close* local date.
        let (_open, close) = session_bounds(&self.hours, t);
        close.with_timezone(&self.hours.tz).date_naive()
    }

    fn is_halt(&self, _instr: &Instrument, t: DateTime<Utc>) -> bool {
        // Delegate: a venue-level "maintenance" window counts as a halt for matching
        self.hours.is_maintenance(t)
    }
}
