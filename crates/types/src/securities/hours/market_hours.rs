//!
//! Market hours: exchange trading calendars, sessions, and bar boundary helpers.
//!
//! This module models exchange trading hours at the exchange level (per [`Exchange`])
//! and exposes utilities to query whether a market is open at a given UTC instant,
//! compute session bounds, and find candle end boundaries that respect trading hours.
//!
//! Key concepts:
//! - Time semantics: All public APIs take and return [`chrono::DateTime<Utc>`]. The
//!   exchange's local time zone (`tz`) is used only internally to interpret session
//!   rules and handle DST transitions correctly.
//! - Session rules: Each exchange is described by a set of [`SessionRule`]s for
//!   "regular" and/or "extended" trading. A rule activates on specific weekdays and
//!   defines an open and close time as seconds since local midnight (SSM). If
//!   `open_ssm <= close_ssm`, the session runs within the same local day; if
//!   `open_ssm > close_ssm`, the session wraps past midnight and closes the next
//!   local day.
//! - Weekday indexing: Weekdays are indexed Monday=0 through Sunday=6. The `days`
//!   mask in [`SessionRule`] follows this convention throughout the module.
//! - Holidays: If an exchange-local calendar date is marked as a holiday, no
//!   sessions occur on that local date. Wrapped sessions from "yesterday" do not
//!   bleed into a holiday; i.e., a previous-day wrap is ignored if the previous
//!   local date is a holiday.
//! - Maintenance windows: A convenience heuristic [`MarketHours::is_maintenance`]
//!   returns true when the market is currently closed but the next session starts
//!   within ~90 minutes (useful for CME’s daily maintenance break).
//!
//! What you can do with this module:
//! - Check open/closed state at an instant: [`MarketHours::is_open`],
//!   [`MarketHours::is_open_with`], [`MarketHours::is_open_regular`],
//!   [`MarketHours::is_open_extended`].
//! - Find the trading session that contains a time (or the next one):
//!   [`session_bounds`], [`session_bounds_with`], [`next_session_after`],
//!   [`next_session_after_with`], [`next_session_open_after`].
//! - Compute bar end times that align with the market calendar: [`candle_end`]
//!   (alias [`time_end_of_day`]). Intraday bars step to the next permitted boundary
//!   skipping closed periods; Daily/Weekly end at session/week boundaries.
//!
//! Exchange presets:
//! - [`hours_for_exchange`] provides pragmatic default hours for major venues
//!   (CME, CBOT, COMEX, NYMEX, EUREX, ICEUS, ICEEU, SGX, CFE). These are exchange-
//!   level defaults for common product profiles (e.g., CME equity-index futures):
//!   Sun 17:00 – Fri 16:00 Central, with a daily break 16:00–17:00, RTH 08:30–15:15,
//!   and a brief 15:30–16:00 window. Always verify specific contract rules when
//!   needed; product-level calendars can be layered later if required.
//!
//! DST and ambiguous times:
//! - Local times are resolved with `Tz::from_local_datetime(...).single()` and
//!   conservative fallbacks to ensure a valid instant is produced even across DST
//!   transitions. Comparisons are done in SSM (seconds since local midnight) and
//!   converted back to UTC only at the edges.
//!
//! Invariants and nuances:
//! - All comparisons against session close are end-exclusive: the instant equal to
//!   `close_ssm` is considered closed. This avoids overlapping sessions.
//! - "Wrapped" sessions are represented by `open_ssm > close_ssm`. When inside a
//!   wrap, times before `close_ssm` on the next day are still considered inside the
//!   same session.
//! - The calendar-wide checks (`is_closed_all_day_*`) interpret a "day" in the
//!   specified calendar TZ and correctly handle DST while checking for any session
//!   overlap within that window.
//!
//! Examples can be found in `standard_lib/tests/market_hours_tests.rs`.
use crate::data::models::Resolution;
use crate::securities::symbols::Exchange;
use chrono::{
    DateTime, Datelike, Duration, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc,
    Weekday,
};
use chrono_tz::{America, Asia, Europe, Tz, US};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use tracing::warn;

/// One schedule slice for a market session.
///
/// - `days`: Monday=0 .. Sunday=6 mask; `true` enables the rule on that weekday.
/// - `open_ssm` / `close_ssm`: seconds since local midnight in the exchange TZ.
///   If `open_ssm <= close_ssm` the session is same-day; if `open_ssm > close_ssm`
///   the session wraps into the next local day and closes at `close_ssm` there.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Archive, RkyvDeserialize, RkyvSerialize)]
pub struct SessionRule {
    /// Weekday activation mask (Mon=0 .. Sun=6)
    pub days: [bool; 7],
    /// Open time in seconds since local midnight (exchange TZ)
    pub open_ssm: u32,
    /// Close time in seconds since local midnight (end-exclusive)
    pub close_ssm: u32,
}

/// Exchange-level trading hours definition.
///
/// This is a pragmatic calendar describing typical hours for an exchange. It may not
/// capture product-specific exceptions; consult contract specs for exact rules.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketHours {
    /// Which exchange these hours represent.
    pub exchange: Exchange,
    /// Exchange’s local time zone (used to interpret `SessionRule`s and handle DST).
    pub tz: Tz,
    /// Primary/pit ("regular") trading sessions (e.g., RTH for equities/futures).
    pub regular: Vec<SessionRule>,
    /// Electronic/overnight and other non-regular sessions.
    pub extended: Vec<SessionRule>,
    /// Exchange-local holiday dates (no sessions occur on these local calendar days).
    pub holidays: Vec<chrono::NaiveDate>,
    /// True if there is a distinct daily close (used for daily candle boundaries).
    pub has_daily_close: bool,
    /// True if the exchange has a true weekend close (used for weekly boundaries).
    pub has_weekend_close: bool,
}


/// Which session set to consult when querying hours
#[derive(Debug, Clone, Copy)]
pub enum SessionKind {
    Regular,
    Extended,
    Both,
}

impl MarketHours {
    #[inline]
    fn iter_rules<'a>(&'a self, kind: SessionKind) -> impl Iterator<Item = &'a SessionRule> + 'a {
        let reg = self.regular.iter();
        let ext = self.extended.iter();
        match kind {
            SessionKind::Regular => reg.take(usize::MAX).chain(ext.take(0)),
            SessionKind::Extended => reg.take(0).chain(ext.take(usize::MAX)),
            SessionKind::Both => reg.take(usize::MAX).chain(ext.take(usize::MAX)),
        }
    }

    /// True if **any** (regular or extended) session is open at `t`.
    pub fn is_open(&self, t: DateTime<Utc>) -> bool {
        self.is_open_with(t, SessionKind::Both)
    }

    /// True if a session of the requested kind is open at `t`.
    pub fn is_open_with(&self, t: DateTime<Utc>, kind: SessionKind) -> bool {
        let local = t.with_timezone(&self.tz);
        if self.holidays.iter().any(|d| *d == local.date_naive()) {
            return false;
        }
        let w_today = local.weekday().num_days_from_monday() as usize;
        let ssm = local.num_seconds_from_midnight();

        // --- Hard gate known CME/CFE halts (source: CME rule filings & specs) ---
        if matches!(self.exchange, Exchange::CME | Exchange::CFE) {
            // 15:15–15:30 CT trading halt; 16:00–17:00 CT daily maintenance (Mon–Thu)
            let ct_1515 = 15 * 3600 + 15 * 60;
            let ct_1530 = 15 * 3600 + 30 * 60;
            let ct_1600 = 16 * 3600;
            let ct_1700 = 17 * 3600;

            // Halt window every weekday (Mon–Fri)
            let is_weekday = w_today <= 4;
            if is_weekday && ssm >= ct_1515 && ssm < ct_1530 {
                return false;
            }

            // Maintenance applies Mon–Thu (no Friday overnight open)
            let is_mon_thu = w_today <= 3;
            if is_mon_thu && ssm >= ct_1600 && ssm < ct_1700 {
                return false;
            }
        }
        // -----------------------------------------------------------------------
        // ...then continue with your existing checks
        if self.iter_rules(kind).any(|r| {
            if !r.days[w_today] {
                return false;
            }
            if r.open_ssm <= r.close_ssm {
                ssm >= r.open_ssm && ssm < r.close_ssm
            } else {
                ssm >= r.open_ssm || ssm < r.close_ssm
            }
        }) {
            return true;
        }

        let yday_date = local.date_naive() - Duration::days(1);
        if self.holidays.iter().any(|d| *d == yday_date) {
            return false;
        }
        let yday = yday_date.weekday().num_days_from_monday() as usize;
        self.iter_rules(kind).any(|r| {
            if !(r.open_ssm > r.close_ssm) {
                return false;
            }
            if !r.days[yday] {
                return false;
            }
            ssm < r.close_ssm
        })
    }

    /// Convenience wrappers
    pub fn is_open_regular(&self, t: DateTime<Utc>) -> bool {
        self.is_open_with(t, SessionKind::Regular)
    }
    pub fn is_open_extended(&self, t: DateTime<Utc>) -> bool {
        self.is_open_with(t, SessionKind::Extended)
    }
    /// This is used to calculate the current bars closing time, based on its open time
    /// Inclusive bar close given its UTC open and resolution.
    pub fn bar_end(&self, open_time: DateTime<Utc>, res: Resolution) -> DateTime<Utc> {
        match res {
            Resolution::Seconds(s) => open_time + chrono::Duration::seconds(s as i64) - chrono::Duration::nanoseconds(1),
            Resolution::Minutes(m) => open_time + chrono::Duration::minutes(m as i64) - chrono::Duration::nanoseconds(1),
            Resolution::Hours(h)   => open_time + chrono::Duration::hours(h as i64)   - chrono::Duration::nanoseconds(1),
            Resolution::Daily => {
                // End at that session’s close (already inclusive in your model)
                let (_open, close) = session_bounds(self, open_time);
                close
            }
            Resolution::Weekly => {
                // Prefer a proper “week bounds” helper; placeholder shown:
                let (_wopen, wclose) = week_session_bounds(self, open_time);
                wclose
            }
        }
    }

    pub fn is_maintenance(&self, t: DateTime<Utc>) -> bool {
        // “Maintenance” := closed now but next session is within ~90 minutes.
        if self.is_open(t) {
            return false;
        }
        let (open, _close) = next_session_after(self, t);
        (open - t) <= chrono::Duration::minutes(90)
    }

    /// Return true iff the market is closed for the entire **calendar day** `day`
    /// interpreted in `calendar_tz`. This checks whether *any* session (of `kind`)
    /// intersects the [day@00:00, next_day@00:00) window.
    pub fn is_closed_all_day_in_calendar(
        &self,
        day: NaiveDate,
        calendar_tz: Tz,
        kind: SessionKind,
    ) -> bool {
        // Convert the calendar day's bounds to UTC, DST-safe.
        fn midnight_utc_for(tz: Tz, d: NaiveDate) -> DateTime<Utc> {
            match tz.with_ymd_and_hms(d.year(), d.month(), d.day(), 0, 0, 0) {
                LocalResult::Single(dt) => dt.with_timezone(&Utc),
                LocalResult::Ambiguous(a, b) => a.min(b).with_timezone(&Utc),
                LocalResult::None => {
                    // Extremely rare at midnight; fallback to 01:00 local.
                    tz.with_ymd_and_hms(d.year(), d.month(), d.day(), 1, 0, 0)
                        .single()
                        .expect("resolvable local time")
                        .with_timezone(&Utc)
                }
            }
        }

        let start_utc = midnight_utc_for(calendar_tz, day);
        let end_utc = midnight_utc_for(calendar_tz, day.succ_opt().expect("valid next day"));

        // If you want “holiday = fully closed”, short-circuit when the exchange-local
        // date(s) overlapped by this window include a holiday.
        let start_local_day = start_utc.with_timezone(&self.tz).date_naive();
        let end_local_day = (end_utc - Duration::nanoseconds(1))
            .with_timezone(&self.tz)
            .date_naive();
        if self.holidays.contains(&start_local_day) || self.holidays.contains(&end_local_day) {
            return true;
        }

        // If any session is active at window start → not closed all day.
        if self.is_open_with(start_utc, kind) {
            return false;
        }

        // Otherwise, check the *next* session after the window start; if it opens
        // before the window ends, the day is not fully closed.
        let (next_open, _next_close) = next_session_after_with(kind, self, start_utc);
        next_open >= end_utc
    }

    /// Convenience: interpret the date in the **exchange TZ** (what your old
    /// `is_closed_all_day_on` did).
    #[inline]
    pub fn is_closed_all_day_on(&self, local_day: NaiveDate, kind: SessionKind) -> bool {
        self.is_closed_all_day_in_calendar(local_day, self.tz, kind)
    }

    /// Convenience: starting from a UTC timestamp, use a chosen calendar TZ to define “the day”.
    #[inline]
    pub fn is_closed_all_day_at(
        &self,
        ts_utc: DateTime<Utc>,
        calendar_tz: Tz,
        kind: SessionKind,
    ) -> bool {
        let day = ts_utc.with_timezone(&calendar_tz).date_naive();
        self.is_closed_all_day_in_calendar(day, calendar_tz, kind)
    }
}

/// Compute the inclusive candle end timestamp for a given `time_open`, `resolution` and `exchange`.
/// The result is **one nanosecond before** the next bar's open (i.e., end-exclusive boundary minus 1ns),
/// so adjacent bars do not collide.
///
/// Rules:
/// - Intraday (Seconds/Minutes/Hours): use the exchange's trading hours to find the next bar boundary,
///   then subtract 1ns.
/// - Daily: if the exchange has a distinct daily close (`has_daily_close = true`), end at the *session close* - 1ns;
///   otherwise, end at the next local midnight - 1ns (in the exchange's timezone).
/// - Weekly: if the exchange has a distinct weekend close (`has_weekend_close = true`), end at the next
///   Monday 00:00 local - 1ns; otherwise, end at the next Sunday 17:00 local - 1ns (common futures reopen).
pub fn candle_end(
    time_open: DateTime<Utc>,
    resolution: Resolution,
    exchange: Exchange,
) -> Option<DateTime<Utc>> {
    let hours = hours_for_exchange(exchange);

    // helper: 1ns subtraction (avoid underflow)
    let minus_1ns = |t: DateTime<Utc>| t.checked_sub_signed(Duration::nanoseconds(1)).unwrap_or(t);

    match resolution {
        Resolution::Seconds(1) => {
            return Some(time_open + Duration::seconds(1) - Duration::nanoseconds(1));
        }
        Resolution::Minutes(1) => {
            return Some(time_open + Duration::minutes(1) - Duration::nanoseconds(1));
        }
        Resolution::Hours(1) => {
            return Some(time_open + Duration::hours(1) - Duration::nanoseconds(1));
        }
        Resolution::Daily => {
            if hours.has_daily_close {
                let (_open, close) = session_bounds(&hours, time_open);
                Some(minus_1ns(close))
            } else {
                // No explicit daily close → use next local midnight in the exchange TZ
                let local = time_open.with_timezone(&hours.tz);
                let tomorrow = local.date_naive() + Duration::days(1);
                let next_midnight_local = hours
                    .tz
                    .with_ymd_and_hms(tomorrow.year(), tomorrow.month(), tomorrow.day(), 0, 0, 0)
                    .earliest()
                    .unwrap()
                    .with_timezone(&Utc);
                Some(minus_1ns(next_midnight_local))
            }
        }
        Resolution::Weekly => {
            use chrono::Weekday;
            let local = time_open.with_timezone(&hours.tz);

            // Find the next Monday 00:00 local
            let mut d = local.date_naive();
            let mut dow = local.weekday();
            while dow != Weekday::Mon {
                d = d + Duration::days(1);
                dow = d.weekday();
            }
            let next_monday_00_local = hours
                .tz
                .with_ymd_and_hms(d.year(), d.month(), d.day(), 0, 0, 0)
                .earliest()
                .unwrap()
                .with_timezone(&Utc);

            if hours.has_weekend_close {
                Some(minus_1ns(next_monday_00_local))
            } else {
                // Continuous products without a true weekend close:
                // Use “Sunday night” as the weekly boundary (commonly 17:00 local on many futures venues).
                // If the computed Monday is today (already Monday), move back to the prior week’s Sunday.
                let mut d_sun = d;
                // step back to Sunday corresponding to that Monday
                d_sun = d_sun - Duration::days(1);
                let sunday_17_local = hours
                    .tz
                    .with_ymd_and_hms(d_sun.year(), d_sun.month(), d_sun.day(), 17, 0, 0)
                    .earliest()
                    .unwrap()
                    .with_timezone(&Utc);
                Some(minus_1ns(sunday_17_local))
            }
        }
        _ => None,
    }
}

/// Back-compat shim: old name pointing to `candle_end`.
#[inline]
pub fn time_end_of_day(
    time_open: DateTime<Utc>,
    resolution: Resolution,
    exchange: Exchange,
) -> Option<DateTime<Utc>> {
    candle_end(time_open, resolution, exchange)
}

/// Build default futures trading hours per exchange.
/// NOTE: These are exchange-level defaults. Product-level variations may differ.
pub fn hours_for_exchange(exch: Exchange) -> MarketHours {
    match exch {
        // ------------------------------------------------------------
        // CME (CME Globex, Equity Index default)
        // Sun 17:00 – Fri 16:00 CT with daily 60-min break at 16:00;
        // RTH 08:30–15:15 CT; continuous 15:15–16:00 CT; no Fri overnight.
        // Sources: CME Globex Notice (eliminated 3:15–3:30), contract specs.
        // ------------------------------------------------------------
        Exchange::CME => MarketHours {
            exchange: Exchange::CME,
            tz: US::Central,
            regular: vec![
                // RTH (Mon–Fri): 08:30–15:15 CT
                SessionRule {
                    days: [true, true, true, true, true, false, false],
                    open_ssm: 8 * 3600 + 30 * 60,
                    close_ssm: 15 * 3600 + 15 * 60,
                },
            ],
            extended: vec![
                // **Continuous** after RTH: 15:15–16:00 CT (Mon–Fri)
                SessionRule {
                    days: [true, true, true, true, true, false, false],
                    open_ssm: 15 * 3600 + 15 * 60, // 15:15:00
                    close_ssm: 16 * 3600,          // 16:00:00 (end-exclusive)
                },
                // Globex overnight (wrap): Sun + Mon–Thu 17:00 → next day 08:30 CT
                // days mask marks the **open** day (Mon=0..Sun=6): Sun + Mon–Thu
                SessionRule {
                    days: [true, true, true, true, false, false, true],
                    open_ssm: 17 * 3600,
                    close_ssm: 8 * 3600 + 30 * 60,
                },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // ------------------------------------------------------------
        // CBOT (Grains/Oilseeds default)
        // Sun–Fri overnight 19:00–07:45 CT, day 08:30–13:20 CT.
        // ------------------------------------------------------------
        Exchange::CBOT => MarketHours {
            exchange: Exchange::CBOT,
            tz: US::Central,
            regular: vec![
                // Day session (Mon–Fri): 08:30–13:20 CT
                SessionRule {
                    days: [false, true, true, true, true, true, false],
                    open_ssm: 8 * 3600 + 30 * 60,
                    close_ssm: 13 * 3600 + 20 * 60,
                },
            ],
            extended: vec![
                // CBOT overnight (wrap): Sun + Mon–Thu 19:00 → next day 07:45 CT
                // Mon=0..Sun=6  =>  [Mon, Tue, Wed, Thu, Fri, Sat, Sun]
                SessionRule {
                    days: [true, true, true, true, false, false, true],
                    open_ssm: 19 * 3600,
                    close_ssm: 7 * 3600 + 45 * 60,
                },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // ------------------------------------------------------------
        // COMEX (Metals default)
        // Nearly 24x5: 17:00–16:00 CT with daily maintenance 16:00–17:00.
        // ------------------------------------------------------------
        Exchange::COMEX => MarketHours {
            exchange: Exchange::COMEX,
            tz: US::Central,
            regular: vec![],
            extended: vec![
                // BEFORE: days: [true, true, true, true, true, true, false]
                SessionRule {
                    days: [true, true, true, true, false, false, true],
                    open_ssm: 17 * 3600,
                    close_ssm: 16 * 3600,
                },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // ------------------------------------------------------------
        // NYMEX (Energy default)
        // Same “17:00–16:00 CT, daily break at 16:00–17:00”.
        // ------------------------------------------------------------
        Exchange::NYMEX => MarketHours {
            exchange: Exchange::NYMEX,
            tz: US::Central,
            regular: vec![],
            // --- NYMEX (energy) overnight ---
            extended: vec![
                // BEFORE: days: [true, true, true, true, true, true, false]
                SessionRule {
                    days: [true, true, true, true, false, false, true],
                    open_ssm: 17 * 3600,
                    close_ssm: 16 * 3600,
                },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // ------------------------------------------------------------
        // EUREX (generic index/IR default)
        // Asian hours 01:00–08:00 CET/CEST, then regular 08:00–22:00 CET/CEST.
        // ------------------------------------------------------------
        Exchange::EUREX => MarketHours {
            exchange: Exchange::EUREX,
            tz: Europe::Berlin,
            regular: vec![
                // Regular: 08:00–22:00 local
                SessionRule {
                    days: [false, true, true, true, true, true, false],
                    open_ssm: 8 * 3600,
                    close_ssm: 22 * 3600,
                },
            ],
            extended: vec![
                // Asian hours: 01:00–08:00 local
                SessionRule {
                    days: [false, true, true, true, true, true, false],
                    open_ssm: 1 * 3600,
                    close_ssm: 8 * 3600,
                },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // ------------------------------------------------------------
        // ICE Futures U.S. (common profile)
        // Many contracts follow ~20:00–18:00 ET (22h) with daily 2h break.
        // ------------------------------------------------------------
        Exchange::ICEUS => MarketHours {
            exchange: Exchange::ICEUS,
            tz: America::New_York,
            regular: vec![],
            extended: vec![
                // Wrapped: 20:00 → 18:00 next day ET (common ICE US schedule)
                SessionRule {
                    days: [true, true, true, true, true, true, false],
                    open_ssm: 20 * 3600,
                    close_ssm: 18 * 3600,
                },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // ------------------------------------------------------------
        // ICE Futures Europe (common profile)
        // Typical 01:00–23:00 London for many equity/fixed income; ICE publishes DST variants.
        // We model a broad window 01:00–23:00 local.
        // ------------------------------------------------------------
        Exchange::ICEEU => MarketHours {
            exchange: Exchange::ICEEU,
            tz: Europe::London,
            regular: vec![SessionRule {
                days: [false, true, true, true, true, true, false],
                open_ssm: 1 * 3600,
                close_ssm: 23 * 3600,
            }],
            extended: vec![],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // ------------------------------------------------------------
        // SGX Derivatives (generic)
        // T session ~07:10–20:00 SGT, T+1 ~20:00:01–05:15 SGT (varies by product).
        // ------------------------------------------------------------
        Exchange::SGX => MarketHours {
            exchange: Exchange::SGX,
            tz: Asia::Singapore,
            regular: vec![
                // T session (Mon–Fri): 07:10–20:00 SGT
                SessionRule {
                    days: [false, true, true, true, true, true, false],
                    open_ssm: 7 * 3600 + 10 * 60,
                    close_ssm: 20 * 3600,
                },
            ],
            extended: vec![
                // T+1 session (wrap): 20:00:01 → 05:15 next day SGT
                SessionRule {
                    days: [false, true, true, true, true, true, false],
                    open_ssm: 20 * 3600,
                    close_ssm: 5 * 3600 + 15 * 60,
                },
                // (The 1-second offset isn’t needed in SSM; end-exclusive comparison prevents overlap.)
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // ------------------------------------------------------------
        // CFE (Cboe Futures – VIX default profile)
        // Nearly 24x5 with daily 15-min halt 15:15–15:30 CT and a short 15:00–16:00 window;
        // use RTH 08:30–15:15, plus 15:30–16:00, plus 17:00–08:30 wrap (Sun open at 17:00).
        // ------------------------------------------------------------
        Exchange::CFE => MarketHours {
            exchange: Exchange::CFE,
            tz: US::Central,
            regular: vec![
                // RTH: 08:30–15:15 CT
                SessionRule {
                    days: [true, true, true, true, true, false, false],
                    open_ssm: 8 * 3600 + 30 * 60,
                    close_ssm: 15 * 3600 + 15 * 60,
                },
            ],
            extended: vec![
                // Short window after RTH: 15:30–16:00 CT (Mon–Thu)
                SessionRule {
                    days: [true, true, true, true, false, false, false],
                    open_ssm: 15 * 3600 + 30 * 60,
                    close_ssm: 16 * 3600,
                },
                // Overnight (wrap): Sun + Mon–Thu 17:00 → next day 08:30 CT
                SessionRule {
                    days: [true, true, true, true, false, false, true],
                    open_ssm: 17 * 3600,
                    close_ssm: 8 * 3600 + 30 * 60,
                },
            ],
            holidays: vec![],
            has_daily_close: true,
            has_weekend_close: true,
        },

        // Fallback
        _ => {
            warn!("No market hours found for : {}", exch);
            MarketHours {
                exchange: exch,
                tz: chrono_tz::UTC,
                regular: vec![SessionRule {
                    days: [true, true, true, true, true, true, true],
                    open_ssm: 0,
                    close_ssm: 24 * 3600,
                }],
                extended: vec![],
                holidays: vec![],
                has_daily_close: true,
                has_weekend_close: true,
            }
        }
    }
}

// ---------------------------
// Session helpers (public API)
// ---------------------------

/// Return the trading session [open, close) that **contains** `t` (if any) for the given kind.
/// If `t` is outside any session (e.g., weekend/holiday), this falls back to:
/// 1) previous day's wrap session (if it spans into today), else
/// 2) the next valid session after `t`.
pub fn session_bounds_with(
    kind: SessionKind,
    hours: &MarketHours,
    t: DateTime<Utc>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let local = t.with_timezone(&hours.tz);
    let day = local.date_naive();
    let ssm = local.num_seconds_from_midnight() as i64;

    let w_today = day.weekday().num_days_from_monday() as usize;
    let yday = (day - chrono::Duration::days(1))
        .weekday()
        .num_days_from_monday() as usize;

    // 1) Try all of TODAY's rules.
    for r in hours.iter_rules(kind).filter(|r| r.days[w_today]) {
        if (r.open_ssm as i64) <= (r.close_ssm as i64) {
            // same-day
            if ssm >= r.open_ssm as i64 && ssm < r.close_ssm as i64 {
                let open = mk_local(hours.tz, day, r.open_ssm);
                let close = mk_local(hours.tz, day, r.close_ssm);
                return (open.with_timezone(&Utc), close.with_timezone(&Utc));
            }
        } else {
            // wrap (open today, close tomorrow)
            if ssm >= r.open_ssm as i64 {
                let open = mk_local(hours.tz, day, r.open_ssm);
                let close = mk_local(hours.tz, day + chrono::Duration::days(1), r.close_ssm);
                return (open.with_timezone(&Utc), close.with_timezone(&Utc));
            }
        }
    }

    // 2) Try all of YESTERDAY's WRAP rules that spill into today.
    //    (This is the case for early-morning times like 02:00 CT.)
    for r in hours.iter_rules(kind).filter(|r| r.days[yday]) {
        if r.open_ssm > r.close_ssm {
            // yesterday had a wrap; if we're before today's wrap close, we are inside it
            if ssm < r.close_ssm as i64 {
                let open = mk_local(hours.tz, day - chrono::Duration::days(1), r.open_ssm);
                let close = mk_local(hours.tz, day, r.close_ssm);
                return (open.with_timezone(&Utc), close.with_timezone(&Utc));
            }
        }
    }

    // 3) Otherwise, fall forward to the next session after t.
    next_session_after_with(kind, hours, t)
}

/// Backwards-compatible wrapper using Both (regular+extended)
pub fn session_bounds(hours: &MarketHours, t: DateTime<Utc>) -> (DateTime<Utc>, DateTime<Utc>) {
    session_bounds_with(SessionKind::Both, hours, t)
}

/// Compute week-level session bounds in UTC that contain the instant `t`.
///
/// Semantics:
/// - If the exchange has a true weekend close (`has_weekend_close = true`):
///   The weekly window is **[Mon 00:00 local, next Mon 00:00 local)**.
/// - Otherwise (continuous products without a true weekend close):
///   The weekly window is **[Sun 17:00 local, next Sun 17:00 local)**,
///   which matches common futures "Sunday evening" reopen conventions.
///
/// Returns `(week_open_utc, week_close_utc)`.
pub fn week_session_bounds(
    hours: &MarketHours,
    t: DateTime<Utc>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let tz = hours.tz;
    let local = t.with_timezone(&tz);
    let day = local.date_naive();

    if hours.has_weekend_close {
        // Start at Monday 00:00 local of the week containing `t`
        let dow_idx_from_mon = day.weekday().num_days_from_monday() as i64; // Mon=0..Sun=6
        let mon_date = day - chrono::Duration::days(dow_idx_from_mon);
        let week_open_local = tz
            .with_ymd_and_hms(mon_date.year(), mon_date.month(), mon_date.day(), 0, 0, 0)
            .earliest()
            .unwrap()
            .with_timezone(&Utc);
        let next_mon = mon_date + chrono::Duration::days(7);
        let week_close_local = tz
            .with_ymd_and_hms(next_mon.year(), next_mon.month(), next_mon.day(), 0, 0, 0)
            .earliest()
            .unwrap()
            .with_timezone(&Utc);
        (week_open_local, week_close_local)
    } else {
        // Start at most-recent Sunday 17:00 local prior to `t`
        use chrono::Weekday;
        let wd = day.weekday();
        let days_since_sun = wd.num_days_from_sunday() as i64; // Sun=0..Sat=6
        let mut sun_date = day - chrono::Duration::days(days_since_sun);
        // If it's Sunday but before 17:00 local, the "most-recent Sunday 17:00" is last week
        let secs = local.time().num_seconds_from_midnight();
        if wd == Weekday::Sun && secs < 17 * 3600 {
            sun_date = sun_date - chrono::Duration::days(7);
        }

        let week_open_local = tz
            .with_ymd_and_hms(sun_date.year(), sun_date.month(), sun_date.day(), 17, 0, 0)
            .earliest()
            .unwrap()
            .with_timezone(&Utc);
        let next_sun = sun_date + chrono::Duration::days(7);
        let week_close_local = tz
            .with_ymd_and_hms(next_sun.year(), next_sun.month(), next_sun.day(), 17, 0, 0)
            .earliest()
            .unwrap()
            .with_timezone(&Utc);
        (week_open_local, week_close_local)
    }
}

/// Next session bounds strictly after `end_excl` (searches forward, skipping holidays).
pub fn next_session_after_with(
    kind: SessionKind,
    hours: &MarketHours,
    end_excl: DateTime<Utc>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    let local = end_excl.with_timezone(&hours.tz);
    let base_day = local.date_naive();
    let ssm_now = local.num_seconds_from_midnight() as i64;

    for dd in 0..14 {
        let d = base_day + Duration::days(dd);

        // Consider ALL rules applicable on this day (don’t stop at the first one)
        let mut best_open: Option<(DateTime<Utc>, DateTime<Utc>)> = None;
        let w = d.weekday().num_days_from_monday() as usize; // Mon=0..Sun=6
        for r in hours.iter_rules(kind) {
            if !r.days[w] {
                continue;
            }
            if r.open_ssm as i64 <= r.close_ssm as i64 {
                if dd > 0 || ssm_now < r.open_ssm as i64 {
                    let open_l = mk_local(hours.tz, d, r.open_ssm);
                    let close_l = mk_local(hours.tz, d, r.close_ssm);
                    let cand = (open_l.with_timezone(&Utc), close_l.with_timezone(&Utc));
                    best_open = match best_open {
                        None => Some(cand),
                        Some(cur) => Some(if cand.0 < cur.0 { cand } else { cur }),
                    };
                }
            } else {
                if dd > 0 || ssm_now < r.open_ssm as i64 {
                    let open_l = mk_local(hours.tz, d, r.open_ssm);
                    let close_l = mk_local(hours.tz, d + Duration::days(1), r.close_ssm);
                    let cand = (open_l.with_timezone(&Utc), close_l.with_timezone(&Utc));
                    best_open = match best_open {
                        None => Some(cand),
                        Some(cur) => Some(if cand.0 < cur.0 { cand } else { cur }),
                    };
                }
            }
        }
        if let Some(b) = best_open {
            return b;
        }
    }
    (end_excl, end_excl)
}

pub fn next_session_after(
    hours: &MarketHours,
    end_excl: DateTime<Utc>,
) -> (DateTime<Utc>, DateTime<Utc>) {
    next_session_after_with(SessionKind::Both, hours, end_excl)
}


fn mk_local(tz: Tz, day: chrono::NaiveDate, ssm: u32) -> chrono::DateTime<Tz> {
    let base: NaiveDateTime = day.and_hms_opt(0, 0, 0).unwrap() + Duration::seconds(ssm as i64);
    // Resolve TZ (handle DST transitions); prefer `.single()` and fallback safely
    tz.from_local_datetime(&base)
        .single()
        .unwrap_or_else(|| tz.from_utc_datetime(&base))
}

#[inline]
pub fn next_session_open_after(mh: &MarketHours, after_utc: DateTime<Utc>) -> DateTime<Utc> {
    // Search up to 14 days ahead (safety bound)
    let tz: Tz = mh.tz;
    let after_local = after_utc.with_timezone(&tz);

    // quick holiday check
    let is_holiday = |d: NaiveDate| mh.holidays.iter().any(|h| *h == d);

    // combine regular + extended for “is open” calendar
    let mut rules: Vec<&SessionRule> = Vec::new();
    rules.extend(mh.regular.iter());
    rules.extend(mh.extended.iter());

    // Walk day-by-day until we find the first session whose OPEN is after `after_local`
    for day_offset in 0..14 {
        let day_local = after_local.date_naive() + chrono::Days::new(day_offset);
        if is_holiday(day_local) {
            continue;
        }

        // Which weekday is this?
        let wd: Weekday = tz
            .with_ymd_and_hms(
                day_local.year(),
                day_local.month(),
                day_local.day(),
                0,
                0,
                0,
            )
            .single()
            .expect("valid local midnight")
            .weekday();
        // Standardize weekday indexing to Mon=0 .. Sun=6 (consistent with rule_for_date_in)
        let wd_idx = wd.num_days_from_monday() as usize; // 0=Mon..6=Sun

        // Evaluate all rules for this day; pick the earliest open strictly after `after_local`
        let mut candidate: Option<DateTime<Tz>> = None;

        for r in &rules {
            if !r.days[wd_idx] {
                continue;
            }

            // Construct local open time
            let open_ssm = r.open_ssm.max(0).min(24 * 3600);
            let open_h = (open_ssm / 3600) as u32;
            let open_m = ((open_ssm % 3600) / 60) as u32;
            let open_s = (open_ssm % 60) as u32;

            let open_local = tz
                .with_ymd_and_hms(
                    day_local.year(),
                    day_local.month(),
                    day_local.day(),
                    open_h,
                    open_m,
                    open_s,
                )
                .single();
            let Some(open_dt) = open_local else { continue }; // skip ambiguous/skipped local-midnight edges

            // Only accept opens strictly after our local time reference
            if open_dt <= after_local {
                continue;
            }

            candidate = match candidate {
                None => Some(open_dt),
                Some(best) => Some(best.min(open_dt)),
            };
        }

        if let Some(open_dt) = candidate {
            return open_dt.with_timezone(&Utc);
        }
    }

    // Fallback: if we didn’t find anything (odd calendar), nudge by 1 day in UTC
    after_utc + Duration::days(1)
}

#[cfg(test)]
pub mod mh_tests {
    use chrono::{Duration, NaiveDate, TimeZone, Utc};
    use chrono_tz::Tz;
    use chrono_tz::US::Central;
    use crate::data::core::Exchange;
    use crate::data::models::Resolution;
    use crate::securities::hours::market_hours::{hours_for_exchange, next_session_after, next_session_open_after, session_bounds, week_session_bounds};
    use crate::securities::hours::market_hours::{candle_end, next_session_after_with, time_end_of_day, SessionKind};

    fn utc(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).single().unwrap()
    }

    // Replace the failing test with these assertions:

    #[test]
    fn test_extended_is_open_1530_to_1600_ct_and_closed_1515_to_1530() {
        use chrono::Utc;

        let mh = hours_for_exchange(Exchange::CME);

        // 2023-06-05 is a Monday in CDT (UTC-5).
        // 15:20 CT -> 20:20 UTC (should be CLOSED: in the 15:15–15:30 halt)
        let t_closed = Utc
            .with_ymd_and_hms(2023, 6, 5, 20, 20, 0)
            .single()
            .unwrap();
        assert!(!mh.is_open(t_closed), "CME is halted 15:15–15:30 CT");

        // 15:45 CT -> 20:45 UTC (should be OPEN: brief 15:30–16:00 window)
        let t_open = Utc
            .with_ymd_and_hms(2023, 6, 5, 20, 45, 0)
            .single()
            .unwrap();
        assert!(mh.is_open(t_open), "CME reopens 15:30–16:00 CT");

        // 16:10 CT -> 21:10 UTC (should be CLOSED: daily maintenance 16:00–17:00 CT)
        let t_maint = Utc
            .with_ymd_and_hms(2023, 6, 5, 21, 10, 0)
            .single()
            .unwrap();
        assert!(!mh.is_open(t_maint), "CME maintenance 16:00–17:00 CT");
    }

    #[test]
    fn test_exact_close_exclusive_regular_1515_ct() {
        use chrono::Utc;

        let mh = hours_for_exchange(Exchange::CME);

        // Exactly 15:15:00 CT -> 20:15:00 UTC should be CLOSED for regular (end-exclusive)
        let close_edge = Utc
            .with_ymd_and_hms(2023, 6, 5, 20, 15, 0)
            .single()
            .unwrap();
        assert!(!mh.is_open_regular(close_edge));

        // But 15:30 CT -> 20:30 UTC should be OPEN via extended
        let ext_open = Utc
            .with_ymd_and_hms(2023, 6, 5, 20, 30, 0)
            .single()
            .unwrap();
        assert!(mh.is_open_extended(ext_open));
    }

    #[test]
    fn test_cme_friday_has_no_overnight() {
        // Friday, 2023-06-09 22:30 UTC = 17:30 CT, but CME equity-index has no 17:00 Friday overnight
        let mh = hours_for_exchange(Exchange::CME);
        let t = utc(2023, 6, 9, 22, 30, 0);
        assert!(!mh.is_open(t));

        // Next open should be Sunday 22:00 UTC (17:00 CT)
        let open = next_session_open_after(&mh, t);
        assert_eq!(open, utc(2023, 6, 11, 22, 0, 0));
    }

    #[test]
    fn test_wrap_does_not_bleed_into_holiday() {
        // Declare Monday 2023-07-03 as a holiday (example) and ensure the Sun wrap doesn't apply
        let mut mh = hours_for_exchange(Exchange::CME);
        let holiday = NaiveDate::from_ymd_opt(2023, 7, 3).unwrap();
        mh.holidays.push(holiday);

        // 2023-07-03 07:00 CT is 12:00 UTC, would normally be inside Sun 17:00 -> Mon 08:30 wrap
        let t = utc(2023, 7, 3, 12, 0, 0);
        assert!(
            !mh.is_open(t),
            "overnight wrap should not bleed into a holiday"
        );

        // And next session after should be the Monday 15:30 CT mini-window or Monday RTH depending on rules
        let (open, close) = next_session_after(&mh, t);
        // It should open strictly after t, and certainly not before 15:30 CT (20:30 UTC)
        assert!(open > t);
        assert!(close > open);
    }

    #[test]
    fn test_session_bounds_extended_wrap_window() {
        let mh = hours_for_exchange(Exchange::CME);
        // Inside the overnight wrap on Tue 2023-06-06 at 02:00 CT (07:00 UTC)
        let t = utc(2023, 6, 6, 7, 0, 0);
        let (open, close) = session_bounds(&mh, t);
        // The returned bounds must represent a session that contains t and has open < close.
        assert!(
            open < close,
            "expected open < close, got open={open} close={close}"
        );
        assert!(
            open <= t && t < close,
            "t must lie within returned session bounds: open={open} t={t} close={close}"
        );
    }

    #[test]
    fn test_exact_open_inclusive_exact_close_exclusive() {
        let mh = hours_for_exchange(Exchange::CME);

        // Regular open 08:30 CT => 13:30 UTC should be open
        let open_edge = utc(2023, 6, 5, 13, 30, 0);
        assert!(mh.is_open(open_edge));

        // Regular close 15:15 CT => 20:15 UTC should be closed at the exact instant
        let close_edge = utc(2023, 6, 5, 20, 15, 0);
        assert!(!mh.is_open(close_edge));
    }

    #[test]
    fn test_sunday_reopen_next_session() {
        let mh = hours_for_exchange(Exchange::CME);
        // Sunday 2023-06-11 at 21:00 UTC (16:00 CT) – before open
        let t = utc(2023, 6, 11, 21, 0, 0);
        let (open, close) = next_session_after(&mh, t);
        assert_eq!(open, utc(2023, 6, 11, 22, 0, 0));
        // Close Monday 08:30 CT => 13:30 UTC
        assert_eq!(close, utc(2023, 6, 12, 13, 30, 0));
    }

    #[test]
    fn test_extended_is_open_between_1515_and_1600_ct() {
        let mh = hours_for_exchange(Exchange::CME);
        // 15:20 CT on 2023-06-05 -> 20:20 UTC: must be CLOSED (15:15–15:30 halt)
        assert!(!mh.is_open(utc(2023, 6, 5, 20, 20, 0)));
    }


    fn ct(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> chrono::DateTime<Utc> {
        Central
            .with_ymd_and_hms(y, m, d, hh, mm, ss)
            .single()
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn test_hours_for_exchange_cme_properties() {
        let mh = hours_for_exchange(Exchange::CME);
        assert_eq!(mh.exchange, Exchange::CME);
        // Exchange TZ should be Central
        assert_eq!(mh.tz, Central);
        assert!(mh.has_daily_close);
        assert!(mh.has_weekend_close);
        // Should have both regular and extended rules defined
        assert!(!mh.regular.is_empty());
        assert!(!mh.extended.is_empty());
    }

    #[test]
    fn test_is_open_regular_and_extended_utc_cdt() {
        // Monday, Jun 5 2023 is in CDT (UTC-5)
        let mh = hours_for_exchange(Exchange::CME);
        // Regular session 08:30–15:15 CT → 13:30–20:15 UTC
        let t_open_reg = utc(2023, 6, 5, 13, 30, 0);
        let t_mid_reg = utc(2023, 6, 5, 17, 0, 0);
        let t_close_reg_edge = utc(2023, 6, 5, 20, 15, 0);
        assert!(mh.is_open_regular(t_open_reg));
        assert!(mh.is_open_regular(t_mid_reg));
        // Close is exclusive; exactly at 20:15:00 should be closed for regular
        assert!(!mh.is_open_regular(t_close_reg_edge));

        // Extended: 15:30–16:00 CT (20:30–21:00 UTC) and 17:00–08:30 CT (22:00–13:30 UTC next day)
        let t_ext1 = utc(2023, 6, 5, 20, 45, 0); // in 15:30–16:00 CT block
        let t_ext_gap = utc(2023, 6, 5, 21, 10, 0); // maintenance 16:00–17:00 CT
        let t_ext2 = utc(2023, 6, 5, 22, 10, 0); // in overnight 17:00–08:30 CT
        assert!(mh.is_open_with(t_ext1, SessionKind::Extended));
        assert!(!mh.is_open_with(t_ext_gap, SessionKind::Extended));
        assert!(mh.is_open_with(t_ext2, SessionKind::Extended));

        // Overall is_open should combine both
        assert!(mh.is_open(t_open_reg));
        assert!(mh.is_open(t_ext1));
        assert!(!mh.is_open(t_ext_gap));
    }

    #[test]
    fn test_is_open_across_timezones() {
        // Pick a time firmly inside regular session
        let mh = hours_for_exchange(Exchange::CME);
        let t_ct = ct(2023, 6, 5, 9, 0, 0); // 09:00 CT => inside regular
        let t_utc = t_ct; // already UTC converted above
        // London/Tokyo views should not change truth of is_open when we pass UTC
        let london: Tz = chrono_tz::Europe::London;
        let tokyo: Tz = chrono_tz::Asia::Tokyo;
        assert!(mh.is_open(t_utc));
        // Sanity: convert to these tzs and back to UTC yields same instant
        let _ = t_utc.with_timezone(&london);
        let _ = t_utc.with_timezone(&tokyo);
        assert!(mh.is_open(t_utc));
    }

    #[test]
    fn test_is_maintenance_during_break() {
        let mh = hours_for_exchange(Exchange::CME);
        // 16:10 CT => 21:10 UTC should be maintenance (within 50 min of 17:00 CT open)
        let t = utc(2023, 6, 5, 21, 10, 0);
        assert!(mh.is_maintenance(t));
        // Far after reopen
        let t2 = utc(2023, 6, 5, 23, 0, 0);
        assert!(!mh.is_maintenance(t2));
    }

    #[test]
    fn test_is_closed_all_day_in_calendar() {
        let mh = hours_for_exchange(Exchange::CME);
        // Saturday should be fully closed (no Friday overnight into Saturday under CME Globex rules)
        let sat = NaiveDate::from_ymd_opt(2023, 6, 10).unwrap();
        assert!(mh.is_closed_all_day_in_calendar(sat, chrono_tz::UTC, SessionKind::Both));
        assert!(mh.is_closed_all_day_in_calendar(sat, Central, SessionKind::Both));

        // Sunday in UTC calendar is not closed all day because trading starts 22:00 UTC
        let sun = NaiveDate::from_ymd_opt(2023, 6, 11).unwrap();
        assert!(!mh.is_closed_all_day_in_calendar(sun, chrono_tz::UTC, SessionKind::Both));

        // In Central calendar, Sunday also has a session (opens 17:00 CT), so not closed all day
        assert!(!mh.is_closed_all_day_in_calendar(sun, Central, SessionKind::Both));
    }

    #[test]
    fn test_candle_end_minutes_hours_daily_weekly() {
        let exch = Exchange::CME;
        // 1-minute: starting at 13:29:00 UTC, end should be 13:29:59.999999999 UTC
        let t = utc(2023, 6, 5, 13, 29, 0);
        let end1 = candle_end(t, Resolution::Minutes(1), exch).unwrap();
        assert_eq!(end1, utc(2023, 6, 5, 13, 30, 0) - Duration::nanoseconds(1));

        // 1-hour: 14:00 UTC => next at 15:00 UTC minus 1ns
        let t2 = utc(2023, 6, 5, 14, 0, 0);
        let endh = candle_end(t2, Resolution::Hours(1), exch).unwrap();
        assert_eq!(endh, utc(2023, 6, 5, 15, 0, 0) - Duration::nanoseconds(1));

        // Daily: any time during Monday session should end at session close 20:15 UTC - 1ns
        let td = utc(2023, 6, 5, 18, 0, 0);
        let endd = candle_end(td, Resolution::Daily, exch).unwrap();
        assert_eq!(endd, utc(2023, 6, 5, 20, 15, 0) - Duration::nanoseconds(1));

        // Weekly: pick Wed 2023-06-07; next Monday 00:00 Central is 2023-06-12 05:00 UTC (CDT), minus 1ns
        let tw = utc(2023, 6, 7, 12, 0, 0);
        let expected_mon_ct = Central
            .with_ymd_and_hms(2023, 6, 12, 0, 0, 0)
            .single()
            .unwrap()
            .with_timezone(&Utc);
        let endw = candle_end(tw, Resolution::Weekly, exch).unwrap();
        assert_eq!(endw, expected_mon_ct - Duration::nanoseconds(1));
    }

    #[test]
    fn test_time_end_of_day_delegates() {
        let exch = Exchange::CME;
        let t = utc(2023, 6, 5, 13, 29, 0);
        let a = candle_end(t, Resolution::Minutes(1), exch).unwrap();
        let b = time_end_of_day(t, Resolution::Minutes(1), exch).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_session_bounds_and_next_session() {
        let mh = hours_for_exchange(Exchange::CME);
        // Inside regular session
        let t = utc(2023, 6, 5, 14, 0, 0);
        let (o, c) = session_bounds(&mh, t);
        assert_eq!(o, utc(2023, 6, 5, 13, 30, 0));
        assert_eq!(c, utc(2023, 6, 5, 20, 15, 0));

        // After regular close but before overnight open
        let t2 = utc(2023, 6, 5, 21, 0, 0);
        let (o2, c2) = next_session_after(&mh, t2);
        assert_eq!(o2, utc(2023, 6, 5, 22, 0, 0));
        // Close next day 08:30 CT => 13:30 UTC on Tue 6th
        assert_eq!(c2, utc(2023, 6, 6, 13, 30, 0));

        // Explicit kind = Regular: next regular session after 21:00 UTC should be Tue 13:30–20:15 UTC
        let (o3, c3) = next_session_after_with(SessionKind::Regular, &mh, t2);
        assert_eq!(o3, utc(2023, 6, 6, 13, 30, 0));
        assert_eq!(c3, utc(2023, 6, 6, 20, 15, 0));
    }

    #[test]
    fn test_next_session_open_after_helper() {
        let mh = hours_for_exchange(Exchange::CME);
        // During maintenance 16:10 CT => 21:10 UTC, next open is 22:00 UTC
        let t = utc(2023, 6, 5, 21, 10, 0);
        let open = next_session_open_after(&mh, t);
        assert_eq!(open, utc(2023, 6, 5, 22, 0, 0));
    }

    #[test]
    fn test_extended_window_edges_1530_open_1600_close() {
        // CME: extended mini-window 15:30–16:00 CT (20:30–21:00 UTC)
        let mh = hours_for_exchange(Exchange::CME);
        // Exactly at 15:30 CT -> 20:30 UTC should be OPEN
        let open_edge = utc(2023, 6, 5, 20, 30, 0);
        assert!(mh.is_open_extended(open_edge), "15:30 CT should be open (extended)");

        // Just before 16:00 CT -> 20:59:59 UTC should still be OPEN
        let just_before_close = utc(2023, 6, 5, 20, 59, 59);
        assert!(mh.is_open_extended(just_before_close));

        // Exactly at 16:00 CT -> 21:00 UTC should be CLOSED (maintenance window begins)
        let close_edge = utc(2023, 6, 5, 21, 0, 0);
        assert!(!mh.is_open_extended(close_edge));
        assert!(!mh.is_open(close_edge));
    }

    #[test]
    fn test_overnight_edge_1700_ct_open() {
        // CME: Overnight opens at 17:00 CT (22:00 UTC) on Sun + Mon–Thu
        let mh = hours_for_exchange(Exchange::CME);

        // One second before 17:00 CT (21:59:59 UTC) is CLOSED
        let before = utc(2023, 6, 5, 21, 59, 59);
        assert!(!mh.is_open(before));

        // Exactly at 17:00 CT -> 22:00 UTC should be OPEN
        let at_open = utc(2023, 6, 5, 22, 0, 0);
        assert!(mh.is_open(at_open));
    }

    #[test]
    fn test_week_session_bounds_weekend_close_true() {
        // With a true weekend close, weekly bounds are Mon 00:00 local .. next Mon 00:00 local
        let mh = hours_for_exchange(Exchange::CME);
        // Pick a time mid-week
        let t = utc(2023, 6, 7, 12, 0, 0); // Wed
        let (wopen, wclose) = week_session_bounds(&mh, t);

        // Expected: Mon 2023-06-05 00:00 Central -> 05:00 UTC (CDT),
        //           Mon 2023-06-12 00:00 Central -> 05:00 UTC.
        let expected_open = chrono_tz::US::Central
            .with_ymd_and_hms(2023, 6, 5, 0, 0, 0).single().unwrap()
            .with_timezone(&Utc);
        let expected_close = chrono_tz::US::Central
            .with_ymd_and_hms(2023, 6, 12, 0, 0, 0).single().unwrap()
            .with_timezone(&Utc);

        assert_eq!(wopen, expected_open);
        assert_eq!(wclose, expected_close);
    }

    #[test]
    fn test_week_session_bounds_continuous_products() {
        // For continuous products (no true weekend close), use Sun 17:00 local week windows.
        let mut mh = hours_for_exchange(Exchange::CME);
        mh.has_weekend_close = false; // simulate a continuous product profile on CME tz/rules

        let t = utc(2023, 6, 7, 12, 0, 0); // Wed
        let (wopen, wclose) = week_session_bounds(&mh, t);

        // Expect the window to be Sun 2023-06-04 17:00 Central -> 22:00 UTC,
        // then Sun 2023-06-11 17:00 Central -> 22:00 UTC.
        let expected_open = chrono_tz::US::Central
            .with_ymd_and_hms(2023, 6, 4, 17, 0, 0).single().unwrap()
            .with_timezone(&Utc);
        let expected_close = chrono_tz::US::Central
            .with_ymd_and_hms(2023, 6, 11, 17, 0, 0).single().unwrap()
            .with_timezone(&Utc);

        assert_eq!(wopen, expected_open);
        assert_eq!(wclose, expected_close);
    }

    #[test]
    fn test_next_session_after_prefers_earliest_after_gap() {
        // Retail profile: no 15:30–16:00 mini-window; after the 15:15–16:00 break,
        // the next session open is the 17:00 CT overnight (22:00 UTC).
        let mh = hours_for_exchange(Exchange::CME);
        // 15:20 CT -> 20:20 UTC
        let t = utc(2023, 6, 5, 20, 20, 0);
        let (o, c) = next_session_after(&mh, t);
        assert_eq!(o, utc(2023, 6, 5, 22, 0, 0));
        // Overnight wraps and closes at 08:30 CT next day -> 13:30 UTC on Tue 6th
        assert_eq!(c, utc(2023, 6, 6, 13, 30, 0));
    }

    #[test]
    fn test_is_closed_all_day_utc_vs_local_calendars() {
        let mh = hours_for_exchange(Exchange::CME);
        // UTC Sunday has trading at 22:00; therefore not closed all day in UTC calendar.
        let sun = chrono::NaiveDate::from_ymd_opt(2023, 6, 11).unwrap();
        assert!(!mh.is_closed_all_day_in_calendar(sun, chrono_tz::UTC, SessionKind::Both));

        // But Saturday is closed in both calendars.
        let sat = chrono::NaiveDate::from_ymd_opt(2023, 6, 10).unwrap();
        assert!(mh.is_closed_all_day_in_calendar(sat, chrono_tz::UTC, SessionKind::Both));
        assert!(mh.is_closed_all_day_in_calendar(sat, chrono_tz::US::Central, SessionKind::Both));
    }

    #[test]
    fn test_session_bounds_yesterday_wrap_capture() {
        // Early morning inside the overnight wrap should attribute to the prior day's open
        let mh = hours_for_exchange(Exchange::CME);
        // Tue 2023-06-06 02:00 CT = 07:00 UTC
        let t = utc(2023, 6, 6, 7, 0, 0);
        let (o, c) = session_bounds(&mh, t);
        // Open should be Mon 17:00 CT -> 22:00 UTC
        assert_eq!(o, utc(2023, 6, 5, 22, 0, 0));
        // Close should be Tue 08:30 CT -> 13:30 UTC
        assert_eq!(c, utc(2023, 6, 6, 13, 30, 0));
    }
}
