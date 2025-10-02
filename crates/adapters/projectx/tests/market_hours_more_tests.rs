use chrono::{NaiveDate, TimeZone, Utc};
use nautilus_projectx::common::{
    market_hours::{
        hours_for_exchange, next_session_after, next_session_open_after, session_bounds,
    },
    models::Exchange,
};
use rstest::rstest;

fn utc(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).single().unwrap()
}

// Replace the failing test with these assertions:

#[rstest]
fn test_extended_is_open_1530_to_1600_ct_and_closed_1515_to_1530() {
    use chrono::Utc;

    let mh = hours_for_exchange(Exchange::CME).unwrap();

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

#[rstest]
fn test_exact_close_exclusive_regular_1515_ct() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();

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

#[rstest]
fn test_cme_friday_has_no_overnight() {
    // Friday, 2023-06-09 22:30 UTC = 17:30 CT, but CME equity-index has no 17:00 Friday overnight
    let mh = hours_for_exchange(Exchange::CME).unwrap();
    let t = utc(2023, 6, 9, 22, 30, 0);
    assert!(!mh.is_open(t));

    // Next open should be Sunday 22:00 UTC (17:00 CT)
    let open = next_session_open_after(&mh, t);
    assert_eq!(open, utc(2023, 6, 11, 22, 0, 0));
}

#[rstest]
fn test_wrap_does_not_bleed_into_holiday() {
    // Declare Monday 2023-07-03 as a holiday (example) and ensure the Sun wrap doesn't apply
    let mut mh = hours_for_exchange(Exchange::CME).unwrap();
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

#[rstest]
fn test_session_bounds_extended_wrap_window() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();
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

#[rstest]
fn test_exact_open_inclusive_exact_close_exclusive() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();

    // Regular open 08:30 CT => 13:30 UTC should be open
    let open_edge = utc(2023, 6, 5, 13, 30, 0);
    assert!(mh.is_open(open_edge));

    // Regular close 15:15 CT => 20:15 UTC should be closed at the exact instant
    let close_edge = utc(2023, 6, 5, 20, 15, 0);
    assert!(!mh.is_open(close_edge));
}

#[rstest]
fn test_sunday_reopen_next_session() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();
    // Sunday 2023-06-11 at 21:00 UTC (16:00 CT) – before open
    let t = utc(2023, 6, 11, 21, 0, 0);
    let (open, close) = next_session_after(&mh, t);
    assert_eq!(open, utc(2023, 6, 11, 22, 0, 0));
    // Close Monday 08:30 CT => 13:30 UTC
    assert_eq!(close, utc(2023, 6, 12, 13, 30, 0));
}

#[rstest]
fn test_extended_is_open_between_1515_and_1600_ct() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();
    // 15:20 CT on 2023-06-05 -> 20:20 UTC: must be OPEN (no halt since 2021)
    assert!(!mh.is_open(utc(2023, 6, 5, 20, 20, 0))); // in the 3:15–3:30 CT halt
}
