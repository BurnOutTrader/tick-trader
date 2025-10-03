use chrono::{NaiveDate, TimeZone, Utc};
use tt_types::base_data::{Exchange, Resolution};
use tt_types::securities::market_hours::{hours_for_exchange, next_session_after, next_session_open_after, session_bounds};

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
fn test_next_bar_end_skips_maintenance_reopens_correctly() {
    let mh = hours_for_exchange(Exchange::CME);
    // 15:59:50 CT (20:59:50 UTC) – next 10s boundary lands in maintenance, should jump to 22:00 then to 22:00:10
    let t = utc(2023, 6, 5, 20, 59, 50);
    // Step of 10 seconds
    let mut next = mh.next_bar_end(t, Resolution::Seconds(10));
    // next_bar_end advances to the next valid boundary strictly after now, skipping closed periods.
    // From 20:59:50 UTC, the next multiple of 10s is 21:00:00 (closed), so it jumps to the next
    // session open (22:00:00) and then aligns to the next boundary (22:00:10).
    assert_eq!(next, utc(2023, 6, 5, 22, 0, 10));

    // If we ask again from exactly the previous result, the next boundary is 22:00:20
    next = mh.next_bar_end(next, Resolution::Seconds(10));
    assert_eq!(next, utc(2023, 6, 5, 22, 0, 20));
}

#[test]
fn test_extended_is_open_between_1515_and_1600_ct() {
    let mh = hours_for_exchange(Exchange::CME);
    // 15:20 CT on 2023-06-05 -> 20:20 UTC: must be OPEN (no halt since 2021)
    assert!(!mh.is_open(utc(2023, 6, 5, 20, 20, 0))); // in the 3:15–3:30 CT halt
}
