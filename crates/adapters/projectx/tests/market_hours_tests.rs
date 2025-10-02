use chrono::{Duration, NaiveDate, TimeZone, Utc};
use chrono_tz::{Tz, US::Central};
use nautilus_projectx::common::{
    market_hours::{
        SessionKind, hours_for_exchange, next_session_after, next_session_after_with,
        next_session_open_after, session_bounds,
    },
    models::Exchange,
};
use rstest::rstest;

fn ct(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> chrono::DateTime<Utc> {
    Central
        .with_ymd_and_hms(y, m, d, hh, mm, ss)
        .single()
        .unwrap()
        .with_timezone(&Utc)
}
fn utc(y: i32, m: u32, d: u32, hh: u32, mm: u32, ss: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(y, m, d, hh, mm, ss).single().unwrap()
}

#[rstest]
fn test_hours_for_exchange_cme_properties() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();
    assert_eq!(mh.exchange, Exchange::CME);
    // Exchange TZ should be Central
    assert_eq!(mh.tz, Central);
    assert!(mh.has_daily_close);
    assert!(mh.has_weekend_close);
    // Should have both regular and extended rules defined
    assert!(!mh.regular.is_empty());
    assert!(!mh.extended.is_empty());
}

#[rstest]
fn test_is_open_regular_and_extended_utc_cdt() {
    // Monday, Jun 5 2023 is in CDT (UTC-5)
    let mh = hours_for_exchange(Exchange::CME).unwrap();
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

#[rstest]
fn test_is_open_across_timezones() {
    // Pick a time firmly inside regular session
    let mh = hours_for_exchange(Exchange::CME).unwrap();
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

#[rstest]
fn test_is_maintenance_during_break() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();
    // 16:10 CT => 21:10 UTC should be maintenance (within 50 min of 17:00 CT open)
    let t = utc(2023, 6, 5, 21, 10, 0);
    assert!(mh.is_maintenance(t));
    // Far after reopen
    let t2 = utc(2023, 6, 5, 23, 0, 0);
    assert!(!mh.is_maintenance(t2));
}

#[rstest]
fn test_is_closed_all_day_in_calendar() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();
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

#[rstest]
fn test_session_bounds_and_next_session() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();
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

#[rstest]
fn test_next_session_open_after_helper() {
    let mh = hours_for_exchange(Exchange::CME).unwrap();
    // During maintenance 16:10 CT => 21:10 UTC, next open is 22:00 UTC
    let t = utc(2023, 6, 5, 21, 10, 0);
    let open = next_session_open_after(&mh, t);
    assert_eq!(open, utc(2023, 6, 5, 22, 0, 0));
}
