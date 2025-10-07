use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use std::str::FromStr;
use tt_database::duck::{create_partitions_schema, earliest_available, latest_available};
use tt_database::ingest::{ingest_bbo, ingest_candles, ingest_ticks};
use tt_database::init::create_identity_schema_if_needed;
use tt_database::models::{BboRow, CandleRow, TickRow};
use tt_database::paths::{data_file_name, partition_dir, provider_kind_to_db_string};
use tt_types::base_data::Resolution;
use tt_types::keys::Topic;
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::{Instrument, MarketType};

fn setup_conn() -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().expect("duckdb mem");
    create_identity_schema_if_needed(&conn).expect("init identity schema");
    create_partitions_schema(&conn).expect("init partitions schema");
    conn
}

fn temp_root() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

// ---------- test data helpers ----------

fn fake_ticks(provider_s: ProviderKind, symbol_s: &str, base_ns: i64, n: usize) -> Vec<TickRow> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        v.push(TickRow {
            provider: provider_kind_to_db_string(provider_s),
            symbol_id: symbol_s.to_string(),
            price: 10000.0 + i as f64,
            size: 1.0,
            side: 1,
            key_ts_utc_ns: base_ns + (i as i64) * 1_000_000, // +1ms
            key_tie: 0,
            venue_seq: Some(i as u32),
            exec_id: None,
        });
    }
    v
}

fn fake_bbo(provider_s: &str, symbol_s: &str, base_ns: i64, n: usize) -> Vec<BboRow> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        v.push(BboRow {
            provider: provider_s.to_string(),
            symbol_id: symbol_s.to_string(),
            key_ts_utc_ns: base_ns + (i as i64) * 1_000_000,
            bid: 10000.0 + i as f64,
            bid_size: 1.0,
            ask: 10000.5 + i as f64,
            ask_size: 1.0,
            bid_orders: None,
            ask_orders: None,
            venue_seq: Some(i as u32),
            is_snapshot: Some(true),
        });
    }
    v
}

fn fake_candles(
    provider_s: &str,
    instrument: &Instrument,
    topic: Topic,
    day: NaiveDate,
    n: usize,
) -> Vec<CandleRow> {
    let res = match topic {
        Topic::Candles1s => Resolution::Seconds(1),
        Topic::Candles1m => Resolution::Minutes(1),
        Topic::Candles1h => Resolution::Hours(1),
        Topic::Candles1d => Resolution::Daily,
        _ => Resolution::Minutes(1),
    };
    let secs: i64 = match res {
        Resolution::Seconds(s) => s as i64,
        Resolution::Minutes(m) => (m as i64) * 60,
        Resolution::Hours(h) => (h as i64) * 3600,
        Resolution::Daily => 24 * 3600,
        Resolution::Weekly => 7 * 24 * 3600,
    };
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let start = Utc
            .with_ymd_and_hms(day.year(), day.month(), day.day(), 0, 0, 0)
            .unwrap()
            + chrono::Duration::seconds(secs * (i as i64));
        let end = start + chrono::Duration::seconds(secs);
        v.push(CandleRow {
            provider: provider_s.to_string(),
            symbol_id: instrument.to_string(),
            res: res.to_os_string(),
            time_start_ns: start.timestamp_nanos_opt().unwrap(),
            time_end_ns: end.timestamp_nanos_opt().unwrap(),
            open: 10000.0 + i as f64,
            high: 10010.0 + i as f64,
            low: 9990.0 + i as f64,
            close: 10005.0 + i as f64,
            volume: 1.0,
            ask_volume: 0.5,
            bid_volume: 0.5,
        });
    }
    v
}

// ---------- tests ----------

#[test]
fn test_ingest_ticks_merge_monthly_file() {
    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    // Provider string must match writer mapping (see provider_kind_to_db_string)
    let provider_s = ProviderKind::ProjectX(ProjectXTenant::Topstep);
    let instrument = Instrument::from_str("TESTSM").unwrap();
    let market = MarketType::Futures;

    let day = NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();
    let base_dt = Utc.with_ymd_and_hms(2025, 3, 15, 14, 0, 0).unwrap();
    let base_ns = base_dt.timestamp_nanos_opt().unwrap();

    let batch1 = fake_ticks(provider_s, &instrument.to_string(), base_ns, 5);
    let mut batch2 = fake_ticks(provider_s, &instrument.to_string(), base_ns + 3_000_000, 5);
    batch2.push(batch1[4].clone());

    let out1 = ingest_ticks(
        &conn,
        &provider_s,
        &instrument,
        market,
        Topic::Ticks,
        &batch1,
        root,
        9,
    )
    .expect("ingest1");
    let out2 = ingest_ticks(
        &conn,
        &provider_s,
        &instrument,
        market,
        Topic::Ticks,
        &batch2,
        root,
        9,
    )
    .expect("ingest2");

    assert_eq!(out1.len(), 1);
    assert_eq!(out2.len(), 1);
    assert_eq!(out1[0], out2[0]);

    let year = day.year() as u32;
    let expected_dir = partition_dir(root, provider_s, market, &instrument, Topic::Ticks, year);
    let expected_name = data_file_name(
        &instrument,
        Topic::Ticks,
        NaiveDate::from_ymd_opt(day.year(), day.month(), 1).unwrap(),
    );
    let expected_path = expected_dir.join(expected_name);
    assert_eq!(out1[0], expected_path);

    let sql = format!(
        "SELECT COUNT(*) FROM read_parquet('{}')",
        expected_path.display()
    );
    let cnt: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 8);
}

#[test]
fn test_candles_and_bbo_monthly_paths_and_catalog() {
    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    let provider_kind = ProviderKind::ProjectX(ProjectXTenant::Demo);
    let provider_s = "projectx";
    let instrument = Instrument::from_str("TESTSM").unwrap();
    let market = MarketType::Futures;

    let day = NaiveDate::from_ymd_opt(2025, 2, 7).unwrap();

    // Candles 1m
    let rows_m1 = fake_candles(provider_s, &instrument, Topic::Candles1m, day, 3);
    let out_m1 = ingest_candles(
        &conn,
        &provider_kind,
        &instrument,
        market,
        Topic::Candles1m,
        &rows_m1,
        root,
        9,
    )
    .unwrap();
    assert_eq!(out_m1.len(), 1);
    let expected_dir_m1 = partition_dir(
        root,
        provider_kind,
        market,
        &instrument,
        Topic::Candles1m,
        day.year() as u32,
    );
    let expected_name_m1 = data_file_name(
        &instrument,
        Topic::Candles1m,
        NaiveDate::from_ymd_opt(day.year(), day.month(), 1).unwrap(),
    );
    assert_eq!(out_m1[0], expected_dir_m1.join(expected_name_m1));

    // BBO
    let base_dt = Utc
        .with_ymd_and_hms(day.year(), day.month(), day.day(), 10, 0, 0)
        .unwrap();
    let base_ns = base_dt.timestamp_nanos_opt().unwrap();
    let b1 = fake_bbo(provider_s, &instrument.to_string(), base_ns, 4);
    let b2 = fake_bbo(provider_s, &instrument.to_string(), base_ns + 2_000_000, 4);
    let out1 = ingest_bbo(
        &conn,
        &provider_kind,
        &instrument,
        market,
        Topic::Quotes,
        &b1,
        root,
        9,
    )
    .unwrap();
    let out2 = ingest_bbo(
        &conn,
        &provider_kind,
        &instrument,
        market,
        Topic::Quotes,
        &b2,
        root,
        9,
    )
    .unwrap();
    assert_eq!(out1, out2);
    let expected_dir_bbo = partition_dir(
        root,
        provider_kind,
        market,
        &instrument,
        Topic::Quotes,
        day.year() as u32,
    );
    let expected_name_bbo = data_file_name(
        &instrument,
        Topic::Quotes,
        NaiveDate::from_ymd_opt(day.year(), day.month(), 1).unwrap(),
    );
    assert_eq!(out1[0], expected_dir_bbo.join(expected_name_bbo));
    let provider = ProviderKind::ProjectX(ProjectXTenant::Demo);
    // Catalog smoke: earliest/latest across these inserts
    let e = earliest_available(&conn, &provider, &instrument, Topic::Quotes)
        .unwrap()
        .unwrap();
    let l = latest_available(&conn, "projectx", &instrument.to_string(), Topic::Quotes)
        .unwrap()
        .unwrap();
    assert!(e.ts <= l.ts);
}

#[test]
fn test_earliest_latest_ticks_across_days() {
    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    let provider_kind = ProviderKind::ProjectX(ProjectXTenant::Demo);
    let instrument = Instrument::from_str("TESTSM").unwrap();
    let market = MarketType::Futures;
    let provider_s = ProviderKind::ProjectX(ProjectXTenant::Demo);
    let day1 = NaiveDate::from_ymd_opt(2025, 6, 10).unwrap();
    let dt1 = Utc.with_ymd_and_hms(2025, 6, 10, 12, 0, 0).unwrap();
    let base1 = dt1.timestamp_nanos_opt().unwrap();
    let b1 = fake_ticks(provider_s, &instrument.to_string(), base1, 3);
    let _ = ingest_ticks(
        &conn,
        &provider_kind,
        &instrument,
        market,
        Topic::Ticks,
        &b1,
        root,
        9,
    )
    .unwrap();

    let day2 = NaiveDate::from_ymd_opt(2025, 6, 11).unwrap();
    let dt2 = Utc.with_ymd_and_hms(2025, 6, 11, 12, 0, 0).unwrap();
    let base2 = dt2.timestamp_nanos_opt().unwrap();
    let b2 = fake_ticks(provider_s, &instrument.to_string(), base2, 5);
    let _ = ingest_ticks(
        &conn,
        &provider_kind,
        &instrument,
        market,
        Topic::Ticks,
        &b2,
        root,
        9,
    )
    .unwrap();

    let e = earliest_available(&conn, &provider_s, &instrument, Topic::Ticks)
        .unwrap()
        .unwrap();
    let l = latest_available(&conn, "projectx", &instrument.to_string(), Topic::Ticks)
        .unwrap()
        .unwrap();
    assert_eq!(e.ts.date_naive(), day1);
    assert_eq!(l.ts.date_naive(), day2);
}

#[test]
fn test_ingest_candles_merge_monthly_file() {
    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    let provider_kind = ProviderKind::ProjectX(ProjectXTenant::Demo);
    let provider_s = "projectx";
    let instrument = Instrument::from_str("TESTSM").unwrap();
    let market = MarketType::Futures;

    let day = NaiveDate::from_ymd_opt(2025, 2, 7).unwrap();

    // First batch: 5 consecutive 1m candles starting at 00:00
    let batch1 = fake_candles(provider_s, &instrument, Topic::Candles1m, day, 5);

    // Second batch: overlap last 2 from batch1 and add 3 new (indices 3..7) -> total unique = 8
    let mut full = fake_candles(provider_s, &instrument, Topic::Candles1m, day, 8);
    let batch2: Vec<CandleRow> = full.drain(3..).collect();

    let out1 = ingest_candles(
        &conn,
        &provider_kind,
        &instrument,
        market,
        Topic::Candles1m,
        &batch1,
        root,
        9,
    )
    .expect("ingest candles 1");
    let out2 = ingest_candles(
        &conn,
        &provider_kind,
        &instrument,
        market,
        Topic::Candles1m,
        &batch2,
        root,
        9,
    )
    .expect("ingest candles 2");

    assert_eq!(out1.len(), 1);
    assert_eq!(out2.len(), 1);
    assert_eq!(out1[0], out2[0]);

    let expected_dir = partition_dir(
        root,
        provider_kind,
        market,
        &instrument,
        Topic::Candles1m,
        day.year() as u32,
    );
    let expected_name = data_file_name(
        &instrument,
        Topic::Candles1m,
        NaiveDate::from_ymd_opt(day.year(), day.month(), 1).unwrap(),
    );
    let expected_path = expected_dir.join(expected_name);
    assert_eq!(out1[0], expected_path);

    let sql = format!(
        "SELECT COUNT(*) FROM read_parquet('{}')",
        expected_path.display()
    );
    let cnt: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 8); // merged unique candles (0..7)
}
