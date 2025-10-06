#![cfg(feature = "queries")]
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use database::paths::{daily_file_name, monthly_file_name, weekly_file_name};
use standard_lib::database;
use standard_lib::database::duck::create_partitions_schema;
use standard_lib::database::ingest::{ingest_bbo, ingest_candles, ingest_ticks};
use standard_lib::database::init::create_identity_schema_if_needed;
use standard_lib::database::models::{BboRow, CandleRow, DataKind, TickRow};
use standard_lib::database::paths::{get_partion, intraday_file_name};
use standard_lib::market_data::base_data::Resolution;
use standard_lib::securities::symbols::Exchange;
use standard_lib::securities::symbols::MarketType;

fn setup_conn() -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().expect("duckdb mem");
    // Initialize the real catalog schema used by the library to avoid drift.
    create_identity_schema_if_needed(&conn).expect("init identity schema");
    create_partitions_schema(&conn).expect("init partitions schema");
    conn
}

fn temp_root() -> tempfile::TempDir {
    tempfile::tempdir().expect("tempdir")
}

fn fake_ticks(provider: &str, symbol: &str, base_ns: i64, n: usize) -> Vec<TickRow> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        v.push(TickRow {
            provider: provider.to_string(),
            symbol_id: symbol.to_string(),
            exchange: "CME".to_string(),
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

fn fake_bbo(provider: &str, symbol: &str, base_ns: i64, n: usize) -> Vec<BboRow> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        v.push(BboRow {
            provider: provider.to_string(),
            symbol_id: symbol.to_string(),
            exchange: "CME".to_string(),
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
    provider: &str,
    symbol: &str,
    res: Resolution,
    day: NaiveDate,
    n: usize,
) -> Vec<CandleRow> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        // 1-minute candles by default for spacing; adapt end time from resolution granularity crudely
        let secs: i64 = match res {
            Resolution::Seconds(s) => s as i64,
            Resolution::Minutes(m) => (m as i64) * 60,
            Resolution::Hours(h) => (h as i64) * 3600,
            Resolution::TickBars(_) | Resolution::Ticks => 1,
            Resolution::Daily => 24 * 3600,
            Resolution::Weekly => 7 * 24 * 3600,
        };
        let start = Utc
            .with_ymd_and_hms(day.year(), day.month(), day.day(), 0, 0, 0)
            .unwrap()
            + chrono::Duration::seconds(secs * (i as i64));
        let end = start + chrono::Duration::seconds(secs);
        v.push(CandleRow {
            provider: provider.to_string(),
            symbol_id: symbol.to_string(),
            exchange: "CME".to_string(),
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
            num_trades: 10,
        });
    }
    v
}

#[test]
fn test_ingest_ticks_merge_daily_file() {
    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    let provider = "TESTPROV"; // fake provider to avoid real paths
    let symbol = "TESTSYM";
    let market = MarketType::Futures;

    // Two timestamps on the same UTC day
    let day = NaiveDate::from_ymd_opt(2025, 3, 15).unwrap();
    let base_dt = Utc.with_ymd_and_hms(2025, 3, 15, 14, 0, 0).unwrap();
    let base_ns = base_dt.timestamp_nanos_opt().unwrap();

    let batch1 = fake_ticks(provider, symbol, base_ns, 5);
    let mut batch2 = fake_ticks(provider, symbol, base_ns + 3_000_000, 5); // overlaps last 2
    // Introduce an exact duplicate to test dedup on key cols
    batch2.push(batch1[4].clone());

    let out1 = ingest_ticks(&conn, root, provider, market, symbol, &batch1, 9).expect("ingest1");
    let out2 = ingest_ticks(&conn, root, provider, market, symbol, &batch2, 9).expect("ingest2");

    // Should be exactly one file path for that day
    assert_eq!(out1.len(), 1);
    assert_eq!(out2.len(), 1);
    assert_eq!(out1[0], out2[0]);

    // Validate the deterministic filename and directory
    let expected_dir = get_partion(
        root,
        provider,
        market,
        symbol,
        DataKind::Tick,
        Resolution::Ticks,
        day,
    );
    let expected_name = intraday_file_name(symbol, DataKind::Tick, Resolution::Ticks, day);
    let expected_path = expected_dir.join(expected_name);
    assert_eq!(out1[0], expected_path);
    assert!(expected_path.exists(), "merged parquet should exist");

    // Query merged row count via DuckDB should be 8 unique rows (5 + 5 - overlap 2) but +1 duplicate removed = still 8
    let sql = format!(
        "SELECT COUNT(*) FROM read_parquet('{}')",
        expected_path.display()
    );
    let cnt: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    assert_eq!(cnt, 8);

    // Partitions catalog should have an entry for this path
    let mut q = conn
        .prepare("SELECT rows, path FROM partitions WHERE path = ?")
        .unwrap();
    let mut rows = q
        .query(duckdb::params![expected_path.to_string_lossy().to_string()])
        .unwrap();
    if let Some(r) = rows.next().unwrap() {
        let rows_val: i64 = r.get(0).unwrap();
        let path_val: String = r.get(1).unwrap();
        assert_eq!(path_val, expected_path.to_string_lossy().to_string());
        assert_eq!(rows_val, cnt);
    } else {
        panic!("no partitions row");
    }
    // tempdir drops and removes files automatically
}

#[test]
fn test_candles_file_naming_by_resolution() {
    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    let provider = "TESTPROV";
    let symbol = "TESTSYM";
    let market = MarketType::Futures;

    let day = NaiveDate::from_ymd_opt(2025, 2, 7).unwrap();

    // Intraday minutes -> daily file
    let rows_m1 = fake_candles(provider, symbol, Resolution::Minutes(1), day, 3);
    let out_m1 = ingest_candles(
        &conn,
        provider,
        symbol,
        market,
        Resolution::Minutes(1),
        &rows_m1,
        root,
        9,
    )
    .unwrap();
    assert_eq!(out_m1.len(), 1);
    let expected_dir_m1 = get_partion(
        root,
        provider,
        market,
        symbol,
        DataKind::Candle,
        Resolution::Minutes(1),
        day,
    );
    let expected_name_m1 =
        database::paths::intraday_file_name(symbol, DataKind::Candle, Resolution::Minutes(1), day);
    assert_eq!(out_m1[0], expected_dir_m1.join(expected_name_m1));

    // Hourly -> monthly file (dir is yearly per paths.rs monthly_partition_dir)
    let rows_h1 = fake_candles(provider, symbol, Resolution::Hours(1), day, 3);
    let out_h1 = ingest_candles(
        &conn,
        provider,
        symbol,
        market,
        Resolution::Hours(1),
        &rows_h1,
        root,
        9,
    )
    .unwrap();
    let expected_dir_h1 = get_partion(
        root,
        provider,
        market,
        symbol,
        DataKind::Candle,
        Resolution::Hours(1),
        day,
    );
    let expected_name_h1 = monthly_file_name(symbol, DataKind::Candle, day.year(), day.month());
    assert_eq!(out_h1[0], expected_dir_h1.join(expected_name_h1));

    // Daily -> yearly file
    let rows_d = fake_candles(provider, symbol, Resolution::Daily, day, 2);
    let out_d = ingest_candles(
        &conn,
        provider,
        symbol,
        market,
        Resolution::Daily,
        &rows_d,
        root,
        9,
    )
    .unwrap();
    let expected_dir_d = get_partion(
        root,
        provider,
        market,
        symbol,
        DataKind::Candle,
        Resolution::Daily,
        day,
    );
    let expected_name_d = daily_file_name(symbol, DataKind::Candle, day.year());
    assert_eq!(out_d[0], expected_dir_d.join(expected_name_d));

    // Weekly -> single file
    let rows_w = fake_candles(provider, symbol, Resolution::Weekly, day, 1);
    let out_w = ingest_candles(
        &conn,
        provider,
        symbol,
        market,
        Resolution::Weekly,
        &rows_w,
        root,
        9,
    )
    .unwrap();
    let expected_dir_w = get_partion(
        root,
        provider,
        market,
        symbol,
        DataKind::Candle,
        Resolution::Weekly,
        day,
    );
    let expected_name_w = weekly_file_name(symbol, DataKind::Candle);
    assert_eq!(out_w[0], expected_dir_w.join(expected_name_w));
}

#[test]
fn test_bbo_daily_file_and_catalog() {
    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    let provider = "TESTPROV";
    let symbol = "TESTSYM";
    let market = MarketType::Futures;

    let day = NaiveDate::from_ymd_opt(2025, 1, 2).unwrap();
    let base_dt = Utc.with_ymd_and_hms(2025, 1, 2, 10, 0, 0).unwrap();
    let base_ns = base_dt.timestamp_nanos_opt().unwrap();

    let batch1 = fake_bbo(provider, symbol, base_ns, 4);
    let batch2 = fake_bbo(provider, symbol, base_ns + 2_000_000, 4); // overlaps by 2

    let out1 = ingest_bbo(
        &conn,
        provider,
        market,
        symbol,
        Resolution::Seconds(1),
        &batch1,
        root,
        9,
    )
    .unwrap();
    let out2 = ingest_bbo(
        &conn,
        provider,
        market,
        symbol,
        Resolution::Seconds(1),
        &batch2,
        root,
        9,
    )
    .unwrap();

    assert_eq!(out1.len(), 1);
    assert_eq!(out1, out2);

    let expected_dir = get_partion(
        root,
        provider,
        market,
        symbol,
        DataKind::Bbo,
        Resolution::Seconds(1),
        day,
    );
    let expected_name = intraday_file_name(symbol, DataKind::Bbo, Resolution::Seconds(1), day);
    let expected_path = expected_dir.join(expected_name);
    assert_eq!(out1[0], expected_path);

    let sql = format!(
        "SELECT COUNT(*) FROM read_parquet('{}')",
        expected_path.display()
    );
    let cnt: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    // 4 + 4 - 2 overlap = 6 unique
    assert_eq!(cnt, 6);

    let present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partitions WHERE path = ?",
            duckdb::params![expected_path.to_string_lossy().to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(present, 1);
}

// ---------------------- New tests ----------------------

fn fake_books(
    symbol: &str,
    base_ns: i64,
    n: usize,
) -> Vec<standard_lib::market_data::base_data::OrderBook> {
    use rust_decimal::Decimal;
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        let t_ns = base_ns + (i as i64) * 1_000_000; // +1ms
        let time = chrono::DateTime::<Utc>::from_timestamp(
            t_ns.div_euclid(1_000_000_000),
            (t_ns.rem_euclid(1_000_000_000)) as u32,
        )
        .unwrap();
        let price = Decimal::from_str_radix(&(10000 + i as i64).to_string(), 10).unwrap();
        let size = Decimal::from_str_radix("1", 10).unwrap();
        v.push(standard_lib::market_data::base_data::OrderBook {
            symbol: symbol.to_string(),
            exchange: Exchange::CME,
            bids: vec![(price, size)],
            asks: vec![(price + Decimal::from_str_radix("1", 10).unwrap(), size)],
            time,
        });
    }
    v
}

#[test]
fn test_books_daily_file_and_catalog() {
    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    let provider = "TESTPROV";
    let symbol = "TESTSYM";
    let market = MarketType::Futures;

    let day = NaiveDate::from_ymd_opt(2025, 4, 5).unwrap();
    let base_dt = Utc.with_ymd_and_hms(2025, 4, 5, 9, 30, 0).unwrap();
    let base_ns = base_dt.timestamp_nanos_opt().unwrap();

    let batch1 = fake_books(symbol, base_ns, 4);
    let batch2 = fake_books(symbol, base_ns + 2_000_000, 4); // overlaps by 2

    let out1 = database::ingest::ingest_books(
        &conn,
        provider,
        market,
        symbol,
        Resolution::Seconds(1),
        &batch1,
        root,
    )
    .unwrap();
    let out2 = database::ingest::ingest_books(
        &conn,
        provider,
        market,
        symbol,
        Resolution::Seconds(1),
        &batch2,
        root,
    )
    .unwrap();

    assert_eq!(out1.len(), 1);
    assert_eq!(out1, out2);

    let expected_dir = get_partion(
        root,
        provider,
        market,
        symbol,
        DataKind::BookL2,
        Resolution::Seconds(1),
        day,
    );
    let expected_name = intraday_file_name(symbol, DataKind::BookL2, Resolution::Seconds(1), day);
    let expected_path = expected_dir.join(expected_name);

    assert_eq!(out1[0], expected_path);

    let sql = format!(
        "SELECT COUNT(*) FROM read_parquet('{}')",
        expected_path.display()
    );
    let cnt: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap();
    // 4 + 4 - 2 overlap = 6 unique
    assert_eq!(cnt, 6);

    let present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partitions WHERE path = ?",
            duckdb::params![expected_path.to_string_lossy().to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(present, 1);
}

#[test]
fn test_earliest_latest_available_across_days() {
    use database::duck::{earliest_available, latest_available};

    let conn = setup_conn();
    let tmp = temp_root();
    let root = tmp.path();

    let provider = "TESTPROV";
    let symbol = "TESTSYM";
    let market = MarketType::Futures;

    // Day 1
    let day1 = NaiveDate::from_ymd_opt(2025, 6, 10).unwrap();
    let dt1 = Utc.with_ymd_and_hms(2025, 6, 10, 12, 0, 0).unwrap();
    let base1 = dt1.timestamp_nanos_opt().unwrap();
    let b1 = fake_ticks(provider, symbol, base1, 3);
    let _ = ingest_ticks(&conn, root, provider, market, symbol, &b1, 9).unwrap();

    // Day 2
    let day2 = NaiveDate::from_ymd_opt(2025, 6, 11).unwrap();
    let dt2 = Utc.with_ymd_and_hms(2025, 6, 11, 12, 0, 0).unwrap();
    let base2 = dt2.timestamp_nanos_opt().unwrap();
    let b2 = fake_ticks(provider, symbol, base2, 5);
    let _ = ingest_ticks(&conn, root, provider, market, symbol, &b2, 9).unwrap();

    // Earliest should point at day1 start
    let e = earliest_available(&conn, provider, symbol, DataKind::Tick, None)
        .unwrap()
        .unwrap();
    assert_eq!(e.ts.date_naive(), day1);
    // Latest should point at day2 end
    let l = latest_available(&conn, provider, symbol, DataKind::Tick, None)
        .unwrap()
        .unwrap();
    assert_eq!(l.ts.date_naive(), day2);
}
