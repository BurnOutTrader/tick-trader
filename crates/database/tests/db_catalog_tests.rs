use chrono::{NaiveDate, TimeZone, Utc};
use std::str::FromStr;
use tt_database::duck::{
    create_partitions_schema, earliest_available, latest_available, prune_missing_partitions,
    quarantine_unreadable_partitions, upsert_dataset, upsert_partition, upsert_provider,
    upsert_symbol,
};
use tt_database::init::create_identity_schema_if_needed;
use tt_database::paths::provider_kind_to_db_string;
use tt_types::keys::Topic;
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;

fn setup_conn() -> duckdb::Connection {
    let conn = duckdb::Connection::open_in_memory().expect("duckdb mem");
    // Initialize catalog schemas used by the library
    create_identity_schema_if_needed(&conn).expect("init identity schema");
    create_partitions_schema(&conn).expect("init partitions schema");
    conn
}

#[test]
fn test_catalog_earliest_latest_ticks_across_partitions() {
    let conn = setup_conn();

    // Create dataset for provider/symbol/topic
    let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);
    let symbol = Instrument::from_str("MNQZ5").unwrap();
    let provider_id = upsert_provider(&conn, &provider_kind_to_db_string(provider)).unwrap();
    let symbol_id = upsert_symbol(&conn, provider_id, &symbol.to_string()).unwrap();
    let dataset_id = upsert_dataset(&conn, provider_id, symbol_id, Topic::Ticks, None).unwrap();

    // Two day partitions with increasing time ranges
    let day1 = NaiveDate::from_ymd_opt(2025, 7, 10).unwrap();
    let day2 = NaiveDate::from_ymd_opt(2025, 7, 11).unwrap();

    let start1 = Utc.with_ymd_and_hms(2025, 7, 10, 9, 30, 0).unwrap();
    let end1 = Utc.with_ymd_and_hms(2025, 7, 10, 16, 0, 0).unwrap();
    let start2 = Utc.with_ymd_and_hms(2025, 7, 11, 9, 30, 0).unwrap();
    let end2 = Utc.with_ymd_and_hms(2025, 7, 11, 16, 0, 0).unwrap();

    // Insert catalog partitions pointing at plausible paths
    upsert_partition(
        &conn,
        dataset_id,
        "/tmp/ticks_2025-07-10.parquet",
        "parquet",
        100,
        10_000,
        start1,
        end1,
        Some(1),
        Some(100),
        Some(day1),
    )
    .unwrap();

    upsert_partition(
        &conn,
        dataset_id,
        "/tmp/ticks_2025-07-11.parquet",
        "parquet",
        120,
        12_000,
        start2,
        end2,
        Some(1),
        Some(200),
        Some(day2),
    )
    .unwrap();

    // Resolve earliest/latest via high-level helpers
    let e = earliest_available(&conn, &provider, &symbol, Topic::Ticks)
        .unwrap()
        .unwrap();
    assert_eq!(e.ts.date_naive(), day1);

    let l = latest_available(
        &conn,
        &provider_kind_to_db_string(provider),
        &symbol.to_string(),
        Topic::Ticks,
    )
    .unwrap()
    .unwrap();
    assert_eq!(l.ts.date_naive(), day2);
}

#[test]
fn test_prune_missing_partitions_keeps_existing() {
    use std::fs;
    use tempfile::TempDir;

    let conn = setup_conn();

    let provider = "TESTPROV2";
    let symbol = "TESTSYM2";
    let provider_id = upsert_provider(&conn, provider).unwrap();
    let symbol_id = upsert_symbol(&conn, provider_id, symbol).unwrap();
    let dataset_id = upsert_dataset(&conn, provider_id, symbol_id, Topic::Ticks, None).unwrap();

    let tmpdir = TempDir::new().unwrap();
    // Create one real file and one missing path
    let existing_path = tmpdir.path().join("exists.parquet");
    fs::write(&existing_path, b"not a parquet but exists").unwrap();
    let missing_path = tmpdir.path().join("missing.parquet");

    let day = NaiveDate::from_ymd_opt(2025, 8, 1).unwrap();
    let start = Utc.with_ymd_and_hms(2025, 8, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 8, 1, 23, 59, 59).unwrap();

    upsert_partition(
        &conn,
        dataset_id,
        &existing_path.to_string_lossy(),
        "parquet",
        1,
        123,
        start,
        end,
        None,
        None,
        Some(day),
    )
    .unwrap();

    upsert_partition(
        &conn,
        dataset_id,
        &missing_path.to_string_lossy(),
        "parquet",
        1,
        456,
        start,
        end,
        None,
        None,
        Some(day),
    )
    .unwrap();

    // Prune should remove only the missing path row
    prune_missing_partitions(&conn).unwrap();

    let count_all: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partitions WHERE dataset_id = ?",
            duckdb::params![dataset_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count_all, 1);

    let present: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partitions WHERE path = ?",
            duckdb::params![existing_path.to_string_lossy().to_string()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(present, 1);
}

#[test]
fn test_quarantine_unreadable_partitions_removes_bad_files() {
    use std::fs;
    use tempfile::TempDir;

    let conn = setup_conn();

    let provider = "TESTPROV3";
    let symbol = "TESTSYM3";
    let provider_id = upsert_provider(&conn, provider).unwrap();
    let symbol_id = upsert_symbol(&conn, provider_id, symbol).unwrap();
    let dataset_id = upsert_dataset(&conn, provider_id, symbol_id, Topic::Ticks, None).unwrap();

    let tmpdir = TempDir::new().unwrap();
    let bad_path = tmpdir.path().join("bad.parquet");
    // Create a file that exists but is not a valid parquet file
    fs::write(&bad_path, b"this is not a parquet file").unwrap();

    let day = NaiveDate::from_ymd_opt(2025, 9, 1).unwrap();
    let start = Utc.with_ymd_and_hms(2025, 9, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2025, 9, 1, 23, 59, 59).unwrap();

    upsert_partition(
        &conn,
        dataset_id,
        &bad_path.to_string_lossy(),
        "parquet",
        1,
        789,
        start,
        end,
        None,
        None,
        Some(day),
    )
    .unwrap();

    // Quarantine should detect unreadable parquet and remove it from catalog
    let removed = quarantine_unreadable_partitions(&conn).unwrap();
    assert_eq!(removed, 1);

    let remaining: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partitions WHERE dataset_id = ?",
            duckdb::params![dataset_id],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(remaining, 0);
}

#[test]
fn test_latest_available_candles_with_legacy_empty_resolution_key() {
    use duckdb::params;

    let conn = setup_conn();

    // Manually insert provider, symbol, and a legacy dataset row with empty resolution_key
    conn.execute("INSERT INTO providers(provider_code) VALUES ('TESTP4')", [])
        .unwrap();
    let provider_id: i64 = conn
        .query_row(
            "SELECT provider_id FROM providers WHERE provider_code='TESTP4'",
            [],
            |r| r.get(0),
        )
        .unwrap();

    conn.execute(
        "INSERT INTO symbols(provider_id, symbol_text) VALUES (?, 'SYM4')",
        params![provider_id],
    )
    .unwrap();
    let symbol_id: i64 = conn
        .query_row(
            "SELECT symbol_id FROM symbols WHERE provider_id=? AND symbol_text='SYM4'",
            params![provider_id],
            |r| r.get(0),
        )
        .unwrap();

    // Insert legacy dataset: kind=candles1m, resolution_key='' (empty)
    conn.execute(
        "INSERT INTO datasets(provider_id, symbol_id, kind, resolution, resolution_key) VALUES (?,?,?,?, '')",
        params![provider_id, symbol_id, "candles1m", Option::<String>::None],
    )
    .unwrap();
    let dataset_id: i64 = conn
        .query_row(
            "SELECT dataset_id FROM datasets WHERE provider_id=? AND symbol_id=? AND kind='candles1m' AND resolution_key=''",
            params![provider_id, symbol_id],
            |r| r.get(0),
        )
        .unwrap();

    // Insert a partition with a valid time range
    let day = chrono::NaiveDate::from_ymd_opt(2025, 10, 1).unwrap();
    let start = chrono::Utc.with_ymd_and_hms(2025, 10, 1, 0, 0, 0).unwrap();
    let end = chrono::Utc.with_ymd_and_hms(2025, 10, 1, 0, 1, 0).unwrap();

    upsert_partition(
        &conn,
        dataset_id,
        "/tmp/candles_2025-10-01.parquet",
        "parquet",
        60,
        1_024,
        start,
        end,
        None,
        None,
        Some(day),
    )
    .unwrap();

    // Now resolve via latest_available using Topic::Candles1m; should find the legacy dataset
    let got = latest_available(&conn, "TESTP4", "SYM4", tt_types::keys::Topic::Candles1m)
        .unwrap()
        .map(|b| b.ts);
    assert!(
        got.is_some(),
        "expected latest_available to find a timestamp for legacy candles dataset"
    );
}
