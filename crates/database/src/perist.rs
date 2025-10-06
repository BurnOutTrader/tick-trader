//! Persistence helpers: write monthly Parquet partitions and maintain the DuckDB catalog.
//!
//! These functions are called by `ingest::*` after grouping rows by (year, month). They:
//! - Ensure the dataset exists in the catalog.
//! - Compute deterministic monthly paths via `paths`.
//! - Write incoming rows to a temporary Parquet with ZSTD compression.
//! - Merge with an existing Parquet (stable de-dup by key) and atomically replace.
//! - Compute/update partition stats efficiently.
//! - Upsert the partition row in the catalog.

use crate::append::append_merge_parquet;
use crate::catalog::ensure_dataset;
use crate::duck::upsert_partition;
use crate::models::{BboRow, CandleRow, TickRow};
use crate::parquet::{write_bbo_zstd, write_candles_zstd, write_ticks_zstd};
use anyhow::{Result, anyhow};
use chrono::{Datelike, Month, NaiveDate, Utc};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tt_types::base_data::OrderBook;
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::{Instrument, MarketType};
use crate::paths::{data_file_name, partition_dir};
// ------------------------------
// Shared time utils
// ------------------------------

// ------------------------------
// TICKS
// ------------------------------

/// Persist a single month's worth of tick rows to Parquet using ZSTD compression, merging with
/// any existing monthly file, and upserting/merging the corresponding partition row in the catalog.
pub fn persist_ticks_partition_zstd(
    conn: &duckdb::Connection,
    provider: &ProviderKind,
    instrument: &Instrument,
    market_type: MarketType,
    topic: Topic,
    month: Month,
    rows_vec: &[TickRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_ticks_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, *provider, instrument, topic)?;

    // Determine the month/year from data for partitioning and naming
    let min_ns_rows = rows_vec.iter().map(|r| r.key_ts_utc_ns).min().unwrap_or(0);
    let min_dt = ns_to_dt(min_ns_rows);
    let year = min_dt.year() as u32;
    let date = NaiveDate::from_ymd_opt(min_dt.year(), month.number_from_month(), 1)
        .ok_or_else(|| anyhow!("invalid year/month for ticks partition"))?;

    let dir = partition_dir(
        data_root,
        *provider,
        market_type,
        instrument,
        topic,
        year,
    );
    fs::create_dir_all(&dir)?;

    // Deterministic monthly target file
    let target_name = data_file_name(instrument, topic, date);
    let target_path = dir.join(target_name);

    // Write incoming batch to a temp parquet in same dir
    let existed_before = target_path.exists();
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let tmp_incoming = dir.join(format!(
        ".__incoming-ticks-{}-{}.parquet",
        ts_ns,
        nanoid::nanoid!(4)
    ));
    write_ticks_zstd(&tmp_incoming, rows_vec, zstd_level)?;

    // Merge with existing (if any) and atomically replace
    // Keys must uniquely identify a tick; we use (ts_ns, key_tie, provider, symbol_id, exchange)
    append_merge_parquet(
        conn,
        &target_path,
        &tmp_incoming,
        &[
            "key_ts_utc_ns",
            "key_tie",
            "provider",
            "symbol_id",
            "exchange",
        ],
        &["key_ts_utc_ns", "key_tie"],
    )?;

    // Clean up temp if still present (append_merge_parquet moves/keeps final only)
    let _ = std::fs::remove_file(&tmp_incoming);

    // Stats: avoid DuckDB read_parquet on first write; compute from in-memory rows
    let (rows, min_ns, max_ns, min_seq, max_seq): (i64, i64, i64, Option<i64>, Option<i64>) =
        if !existed_before {
            let rows = rows_vec.len() as i64;
            let min_ns = rows_vec.iter().map(|r| r.key_ts_utc_ns).min().unwrap_or(0);
            let max_ns = rows_vec.iter().map(|r| r.key_ts_utc_ns).max().unwrap_or(0);
            let min_seq = rows_vec
                .iter()
                .filter_map(|r| r.venue_seq.map(|v| v as i64))
                .min();
            let max_seq = rows_vec
                .iter()
                .filter_map(|r| r.venue_seq.map(|v| v as i64))
                .max();
            (rows, min_ns, max_ns, min_seq, max_seq)
        } else {
            // Avoid DuckDB parquet read for stats; use Rust parquet reader
            let (rows, min_ns, max_ns, min_seq, max_seq) =
                crate::parquet::parquet_count_min_max_i64_with_seq(
                    &target_path,
                    "key_ts_utc_ns",
                    "venue_seq",
                )?;
            (rows, min_ns, max_ns, min_seq, max_seq)
        };

    let bytes = fs::metadata(&target_path)?.len() as i64;

    let min_ts = ns_to_dt(min_ns);
    let max_ts = ns_to_dt(max_ns);

    upsert_partition(
        conn,
        dataset_id,
        target_path.to_string_lossy().as_ref(),
        "parquet",
        rows,
        bytes,
        min_ts,
        max_ts,
        min_seq,
        max_seq,
        Some(date),
    )?;

    Ok(target_path)
}

// ------------------------------
// CANDLES (use time_end for max)
// ------------------------------

pub fn persist_candles_partition_zstd(
    conn: &duckdb::Connection,
    provider: &ProviderKind,
    market_type: MarketType,
    instrument: &Instrument,
    topic: Topic,
    month: Month,
    rows_vec: &[CandleRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_candles_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, *provider, instrument, topic)?;

    // Determine the month/year from data (use the earliest candle start)
    let min_ns_rows = rows_vec.iter().map(|r| r.time_start_ns).min().unwrap_or(0);
    let min_dt = ns_to_dt(min_ns_rows);
    let year = min_dt.year() as u32;
    let date = NaiveDate::from_ymd_opt(min_dt.year(), month.number_from_month(), 1)
        .ok_or_else(|| anyhow!("invalid year/month for candles partition"))?;

    let dir = partition_dir(
        data_root,
        *provider,
        market_type,
        instrument,
        topic,
        year,
    );
    fs::create_dir_all(&dir)?;

    // Deterministic monthly target file
    let target_name = data_file_name(instrument, topic, date);
    let target_path = dir.join(target_name);

    // Write incoming batch to temp and merge
    let existed_before = target_path.exists();
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let tmp_incoming = dir.join(format!(
        ".__incoming-candles-{}-{}.parquet",
        ts_ns,
        nanoid::nanoid!(4)
    ));
    write_candles_zstd(&tmp_incoming, rows_vec, zstd_level)?;

    append_merge_parquet(
        conn,
        &target_path,
        &tmp_incoming,
        &[
            "provider",
            "symbol_id",
            "exchange",
            "res",
            "time_start_ns",
            "time_end_ns",
        ],
        &["time_start_ns", "time_end_ns"],
    )?;
    let _ = std::fs::remove_file(&tmp_incoming);

    // Stats: avoid DuckDB read_parquet on first write; compute from in-memory rows
    let (rows, min_ns, max_ns) = if !existed_before {
        let rows = rows_vec.len() as i64;
        let min_ns = rows_vec.iter().map(|r| r.time_start_ns).min().unwrap_or(0);
        let max_ns = rows_vec.iter().map(|r| r.time_end_ns).max().unwrap_or(0);
        (rows as i64, min_ns, max_ns)
    } else {
        // Avoid DuckDB parquet read for stats; use Rust parquet reader
        let (rows, min_start, _max_start) =
            crate::parquet::parquet_count_min_max_i64(&target_path, "time_start_ns")?;
        let (_rows2, _min_end, max_end) =
            crate::parquet::parquet_count_min_max_i64(&target_path, "time_end_ns")?;
        (rows, min_start, max_end)
    };

    let bytes = fs::metadata(&target_path)?.len() as i64;
    let min_ts = ns_to_dt(min_ns);
    let max_ts = ns_to_dt(max_ns);

    upsert_partition(
        conn,
        dataset_id,
        target_path.to_string_lossy().as_ref(),
        "parquet",
        rows,
        bytes,
        min_ts,
        max_ts,
        None,
        None,
        Some(date),
    )?;

    Ok(target_path)
}

// ------------------------------
// BBO
// ------------------------------

pub fn persist_bbo_partition_zstd(
    conn: &duckdb::Connection,
    provider: &ProviderKind,
    market_type: MarketType,
    instrument: &Instrument,
    topic: Topic,
    month: Month,
    rows_vec: &[BboRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_bbo_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, *provider, instrument, topic)?;

    // Determine the month/year from data
    let min_ns_rows = rows_vec.iter().map(|r| r.key_ts_utc_ns).min().unwrap_or(0);
    let min_dt = ns_to_dt(min_ns_rows);
    let year = min_dt.year() as u32;
    let date = NaiveDate::from_ymd_opt(min_dt.year(), month.number_from_month(), 1)
        .ok_or_else(|| anyhow!("invalid year/month for bbo partition"))?;

    let dir = partition_dir(
        data_root,
        *provider,
        market_type,
        instrument,
        topic,
        year,
    );
    fs::create_dir_all(&dir)?;

    // Deterministic monthly target file
    let target_name = data_file_name(instrument, topic, date);
    let target_path = dir.join(target_name);

    // Write incoming to temp parquet then merge
    let existed_before = target_path.exists();
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let tmp_incoming = dir.join(format!(
        ".__incoming-bbo-{}-{}.parquet",
        ts_ns,
        nanoid::nanoid!(4)
    ));
    write_bbo_zstd(&tmp_incoming, rows_vec, zstd_level)?;

    // Keys: (ts_ns, provider, symbol_id, exchange, venue_seq)
    append_merge_parquet(
        conn,
        &target_path,
        &tmp_incoming,
        &["key_ts_utc_ns", "provider", "symbol_id", "exchange"],
        &["key_ts_utc_ns"],
    )?;
    let _ = std::fs::remove_file(&tmp_incoming);

    // Stats: avoid DuckDB read_parquet on first write; compute from in-memory rows
    let (rows, min_ns, max_ns) = if !existed_before {
        let rows = rows_vec.len() as i64;
        let min_ns = rows_vec.iter().map(|r| r.key_ts_utc_ns).min().unwrap_or(0);
        let max_ns = rows_vec.iter().map(|r| r.key_ts_utc_ns).max().unwrap_or(0);
        (rows, min_ns, max_ns)
    } else {
        // Avoid DuckDB parquet read for stats; use Rust parquet reader
        let (rows, min_ns, max_ns) =
            crate::parquet::parquet_count_min_max_i64(&target_path, "key_ts_utc_ns")?;
        (rows, min_ns, max_ns)
    };

    let bytes = fs::metadata(&target_path)?.len() as i64;
    let min_ts = ns_to_dt(min_ns);
    let max_ts = ns_to_dt(max_ns);

    upsert_partition(
        conn,
        dataset_id,
        target_path.to_string_lossy().as_ref(),
        "parquet",
        rows,
        bytes,
        min_ts,
        max_ts,
        None,
        None,
        Some(date),
    )?;

    Ok(target_path)
}

// ------------------------------
// ORDER BOOKS (JSON ladders)
// ------------------------------
//
// If you already added `write_orderbooks_partition(...)` that writes and
// registers, you can keep that. If you prefer symmetry with the others,
// here is a similar writer that uses DuckDB COPY over a temp table.
//
// If you keep your existing `write_orderbooks_partition`, you can skip this.

pub fn persist_books_partition_duckdb(
    conn: &duckdb::Connection,
    provider: &ProviderKind,
    market_type: MarketType,
    instrument: &Instrument,
    topic: Topic,
    month: Month,
    snapshots: &[OrderBook],
    data_root: &std::path::Path,
) -> anyhow::Result<std::path::PathBuf> {
    use chrono::Utc;
    use std::{fs, path::PathBuf};

    if snapshots.is_empty() {
        anyhow::bail!("persist_books_partition_duckdb: empty batch");
    }

    todo!("persist_books_partition_duckdb: not implemented");
}

#[inline]
fn ns_to_dt(ns: i64) -> chrono::DateTime<chrono::Utc> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos).expect("valid ns timestamp")
}
