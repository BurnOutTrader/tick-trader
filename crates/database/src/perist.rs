use crate::append::append_merge_parquet;
use crate::catalog::ensure_dataset;
use crate::duck::upsert_partition;
use crate::models::{BboRow, CandleRow, DataKind, TickRow};
use crate::parquet::{write_bbo_zstd, write_candles_zstd, write_ticks_zstd};
use crate::paths::get_partion;
use crate::paths::{daily_file_name, intraday_file_name, monthly_file_name, weekly_file_name};
use anyhow::{Result, anyhow};
use chrono::Datelike;
use chrono::{NaiveDate, Utc};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tt_types::securities::symbols::MarketType;
// ------------------------------
// Shared time utils
// ------------------------------

// ------------------------------
// TICKS
// ------------------------------

pub fn persist_ticks_partition_zstd(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
    market_type: MarketType,
    date: NaiveDate,
    rows_vec: &[TickRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_ticks_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::Tick, None)?;

    let dir = get_partion(
        data_root,
        provider,
        market_type,
        symbol,
        DataKind::Tick,
        Resolution::Ticks,
        date,
    );
    fs::create_dir_all(&dir)?;

    // Deterministic target file (one per day per symbol/provider)
    let target_name = intraday_file_name(symbol, DataKind::Tick, Resolution::Ticks, date);
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
                crate::database::parquet::parquet_count_min_max_i64_with_seq(
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
    provider: &str,
    market_type: MarketType,
    symbol: &str,
    resolution: Resolution,
    date: NaiveDate,
    rows_vec: &[CandleRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_candles_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::Candle, Some(resolution))?;

    let dir = get_partion(
        data_root,
        provider,
        market_type,
        symbol,
        DataKind::Candle,
        resolution,
        date,
    );
    fs::create_dir_all(&dir)?;

    // Choose deterministic target file path based on resolution (align with paths.rs)
    let target_name = match resolution {
        Resolution::Weekly => weekly_file_name(symbol, DataKind::Candle),
        Resolution::Daily => daily_file_name(symbol, DataKind::Candle, date.year()),
        Resolution::Hours(_) => {
            monthly_file_name(symbol, DataKind::Candle, date.year(), date.month())
        }
        _ => intraday_file_name(symbol, DataKind::Candle, resolution, date),
    };
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
            crate::database::parquet::parquet_count_min_max_i64(&target_path, "time_start_ns")?;
        let (_rows2, _min_end, max_end) =
            crate::database::parquet::parquet_count_min_max_i64(&target_path, "time_end_ns")?;
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
    provider: &str,
    market_type: MarketType,
    symbol: &str,
    resolution: Resolution,
    date: NaiveDate,
    rows_vec: &[BboRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<PathBuf> {
    if rows_vec.is_empty() {
        return Err(anyhow!("persist_bbo_partition_zstd: empty batch"));
    }

    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::Bbo, Some(resolution))?;

    let dir = get_partion(
        data_root,
        provider,
        market_type,
        symbol,
        DataKind::Bbo,
        resolution,
        date,
    );
    fs::create_dir_all(&dir)?;

    // Deterministic target file per day/resolution
    let target_name = intraday_file_name(symbol, DataKind::Bbo, resolution, date);
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
            crate::database::parquet::parquet_count_min_max_i64(&target_path, "key_ts_utc_ns")?;
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
    provider: &str,
    market_type: MarketType,
    symbol: &str,
    resolution: Resolution,
    date: chrono::NaiveDate,
    snapshots: &[OrderBook],
    data_root: &std::path::Path,
) -> anyhow::Result<std::path::PathBuf> {
    use chrono::Utc;
    use std::{fs, path::PathBuf};

    if snapshots.is_empty() {
        anyhow::bail!("persist_books_partition_duckdb: empty batch");
    }

    // 1) dataset
    let dataset_id = ensure_dataset(conn, provider, symbol, DataKind::BookL2, Some(resolution))?;

    // 2) target directory (deterministic per-day file like other intraday data)
    let dir = get_partion(
        data_root,
        provider,
        market_type,
        symbol,
        DataKind::BookL2,
        resolution,
        date,
    );
    fs::create_dir_all(&dir)?;
    let ts_ns = Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let tmp_incoming: PathBuf = dir.join(format!(
        ".__incoming-books-{}-{}.parquet",
        ts_ns,
        nanoid::nanoid!(4)
    ));

    // 3) staging table with BIGINT ns time
    conn.execute_batch(
        r#"
        create temp table if not exists _tmp_books_ingest (
            symbol     TEXT,
            exchange   TEXT,
            time_ns    BIGINT,    -- epoch ns, NOT TIMESTAMP
            bids_json  TEXT,
            asks_json  TEXT
        );
        delete from _tmp_books_ingest;
        "#,
    )?;

    let mut ins = conn.prepare(
        "insert into _tmp_books_ingest(symbol, exchange, time_ns, bids_json, asks_json)
         values (?, ?, ?, ?, ?)",
    )?;

    let mut _rows = 0i64;
    let mut min_ns: Option<i64> = None;
    let mut max_ns: Option<i64> = None;

    for ob in snapshots {
        // encode ladders as compact [[price,size], ...] strings
        let bids_arr: Vec<_> = ob
            .bids
            .iter()
            .map(|(p, s)| serde_json::json!([p.to_string(), s.to_string()]))
            .collect();
        let asks_arr: Vec<_> = ob
            .asks
            .iter()
            .map(|(p, s)| serde_json::json!([p.to_string(), s.to_string()]))
            .collect();
        let bids_json = serde_json::to_string(&bids_arr)?;
        let asks_json = serde_json::to_string(&asks_arr)?;
        let exch_str = format!("{:?}", ob.exchange);

        // epoch ns (i128 -> i64 clamp: your times should be safe within i64 epoch ns range)
        let t_ns = ob
            .time
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("invalid timestamp in OrderBook"))?
            as i64;

        ins.execute(duckdb::params![
            &ob.symbol, &exch_str, t_ns, &bids_json, &asks_json
        ])?;

        _rows += 1;

        let t_ns = t_ns;
        min_ns = Some(min_ns.map_or(t_ns, |m| m.min(t_ns)));
        max_ns = Some(max_ns.map_or(t_ns, |m| m.max(t_ns)));
    }

    // 4) COPY temp parquet, then merge into deterministic target file
    let tmp_str = tmp_incoming.to_string_lossy().replace('\'', "''");
    conn.execute_batch(&format!(
        r#"
        -- Use explicit COPY options; avoid PRAGMA for portability across DuckDB versions
        copy _tmp_books_ingest to '{path}'
        (format parquet, compression 'zstd');
        "#,
        path = tmp_str,
    ))?;

    // Deterministic target file (per day)
    let target_name = intraday_file_name(symbol, DataKind::BookL2, resolution, date);
    let target_path = dir.join(target_name);

    // Merge/replace (dedup by time + identifiers)
    append_merge_parquet(
        conn,
        &target_path,
        &tmp_incoming,
        &["time_ns", "symbol", "exchange"],
        &["time_ns"],
    )?;
    let _ = std::fs::remove_file(&tmp_incoming);

    // Stats from merged parquet
    let mut out_rows: i64 = 0;
    let mut min_ns_q: i64 = 0;
    let mut max_ns_q: i64 = 0;
    {
        let sql = format!(
            "SELECT COUNT(*)::BIGINT AS cnt, MIN(time_ns)::BIGINT AS min_ns, MAX(time_ns)::BIGINT AS max_ns FROM read_parquet('{}')",
            target_path.display()
        );
        let mut stmt = conn.prepare(&sql)?;
        let mut r = stmt.query([])?;
        if let Some(row) = r.next()? {
            out_rows = row.get::<usize, i64>(0)?;
            min_ns_q = row.get::<usize, i64>(1)?;
            max_ns_q = row.get::<usize, i64>(2)?;
        }
    }

    let bytes = fs::metadata(&target_path)?.len() as i64;

    // catalog upsert
    let min_ts = ns_to_dt(min_ns_q);
    let max_ts = ns_to_dt(max_ns_q);

    upsert_partition(
        conn,
        dataset_id,
        target_path.to_string_lossy().as_ref(),
        "parquet",
        out_rows,
        bytes,
        min_ts,
        max_ts,
        None,
        None,
        Some(date),
    )?;

    Ok(target_path)
}

#[inline]
fn ns_to_dt(ns: i64) -> chrono::DateTime<chrono::Utc> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos).expect("valid ns timestamp")
}
