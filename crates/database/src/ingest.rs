use anyhow::anyhow;
use chrono::{DateTime, NaiveDate, Utc};
use duckdb::Connection;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tt_types::base_data::{OrderBook, Resolution};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::{Instrument, MarketType};
use crate::duck::{upsert_dataset, upsert_provider, upsert_symbol};
use crate::models::{BboRow, CandleRow, DataKind, TickRow};
use crate::perist::{persist_bbo_partition_zstd, persist_books_partition_duckdb, persist_candles_partition_zstd, persist_ticks_partition_zstd};

/// Ingest a batch of ticks
pub fn ingest_ticks(
    conn: &Connection,
    data_root: &Path,
    provider: &str,
    market_type: MarketType,
    symbol: &str,
    rows: &[TickRow],
    zstd_level: i32,
) -> anyhow::Result<Vec<PathBuf>> {
    if rows.is_empty() {
        return Err(anyhow!("ingest_ticks: empty batch"));
    }

    // 1) ensure catalog ids
    let provider_id = upsert_provider(conn, provider)?;
    let symbol_id = upsert_symbol(conn, provider_id, symbol)?;
    let _dataset_id = upsert_dataset(conn, provider_id, symbol_id, DataKind::Tick, None)?;

    let buckets = group_by_day(rows, |t| t.key_ts_utc_ns)?;
    let mut paths = vec![];
    for (day, day_rows) in buckets {
        // Convert Vec<&TickRow> to Vec<TickRow> for persistence API
        let mut scratch: Vec<TickRow> = Vec::with_capacity(day_rows.len());
        for r in day_rows {
            scratch.push(r.clone());
        }
        let out = persist_ticks_partition_zstd(
            conn,
            provider,
            symbol,
            market_type,
            day,
            &scratch,
            data_root,
            zstd_level,
        )?;
        paths.push(out);
    }

    Ok(paths)
}

#[inline]
fn ns_to_utc_day(ns: i64) -> anyhow::Result<NaiveDate> {
    let secs = ns.div_euclid(1_000_000_000);
    let nsec = (ns.rem_euclid(1_000_000_000)) as u32;
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, nsec)
        .ok_or_else(|| anyhow!("invalid ns timestamp: {ns}"))?;
    Ok(dt.date_naive())
}

/// Group a slice by UTC day derived from the provided extractor.
fn group_by_day<T, F>(rows: &[T], ts_ns: F) -> anyhow::Result<BTreeMap<NaiveDate, Vec<&T>>>
where
    F: Fn(&T) -> i64,
{
    let mut buckets: BTreeMap<NaiveDate, Vec<&T>> = BTreeMap::new();
    for r in rows {
        let d = ns_to_utc_day(ts_ns(r))?;
        buckets.entry(d).or_default().push(r);
    }
    Ok(buckets)
}

/// One-call ingestion for **candles** (uses candle **end time** for day bucketing).
pub fn ingest_candles(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str, // carried inside rows but passed for symmetry with ticks API if you have one
    market_type: MarketType,
    resolution: Resolution, // which candle dataset
    all_rows: &[CandleRow],
    data_root: &Path,
    zstd_level: i32,
) -> anyhow::Result<Vec<PathBuf>> {
    // Bucket by UTC day from time_end_ns (policy choice).
    let buckets = group_by_day(all_rows, |c| c.time_end_ns)?;
    let mut out_paths = Vec::with_capacity(buckets.len());

    for (day, rows) in buckets {
        // rows is Vec<&CandleRow> → make a contiguous slice view
        // persist_* expects &[CandleRow]; collect into a small Vec<&>→Vec<CandleRow> clone-free?
        // We must pass owned slice; do a lightweight copy if you want (these are on-disk types).
        // If you prefer zero-copy, change persist_* to accept iter of &CandleRow.
        let mut scratch: Vec<CandleRow> = Vec::with_capacity(rows.len());
        for r in rows {
            scratch.push(r.clone());
        }

        let p = persist_candles_partition_zstd(
            conn,
            provider,
            market_type,
            symbol,
            resolution,
            day,
            &scratch,
            data_root,
            zstd_level,
        )?;
        out_paths.push(p);
    }
    Ok(out_paths)
}

/// One-call ingestion for **BBO** snapshots.
pub fn ingest_bbo(
    conn: &duckdb::Connection,
    provider: &str,
    market_type: MarketType,
    symbol: &str,
    res: Resolution,
    all_rows: &[BboRow],
    data_root: &Path,
    zstd_level: i32,
) -> anyhow::Result<Vec<PathBuf>> {
    let buckets = group_by_day(all_rows, |q| q.key_ts_utc_ns)?;
    let mut out_paths = Vec::with_capacity(buckets.len());

    for (day, rows) in buckets {
        let mut scratch: Vec<BboRow> = Vec::with_capacity(rows.len());
        for r in rows {
            scratch.push(r.clone());
        }

        let p = persist_bbo_partition_zstd(
            conn,
            provider,
            market_type,
            symbol,
            res,
            day,
            &scratch,
            data_root,
            zstd_level,
        )?;
        out_paths.push(p);
    }
    Ok(out_paths)
}

/// One-call ingestion for **order-book** snapshots.
/// (This uses your DuckDB temp-table + COPY writer.)
pub fn ingest_books(
    conn: &duckdb::Connection,
    provider: &ProviderKind,
    market_type: MarketType,
    symbol: &Instrument,
    res: Resolution,
    all_snaps: &[OrderBook],
    data_root: &Path,
) -> anyhow::Result<Vec<PathBuf>> {
    // Bucket by UTC day using OrderBook.time
    let mut buckets: BTreeMap<NaiveDate, Vec<&OrderBook>> = BTreeMap::new();
    for ob in all_snaps {
        buckets.entry(ob.time.date_naive()).or_default().push(ob);
    }

    let mut out_paths = Vec::with_capacity(buckets.len());
    for (day, snaps) in buckets {
        // persist_* expects &[OrderBook]; build a scratch Vec<OrderBook> (OrderBook is small)
        let mut scratch: Vec<OrderBook> = Vec::with_capacity(snaps.len());
        for s in snaps {
            scratch.push(s.clone());
        }

        let p = persist_books_partition_duckdb(
            conn,
            provider,
            market_type,
            symbol,
            res,
            day,
            &scratch,
            data_root,
        )?;
        out_paths.push(p);
    }
    Ok(out_paths)
}
