//! High-level ingestion APIs that bucket incoming rows by (year, month) and delegate to persistence.
//! Each function validates non-empty input, groups rows by the month of their timestamp, and
//! calls the corresponding `perist::*_partition_zstd` writer to update Parquet and the catalog.

use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, Month, Utc};
use duckdb::Connection;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::{Instrument, MarketType};

use crate::models::{BboRow, CandleRow, TickRow};
use crate::perist::{
    persist_bbo_partition_zstd, persist_candles_partition_zstd, persist_ticks_partition_zstd,
};

fn month_from_u32(m: u32) -> Month {
    match m {
        1 => Month::January,
        2 => Month::February,
        3 => Month::March,
        4 => Month::April,
        5 => Month::May,
        6 => Month::June,
        7 => Month::July,
        8 => Month::August,
        9 => Month::September,
        10 => Month::October,
        11 => Month::November,
        12 => Month::December,
        _ => Month::January,
    }
}

fn ns_to_year_month(ns: i64) -> Result<(i32, Month)> {
    let secs = ns.div_euclid(1_000_000_000);
    let nsec = (ns.rem_euclid(1_000_000_000)) as u32;
    let dt: DateTime<Utc> = DateTime::from_timestamp(secs, nsec)
        .ok_or_else(|| anyhow!("invalid ns timestamp: {ns}"))?;
    Ok((dt.year(), month_from_u32(dt.month())))
}

fn group_by_year_month<T, F>(rows: &[T], ts_ns: F) -> Result<BTreeMap<(i32, Month), Vec<&T>>>
where
    F: Fn(&T) -> i64,
{
    let mut buckets: BTreeMap<(i32, Month), Vec<&T>> = BTreeMap::new();
    for r in rows {
        let (y, m) = ns_to_year_month(ts_ns(r))?;
        buckets.entry((y, m)).or_default().push(r);
    }
    Ok(buckets)
}

#[allow(clippy::too_many_arguments)]
/// Ingest a batch of ticks grouped monthly
pub fn ingest_ticks(
    conn: &Connection,
    provider: &ProviderKind,
    instrument: &Instrument,
    market_type: MarketType,
    topic: Topic,
    rows: &[TickRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<Vec<PathBuf>> {
    if rows.is_empty() {
        return Err(anyhow!("ingest_ticks: empty batch"));
    }

    let buckets = group_by_year_month(rows, |t| t.key_ts_utc_ns)?;
    let mut paths = Vec::with_capacity(buckets.len());
    for ((_year, month), group) in buckets {
        let mut scratch: Vec<TickRow> = Vec::with_capacity(group.len());
        for r in group {
            scratch.push(r.clone());
        }
        let out = persist_ticks_partition_zstd(
            conn,
            provider,
            instrument,
            market_type,
            topic,
            month,
            &scratch,
            data_root,
            zstd_level,
        )?;
        paths.push(out);
    }

    Ok(paths)
}

#[allow(clippy::too_many_arguments)]
/// One-call ingestion for candles (monthly, keyed by end time)
pub fn ingest_candles(
    conn: &Connection,
    provider: &ProviderKind,
    instrument: &Instrument,
    market_type: MarketType,
    topic: Topic,
    rows: &[CandleRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<Vec<PathBuf>> {
    if rows.is_empty() {
        return Err(anyhow!("ingest_candles: empty batch"));
    }

    let buckets = group_by_year_month(rows, |c| c.time_end_ns)?;
    let mut out_paths = Vec::with_capacity(buckets.len());

    for ((_year, month), group) in buckets {
        let mut scratch: Vec<CandleRow> = Vec::with_capacity(group.len());
        for r in group {
            scratch.push(r.clone());
        }

        let p = persist_candles_partition_zstd(
            conn,
            provider,
            market_type,
            instrument,
            topic,
            month,
            &scratch,
            data_root,
            zstd_level,
        )?;
        out_paths.push(p);
    }
    Ok(out_paths)
}

#[allow(clippy::too_many_arguments)]
/// One-call ingestion for BBO snapshots (monthly)
pub fn ingest_bbo(
    conn: &Connection,
    provider: &ProviderKind,
    instrument: &Instrument,
    market_type: MarketType,
    topic: Topic,
    rows: &[BboRow],
    data_root: &Path,
    zstd_level: i32,
) -> Result<Vec<PathBuf>> {
    if rows.is_empty() {
        return Err(anyhow!("ingest_bbo: empty batch"));
    }

    let buckets = group_by_year_month(rows, |b| b.key_ts_utc_ns)?;
    let mut out_paths = Vec::with_capacity(buckets.len());

    for ((_year, month), group) in buckets {
        let mut scratch: Vec<BboRow> = Vec::with_capacity(group.len());
        for r in group {
            scratch.push(r.clone());
        }

        let p = persist_bbo_partition_zstd(
            conn,
            provider,
            market_type,
            instrument,
            topic,
            month,
            &scratch,
            data_root,
            zstd_level,
        )?;
        out_paths.push(p);
    }
    Ok(out_paths)
}
