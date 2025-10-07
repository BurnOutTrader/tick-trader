use crate::duck::{latest_available, resolve_dataset_id};
use crate::layout::Layout;
use crate::models::SeqBound;
use crate::paths::{provider_kind_to_db_string, topic_to_db_string};
use ahash::AHashMap;
use anyhow::{Context, anyhow};
use chrono::{DateTime, Utc};
use duckdb::{Connection, OptionalExt, params};
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use std::str::FromStr;
use std::sync::Arc;
use tt_types::base_data::Resolution;
use tt_types::base_data::{Bbo, BookLevel, Candle, OrderBook, Side, Tick};
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::{Exchange, Instrument, MarketType};

#[inline]
fn dt_to_us(dt: DateTime<Utc>) -> i64 {
    dt.timestamp()
        .saturating_mul(1_000_000)
        .saturating_add((dt.timestamp_subsec_nanos() / 1_000) as i64)
}

#[inline]
fn us_to_dt(us: i64) -> DateTime<Utc> {
    let secs = us.div_euclid(1_000_000);
    let micros = (us.rem_euclid(1_000_000)) as u32;
    DateTime::from_timestamp(secs, micros * 1_000).expect("valid micros ts")
}

/// Return earliest timestamp available for a single (provider, kind, symbol, exchange, res).
/// Uses partition pruning + Parquet stats; reads no payloads when min/max suffice.
pub fn earliest_event_ts(
    conn: &Connection,
    layout: &Layout,
    provider: ProviderKind,
    topic: Topic,
    instrument: &Instrument,
    exchange: Exchange,
    market_type: MarketType,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    // choose the “min” column per kind
    let (time_col, is_bigint) = match topic {
        Topic::Ticks => ("key_ts_utc_us", true),
        Topic::Quotes => ("key_ts_utc_us", true), // you keep both key_ts_utc_us and time_us; choose key*
        Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d => {
            ("time_end_us", true)
        }
        Topic::Depth => ("time", false), // still TIMESTAMP in your writer
        _ => ("key_ts_utc_us", true),
    };

    let glob = layout.glob_for(provider, topic, instrument, market_type, exchange);
    let kind_s = topic_to_db_string(topic);

    let sql = format!(
        r#"
        SELECT {agg} AS min_ts
        FROM read_parquet(?, HIVE_PARTITIONING=1, UNION_BY_NAME=1)
        WHERE provider = ? AND kind = ? AND symbol = ? AND exchange = ? AND res = ?
        "#,
        agg = if is_bigint {
            "min(CAST(NULLIF("
        } else {
            "min("
        }
        .to_owned()
            + time_col
            + if is_bigint { ", 0) AS BIGINT))" } else { ")" }
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![
        glob,
        provider_kind_to_db_string(provider),
        kind_s,
        instrument.to_string(),
        exchange.to_string()
    ])?;

    if let Some(row) = rows.next()? {
        if is_bigint {
            let v: Option<i64> = row.get(0)?;
            Ok(v.map(us_to_dt))
        } else {
            let s: Option<String> = row.get(0)?;
            Ok(s.map(|x| {
                DateTime::parse_from_rfc3339(&x)
                    .unwrap()
                    .with_timezone(&Utc)
            }))
        }
    } else {
        Ok(None)
    }
}

/// Batch version: earliest timestamp per symbol (same provider/kind/exchange/res).
pub fn earliest_per_symbol(
    conn: &Connection,
    layout: &Layout,
    provider: ProviderKind,
    topic: Topic,
    instruments: &[Instrument],
    exchange: Exchange,
    market_type: MarketType,
) -> anyhow::Result<AHashMap<Instrument, Option<DateTime<Utc>>>> {
    let mut out = AHashMap::with_capacity(instruments.len());
    for s in instruments {
        let v = earliest_event_ts(conn, layout, provider, topic, s, exchange, market_type)?;
        out.insert(s.clone(), v);
    }
    Ok(out)
}

/// Earliest across many symbols (useful for “universe” queries).
pub fn earliest_any(
    conn: &Connection,
    layout: &Layout,
    provider: ProviderKind,
    topic: Topic,
    instruments: &[Instrument],
    exchange: Exchange,
    market_type: MarketType,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    // union all with parameterized table functions is awkward; simplest path:
    let mut best: Option<DateTime<Utc>> = None;
    for s in instruments {
        if let Some(ts) =
            earliest_event_ts(conn, layout, provider, topic, s, exchange, market_type)?
        {
            best = match best {
                None => Some(ts),
                Some(b) if ts < b => Some(ts),
                Some(b) => Some(b),
            };
        }
    }
    Ok(best)
}

/// Latest data time for a dataset (provider, symbol, kind, optional resolution).
/// Returns the timestamp of the newest row persisted for that dataset
/// (as recorded in the partitions catalog). For candles, pass the
/// specific resolution (e.g., Minutes(1)); for non-resolutioned
/// datasets (ticks, bbo, orderbook), pass `None`.
pub fn latest_data_time(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    topic: Topic,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    let v: Option<SeqBound> = latest_available(
        conn,
        &provider_kind_to_db_string(provider),
        &instrument.to_string(),
        topic,
    )?;
    Ok(v.map(|b| b.ts))
}

// ---------- helpers ----------

#[inline]
fn epoch_ns_to_dt(ns: i64) -> DateTime<Utc> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nanos).expect("valid timestamp from parquet epoch ns")
}

/// Escape a path for embedding in a DuckDB string literal.
#[inline]
fn duck_string(s: &str) -> String {
    // escape single quotes for SQL literal
    format!("'{}'", s.replace('\'', "''"))
}

/// Build a read_parquet() invocation over an explicit list of files.
fn build_read_parquet_list(paths: &[String]) -> anyhow::Result<String> {
    if paths.is_empty() {
        return Err(anyhow::anyhow!("no partition paths provided"));
    }
    // If duck_string takes &str, use p.as_str()
    let list = paths
        .iter()
        .map(|p| duck_string(p.as_str()))
        .collect::<Vec<String>>()
        .join(", ");

    Ok(format!("read_parquet([{}])", list))
}

/// Find all partition files overlapping a time window.
fn partition_paths_for_range(
    conn: &Connection,
    dataset_id: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<Vec<String>> {
    // Compare against BIGINT ns columns in partitions
    let start_ns = start.timestamp_nanos_opt().unwrap_or(0);
    let end_ns = end.timestamp_nanos_opt().unwrap_or(0);
    let mut q = conn.prepare(
        r#"
        select path
          from partitions
         where dataset_id = ?
           and max_ts_ns >= ?
           and min_ts_ns <  ?
         order by min_ts_ns asc
        "#,
    )?;
    let mut rows = q.query(params![dataset_id, start_ns, end_ns])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let p: String = r.get(0)?;
        out.push(p);
    }
    Ok(out)
}

pub fn get_bbo_in_range(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<Vec<Bbo>> {
    if start >= end {
        return Ok(Vec::new());
    }

    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, Topic::Quotes)? else {
        return Ok(Vec::new());
    };

    let paths = partition_paths_for_range(conn, dataset_id, start, end)?;
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let src = build_read_parquet_list(&paths)?;
    let start_us = dt_to_us(start);
    let end_us = dt_to_us(end);

    let sql = format!(
        r#"
        select
            symbol, exchange,
            CAST(bid AS VARCHAR), CAST(bid_size AS VARCHAR),
            CAST(ask AS VARCHAR), CAST(ask_size AS VARCHAR),
            key_ts_utc_us, time_us,
            bid_orders, ask_orders, venue_seq, is_snapshot
        from {src}
        where symbol = ?
          and key_ts_utc_us >= ?
          and key_ts_utc_us <  ?
        order by key_ts_utc_us asc
        "#,
        src = src
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![symbol, start_us, end_us])?;

    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let sym: String = r.get(0)?;
        let exch: String = r.get(1)?;
        let bid_s: String = r.get(2)?;
        let bid_sz_s: String = r.get(3)?;
        let ask_s: String = r.get(4)?;
        let ask_sz_s: String = r.get(5)?;
        let key_us: i64 = r.get(6)?;
        let _time_us: i64 = r.get(7)?; // keep if you need separate device time
        let bid_orders: Option<u32> = r.get(8)?;
        let ask_orders: Option<u32> = r.get(9)?;
        let venue_seq: Option<i32> = r.get(10)?;
        let is_snapshot: Option<bool> = r.get(11)?;

        let exchange =
            Exchange::from_str(&exch).ok_or_else(|| anyhow!("unknown exchange '{exch}'"))?;
        out.push(Bbo {
            symbol: sym.clone(),
            instrument: Instrument::from_str(&sym)
                .map_err(|_| anyhow!("invalid instrument '{}", sym))?,
            bid: Decimal::from_str(&bid_s)?,
            bid_size: Decimal::from_str(&bid_sz_s)?,
            ask: Decimal::from_str(&ask_s)?,
            ask_size: Decimal::from_str(&ask_sz_s)?,
            time: us_to_dt(key_us),
            bid_orders,
            ask_orders,
            venue_seq: venue_seq.map(|v| v as u32),
            is_snapshot,
        });
    }
    Ok(out)
}

// ---------- Ticks ----------

/// Get ticks for [start, end) using DuckDB to scan the *overlapping* parquet files only.
/// Dedupe rule is conservative: first row per (time_ns, exec_id, maker_order_id, taker_order_id).
pub fn get_ticks_in_range(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<Vec<Tick>> {
    if start >= end {
        return Ok(Vec::new());
    }

    let Some(dataset_id) = resolve_dataset_id(
        conn,
        &provider_kind_to_db_string(provider),
        &instrument.to_string(),
        Topic::Ticks,
    )?
    else {
        return Ok(Vec::new());
    };

    let paths = partition_paths_for_range(conn, dataset_id, start, end)?;
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let src = build_read_parquet_list(&paths)?;
    let start_us = dt_to_us(start);
    let end_us = dt_to_us(end);

    // Dedup: first per (time, exec_id) ordered by venue_seq; adjust if you also store maker/taker IDs.
    let sql = format!(
        r#"
        with src as (
          select
              symbol, exchange, price, size, side,
              exec_id, venue_seq,
              key_ts_utc_us as t_us,
              ts_event_us, ts_recv_us
          from {src}
          where symbol = ?
            and key_ts_utc_us >= ?
            and key_ts_utc_us <  ?
        ),
        ranked as (
          select *,
                 row_number() over (
                   partition by t_us, coalesce(exec_id,'')
                   order by coalesce(venue_seq, 2147483647)
                 ) as rn
          from src
        )
        select
            symbol, exchange,
            CAST(price AS VARCHAR), CAST(size AS VARCHAR),
            side, exec_id, venue_seq,
            t_us, ts_event_us, ts_recv_us
        from ranked
        where rn = 1
        order by t_us asc, coalesce(venue_seq, 2147483647) asc
        "#,
        src = src
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![instrument.to_string(), start_us, end_us])?;

    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let sym: String = r.get(0)?;
        let exch: String = r.get(1)?;
        let price_s: String = r.get(2)?;
        let size_s: String = r.get(3)?;
        let side_str: Option<String> = r.get(4)?;
        let venue_seq: Option<i32> = r.get(6)?;
        let t_us: i64 = r.get(7)?;

        let price = Decimal::from_str(&price_s)?;
        let size = Decimal::from_str(&size_s)?;
        let exchange =
            Exchange::from_str(&exch).ok_or_else(|| anyhow!("unknown exchange '{exch}'"))?;
        let side = match side_str.as_deref() {
            Some("Buy") | Some("B") => Side::Buy,
            Some("Sell") | Some("S") => Side::Sell,
            _ => Side::None,
        };

        out.push(Tick {
            symbol: sym,
            instrument: instrument.clone(),
            price,
            volume: size,
            side,
            time: us_to_dt(t_us),
            venue_seq: venue_seq.map(|v| v as u32),
        });
    }
    Ok(out)
}

/// Convenience: from `start` to latest available.
pub fn get_ticks_from_date_to_latest(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    start: DateTime<Utc>,
) -> anyhow::Result<Vec<Tick>> {
    let Some(SeqBound { ts: latest_ts, .. }) = latest_available(
        conn,
        &provider_kind_to_db_string(provider),
        &instrument.to_string(),
        Topic::Ticks,
    )?
    else {
        return Ok(Vec::new());
    };
    get_ticks_in_range(conn, provider, instrument, start, latest_ts)
}

// ---------- Candles ----------

/// Get candles for [start, end) for a specific resolution.
/// Assumes parquet column names matching your Candle schema.
/// If your parquet has `resolution` as text, we can filter it here too.
pub fn get_candles_in_range(
    conn: &Connection,
    provider: &str,
    instrument: &Instrument,
    resolution: Resolution,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    if start >= end {
        return Ok(Vec::new());
    }

    let topic = match resolution {
        Resolution::Seconds(1) => Some(Topic::Candles1s),
        Resolution::Minutes(1) => Some(Topic::Candles1m),
        Resolution::Hours(1) => Some(Topic::Candles1h),
        Resolution::Daily => Some(Topic::Candles1d),
        _ => None,
    };
    let Some(topic) = topic else {
        return Ok(Vec::new());
    };
    let Some(dataset_id) = resolve_dataset_id(conn, provider, &instrument.to_string(), topic)?
    else {
        return Ok(Vec::new());
    };

    let paths = partition_paths_for_range(conn, dataset_id, start, end)?;
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let src = build_read_parquet_list(&paths)?;
    let start_ns = start.timestamp_nanos_opt().unwrap_or(0);
    let end_ns = end.timestamp_nanos_opt().unwrap_or(0);

    let sql = format!(
        r#"
        select
            symbol_id, exchange,
            CAST(open  AS VARCHAR), CAST(high  AS VARCHAR),
            CAST(low   AS VARCHAR), CAST(close AS VARCHAR),
            CAST(volume      AS VARCHAR),
            CAST(ask_volume  AS VARCHAR),
            CAST(bid_volume  AS VARCHAR),
            time_start_ns, time_end_ns
        from {src}
        where time_start_ns >= ?
          and time_end_ns   <= ?
        order by time_start_ns asc
        "#,
        src = src
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![start_ns, end_ns])?;

    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let sym: String = r.get(0)?;
        let _exch: String = r.get(1)?; // exchange currently unused in Candle

        let open_s: String = r.get(2)?;
        let high_s: String = r.get(3)?;
        let low_s: String = r.get(4)?;
        let close_s: String = r.get(5)?;
        let volume_s: String = r.get(6)?;
        let ask_volume_s: String = r.get(7)?;
        let bid_volume_s: String = r.get(8)?;
        let ts_start_ns: i64 = r.get(9)?;
        let ts_end_ns: i64 = r.get(10)?;

        out.push(Candle {
            symbol: sym,
            instrument: instrument.clone(),
            time_start: epoch_ns_to_dt(ts_start_ns),
            time_end: epoch_ns_to_dt(ts_end_ns),
            open: Decimal::from_str(&open_s)?,
            high: Decimal::from_str(&high_s)?,
            low: Decimal::from_str(&low_s)?,
            close: Decimal::from_str(&close_s)?,
            volume: Decimal::from_str(&volume_s)?,
            ask_volume: Decimal::from_str(&ask_volume_s)?,
            bid_volume: Decimal::from_str(&bid_volume_s)?,
            resolution,
        });
    }
    Ok(out)
}

pub fn get_candles_from_date_to_latest(
    conn: &Connection,
    provider: &str,
    instrument: &Instrument,
    resolution: Resolution,
    start: DateTime<Utc>,
) -> anyhow::Result<Vec<Candle>> {
    let topic = match resolution {
        Resolution::Seconds(1) => Topic::Candles1s,
        Resolution::Minutes(1) => Topic::Candles1m,
        Resolution::Hours(1) => Topic::Candles1h,
        Resolution::Daily => Topic::Candles1d,
        _ => return Ok(Vec::new()),
    };
    let Some(SeqBound { ts: latest_ts, .. }) =
        latest_available(conn, provider, &instrument.to_string(), topic)?
    else {
        return Ok(Vec::new());
    };
    get_candles_in_range(conn, provider, instrument, resolution, start, latest_ts)
}

/// Get order-book snapshots for [start, end) using DuckDB over the *overlapping* parquet partitions.
/// Ladders (bids/asks) are stored as JSON text for robustness; we parse in Rust.
///
/// Optional `depth`: truncate ladders to top-N after parsing.
pub fn get_books_in_range(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    depth: Option<usize>,
) -> anyhow::Result<Vec<OrderBook>> {
    if start >= end {
        return Ok(Vec::new());
    }

    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, Topic::Depth)? else {
        return Ok(Vec::new());
    };

    let paths = partition_paths_for_range(conn, dataset_id, start, end)?;
    if paths.is_empty() {
        return Ok(Vec::new());
    }

    let src = build_read_parquet_list(&paths)?;
    // Project minimal columns, push down time predicate
    let sql = format!(
        r#"
        select
            symbol,
            exchange,
            epoch_ns(time) as t_ns,
            bids_json,
            asks_json
        from {src}
        where symbol = ?
          and time >= to_timestamp(?)
          and time <  to_timestamp(?)
        order by t_ns asc
        "#,
        src = src
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![symbol, start.to_rfc3339(), end.to_rfc3339()])?;

    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        let sym: String = r.get(0)?;
        let exch_str: String = r.get(1)?;
        let t_ns: i64 = r.get(2)?;
        let bids_txt: String = r.get(3)?;
        let asks_txt: String = r.get(4)?;

        let exchange = Exchange::from_str(&exch_str)
            .ok_or_else(|| anyhow!("unknown exchange '{}' in parquet", exch_str))?;

        let mut bids = parse_ladder_json(&bids_txt)?;
        let mut asks = parse_ladder_json(&asks_txt)?;

        if let Some(d) = depth {
            if bids.len() > d {
                bids.truncate(d);
            }
            if asks.len() > d {
                asks.truncate(d);
            }
        }

        out.push(OrderBook {
            symbol: sym.clone(),
            instrument: Instrument::from_str(&sym)
                .map_err(|_| anyhow!("invalid instrument '{}", sym))?,
            bids: bids
                .into_iter()
                .map(|(p, v)| BookLevel {
                    price: p,
                    volume: v,
                    level: 0,
                })
                .collect(),
            asks: asks
                .into_iter()
                .map(|(p, v)| BookLevel {
                    price: p,
                    volume: v,
                    level: 0,
                })
                .collect(),
            time: epoch_ns_to_dt(t_ns),
        });
    }

    Ok(out)
}

/// Convenience: latest snapshot at/after `hint` (or globally latest if `hint` is None).
pub fn get_latest_book(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    hint_from: Option<DateTime<Utc>>,
    depth: Option<usize>,
) -> anyhow::Result<Option<OrderBook>> {
    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, Topic::Depth)? else {
        return Ok(None);
    };

    // Use catalog to find newest partition; fall back to SQL max(time)
    let (start, end) = if let Some(h) = hint_from {
        (h, DateTime::<Utc>::MAX_UTC)
    } else {
        (DateTime::<Utc>::MIN_UTC, DateTime::<Utc>::MAX_UTC)
    };

    let paths = partition_paths_for_range(conn, dataset_id, start, end)?;
    if paths.is_empty() {
        return Ok(None);
    }

    let src = build_read_parquet_list(&paths)?;
    let sql = format!(
        r#"
        select
            symbol, exchange, epoch_ns(time) as t_ns, bids_json, asks_json
        from {src}
        where symbol = ?
        order by t_ns desc
        limit 1
        "#,
        src = src
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![symbol])?;

    if let Some(r) = rows.next()? {
        let sym: String = r.get(0)?;
        let t_ns: i64 = r.get(2)?;
        let bids_txt: String = r.get(3)?;
        let asks_txt: String = r.get(4)?;

        let mut bids = parse_ladder_json(&bids_txt)?;
        let mut asks = parse_ladder_json(&asks_txt)?;
        if let Some(d) = depth {
            if bids.len() > d {
                bids.truncate(d);
            }
            if asks.len() > d {
                asks.truncate(d);
            }
        }

        return Ok(Some(OrderBook {
            symbol: sym.clone(),
            instrument: Instrument::from_str(&sym)
                .map_err(|_| anyhow!("invalid instrument '{}", sym))?,
            bids: bids
                .into_iter()
                .map(|(p, v)| BookLevel {
                    price: p,
                    volume: v,
                    level: 0,
                })
                .collect(),
            asks: asks
                .into_iter()
                .map(|(p, v)| BookLevel {
                    price: p,
                    volume: v,
                    level: 0,
                })
                .collect(),
            time: epoch_ns_to_dt(t_ns),
        }));
    }

    Ok(None)
}

/// Earliest available order-book snapshot (ts only).
pub fn earliest_book_available(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    // If your generic earliest_available(kind) already exists, you can just:
    // return earliest_available(conn, provider, symbol, DataKind::Book, None).map(|o| o.map(|b| b.ts));

    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, Topic::Depth)? else {
        return Ok(None);
    };

    let mut q = conn.prepare(
        "select min(min_ts_ns) as ts_ns
            from partitions
           where dataset_id = ?",
    )?;
    let ts_ns: Option<i64> = q
        .query_row(duckdb::params![dataset_id], |r| r.get(0))
        .optional()?;
    Ok(ts_ns.map(epoch_ns_to_dt))
}

/// Latest available order-book snapshot (ts only).
pub fn latest_book_available(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    // If your generic latest_available(kind) already exists, you can just:
    // return latest_available(conn, provider, symbol, DataKind::Book, None).map(|o| o.map(|b| b.ts));

    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, Topic::Depth)? else {
        return Ok(None);
    };

    let mut q = conn.prepare(
        "select max(max_ts_ns) as ts_ns
            from partitions
           where dataset_id = ?",
    )?;
    let ts_ns: Option<i64> = q
        .query_row(duckdb::params![dataset_id], |r| r.get(0))
        .optional()?;
    Ok(ts_ns.map(epoch_ns_to_dt))
}

/// Parse a compact JSON [[price, size], ...] into Vec<(Decimal, Decimal)>.
/// Accepts each cell as either a JSON string or number.
/// Example payload: `[[ "5043.25","7"], [5043.50, 3.5], [1e-6, "2.0"]]`
fn parse_ladder_json(txt: &str) -> anyhow::Result<Vec<(Decimal, Decimal)>> {
    let v: JsonValue = serde_json::from_str(txt).with_context(|| "failed to parse ladder JSON")?;
    let arr = v
        .as_array()
        .ok_or_else(|| anyhow!("ladder json not array"))?;

    fn dec_from_json(x: &JsonValue, what: &str) -> anyhow::Result<Decimal> {
        match x {
            JsonValue::String(s) => {
                // Prefer exact if you guarantee canonical strings; fall back to FromStr for flexibility.
                Decimal::from_str_exact(s)
                    .or_else(|_| Decimal::from_str(s))
                    .with_context(|| format!("invalid decimal string for {what}: {s}"))
            }
            JsonValue::Number(n) => {
                // Convert the original number to string (preserves exponent form) and parse Decimal.
                let s = n.to_string();
                Decimal::from_str(&s)
                    .with_context(|| format!("invalid decimal number for {what}: {s}"))
            }
            _ => Err(anyhow!("{} must be string or number", what)),
        }
    }

    let mut out = Vec::with_capacity(arr.len());
    for (i, item) in arr.iter().enumerate() {
        let pair = item
            .as_array()
            .ok_or_else(|| anyhow!("ladder row {i} not array"))?;
        if pair.len() != 2 {
            return Err(anyhow!("ladder row {i} length != 2"));
        }
        let px = dec_from_json(&pair[0], "price")?;
        let sz = dec_from_json(&pair[1], "size")?;
        out.push((px, sz));
    }
    Ok(out)
}
