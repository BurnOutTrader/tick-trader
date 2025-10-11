use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::Row;
use std::str::FromStr;
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::models::{Resolution, TradeSide};
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;

use crate::init::Connection;
use crate::schema::get_or_create_instrument_id;

/// Return the latest timestamp available for a (provider, instrument, topic) tuple.
/// - Candles1m → latest_bar_1m hot table
/// - Ticks → max ts from tick
/// - Quotes → max ts from bbo
pub async fn latest_data_time(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    topic: Topic,
) -> Result<Option<DateTime<Utc>>> {
    let sym = instrument.to_string();
    let inst_id = get_or_create_instrument_id(conn, &sym).await?;
    Ok(match topic {
        Topic::Candles1m => {
            let prov = crate::paths::provider_kind_to_db_string(provider);
            let row = sqlx::query(
                "SELECT time_end FROM latest_bar_1m WHERE provider=$1 AND symbol_id = $2",
            )
            .bind(prov)
            .bind(inst_id)
            .fetch_optional(conn)
            .await?;
            row.map(|r| r.get::<DateTime<Utc>, _>("time_end"))
        }
        Topic::Ticks => {
            let prov = crate::paths::provider_kind_to_db_string(provider);
            let row = sqlx::query("SELECT ts_ns FROM tick WHERE provider=$1 AND symbol_id=$2 ORDER BY ts_ns DESC LIMIT 1")
                .bind(prov)
                .bind(inst_id)
                .fetch_optional(conn)
                .await?;
            row.map(|r| {
                let ns: i64 = r.get("ts_ns");
                let secs = ns / 1_000_000_000;
                let nanos = (ns % 1_000_000_000) as u32;
                DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or_default()
            })
        }
        Topic::Quotes => {
            let prov = crate::paths::provider_kind_to_db_string(provider);
            let row = sqlx::query("SELECT ts_ns FROM bbo WHERE provider=$1 AND symbol_id=$2 ORDER BY ts_ns DESC LIMIT 1")
                .bind(prov)
                .bind(inst_id)
                .fetch_optional(conn)
                .await?;
            row.map(|r| {
                let ns: i64 = r.get("ts_ns");
                let secs = ns / 1_000_000_000;
                let nanos = (ns % 1_000_000_000) as u32;
                DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or_default()
            })
        }
        _ => None,
    })
}

/// Get earliest and latest timestamps for a series; uses series_extent if populated, otherwise computes on the fly.
pub async fn get_extent(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    topic: Topic,
) -> Result<(Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
    use sqlx::Row;
    let sym = instrument.to_string();
    let inst_id = get_or_create_instrument_id(conn, &sym).await?;
    let prov = crate::paths::provider_kind_to_db_string(provider);
    // Try cache first
    let topic_id = topic as i16 as i32; // cast to smallint-compatible
    if let Some(r) = sqlx::query("SELECT earliest, latest FROM series_extent WHERE provider=$1 AND symbol_id=$2 AND topic=$3")
        .bind(&prov)
        .bind(inst_id)
        .bind(topic_id)
        .fetch_optional(conn)
        .await? {
        let e = r.get::<Option<DateTime<Utc>>, _>("earliest");
        let l = r.get::<Option<DateTime<Utc>>, _>("latest");
        return Ok((e, l));
    }
    // Fallback scan per topic
    let (sql, is_ns) = match topic {
        Topic::Candles1m => (
            "SELECT MIN(time_end) e, MAX(time_end) l FROM bars_1m WHERE provider=$1 AND symbol_id=$2",
            false,
        ),
        Topic::Ticks => (
            "SELECT MIN(ts_ns) e, MAX(ts_ns) l FROM tick WHERE provider=$1 AND symbol_id=$2",
            true,
        ),
        Topic::Quotes => (
            "SELECT MIN(ts_ns) e, MAX(ts_ns) l FROM bbo WHERE provider=$1 AND symbol_id=$2",
            true,
        ),
        Topic::MBP10 => (
            "SELECT MIN(ts_event_ns) e, MAX(ts_event_ns) l FROM mbp10 WHERE provider=$1 AND symbol_id=$2",
            true,
        ),
        _ => ("SELECT NULL::TIMESTAMPTZ e, NULL::TIMESTAMPTZ l", false),
    };
    let row = sqlx::query(sql)
        .bind(&prov)
        .bind(inst_id)
        .fetch_one(conn)
        .await?;
    if is_ns {
        let e_ns: Option<i64> = row.get("e");
        let l_ns: Option<i64> = row.get("l");
        let to_dt = |ns: i64| {
            let secs = ns / 1_000_000_000;
            let nanos = (ns % 1_000_000_000) as u32;
            DateTime::<Utc>::from_timestamp(secs, nanos)
        };
        Ok((e_ns.and_then(to_dt), l_ns.and_then(to_dt)))
    } else {
        let e: Option<DateTime<Utc>> = row.get("e");
        let l: Option<DateTime<Utc>> = row.get("l");
        Ok((e, l))
    }
}

/// Keyset-paginated range query returning typed rows converted to tt-types items.
pub async fn get_range(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    topic: Topic,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    limit: u32,
) -> Result<(Vec<tt_types::wire::Response>, Option<String>)> {
    use sqlx::Row;
    let sym = instrument.to_string();
    let inst_id = get_or_create_instrument_id(conn, &sym).await?;
    let prov = crate::paths::provider_kind_to_db_string(provider);
    let lim = limit.clamp(1, 10_000) as i64;
    match topic {
        Topic::Candles1m => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume FROM bars_1m WHERE provider=$1 AND symbol_id=$2 AND time_end >= $3 AND time_end < $4 ORDER BY time_end ASC LIMIT $5"
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(start)
            .bind(end)
            .bind(lim)
            .fetch_all(conn)
            .await?;
            let bars: Vec<Candle> = rows
                .iter()
                .map(|r| Candle {
                    symbol: instrument.to_string(),
                    instrument: instrument.clone(),
                    time_start: r.get("time_start"),
                    time_end: r.get("time_end"),
                    open: r.get("open"),
                    high: r.get("high"),
                    low: r.get("low"),
                    close: r.get("close"),
                    volume: r.get("volume"),
                    ask_volume: r.get("ask_volume"),
                    bid_volume: r.get("bid_volume"),
                    resolution: Resolution::Minutes(1),
                })
                .collect();
            let batch = tt_types::wire::BarBatch {
                topic,
                seq: 0,
                bars,
                provider_kind: provider,
            };
            let next = rows
                .last()
                .map(|r| r.get::<DateTime<Utc>, _>("time_end").to_rfc3339());
            Ok((vec![tt_types::wire::Response::BarBatch(batch)], next))
        }
        Topic::Ticks => {
            let rows = sqlx::query(
                "SELECT ts_ns, price, volume, side, venue_seq FROM tick WHERE provider=$1 AND symbol_id=$2 AND ts_ns >= $3 AND ts_ns < $4 ORDER BY ts_ns ASC LIMIT $5"
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(start.timestamp_nanos_opt().unwrap_or(0))
            .bind(end.timestamp_nanos_opt().unwrap_or(0))
            .bind(lim)
            .fetch_all(conn)
            .await?;
            let ticks: Vec<Tick> = rows
                .iter()
                .map(|r| {
                    let ns: i64 = r.get("ts_ns");
                    let secs = ns / 1_000_000_000;
                    let nanos = (ns % 1_000_000_000) as u32;
                    let time = DateTime::<Utc>::from_timestamp(secs, nanos).unwrap();
                    let side_i: i16 = r.get("side");
                    let side = match side_i {
                        1 => TradeSide::Buy,
                        2 => TradeSide::Sell,
                        _ => TradeSide::None,
                    };
                    let venue_seq = r
                        .try_get::<i64, _>("venue_seq")
                        .ok()
                        .and_then(|v| u32::try_from(v).ok());
                    Tick {
                        symbol: instrument.to_string(),
                        instrument: instrument.clone(),
                        price: r.get("price"),
                        volume: r.get("volume"),
                        time,
                        side,
                        venue_seq,
                    }
                })
                .collect();
            let batch = tt_types::wire::TickBatch {
                topic,
                seq: 0,
                ticks,
                provider_kind: provider,
            };
            let last_ns = rows.last().map(|r| r.get::<i64, _>("ts_ns"));
            let next = last_ns.map(|ns| ns.to_string());
            Ok((vec![tt_types::wire::Response::TickBatch(batch)], next))
        }
        Topic::Quotes => {
            let rows = sqlx::query(
                "SELECT ts_ns, bid, bid_size, ask, ask_size, bid_orders, ask_orders, venue_seq, is_snapshot FROM bbo WHERE provider=$1 AND symbol_id=$2 AND ts_ns >= $3 AND ts_ns < $4 ORDER BY ts_ns ASC LIMIT $5"
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(start.timestamp_nanos_opt().unwrap_or(0))
            .bind(end.timestamp_nanos_opt().unwrap_or(0))
            .bind(lim)
            .fetch_all(conn)
            .await?;
            let quotes: Vec<Bbo> = rows
                .iter()
                .map(|r| {
                    let ns: i64 = r.get("ts_ns");
                    let secs = ns / 1_000_000_000;
                    let nanos = (ns % 1_000_000_000) as u32;
                    let time = DateTime::<Utc>::from_timestamp(secs, nanos).unwrap();
                    Bbo {
                        symbol: instrument.to_string(),
                        instrument: instrument.clone(),
                        bid: r
                            .try_get("bid")
                            .ok()
                            .unwrap_or_else(|| Decimal::from_i32(0).unwrap()),
                        bid_size: r
                            .try_get("bid_size")
                            .ok()
                            .unwrap_or_else(|| Decimal::from_i32(0).unwrap()),
                        ask: r
                            .try_get("ask")
                            .ok()
                            .unwrap_or_else(|| Decimal::from_i32(0).unwrap()),
                        ask_size: r
                            .try_get("ask_size")
                            .ok()
                            .unwrap_or_else(|| Decimal::from_i32(0).unwrap()),
                        time,
                        bid_orders: r.try_get::<i32, _>("bid_orders").ok().map(|v| v as u32),
                        ask_orders: r.try_get::<i32, _>("ask_orders").ok().map(|v| v as u32),
                        venue_seq: r
                            .try_get::<i64, _>("venue_seq")
                            .ok()
                            .and_then(|v| u32::try_from(v).ok()),
                        is_snapshot: r.try_get::<bool, _>("is_snapshot").ok(),
                    }
                })
                .collect();
            let batch = tt_types::wire::QuoteBatch {
                topic,
                seq: 0,
                quotes,
                provider_kind: provider,
            };
            let last_ns = rows.last().map(|r| r.get::<i64, _>("ts_ns"));
            let next = last_ns.map(|ns| ns.to_string());
            Ok((vec![tt_types::wire::Response::QuoteBatch(batch)], next))
        }
        _ => Ok((Vec::new(), None)),
    }
}

/// List instruments for a provider with any data across known tables.
pub async fn get_symbols(
    conn: &Connection,
    provider: ProviderKind,
    pattern: Option<String>,
) -> Result<Vec<Instrument>> {
    use sqlx::Row;
    let prov = crate::paths::provider_kind_to_db_string(provider);
    let like_sql = if pattern.is_some() {
        " AND i.sym ILIKE $2"
    } else {
        ""
    };
    let sql = format!(
        "SELECT DISTINCT i.sym FROM instrument i
         WHERE EXISTS (SELECT 1 FROM bars_1m b WHERE b.symbol_id=i.id) OR
               EXISTS (SELECT 1 FROM tick t WHERE t.symbol_id=i.id AND t.provider=$1) OR
               EXISTS (SELECT 1 FROM bbo q WHERE q.symbol_id=i.id AND q.provider=$1){} ORDER BY i.sym",
        like_sql
    );
    let rows = if let Some(p) = pattern {
        sqlx::query(&sql)
            .bind(&prov)
            .bind(p)
            .fetch_all(conn)
            .await?
    } else {
        sqlx::query(&sql).bind(&prov).fetch_all(conn).await?
    };
    Ok(rows
        .into_iter()
        .filter_map(|r| {
            let s: String = r.get("sym");
            Instrument::from_str(&s).ok()
        })
        .collect())
}

/// Fetch latest 1m bars for a list of instruments from the hot cache table.
pub async fn latest_bars_1m(
    conn: &Connection,
    provider: ProviderKind,
    instruments: &[Instrument],
) -> Result<Vec<Candle>> {
    use sqlx::Row;
    if instruments.is_empty() {
        return Ok(Vec::new());
    }
    // map instruments to ids and build reverse map
    let mut ids: Vec<i64> = Vec::with_capacity(instruments.len());
    let mut id_to_inst: std::collections::HashMap<i64, Instrument> =
        std::collections::HashMap::with_capacity(instruments.len());
    for inst in instruments.iter() {
        let id = get_or_create_instrument_id(conn, &inst.to_string()).await?;
        ids.push(id);
        id_to_inst.insert(id, inst.clone());
    }
    let prov = crate::paths::provider_kind_to_db_string(provider);
    // Use = ANY($2) for array binding
    let rows = sqlx::query(
        "SELECT symbol_id, time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume, resolution FROM latest_bar_1m WHERE provider=$1 AND symbol_id = ANY($2)"
    )
    .bind(&prov)
    .bind(&ids)
    .fetch_all(conn)
    .await?;
    let mut out: Vec<Candle> = Vec::with_capacity(rows.len());
    for r in rows.iter() {
        let symbol_id: i64 = r.get("symbol_id");
        if let Some(inst) = id_to_inst.get(&symbol_id) {
            out.push(Candle {
                symbol: inst.to_string(),
                instrument: inst.clone(),
                time_start: r.get("time_start"),
                time_end: r.get("time_end"),
                open: r.get("open"),
                high: r.get("high"),
                low: r.get("low"),
                close: r.get("close"),
                volume: r.get("volume"),
                ask_volume: r.get("ask_volume"),
                bid_volume: r.get("bid_volume"),
                resolution: Resolution::Minutes(1),
            });
        }
    }
    Ok(out)
}
