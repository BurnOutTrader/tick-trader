use anyhow::Result;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use sqlx::Row;
use std::str::FromStr;
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::Mbp10;
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
        Topic::Candles1s => {
            let prov = crate::paths::provider_kind_to_db_string(provider);
            let row = sqlx::query(
                "SELECT MAX(time_end) AS time_end FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution = $3",
            )
            .bind(prov)
            .bind(inst_id)
            .bind(Resolution::Seconds(1).to_string())
            .fetch_optional(conn)
            .await?;
            row.and_then(|r| r.try_get::<Option<DateTime<Utc>>, _>("time_end").ok())
                .flatten()
        }
        Topic::Candles1m => {
            let prov = crate::paths::provider_kind_to_db_string(provider);
            // Prefer hot cache when present, fallback to base table
            let row_hot = sqlx::query(
                "SELECT time_end FROM latest_bar_1m WHERE provider=$1 AND symbol_id = $2",
            )
            .bind(&prov)
            .bind(inst_id)
            .fetch_optional(conn)
            .await?;
            if let Some(r) = row_hot {
                Some(r.get::<DateTime<Utc>, _>("time_end"))
            } else {
                let row = sqlx::query(
                    "SELECT MAX(time_end) AS time_end FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution = $3",
                )
                .bind(prov)
                .bind(inst_id)
                .bind(Resolution::Minutes(1).to_string())
                .fetch_optional(conn)
                .await?;
                row.and_then(|r| r.try_get::<Option<DateTime<Utc>>, _>("time_end").ok())
                    .flatten()
            }
        }
        Topic::Candles1h => {
            let prov = crate::paths::provider_kind_to_db_string(provider);
            let row = sqlx::query(
                "SELECT MAX(time_end) AS time_end FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution LIKE 'Hours(%'",
            )
            .bind(prov)
            .bind(inst_id)
            .fetch_optional(conn)
            .await?;
            row.and_then(|r| r.try_get::<Option<DateTime<Utc>>, _>("time_end").ok())
                .flatten()
        }
        Topic::Candles1d => {
            let prov = crate::paths::provider_kind_to_db_string(provider);
            let row = sqlx::query(
                "SELECT MAX(time_end) AS time_end FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution = $3",
            )
            .bind(prov)
            .bind(inst_id)
            .bind(Resolution::Daily.to_string())
            .fetch_optional(conn)
            .await?;
            row.and_then(|r| r.try_get::<Option<DateTime<Utc>>, _>("time_end").ok())
                .flatten()
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
    // Fallback scan per topic (handle all candle resolutions)
    let (e, l) = match topic {
        Topic::Candles1s => {
            let row = sqlx::query(
                "SELECT MIN(time_end) e, MAX(time_end) l FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution=$3",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(tt_types::data::models::Resolution::Seconds(1).to_string())
            .fetch_one(conn)
            .await?;
            (
                row.get::<Option<DateTime<Utc>>, _>("e"),
                row.get::<Option<DateTime<Utc>>, _>("l"),
            )
        }
        Topic::Candles1m => {
            let row = sqlx::query(
                "SELECT MIN(time_end) e, MAX(time_end) l FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution=$3",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(tt_types::data::models::Resolution::Minutes(1).to_string())
            .fetch_one(conn)
            .await?;
            (
                row.get::<Option<DateTime<Utc>>, _>("e"),
                row.get::<Option<DateTime<Utc>>, _>("l"),
            )
        }
        Topic::Candles1h => {
            let row = sqlx::query(
                "SELECT MIN(time_end) e, MAX(time_end) l FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution LIKE 'Hours(%'",
            )
            .bind(&prov)
            .bind(inst_id)
            .fetch_one(conn)
            .await?;
            (
                row.get::<Option<DateTime<Utc>>, _>("e"),
                row.get::<Option<DateTime<Utc>>, _>("l"),
            )
        }
        Topic::Candles1d => {
            let row = sqlx::query(
                "SELECT MIN(time_end) e, MAX(time_end) l FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution=$3",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(tt_types::data::models::Resolution::Daily.to_string())
            .fetch_one(conn)
            .await?;
            (
                row.get::<Option<DateTime<Utc>>, _>("e"),
                row.get::<Option<DateTime<Utc>>, _>("l"),
            )
        }
        Topic::Ticks => {
            let row = sqlx::query(
                "SELECT MIN(ts_ns) e, MAX(ts_ns) l FROM tick WHERE provider=$1 AND symbol_id=$2",
            )
            .bind(&prov)
            .bind(inst_id)
            .fetch_one(conn)
            .await?;
            let e_ns: Option<i64> = row.get("e");
            let l_ns: Option<i64> = row.get("l");
            let to_dt = |ns: i64| {
                let secs = ns / 1_000_000_000;
                let nanos = (ns % 1_000_000_000) as u32;
                DateTime::<Utc>::from_timestamp(secs, nanos)
            };
            (e_ns.and_then(to_dt), l_ns.and_then(to_dt))
        }
        Topic::Quotes => {
            let row = sqlx::query(
                "SELECT MIN(ts_ns) e, MAX(ts_ns) l FROM bbo WHERE provider=$1 AND symbol_id=$2",
            )
            .bind(&prov)
            .bind(inst_id)
            .fetch_one(conn)
            .await?;
            let e_ns: Option<i64> = row.get("e");
            let l_ns: Option<i64> = row.get("l");
            let to_dt = |ns: i64| {
                let secs = ns / 1_000_000_000;
                let nanos = (ns % 1_000_000_000) as u32;
                DateTime::<Utc>::from_timestamp(secs, nanos)
            };
            (e_ns.and_then(to_dt), l_ns.and_then(to_dt))
        }
        Topic::MBP10 => {
            let row = sqlx::query("SELECT MIN(ts_event_ns) e, MAX(ts_event_ns) l FROM mbp10 WHERE provider=$1 AND symbol_id=$2")
                .bind(&prov)
                .bind(inst_id)
                .fetch_one(conn)
                .await?;
            let e_ns: Option<i64> = row.get("e");
            let l_ns: Option<i64> = row.get("l");
            let to_dt = |ns: i64| {
                let secs = ns / 1_000_000_000;
                let nanos = (ns % 1_000_000_000) as u32;
                DateTime::<Utc>::from_timestamp(secs, nanos)
            };
            (e_ns.and_then(to_dt), l_ns.and_then(to_dt))
        }
        _ => (None, None),
    };
    Ok((e, l))
}

pub enum TopicDataEnum {
    Tick(Tick),
    Bbo(Bbo),
    Mbp10(Mbp10),
    Candle(Candle),
}

use std::collections::BTreeMap;

/// Return all rows for a given (provider, instrument, topic) over [start, end),
/// grouped by timestamp into a BTreeMap<DateTime, Vec<TopicDataEnum>>.
/// Note: this is an unpaginated call intended for modest ranges; prefer paginated get_range for very large scans.
pub async fn get_time_indexed(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    topic: Topic,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<BTreeMap<DateTime<Utc>, Vec<TopicDataEnum>>> {
    use sqlx::Row;
    let sym = instrument.to_string();
    let inst_id = get_or_create_instrument_id(conn, &sym).await?;
    let prov = crate::paths::provider_kind_to_db_string(provider);
    let mut out: BTreeMap<DateTime<Utc>, Vec<TopicDataEnum>> = BTreeMap::new();

    match topic {
        Topic::Candles1s => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution=$3 AND time_end >= $4 AND time_end < $5 ORDER BY time_end ASC",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(Resolution::Seconds(1).to_string())
            .bind(start)
            .bind(end)
            .fetch_all(conn)
            .await?;
            for r in rows.iter() {
                let c = Candle {
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
                    resolution: Resolution::Seconds(1),
                };
                out.entry(c.time_end)
                    .or_default()
                    .push(TopicDataEnum::Candle(c));
            }
        }
        Topic::Candles1m => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution=$3 AND time_end >= $4 AND time_end < $5 ORDER BY time_end ASC",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(Resolution::Minutes(1).to_string())
            .bind(start)
            .bind(end)
            .fetch_all(conn)
            .await?;
            for r in rows.iter() {
                let c = Candle {
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
                };
                out.entry(c.time_end)
                    .or_default()
                    .push(TopicDataEnum::Candle(c));
            }
        }
        Topic::Candles1h => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume, resolution FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution LIKE 'Hours(%' AND time_end >= $3 AND time_end < $4 ORDER BY time_end ASC",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(start)
            .bind(end)
            .fetch_all(conn)
            .await?;
            for r in rows.iter() {
                let res: String = r.get("resolution");
                let resolution = if let Some(num) =
                    res.strip_prefix("Hours(").and_then(|s| s.strip_suffix(")"))
                {
                    num.parse::<u8>()
                        .ok()
                        .map(Resolution::Hours)
                        .unwrap_or(Resolution::Hours(1))
                } else if res.starts_with("hr") {
                    res.trim_start_matches("hr")
                        .parse::<u8>()
                        .ok()
                        .map(Resolution::Hours)
                        .unwrap_or(Resolution::Hours(1))
                } else {
                    Resolution::Hours(1)
                };
                let c = Candle {
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
                    resolution,
                };
                out.entry(c.time_end)
                    .or_default()
                    .push(TopicDataEnum::Candle(c));
            }
        }
        Topic::Candles1d => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution=$3 AND time_end >= $4 AND time_end < $5 ORDER BY time_end ASC",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(Resolution::Daily.to_string())
            .bind(start)
            .bind(end)
            .fetch_all(conn)
            .await?;
            for r in rows.iter() {
                let c = Candle {
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
                    resolution: Resolution::Daily,
                };
                out.entry(c.time_end)
                    .or_default()
                    .push(TopicDataEnum::Candle(c));
            }
        }
        Topic::Ticks => {
            let rows = sqlx::query(
                "SELECT ts_ns, price, volume, side, venue_seq FROM tick WHERE provider=$1 AND symbol_id=$2 AND ts_ns >= $3 AND ts_ns < $4 ORDER BY ts_ns ASC",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(start.timestamp_nanos_opt().unwrap_or(0))
            .bind(end.timestamp_nanos_opt().unwrap_or(0))
            .fetch_all(conn)
            .await?;
            for r in rows.iter() {
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
                let t = Tick {
                    symbol: instrument.to_string(),
                    instrument: instrument.clone(),
                    price: r.get("price"),
                    volume: r.get("volume"),
                    time,
                    side,
                    venue_seq,
                };
                out.entry(time).or_default().push(TopicDataEnum::Tick(t));
            }
        }
        Topic::Quotes => {
            let rows = sqlx::query(
                "SELECT ts_ns, COALESCE(bid, 0) AS bid, COALESCE(bid_size, 0) AS bid_size, COALESCE(ask, 0) AS ask, COALESCE(ask_size, 0) AS ask_size, bid_orders, ask_orders, venue_seq, is_snapshot FROM bbo WHERE provider=$1 AND symbol_id=$2 AND ts_ns >= $3 AND ts_ns < $4 ORDER BY ts_ns ASC",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(start.timestamp_nanos_opt().unwrap_or(0))
            .bind(end.timestamp_nanos_opt().unwrap_or(0))
            .fetch_all(conn)
            .await?;
            for r in rows.iter() {
                let ns: i64 = r.get("ts_ns");
                let secs = ns / 1_000_000_000;
                let nanos = (ns % 1_000_000_000) as u32;
                let time = DateTime::<Utc>::from_timestamp(secs, nanos).unwrap();
                let bid_orders = r.try_get::<i32, _>("bid_orders").ok().map(|v| v as u32);
                let ask_orders = r.try_get::<i32, _>("ask_orders").ok().map(|v| v as u32);
                let venue_seq = r
                    .try_get::<i64, _>("venue_seq")
                    .ok()
                    .and_then(|v| u32::try_from(v).ok());
                let b = Bbo {
                    symbol: instrument.to_string(),
                    instrument: instrument.clone(),
                    time,
                    bid: r.get("bid"),
                    bid_size: r.get("bid_size"),
                    ask: r.get("ask"),
                    ask_size: r.get("ask_size"),
                    bid_orders,
                    ask_orders,
                    is_snapshot: Some(r.get::<bool, _>("is_snapshot")),
                    venue_seq,
                };
                out.entry(time).or_default().push(TopicDataEnum::Bbo(b));
            }
        }
        Topic::MBP10 => {
            let rows = sqlx::query(
                "SELECT ts_recv_ns, ts_event_ns, rtype, publisher_id, instrument_ref, action, side, depth, price, size, flags, ts_in_delta, sequence, book_bid_px, book_ask_px, book_bid_sz, book_ask_sz, book_bid_ct, book_ask_ct FROM mbp10 WHERE provider=$1 AND symbol_id=$2 AND ts_event_ns >= $3 AND ts_event_ns < $4 ORDER BY ts_event_ns ASC",
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(start.timestamp_nanos_opt().unwrap_or(0))
            .bind(end.timestamp_nanos_opt().unwrap_or(0))
            .fetch_all(conn)
            .await?;
            for r in rows.iter() {
                let ns: i64 = r.get("ts_event_ns");
                let secs = ns / 1_000_000_000;
                let nanos = (ns % 1_000_000_000) as u32;
                let ts_event = DateTime::<Utc>::from_timestamp(secs, nanos).unwrap();
                let ts_recv_ns: i64 = r.get("ts_recv_ns");
                let rs = ts_recv_ns / 1_000_000_000;
                let rn = (ts_recv_ns % 1_000_000_000) as u32;
                let ts_recv = DateTime::<Utc>::from_timestamp(rs, rn).unwrap();
                let action_i16: i16 = r.get("action");
                let side_i16: i16 = r.get("side");
                let flags_i16: i16 = r.get("flags");
                let action = tt_types::data::mbp10::Action::from(action_i16 as u8);
                let side = tt_types::data::mbp10::BookSide::from(side_i16 as u8);
                let flags = tt_types::data::mbp10::Flags::from(flags_i16 as u8);
                let book_bid_ct: Option<Vec<i32>> = r.try_get("book_bid_ct").ok();
                let book_ask_ct: Option<Vec<i32>> = r.try_get("book_ask_ct").ok();
                let mbp = Mbp10 {
                    instrument: instrument.clone(),
                    ts_recv,
                    ts_event,
                    rtype: r.get::<i16, _>("rtype") as u8,
                    publisher_id: r.get::<i32, _>("publisher_id") as u16,
                    instrument_id: r.get::<i32, _>("instrument_ref") as u32,
                    action,
                    side,
                    depth: r.get::<i16, _>("depth") as u8,
                    price: r.get("price"),
                    size: r.get("size"),
                    flags,
                    ts_in_delta: r.get::<i32, _>("ts_in_delta"),
                    sequence: r.get::<i64, _>("sequence") as u32,
                    book: {
                        let px_b: Option<Vec<Decimal>> = r.try_get("book_bid_px").ok();
                        let px_a: Option<Vec<Decimal>> = r.try_get("book_ask_px").ok();
                        let sz_b: Option<Vec<Decimal>> = r.try_get("book_bid_sz").ok();
                        let sz_a: Option<Vec<Decimal>> = r.try_get("book_ask_sz").ok();
                        match (px_b, px_a, sz_b, sz_a, book_bid_ct, book_ask_ct) {
                            (Some(bpx), Some(apx), Some(bsz), Some(asz), Some(bct), Some(act)) => {
                                Some(tt_types::data::mbp10::BookLevels {
                                    bid_px: bpx,
                                    ask_px: apx,
                                    bid_sz: bsz,
                                    ask_sz: asz,
                                    bid_ct: bct
                                        .into_iter()
                                        .map(|v| Decimal::from_i32(v).unwrap())
                                        .collect(),
                                    ask_ct: act
                                        .into_iter()
                                        .map(|v| Decimal::from_i32(v).unwrap())
                                        .collect(),
                                })
                            }
                            _ => None,
                        }
                    },
                };
                out.entry(ts_event)
                    .or_default()
                    .push(TopicDataEnum::Mbp10(mbp));
            }
        }
        _ => {}
    }

    Ok(out)
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
        Topic::Candles1s => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution=$3 AND time_end >= $4 AND time_end < $5 ORDER BY time_end ASC LIMIT $6"
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(Resolution::Seconds(1).to_string())
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
                    resolution: Resolution::Seconds(1),
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
        Topic::Candles1m => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume FROM bars WHERE provider=$1 AND symbol_id=$2 AND resolution=$3 AND time_end >= $4 AND time_end < $5 ORDER BY time_end ASC LIMIT $6"
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(Resolution::Minutes(1).to_string())
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
        Topic::Candles1h => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume, resolution FROM bars_1m WHERE provider=$1 AND symbol_id=$2 AND resolution LIKE 'Hours(%' AND time_end >= $3 AND time_end < $4 ORDER BY time_end ASC LIMIT $5"
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
                    // If you stored explicit hr10, keep it; otherwise default to 1h
                    resolution: {
                        let res: String = r.get("resolution");
                        if let Some(num) =
                            res.strip_prefix("Hours(").and_then(|s| s.strip_suffix(")"))
                        {
                            num.parse::<u8>()
                                .ok()
                                .map(Resolution::Hours)
                                .unwrap_or(Resolution::Hours(1))
                        } else if res.starts_with("hr") {
                            // Backward compatibility if older keys were stored
                            res.trim_start_matches("hr")
                                .parse::<u8>()
                                .ok()
                                .map(Resolution::Hours)
                                .unwrap_or(Resolution::Hours(1))
                        } else {
                            Resolution::Hours(1)
                        }
                    },
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
        Topic::Candles1d => {
            let rows = sqlx::query(
                "SELECT time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume FROM bars_1m WHERE provider=$1 AND symbol_id=$2 AND resolution=$3 AND time_end >= $4 AND time_end < $5 ORDER BY time_end ASC LIMIT $6"
            )
            .bind(&prov)
            .bind(inst_id)
            .bind(Resolution::Daily.to_string())
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
                    resolution: Resolution::Daily,
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
         WHERE EXISTS (SELECT 1 FROM bars b WHERE b.symbol_id=i.id) OR
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

// === Futures contracts map storage ===
use ahash::AHashMap;
use tt_types::securities::security::FuturesContract;

/// Append-only insert for a provider's contracts map: only insert instruments that
/// do not already exist for the provider. Existing rows are left untouched.
pub async fn inject_contracts_map(
    conn: &crate::init::Connection,
    provider: tt_types::providers::ProviderKind,
    contracts: &AHashMap<tt_types::securities::symbols::Instrument, FuturesContract>,
) -> anyhow::Result<()> {
    use std::collections::HashSet;

    let prov = crate::paths::provider_kind_to_db_string(provider);

    // Fetch existing instrument symbols for this provider to avoid re-inserting.
    let existing_rows = sqlx::query(
        "SELECT i.sym AS sym
         FROM futures_contracts f
         JOIN instrument i ON i.id = f.symbol_id
         WHERE f.provider = $1",
    )
    .bind(&prov)
    .fetch_all(conn)
    .await?;

    let mut existing: HashSet<String> = HashSet::with_capacity(existing_rows.len());
    for r in existing_rows.iter() {
        let s: String = r.get("sym");
        existing.insert(s);
    }

    let mut tx = conn.begin().await?;

    for (instrument, contract) in contracts.iter() {
        let sym = instrument.to_string();
        if existing.contains(&sym) {
            // Skip existing instrument for this provider (append-only behavior)
            continue;
        }
        let symbol_id = crate::schema::get_or_create_instrument_id(conn, &sym).await?;
        // Serialize contract using rkyv
        let bytes = rkyv::to_bytes::<_, 256>(contract)?;
        // Insert only; if a concurrent insert happens, ignore via DO NOTHING.
        sqlx::query(
            "INSERT INTO futures_contracts (provider, symbol_id, contract) VALUES ($1, $2, $3)
             ON CONFLICT (provider, symbol_id) DO NOTHING",
        )
        .bind(&prov)
        .bind(symbol_id)
        .bind(bytes.as_slice())
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Retrieve the contracts map for a provider.
pub async fn get_contracts_map(
    conn: &crate::init::Connection,
    provider: tt_types::providers::ProviderKind,
) -> anyhow::Result<AHashMap<tt_types::securities::symbols::Instrument, FuturesContract>> {
    use std::str::FromStr;

    let prov = crate::paths::provider_kind_to_db_string(provider);
    let rows = sqlx::query(
        "SELECT i.sym AS sym, f.contract AS contract
         FROM futures_contracts f
         JOIN instrument i ON i.id = f.symbol_id
         WHERE f.provider = $1",
    )
    .bind(&prov)
    .fetch_all(conn)
    .await?;

    let mut out: AHashMap<tt_types::securities::symbols::Instrument, FuturesContract> =
        AHashMap::with_capacity(rows.len());
    for r in rows.iter() {
        let sym: String = r.get("sym");
        if let Ok(instr) = tt_types::securities::symbols::Instrument::from_str(&sym) {
            let bytes: Vec<u8> = r.get("contract");
            // Deserialize using rkyv
            let val: FuturesContract = rkyv::from_bytes(&bytes)
                .map_err(|_e| anyhow::anyhow!("contracts decode failed"))?;
            out.insert(instr, val);
        }
    }
    Ok(out)
}

/// Insert a single futures contract for (provider, instrument) in append-only mode.
/// If a row already exists for this pair, it is left unchanged.
pub async fn inject_contract(
    conn: &crate::init::Connection,
    provider: tt_types::providers::ProviderKind,
    instrument: &tt_types::securities::symbols::Instrument,
    contract: &FuturesContract,
) -> anyhow::Result<()> {
    let prov = crate::paths::provider_kind_to_db_string(provider);
    let sym = instrument.to_string();
    let symbol_id = crate::schema::get_or_create_instrument_id(conn, &sym).await?;

    // Serialize using rkyv
    let bytes = rkyv::to_bytes::<_, 256>(contract)?;

    // Append-only insert
    sqlx::query(
        "INSERT INTO futures_contracts (provider, symbol_id, contract) VALUES ($1, $2, $3)
         ON CONFLICT (provider, symbol_id) DO NOTHING",
    )
    .bind(&prov)
    .bind(symbol_id)
    .bind(bytes.as_slice())
    .execute(conn)
    .await?;

    Ok(())
}

/// Retrieve a single futures contract for (provider, instrument).
pub async fn get_contract(
    conn: &crate::init::Connection,
    provider: tt_types::providers::ProviderKind,
    instrument: &tt_types::securities::symbols::Instrument,
) -> anyhow::Result<Option<FuturesContract>> {
    use sqlx::Row;

    let prov = crate::paths::provider_kind_to_db_string(provider);
    let sym = instrument.to_string();
    let symbol_id = crate::schema::get_or_create_instrument_id(conn, &sym).await?;

    let row = sqlx::query(
        "SELECT contract FROM futures_contracts WHERE provider = $1 AND symbol_id = $2",
    )
    .bind(&prov)
    .bind(symbol_id)
    .fetch_optional(conn)
    .await?;

    if let Some(r) = row {
        let bytes: Vec<u8> = r.get("contract");
        let contract: FuturesContract =
            rkyv::from_bytes(&bytes).map_err(|_e| anyhow::anyhow!("contracts decode failed"))?;
        Ok(Some(contract))
    } else {
        Ok(None)
    }
}
