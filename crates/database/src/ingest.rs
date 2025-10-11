use anyhow::Result;
use rust_decimal::prelude::ToPrimitive;
use sqlx::QueryBuilder;
use tt_types::data::models::TradeSide;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;

use crate::init::Connection;
use crate::schema::{get_or_create_instrument_id, upsert_latest_bar_1m, upsert_series_extent};
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::Topic;

/// Insert a batch of ticks with de-duplication by (provider, symbol_id, ts_ns, key_tie).
pub async fn ingest_ticks(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    rows: Vec<Tick>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let sym = instrument.to_string();
    let inst_id = get_or_create_instrument_id(conn, &sym).await?;
    let provider_code = crate::paths::provider_kind_to_db_string(provider);

    let mut qb = QueryBuilder::new(
        "INSERT INTO tick (ts_ns, provider, symbol_id, price, volume, side, venue_seq, key_tie, exec_id) ",
    );
    qb.push_values(rows.iter(), |mut b, r| {
        let ts_ns = r.time.timestamp_nanos_opt().unwrap_or(0);
        let side_i16: i16 = match r.side {
            TradeSide::Buy => 1,
            TradeSide::Sell => 2,
            TradeSide::None => 0,
        };
        let venue_seq_i64: Option<i64> = r.venue_seq.map(|v| v as i64);
        let key_tie: i64 = r.venue_seq.map(|v| v as i64).unwrap_or(0);
        b.push_bind(ts_ns)
            .push_bind(&provider_code)
            .push_bind(inst_id)
            .push_bind(r.price)
            .push_bind(r.volume)
            .push_bind(side_i16)
            .push_bind(venue_seq_i64)
            .push_bind(key_tie)
            .push_bind::<Option<&str>>(None);
    });
    qb.push(" ON CONFLICT (provider, symbol_id, ts_ns, key_tie) DO NOTHING ");
    let res = qb.build().execute(conn).await?;

    // Update extent cache with min/max tick times
    if let (Some(min_ts), Some(max_ts)) = (
        rows.iter().map(|r| r.time).min(),
        rows.iter().map(|r| r.time).max(),
    ) {
        upsert_series_extent(
            conn,
            &provider_code,
            inst_id,
            Topic::Ticks as i16,
            min_ts,
            max_ts,
        )
        .await?;
    }

    Ok(res.rows_affected())
}

/// Insert a batch of best bid/offer quotes with de-duplication.
pub async fn ingest_bbo(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    rows: Vec<Bbo>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let sym = instrument.to_string();
    let inst_id = get_or_create_instrument_id(conn, &sym).await?;
    let provider_code = crate::paths::provider_kind_to_db_string(provider);

    let mut qb = QueryBuilder::new(
        "INSERT INTO bbo (ts_ns, provider, symbol_id, bid, bid_size, ask, ask_size, bid_orders, ask_orders, venue_seq, is_snapshot, key_tie) ",
    );
    qb.push_values(rows.iter(), |mut b, r| {
        let ts_ns = r.time.timestamp_nanos_opt().unwrap_or(0);
        let bid_ct: Option<i32> = r.bid_orders.map(|v| v as i32);
        let ask_ct: Option<i32> = r.ask_orders.map(|v| v as i32);
        let venue_seq_i64: Option<i64> = r.venue_seq.map(|v| v as i64);
        let key_tie: i64 = venue_seq_i64.unwrap_or(0);
        let is_snapshot = r.is_snapshot.unwrap_or(false);
        b.push_bind(ts_ns)
            .push_bind(&provider_code)
            .push_bind(inst_id)
            .push_bind(r.bid)
            .push_bind(r.bid_size)
            .push_bind(r.ask)
            .push_bind(r.ask_size)
            .push_bind(bid_ct)
            .push_bind(ask_ct)
            .push_bind(venue_seq_i64)
            .push_bind(is_snapshot)
            .push_bind(key_tie);
    });
    qb.push(" ON CONFLICT (provider, symbol_id, ts_ns, key_tie) DO NOTHING ");
    let res = qb.build().execute(conn).await?;

    // Update extent cache with min/max quote times
    if let (Some(min_ts), Some(max_ts)) = (
        rows.iter().map(|r| r.time).min(),
        rows.iter().map(|r| r.time).max(),
    ) {
        upsert_series_extent(
            conn,
            &provider_code,
            inst_id,
            Topic::Quotes as i16,
            min_ts,
            max_ts,
        )
        .await?;
    }

    Ok(res.rows_affected())
}

/// Insert a batch of candles into bars_1m and update latest_bar_1m.
pub async fn ingest_candles(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    rows: Vec<Candle>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let sym = instrument.to_string();
    let inst_id = get_or_create_instrument_id(conn, &sym).await?;

    // Bulk insert bars mapping Candle fields exactly
    let provider_code = crate::paths::provider_kind_to_db_string(provider);
    let mut qb = QueryBuilder::new(
        "INSERT INTO bars (provider, symbol_id, time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume, resolution) ",
    );
    qb.push_values(rows.iter(), |mut b, r| {
        b.push_bind(&provider_code)
            .push_bind(inst_id)
            .push_bind(r.time_start)
            .push_bind(r.time_end)
            .push_bind(r.open)
            .push_bind(r.high)
            .push_bind(r.low)
            .push_bind(r.close)
            .push_bind(r.volume)
            .push_bind(r.ask_volume)
            .push_bind(r.bid_volume)
            .push_bind(r.resolution.to_string());
    });
    qb.push(" ON CONFLICT (provider, symbol_id, time_end, resolution) DO NOTHING ");
    let res = qb.build().execute(conn).await?;

    // Update extent cache per topic based on each row's resolution (batch may mix resolutions)
    use std::collections::HashMap;
    let mut extent_map: HashMap<
        i16,
        (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>),
    > = HashMap::new();
    for r in rows.iter() {
        let topic_i16: i16 = match r.resolution {
            tt_types::data::models::Resolution::Seconds(1) => Topic::Candles1s as i16,
            tt_types::data::models::Resolution::Minutes(1) => Topic::Candles1m as i16,
            tt_types::data::models::Resolution::Hours(_) => Topic::Candles1h as i16,
            tt_types::data::models::Resolution::Daily => Topic::Candles1d as i16,
            _ => Topic::Candles1m as i16,
        };
        let entry = extent_map
            .entry(topic_i16)
            .or_insert((r.time_end, r.time_end));
        if r.time_end < entry.0 {
            entry.0 = r.time_end;
        }
        if r.time_end > entry.1 {
            entry.1 = r.time_end;
        }
    }
    for (topic_i16, (min_end, max_end)) in extent_map.into_iter() {
        upsert_series_extent(conn, &provider_code, inst_id, topic_i16, min_end, max_end).await?;
    }

    // Update latest cache with the max time_end among ONLY 1m bars
    if let Some(last_1m) = rows
        .iter()
        .filter(|r| matches!(r.resolution, tt_types::data::models::Resolution::Minutes(1)))
        .max_by_key(|r| r.time_end)
    {
        upsert_latest_bar_1m(
            conn,
            &provider_code,
            inst_id,
            last_1m.time_start,
            last_1m.time_end,
            last_1m.open,
            last_1m.high,
            last_1m.low,
            last_1m.close,
            last_1m.volume,
            last_1m.ask_volume,
            last_1m.bid_volume,
            &last_1m.resolution.to_string(),
        )
        .await?;
    }

    Ok(res.rows_affected())
}

pub async fn ingest_mbp10(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    rows: Vec<Mbp10>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    let sym = instrument.to_string();
    let inst_id = get_or_create_instrument_id(conn, &sym).await?;
    let provider_code = crate::paths::provider_kind_to_db_string(provider);

    let mut qb = QueryBuilder::new(
        "INSERT INTO mbp10 (provider, symbol_id, ts_recv_ns, ts_event_ns, rtype, publisher_id, instrument_ref, action, side, depth, price, size, flags, ts_in_delta, sequence, book_bid_px, book_ask_px, book_bid_sz, book_ask_sz, book_bid_ct, book_ask_ct) ",
    );
    qb.push_values(rows.iter(), |mut b, r| {
        let ts_recv_ns = r.ts_recv.timestamp_nanos_opt().unwrap_or(0);
        let ts_event_ns = r.ts_event.timestamp_nanos_opt().unwrap_or(0);
        let action_i16: i16 = u8::from(r.action.clone()) as i16;
        let side_i16: i16 = u8::from(r.side) as i16;
        let flags_i16: i16 = u8::from(r.flags) as i16;
        #[allow(clippy::type_complexity)]
        let (bbpx, bapx, bbsz, basz, bbct, bact): (
            Option<Vec<_>>,
            Option<Vec<_>>,
            Option<Vec<_>>,
            Option<Vec<_>>,
            Option<Vec<i32>>,
            Option<Vec<i32>>,
        ) = match &r.book {
            Some(book) => (
                Some(book.bid_px.clone()),
                Some(book.ask_px.clone()),
                Some(book.bid_sz.clone()),
                Some(book.ask_sz.clone()),
                Some(
                    book.bid_ct
                        .iter()
                        .map(|d| d.to_i32().unwrap_or(0))
                        .collect(),
                ),
                Some(
                    book.ask_ct
                        .iter()
                        .map(|d| d.to_i32().unwrap_or(0))
                        .collect(),
                ),
            ),
            None => (None, None, None, None, None, None),
        };
        b.push_bind(&provider_code)
            .push_bind(inst_id)
            .push_bind(ts_recv_ns)
            .push_bind(ts_event_ns)
            .push_bind(r.rtype as i16)
            .push_bind(r.publisher_id as i32)
            .push_bind(r.instrument_id as i32)
            .push_bind(action_i16)
            .push_bind(side_i16)
            .push_bind(r.depth as i16)
            .push_bind(r.price)
            .push_bind(r.size)
            .push_bind(flags_i16)
            .push_bind(r.ts_in_delta)
            .push_bind(r.sequence as i64)
            .push_bind(bbpx)
            .push_bind(bapx)
            .push_bind(bbsz)
            .push_bind(basz)
            .push_bind(bbct)
            .push_bind(bact);
    });
    qb.push(" ON CONFLICT (provider, symbol_id, ts_event_ns, sequence) DO NOTHING ");
    let res = qb.build().execute(conn).await?;

    // Update extent cache with min/max ts_event
    if let (Some(min_ts), Some(max_ts)) = (
        rows.iter().map(|r| r.ts_event).min(),
        rows.iter().map(|r| r.ts_event).max(),
    ) {
        upsert_series_extent(
            conn,
            &provider_code,
            inst_id,
            Topic::MBP10 as i16,
            min_ts,
            max_ts,
        )
        .await?;
    }

    Ok(res.rows_affected())
}
