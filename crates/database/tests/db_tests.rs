use anyhow::Result;
use chrono::{Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;
use tt_database::ingest::{ingest_bbo, ingest_candles, ingest_mbp10, ingest_ticks};
use tt_database::init::pool_from_env;
use tt_database::paths::provider_kind_to_db_string;
use tt_database::queries;
use tt_database::schema::{
    ensure_schema, get_or_create_instrument_id, upsert_latest_bar_1m, upsert_series_extent,
};
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::{BookLevels, Mbp10};
use tt_types::data::models::{Resolution, TradeSide};
use tt_types::keys::Topic;
use tt_types::providers::{ProjectXTenant, ProviderKind, RithmicSystem};
use tt_types::securities::symbols::Instrument;

// Helper: return early if DATABASE_URL is not set, to avoid hard dependency in CI without DB.
fn require_db() -> Option<()> {
    std::env::var("DATABASE_URL").ok()?;
    Some(())
}

async fn setup() -> Result<(sqlx::Pool<sqlx::Postgres>, ProviderKind, Instrument)> {
    let pool = pool_from_env()?;
    // fixed provider maps to "projectx" so tests can clean by provider + symbol
    let provider = ProviderKind::ProjectX(ProjectXTenant::Demo);
    let instrument = Instrument::from_str("TST.Z25").unwrap();
    // clean any leftovers for our symbol
    let sym_id = get_or_create_instrument_id(&pool, &instrument.to_string()).await?;
    let prov = provider_kind_to_db_string(provider);
    // delete existing rows in dependent tables (order matters due to FKs)
    sqlx::query("DELETE FROM latest_bar_1m WHERE provider=$1 AND symbol_id=$2")
        .bind(&prov)
        .bind(sym_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM bars_1m WHERE provider=$1 AND symbol_id=$2")
        .bind(&prov)
        .bind(sym_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM tick WHERE provider=$1 AND symbol_id=$2")
        .bind(&prov)
        .bind(sym_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM bbo WHERE provider=$1 AND symbol_id=$2")
        .bind(&prov)
        .bind(sym_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM mbp10 WHERE provider=$1 AND symbol_id=$2")
        .bind(&prov)
        .bind(sym_id)
        .execute(&pool)
        .await?;
    sqlx::query("DELETE FROM series_extent WHERE provider=$1 AND symbol_id=$2")
        .bind(&prov)
        .bind(sym_id)
        .execute(&pool)
        .await?;
    Ok((pool, provider, instrument))
}

#[tokio::test]
async fn test_paths_provider_kind_to_db_string() -> Result<()> {
    if require_db().is_none() {
        return Ok(());
    }
    assert_eq!(
        provider_kind_to_db_string(ProviderKind::ProjectX(ProjectXTenant::Demo)),
        "projectx"
    );
    assert_eq!(
        provider_kind_to_db_string(ProviderKind::Rithmic(RithmicSystem::Test)),
        "rithmic"
    );
    Ok(())
}

#[tokio::test]
async fn test_schema_ensure_and_instrument() -> Result<()> {
    if require_db().is_none() {
        return Ok(());
    }
    let (pool, _prov, inst) = setup().await?;
    ensure_schema(&pool).await?; // idempotent
    let id1 = get_or_create_instrument_id(&pool, &inst.to_string()).await?;
    let id2 = get_or_create_instrument_id(&pool, &inst.to_string()).await?;
    assert_eq!(id1, id2);
    Ok(())
}

#[tokio::test]
async fn test_upsert_latest_bar_and_query_latest() -> Result<()> {
    if require_db().is_none() {
        return Ok(());
    }
    let (pool, provider, inst) = setup().await?;
    let sym_id = get_or_create_instrument_id(&pool, &inst.to_string()).await?;
    let prov = provider_kind_to_db_string(provider);
    let t0 = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let bar1 = Candle {
        symbol: inst.to_string(),
        instrument: inst.clone(),
        time_start: t0,
        time_end: t0 + Duration::minutes(1),
        open: Decimal::from(10),
        high: Decimal::from(11),
        low: Decimal::from(9),
        close: Decimal::from(10),
        volume: Decimal::from(100),
        ask_volume: Decimal::from(60),
        bid_volume: Decimal::from(40),
        resolution: Resolution::Minutes(1),
    };
    // Insert into base and latest
    ingest_candles(&pool, provider, &inst, vec![bar1.clone()]).await?;
    // Upsert latest manually with newer ts
    let bar2_end = bar1.time_end + Duration::minutes(1);
    upsert_latest_bar_1m(
        &pool,
        &prov,
        sym_id,
        bar1.time_start + Duration::minutes(1),
        bar2_end,
        Decimal::from(10),
        Decimal::from(12),
        Decimal::from(9),
        Decimal::from(11),
        Decimal::from(110),
        Decimal::from(65),
        Decimal::from(45),
        &bar1.resolution.to_string(),
    )
    .await?;
    // Query latest time via queries::latest_data_time
    let latest_ts = queries::latest_data_time(&pool, provider, &inst, Topic::Candles1m).await?;
    assert_eq!(latest_ts, Some(bar2_end));
    // Fetch latest bars for a list
    let latest = queries::latest_bars_1m(&pool, provider, std::slice::from_ref(&inst)).await?;
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].time_end, bar2_end);
    Ok(())
}

#[tokio::test]
async fn test_ingest_and_get_range_multi_res_candles() -> Result<()> {
    if require_db().is_none() {
        return Ok(());
    }
    let (pool, provider, inst) = setup().await?;
    let t0 = Utc.with_ymd_and_hms(2024, 3, 1, 9, 30, 0).unwrap();

    // 1-second candles
    let mut sec_bars = Vec::new();
    for i in 0..3 {
        let start = t0 + Duration::seconds(i);
        let end = start + Duration::seconds(1);
        sec_bars.push(Candle {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            time_start: start,
            time_end: end,
            open: Decimal::from(10 + i),
            high: Decimal::from(11 + i),
            low: Decimal::from(9 + i),
            close: Decimal::from(10 + i),
            volume: Decimal::from(100 + i),
            ask_volume: Decimal::from(60 + i),
            bid_volume: Decimal::from(40 + i),
            resolution: Resolution::Seconds(1),
        });
    }
    ingest_candles(&pool, provider, &inst, sec_bars.clone()).await?;
    let (resp_sec, _next_sec) = queries::get_range(
        &pool,
        provider,
        &inst,
        Topic::Candles1s,
        t0,
        t0 + Duration::seconds(10),
        10,
    )
    .await?;
    match &resp_sec[0] {
        tt_types::wire::Response::BarBatch(b) => assert_eq!(b.bars.len(), 3),
        _ => panic!("expected BarBatch"),
    }

    // 10-hour candles (fall under Candles1h family)
    let mut hr_bars = Vec::new();
    for i in 0..2 {
        let start = t0 + Duration::hours(i as i64 * 10);
        let end = start + Duration::hours(10);
        hr_bars.push(Candle {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            time_start: start,
            time_end: end,
            open: Decimal::from(200 + i),
            high: Decimal::from(201 + i),
            low: Decimal::from(199 + i),
            close: Decimal::from(200 + i),
            volume: Decimal::from(2000 + i),
            ask_volume: Decimal::from(1200 + i),
            bid_volume: Decimal::from(800 + i),
            resolution: Resolution::Hours(10),
        });
    }
    ingest_candles(&pool, provider, &inst, hr_bars.clone()).await?;
    let (resp_hr, _next_hr) = queries::get_range(
        &pool,
        provider,
        &inst,
        Topic::Candles1h,
        t0,
        t0 + Duration::days(1),
        10,
    )
    .await?;
    match &resp_hr[0] {
        tt_types::wire::Response::BarBatch(b) => assert_eq!(b.bars.len(), 2),
        _ => panic!("expected BarBatch"),
    }

    // Daily candles
    let mut day_bars = Vec::new();
    for i in 0..2 {
        let start = t0 + Duration::days(i);
        let end = start + Duration::days(1);
        day_bars.push(Candle {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            time_start: start,
            time_end: end,
            open: Decimal::from(300 + i),
            high: Decimal::from(301 + i),
            low: Decimal::from(299 + i),
            close: Decimal::from(300 + i),
            volume: Decimal::from(3000 + i),
            ask_volume: Decimal::from(1800 + i),
            bid_volume: Decimal::from(1200 + i),
            resolution: Resolution::Daily,
        });
    }
    ingest_candles(&pool, provider, &inst, day_bars.clone()).await?;
    let (resp_day, _next_day) = queries::get_range(
        &pool,
        provider,
        &inst,
        Topic::Candles1d,
        t0,
        t0 + Duration::days(3),
        10,
    )
    .await?;
    match &resp_day[0] {
        tt_types::wire::Response::BarBatch(b) => assert_eq!(b.bars.len(), 2),
        _ => panic!("expected BarBatch"),
    }

    // latest_data_time should return max per-topic
    let latest_1s = queries::latest_data_time(&pool, provider, &inst, Topic::Candles1s).await?;
    assert!(latest_1s.is_some());
    let latest_1h = queries::latest_data_time(&pool, provider, &inst, Topic::Candles1h).await?;
    assert!(latest_1h.is_some());
    let latest_1d = queries::latest_data_time(&pool, provider, &inst, Topic::Candles1d).await?;
    assert!(latest_1d.is_some());

    Ok(())
}

#[tokio::test]
async fn test_ingest_and_get_range_bars_ticks_quotes() -> Result<()> {
    if require_db().is_none() {
        return Ok(());
    }
    let (pool, provider, inst) = setup().await?;
    let t0 = Utc.with_ymd_and_hms(2024, 2, 1, 9, 30, 0).unwrap();
    // Bars
    let mut bars = Vec::new();
    for i in 0..5 {
        // 5 minutes
        let start = t0 + Duration::minutes(i);
        let end = start + Duration::minutes(1);
        bars.push(Candle {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            time_start: start,
            time_end: end,
            open: Decimal::from(100 + i),
            high: Decimal::from(101 + i),
            low: Decimal::from(99 + i),
            close: Decimal::from(100 + i),
            volume: Decimal::from(1000 + i),
            ask_volume: Decimal::from(600 + i),
            bid_volume: Decimal::from(400 + i),
            resolution: Resolution::Minutes(1),
        });
    }
    ingest_candles(&pool, provider, &inst, bars.clone()).await?;
    // Range bars
    let (resp, next) = queries::get_range(
        &pool,
        provider,
        &inst,
        Topic::Candles1m,
        t0,
        t0 + Duration::minutes(10),
        10,
    )
    .await?;
    assert!(next.is_some());
    assert_eq!(resp.len(), 1);
    match &resp[0] {
        tt_types::wire::Response::BarBatch(b) => assert_eq!(b.bars.len(), 5),
        _ => panic!("expected BarBatch"),
    }

    // Ticks
    let mut ticks = Vec::new();
    for i in 0..10 {
        // 10 ticks spaced 100ns
        let ts = t0 + Duration::nanoseconds(i * 100);
        ticks.push(Tick {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            price: Decimal::from(3000),
            volume: Decimal::from(1),
            time: ts,
            side: TradeSide::Buy,
            venue_seq: Some(i as u32),
        });
    }
    let rows_inserted = ingest_ticks(&pool, provider, &inst, ticks.clone()).await?;
    assert!(rows_inserted >= 10);
    let (resp_t, next_t) = queries::get_range(
        &pool,
        provider,
        &inst,
        Topic::Ticks,
        t0,
        t0 + Duration::seconds(1),
        100,
    )
    .await?;
    assert!(next_t.is_some());
    match &resp_t[0] {
        tt_types::wire::Response::TickBatch(b) => assert_eq!(b.ticks.len(), 10),
        _ => panic!("expected TickBatch"),
    }

    // Quotes
    let mut quotes = Vec::new();
    for i in 0..8 {
        // 8 quotes spaced 200ns
        let ts = t0 + Duration::nanoseconds(i * 200);
        quotes.push(Bbo {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            bid: Decimal::from(2999),
            bid_size: Decimal::from(1),
            ask: Decimal::from(3001),
            ask_size: Decimal::from(1),
            time: ts,
            bid_orders: Some(1),
            ask_orders: Some(1),
            venue_seq: Some(i as u32),
            is_snapshot: Some(false),
        });
    }
    let rows_q = ingest_bbo(&pool, provider, &inst, quotes.clone()).await?;
    assert!(rows_q >= 8);
    let (resp_q, next_q) = queries::get_range(
        &pool,
        provider,
        &inst,
        Topic::Quotes,
        t0,
        t0 + Duration::seconds(1),
        100,
    )
    .await?;
    assert!(next_q.is_some());
    match &resp_q[0] {
        tt_types::wire::Response::QuoteBatch(b) => assert_eq!(b.quotes.len(), 8),
        _ => panic!("expected QuoteBatch"),
    }

    Ok(())
}

#[tokio::test]
async fn test_series_extent_and_latest_time() -> Result<()> {
    if require_db().is_none() {
        return Ok(());
    }
    let (pool, provider, inst) = setup().await?;
    let t0 = Utc.with_ymd_and_hms(2023, 12, 31, 23, 59, 0).unwrap();
    // Ingest a couple of ticks and quotes
    let ticks = vec![
        Tick {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            price: Decimal::from(1),
            volume: Decimal::from(1),
            time: t0,
            side: TradeSide::Buy,
            venue_seq: Some(1),
        },
        Tick {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            price: Decimal::from(2),
            volume: Decimal::from(1),
            time: t0 + Duration::seconds(1),
            side: TradeSide::Sell,
            venue_seq: Some(2),
        },
    ];
    ingest_ticks(&pool, provider, &inst, ticks).await?;

    let quotes = vec![
        Bbo {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            bid: Decimal::from(1),
            bid_size: Decimal::from(1),
            ask: Decimal::from(2),
            ask_size: Decimal::from(1),
            time: t0,
            bid_orders: Some(1),
            ask_orders: Some(1),
            venue_seq: Some(1),
            is_snapshot: Some(false),
        },
        Bbo {
            symbol: inst.to_string(),
            instrument: inst.clone(),
            bid: Decimal::from(1),
            bid_size: Decimal::from(1),
            ask: Decimal::from(2),
            ask_size: Decimal::from(1),
            time: t0 + Duration::seconds(2),
            bid_orders: Some(1),
            ask_orders: Some(1),
            venue_seq: Some(2),
            is_snapshot: Some(false),
        },
    ];
    ingest_bbo(&pool, provider, &inst, quotes).await?;

    // get_extent should return min/max
    let (e_tick, l_tick) = queries::get_extent(&pool, provider, &inst, Topic::Ticks).await?;
    assert!(e_tick.is_some() && l_tick.is_some() && l_tick >= e_tick);
    let (e_q, l_q) = queries::get_extent(&pool, provider, &inst, Topic::Quotes).await?;
    assert!(e_q.is_some() && l_q.is_some() && l_q >= e_q);

    let lt_tick = queries::latest_data_time(&pool, provider, &inst, Topic::Ticks).await?;
    assert_eq!(lt_tick, l_tick);
    let lt_q = queries::latest_data_time(&pool, provider, &inst, Topic::Quotes).await?;
    assert_eq!(lt_q, l_q);

    // And manual upsert_series_extent should widen window
    let widened = l_q.unwrap() + Duration::seconds(5);
    let id = get_or_create_instrument_id(&pool, &inst.to_string()).await?;
    upsert_series_extent(
        &pool,
        &provider_kind_to_db_string(provider),
        id,
        Topic::Quotes as i16,
        e_q.unwrap(),
        widened,
    )
    .await?;
    let (_e2, l2) = queries::get_extent(&pool, provider, &inst, Topic::Quotes).await?;
    assert_eq!(l2, Some(widened));

    Ok(())
}

#[tokio::test]
async fn test_ingest_mbp10_and_extent() -> Result<()> {
    if require_db().is_none() {
        return Ok(());
    }
    let (pool, provider, inst) = setup().await?;
    let t0 = Utc.with_ymd_and_hms(2024, 5, 1, 12, 0, 0).unwrap();
    let rec = Mbp10 {
        instrument: inst.clone(),
        ts_recv: t0,
        ts_event: t0 + Duration::nanoseconds(10),
        rtype: 10,
        publisher_id: 1,
        instrument_id: 42,
        action: tt_types::data::mbp10::Action::Add,
        side: tt_types::data::mbp10::BookSide::Bid,
        depth: 0,
        price: Decimal::from(100),
        size: Decimal::from(1),
        flags: tt_types::data::mbp10::Flags(0),
        ts_in_delta: 0,
        sequence: 1,
        book: Some(BookLevels {
            bid_px: vec![Decimal::from(100)],
            ask_px: vec![Decimal::from(101)],
            bid_sz: vec![Decimal::from(1)],
            ask_sz: vec![Decimal::from(1)],
            bid_ct: vec![Decimal::from(1)],
            ask_ct: vec![Decimal::from(1)],
        }),
    };
    let rows = ingest_mbp10(&pool, provider, &inst, vec![rec]).await?;
    assert_eq!(rows, 1);
    // Extent should be populated for MBP10
    let (e, l) = queries::get_extent(&pool, provider, &inst, Topic::MBP10).await?;
    assert!(e.is_some() && l.is_some());
    Ok(())
}

#[tokio::test]
async fn test_get_symbols_lists_instrument() -> Result<()> {
    if require_db().is_none() {
        return Ok(());
    }
    let (pool, provider, inst) = setup().await?;
    // Ensure something exists: a bar
    let t0 = Utc.with_ymd_and_hms(2024, 6, 1, 0, 0, 0).unwrap();
    let bar = Candle {
        symbol: inst.to_string(),
        instrument: inst.clone(),
        time_start: t0,
        time_end: t0 + Duration::minutes(1),
        open: Decimal::from(1),
        high: Decimal::from(2),
        low: Decimal::from(1),
        close: Decimal::from(2),
        volume: Decimal::from(10),
        ask_volume: Decimal::from(6),
        bid_volume: Decimal::from(4),
        resolution: Resolution::Minutes(1),
    };
    ingest_candles(&pool, provider, &inst, vec![bar]).await?;
    let syms = queries::get_symbols(&pool, provider, None).await?;
    assert!(syms.iter().any(|s| s.to_string() == inst.to_string()));
    Ok(())
}
