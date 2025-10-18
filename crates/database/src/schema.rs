use anyhow::Result;
use sqlx::{Executor, Pool, Postgres};

/// Ensure core tables needed for historical data and hot latest cache exist.
pub async fn ensure_schema(pool: &Pool<Postgres>) -> Result<()> {
    // Reference tables for instruments (symbol catalog). Providers are currently encoded in enums.
    // Use a simple instruments table keyed by unique symbol string.
    let create_instrument = r#"
    CREATE TABLE IF NOT EXISTS instrument (
        id   BIGSERIAL PRIMARY KEY,
        sym  TEXT NOT NULL UNIQUE
    );
    "#;

    // New per-resolution bar tables
    let create_bars_1s = r#"
    CREATE TABLE IF NOT EXISTS bars_1s (
        provider   TEXT         NOT NULL,
        symbol_id  BIGINT       NOT NULL REFERENCES instrument(id),
        time_start TIMESTAMPTZ  NOT NULL,
        time_end   TIMESTAMPTZ  NOT NULL,
        open       NUMERIC(18,9) NOT NULL,
        high       NUMERIC(18,9) NOT NULL,
        low        NUMERIC(18,9) NOT NULL,
        close      NUMERIC(18,9) NOT NULL,
        volume     NUMERIC(18,9) NOT NULL,
        ask_volume NUMERIC(18,9) NOT NULL,
        bid_volume NUMERIC(18,9) NOT NULL,
        PRIMARY KEY (provider, symbol_id, time_end)
    );
    CREATE INDEX IF NOT EXISTS ix_bars_1s_key ON bars_1s (provider, symbol_id, time_end);
    "#;

    let create_bars_1m = r#"
    CREATE TABLE IF NOT EXISTS bars_1m (
        provider   TEXT         NOT NULL,
        symbol_id  BIGINT       NOT NULL REFERENCES instrument(id),
        time_start TIMESTAMPTZ  NOT NULL,
        time_end   TIMESTAMPTZ  NOT NULL,
        open       NUMERIC(18,9) NOT NULL,
        high       NUMERIC(18,9) NOT NULL,
        low        NUMERIC(18,9) NOT NULL,
        close      NUMERIC(18,9) NOT NULL,
        volume     NUMERIC(18,9) NOT NULL,
        ask_volume NUMERIC(18,9) NOT NULL,
        bid_volume NUMERIC(18,9) NOT NULL,
        PRIMARY KEY (provider, symbol_id, time_end)
    );
    CREATE INDEX IF NOT EXISTS ix_bars_1m_key ON bars_1m (provider, symbol_id, time_end);
    "#;

    let create_bars_1h = r#"
    CREATE TABLE IF NOT EXISTS bars_1h (
        provider   TEXT         NOT NULL,
        symbol_id  BIGINT       NOT NULL REFERENCES instrument(id),
        time_start TIMESTAMPTZ  NOT NULL,
        time_end   TIMESTAMPTZ  NOT NULL,
        open       NUMERIC(18,9) NOT NULL,
        high       NUMERIC(18,9) NOT NULL,
        low        NUMERIC(18,9) NOT NULL,
        close      NUMERIC(18,9) NOT NULL,
        volume     NUMERIC(18,9) NOT NULL,
        ask_volume NUMERIC(18,9) NOT NULL,
        bid_volume NUMERIC(18,9) NOT NULL,
        PRIMARY KEY (provider, symbol_id, time_end)
    );
    CREATE INDEX IF NOT EXISTS ix_bars_1h_key ON bars_1h (provider, symbol_id, time_end);
    "#;

    let create_bars_1d = r#"
    CREATE TABLE IF NOT EXISTS bars_1d (
        provider   TEXT         NOT NULL,
        symbol_id  BIGINT       NOT NULL REFERENCES instrument(id),
        time_start TIMESTAMPTZ  NOT NULL,
        time_end   TIMESTAMPTZ  NOT NULL,
        open       NUMERIC(18,9) NOT NULL,
        high       NUMERIC(18,9) NOT NULL,
        low        NUMERIC(18,9) NOT NULL,
        close      NUMERIC(18,9) NOT NULL,
        volume     NUMERIC(18,9) NOT NULL,
        ask_volume NUMERIC(18,9) NOT NULL,
        bid_volume NUMERIC(18,9) NOT NULL,
        PRIMARY KEY (provider, symbol_id, time_end)
    );
    CREATE INDEX IF NOT EXISTS ix_bars_1d_key ON bars_1d (provider, symbol_id, time_end);
    "#;

    // Hot cache: one row per (provider, symbol) with the latest 1m bar, mirroring Candle fields.
    let create_latest = r#"
    CREATE TABLE IF NOT EXISTS latest_bar_1m (
        provider   TEXT         NOT NULL,
        symbol_id  BIGINT       NOT NULL REFERENCES instrument(id),
        time_start TIMESTAMPTZ  NOT NULL,
        time_end   TIMESTAMPTZ  NOT NULL,
        open       NUMERIC(18,9) NOT NULL,
        high       NUMERIC(18,9) NOT NULL,
        low        NUMERIC(18,9) NOT NULL,
        close      NUMERIC(18,9) NOT NULL,
        volume     NUMERIC(18,9) NOT NULL,
        ask_volume NUMERIC(18,9) NOT NULL,
        bid_volume NUMERIC(18,9) NOT NULL,
        resolution TEXT          NOT NULL,
        PRIMARY KEY (provider, symbol_id)
    );
    "#;

    // Ticks: de-dup via nanosecond timestamp and sequence tiebreaker.
    let create_ticks = r#"
    CREATE TABLE IF NOT EXISTS tick (
        ts_ns      BIGINT       NOT NULL, -- UTC nanos for partition/keyset (Tick::time)
        provider   TEXT         NOT NULL, -- provider code string
        symbol_id  BIGINT       NOT NULL REFERENCES instrument(id),
        price      NUMERIC(18,9) NOT NULL,
        volume     NUMERIC(18,9) NOT NULL,
        side       SMALLINT     NOT NULL,
        venue_seq  BIGINT,
        key_tie    BIGINT       NOT NULL DEFAULT 0,
        exec_id    TEXT,
        PRIMARY KEY (provider, symbol_id, ts_ns, key_tie)
    );
    CREATE INDEX IF NOT EXISTS ix_tick_key ON tick (provider, symbol_id, ts_ns);
    CREATE INDEX IF NOT EXISTS brin_tick_ts ON tick USING brin (ts_ns);
    "#;

    // Quotes (BBO): de-dup similarly; allow snapshots/updates.
    let create_bbo = r#"
    CREATE TABLE IF NOT EXISTS bbo (
        ts_ns      BIGINT       NOT NULL,
        provider   TEXT         NOT NULL,
        symbol_id  BIGINT       NOT NULL REFERENCES instrument(id),
        bid        NUMERIC(18,9),
        bid_size   NUMERIC(18,9),
        ask        NUMERIC(18,9),
        ask_size   NUMERIC(18,9),
        bid_orders INTEGER,
        ask_orders INTEGER,
        venue_seq  BIGINT,
        is_snapshot BOOLEAN NOT NULL DEFAULT false,
        key_tie    BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (provider, symbol_id, ts_ns, key_tie)
    );
    CREATE INDEX IF NOT EXISTS ix_bbo_key ON bbo (provider, symbol_id, ts_ns);
    CREATE INDEX IF NOT EXISTS brin_bbo_ts ON bbo USING brin (ts_ns);
    "#;

    // MBP10 events: structured columns mirroring tt-types::data::mbp10::Mbp10
    let create_mbp10 = r#"
    CREATE TABLE IF NOT EXISTS mbp10 (
        provider      TEXT         NOT NULL,
        symbol_id     BIGINT       NOT NULL REFERENCES instrument(id),
        ts_recv_ns    BIGINT       NOT NULL,
        ts_event_ns   BIGINT       NOT NULL,
        rtype         SMALLINT     NOT NULL,
        publisher_id  INTEGER      NOT NULL,
        instrument_ref INTEGER     NOT NULL,
        action        SMALLINT     NOT NULL,
        side          SMALLINT     NOT NULL,
        depth         SMALLINT     NOT NULL,
        price         NUMERIC(18,9) NOT NULL,
        size          NUMERIC(18,9) NOT NULL,
        flags         SMALLINT     NOT NULL,
        ts_in_delta   INTEGER      NOT NULL,
        sequence      BIGINT       NOT NULL,
        -- Optional aggregated book levels (arrays)
        book_bid_px   NUMERIC(18,9)[],
        book_ask_px   NUMERIC(18,9)[],
        book_bid_sz   NUMERIC(18,9)[],
        book_ask_sz   NUMERIC(18,9)[],
        book_bid_ct   INTEGER[],
        book_ask_ct   INTEGER[],
        PRIMARY KEY (provider, symbol_id, ts_event_ns, sequence)
    );
    CREATE INDEX IF NOT EXISTS ix_mbp10_ts ON mbp10 (provider, symbol_id, ts_event_ns);
    "#;

    // Series extents cache for fast earliest/latest queries.
    let create_series_extent = r#"
    CREATE TABLE IF NOT EXISTS series_extent (
        provider   TEXT   NOT NULL,
        symbol_id  BIGINT NOT NULL REFERENCES instrument(id),
        topic      SMALLINT NOT NULL,
        earliest   TIMESTAMPTZ,
        latest     TIMESTAMPTZ,
        PRIMARY KEY (provider, symbol_id, topic)
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_series_extent ON series_extent(provider, symbol_id, topic);
    "#;

    // Future metrics/KVP table for strategies.
    let create_kvp = r#"
    CREATE TABLE IF NOT EXISTS kvp (
        ns         TEXT NOT NULL,   -- namespace, e.g., strategy name
        key        TEXT NOT NULL,
        ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        value      JSONB NOT NULL,
        PRIMARY KEY (ns, key)
    );
    "#;

    pool.execute(create_instrument).await?;
    pool.execute(create_bars_1s).await?;
    pool.execute(create_bars_1m).await?;
    pool.execute(create_bars_1h).await?;
    pool.execute(create_bars_1d).await?;

    pool.execute(create_latest).await?;
    pool.execute(create_ticks).await?;
    pool.execute(create_bbo).await?;
    pool.execute(create_mbp10).await?;
    pool.execute(create_series_extent).await?;
    pool.execute(create_kvp).await?;

    // Futures contracts table storing serialized contract per (provider, symbol)
    let create_contracts = r#"
    CREATE TABLE IF NOT EXISTS futures_contracts (
        provider   TEXT   NOT NULL,
        symbol_id  BIGINT NOT NULL REFERENCES instrument(id),
        contract   BYTEA  NOT NULL,
        PRIMARY KEY (provider, symbol_id)
    );
    "#;
    pool.execute(create_contracts).await?;

    Ok(())
}

/// Get or create an instrument id by its symbol string.
pub async fn get_or_create_instrument_id(pool: &Pool<Postgres>, sym: &str) -> Result<i64> {
    use sqlx::Row;
    let rec = sqlx::query(
        r#"INSERT INTO instrument(sym)
           VALUES ($1)
           ON CONFLICT(sym) DO UPDATE SET sym = EXCLUDED.sym
           RETURNING id"#,
    )
    .bind(sym)
    .fetch_one(pool)
    .await?;
    Ok(rec.get::<i64, _>("id"))
}

#[allow(clippy::too_many_arguments)]
/// Upsert the latest bar for a symbol into latest_bar_1m.
pub async fn upsert_latest_bar_1m(
    pool: &Pool<Postgres>,
    provider: &str,
    symbol_id: i64,
    time_start: chrono::DateTime<chrono::Utc>,
    time_end: chrono::DateTime<chrono::Utc>,
    open: rust_decimal::Decimal,
    high: rust_decimal::Decimal,
    low: rust_decimal::Decimal,
    close: rust_decimal::Decimal,
    volume: rust_decimal::Decimal,
    ask_volume: rust_decimal::Decimal,
    bid_volume: rust_decimal::Decimal,
    resolution: &str,
) -> Result<()> {
    // Only update if the new time_end is >= existing time_end to avoid regressions.
    let sql = r#"
    INSERT INTO latest_bar_1m(provider, symbol_id, time_start, time_end, open, high, low, close, volume, ask_volume, bid_volume, resolution)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    ON CONFLICT (provider, symbol_id)
    DO UPDATE SET
        time_start = EXCLUDED.time_start,
        time_end = EXCLUDED.time_end,
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        ask_volume = EXCLUDED.ask_volume,
        bid_volume = EXCLUDED.bid_volume,
        resolution = EXCLUDED.resolution
    WHERE latest_bar_1m.time_end <= EXCLUDED.time_end;
    "#;
    sqlx::query(sql)
        .bind(provider)
        .bind(symbol_id)
        .bind(time_start)
        .bind(time_end)
        .bind(open)
        .bind(high)
        .bind(low)
        .bind(close)
        .bind(volume)
        .bind(ask_volume)
        .bind(bid_volume)
        .bind(resolution)
        .execute(pool)
        .await?;
    Ok(())
}

/// Upsert the earliest/latest extent for a (provider,symbol,topic) using LEAST/GREATEST logic.
pub async fn upsert_series_extent(
    pool: &Pool<Postgres>,
    provider: &str,
    symbol_id: i64,
    topic: i16,
    earliest: chrono::DateTime<chrono::Utc>,
    latest: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    let sql = r#"
    INSERT INTO series_extent(provider, symbol_id, topic, earliest, latest)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT(provider, symbol_id, topic)
    DO UPDATE SET
        earliest = LEAST(series_extent.earliest, EXCLUDED.earliest),
        latest   = GREATEST(series_extent.latest, EXCLUDED.latest);
    "#;
    sqlx::query(sql)
        .bind(provider)
        .bind(symbol_id)
        .bind(topic as i32)
        .bind(earliest)
        .bind(latest)
        .execute(pool)
        .await?;
    Ok(())
}
