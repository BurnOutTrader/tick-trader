use duckdb::Connection;
use std::sync::Arc;
use crate::duck::{create_partitions_schema, prune_missing_partitions, quarantine_unreadable_partitions};

pub fn connect(db_file: &std::path::Path) -> anyhow::Result<duckdb::Connection> {
    let conn = duckdb::Connection::open(db_file)?;
    Ok(conn)
}

/// Initialize catalog.duckdb in the repo root
pub fn init_db() -> duckdb::Result<Connection> {
    let repo_root = std::env::current_dir().unwrap();
    let storage = repo_root.join("../storage").join("market_data");
    std::fs::create_dir_all(&storage).unwrap();
    let db_path = storage.join("catalog.duckdb");

    // Try to open the catalog; if the file exists but is not a valid DuckDB database, remove and recreate.
    let conn = match duckdb::Connection::open(&db_path) {
        Ok(c) => c,
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("not a valid DuckDB database file")
                || msg.contains("Failed to read database header")
            {
                tracing::warn!(path=%db_path.display().to_string(), "catalog file is invalid; removing DB and WAL, recreating a fresh DuckDB catalog");
                let _ = std::fs::remove_file(&db_path);
                let wal_path = db_path.with_extension("duckdb.wal");
                let _ = std::fs::remove_file(&wal_path);
                duckdb::Connection::open(&db_path)?
            } else {
                return Err(e);
            }
        }
    };
    // Initialize canonical schema (identity + partitions)
    create_identity_schema_if_needed(&conn)?;
    create_partitions_schema(&conn)?;
    // Clean stale catalog entries that point to missing parquet files
    let _ = prune_missing_partitions(&conn);
    // Quarantine unreadable/corrupt parquet paths from the catalog
    if let Ok(removed) = quarantine_unreadable_partitions(&conn)
        && removed > 0
    {
        tracing::warn!(removed, "catalog: quarantined unreadable partition entries");
    }
    Ok(conn)
}

pub fn create_identity_schema_if_needed(conn: &duckdb::Connection) -> duckdb::Result<()> {
    conn.execute_batch(
        r#"
        -- Use sequences for stable BIGINT surrogate keys (DuckDB idiom)
        CREATE SEQUENCE IF NOT EXISTS providers_seq START 1;
        CREATE SEQUENCE IF NOT EXISTS symbols_seq   START 1;
        CREATE SEQUENCE IF NOT EXISTS datasets_seq  START 1;

        -- Identity tables matching what upsert_* helpers expect
        CREATE TABLE IF NOT EXISTS providers (
          provider_id   BIGINT PRIMARY KEY DEFAULT nextval('providers_seq'),
          provider_code TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS symbols (
          symbol_id   BIGINT PRIMARY KEY DEFAULT nextval('symbols_seq'),
          provider_id BIGINT NOT NULL REFERENCES providers(provider_id),
          symbol_text TEXT  NOT NULL,
          UNIQUE(provider_id, symbol_text)
        );

        -- kind: 'tick' | 'bbo' | 'candle' | 'orderbook'
        -- include resolution_key to align with duck::upsert_dataset
        CREATE TABLE IF NOT EXISTS datasets (
          dataset_id     BIGINT PRIMARY KEY DEFAULT nextval('datasets_seq'),
          provider_id    BIGINT NOT NULL REFERENCES providers(provider_id),
          symbol_id      BIGINT NOT NULL REFERENCES symbols(symbol_id),
          kind           TEXT   NOT NULL,
          resolution     TEXT,
          resolution_key TEXT   NOT NULL DEFAULT '',
          UNIQUE(provider_id, symbol_id, kind, resolution_key)
        );

        -- Migration guard: add resolution_key if the table exists from legacy schema
        -- DuckDB limitation: cannot ADD COLUMN with constraints; do plain add then backfill.
        ALTER TABLE datasets ADD COLUMN IF NOT EXISTS resolution_key TEXT;
        UPDATE datasets SET resolution_key = '' WHERE resolution_key IS NULL;
    "#,
    )
}

pub fn init_catalog_duckdb(conn: &duckdb::Connection) -> duckdb::Result<()> {
    conn.execute_batch(
        r#"
        -- Sequences for surrogate keys (DuckDB way)
        CREATE SEQUENCE IF NOT EXISTS providers_seq START 1;
        CREATE SEQUENCE IF NOT EXISTS symbols_seq   START 1;
        CREATE SEQUENCE IF NOT EXISTS datasets_seq  START 1;
        CREATE SEQUENCE IF NOT EXISTS partitions_seq START 1;

        -- Identity tables
        CREATE TABLE IF NOT EXISTS providers (
          provider_id   BIGINT PRIMARY KEY DEFAULT nextval('providers_seq'),
          provider_code TEXT UNIQUE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS symbols (
          symbol_id   BIGINT PRIMARY KEY DEFAULT nextval('symbols_seq'),
          provider_id BIGINT NOT NULL REFERENCES providers(provider_id),
          symbol_text TEXT  NOT NULL,
          UNIQUE(provider_id, symbol_text)
        );

        -- kind: 'tick' | 'quote' | 'candle' | 'orderbook'
        -- resolution: NULL for non-candles; e.g. 'S1','M5','D','W','TBAR_500'
        CREATE TABLE IF NOT EXISTS datasets (
          dataset_id  BIGINT PRIMARY KEY DEFAULT nextval('datasets_seq'),
          provider_id BIGINT NOT NULL REFERENCES providers(provider_id),
          symbol_id   BIGINT NOT NULL REFERENCES symbols(symbol_id),
          kind        TEXT   NOT NULL,
          resolution  TEXT,
          UNIQUE(provider_id, symbol_id, kind, resolution)
        );

        -- Catalog of physical partitions (we store times as BIGINT = epoch millis)
        CREATE TABLE IF NOT EXISTS partitions (
          partition_id BIGINT PRIMARY KEY DEFAULT nextval('partitions_seq'),
          dataset_id   BIGINT NOT NULL REFERENCES datasets(dataset_id),
          path         TEXT   NOT NULL,   -- file path
          storage      TEXT   NOT NULL,   -- 'parquet' | 'duckdb'
          rows         BIGINT NOT NULL,
          bytes        BIGINT NOT NULL,
          min_ts       BIGINT NOT NULL,   -- epoch millis UTC
          max_ts       BIGINT NOT NULL,   -- epoch millis UTC
          min_seq      BIGINT,            -- optional
          max_seq      BIGINT,            -- optional
          day_key      DATE,              -- optional YYYY-MM-DD
          created_at   TIMESTAMP DEFAULT now(),
          UNIQUE(dataset_id, path)
        );

        -- Helpful indexes
        CREATE INDEX IF NOT EXISTS idx_symbols_provider_symbol
            ON symbols(provider_id, symbol_text);

        CREATE INDEX IF NOT EXISTS idx_datasets_key
            ON datasets(provider_id, symbol_id, kind, COALESCE(resolution,''));

        CREATE INDEX IF NOT EXISTS idx_partitions_dataset_time
            ON partitions(dataset_id, min_ts, max_ts);

        CREATE INDEX IF NOT EXISTS idx_partitions_dataset_day
            ON partitions(dataset_id, day_key);
        "#,
    )
}
