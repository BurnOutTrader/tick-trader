//! DuckDB catalog and query helpers.
//!
//! This module owns the in-DB catalog schema (datasets, partitions) and provides helpers to:
//! - Upsert providers/symbols/datasets and monthly partitions with stats.
//! - Discover earliest/latest availability across partitions without reading payloads.
//! - Prune catalog rows that reference missing files and quarantine unreadable Parquet.
//! - Resolve dataset IDs from human-friendly keys and bridge Topic -> resolution keys.

use crate::models::SeqBound;
use crate::paths::{provider_kind_to_db_string, topic_to_db_string};
use anyhow::Result;
use chrono::{DateTime, Utc};
use duckdb::{Connection, Error, OptionalExt, params};
use std::path::Path;
use thiserror::Error;
use tracing::error;
use tt_types::data::models::Resolution;
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;

// --- Helpers bridging legacy dataset keys to new Topic layout ---
fn topic_to_kind_key(topic: Topic) -> String {
    // Reuse canonical mapping from paths.rs
    topic_to_db_string(topic)
}

fn topic_to_resolution(topic: Topic) -> Option<Resolution> {
    match topic {
        Topic::Candles1s => Some(Resolution::Seconds(1)),
        Topic::Candles1m => Some(Resolution::Minutes(1)),
        Topic::Candles1h => Some(Resolution::Hours(1)),
        Topic::Candles1d => Some(Resolution::Daily),
        _ => None,
    }
}

fn resolution_key(res: Option<Resolution>) -> String {
    // Use empty string for non-candle datasets to match ensure_dataset_row inserts
    res.map(|r| r.to_os_string())
        .unwrap_or_else(|| "".to_string())
}

/// Create or reuse a DuckDB connection (file-backed or in-memory).
pub fn connect(db_file: Option<&Path>) -> anyhow::Result<Connection> {
    let conn = match db_file {
        Some(p) => Connection::open(p)?,
        None => Connection::open_in_memory()?,
    };
    // Sensible defaults for Parquet discovery & pushdown:
    // (DuckDB enables most pushdowns by default; hive partitioning is per-scan arg below.)
    Ok(conn)
}

#[derive(Debug, Error)]
pub enum DuckError {
    #[error("duckdb: {0}")]
    Duck(#[from] duckdb::Error),
}

pub struct Duck {
    pub(crate) conn: Connection,
}
impl Duck {
    /// `db_path` is a small persistent catalog DB (tables: providers, symbols, universes).
    /// Parquet lives under your FS lake; DuckDB queries it in-place with pushdown.
    pub fn open(db_path: &Path) -> Result<Self, DuckError> {
        let conn = Connection::open(db_path)?;
        let this = Self { conn };
        this.init()?;
        Ok(this)
    }

    fn init(&self) -> Result<(), DuckError> {
        // Align Duck::init with the canonical schema initializers used elsewhere in the crate.
        // We intentionally delegate to init.rs and to create_partitions_schema here to avoid
        // schema drift between different entrypoints.
        crate::init::create_identity_schema_if_needed(&self.conn)?;
        create_partitions_schema(&self.conn)?;
        Ok(())
    }

    pub fn upsert_provider(conn: &duckdb::Connection, provider_code: &str) -> anyhow::Result<i64> {
        conn.execute(
            "INSERT OR IGNORE INTO providers(provider_code) VALUES (?)",
            duckdb::params![provider_code],
        )?;
        let mut q = conn.prepare("SELECT provider_id FROM providers WHERE provider_code = ?")?;
        Ok(q.query_row(duckdb::params![provider_code], |r| r.get(0))?)
    }

    pub fn upsert_symbol(
        conn: &duckdb::Connection,
        provider_id: i64,
        symbol: &str,
    ) -> anyhow::Result<i64> {
        conn.execute(
            "INSERT OR IGNORE INTO symbols(provider_id, symbol_text) VALUES (?, ?)",
            duckdb::params![provider_id, symbol],
        )?;
        let mut q = conn
            .prepare("SELECT symbol_id FROM symbols WHERE provider_id = ? AND symbol_text = ?")?;
        Ok(q.query_row(duckdb::params![provider_id, symbol], |r| r.get(0))?)
    }

    pub fn add_universe_member(
        &self,
        universe: &str,
        symbol_id: &str,
        provider: &str,
    ) -> Result<(), DuckError> {
        self.conn.execute(
            "INSERT OR IGNORE INTO universes(universe, symbol_id, provider) VALUES(?,?,?)",
            params![universe, symbol_id, provider],
        )?;
        Ok(())
    }

    /// Register a view that scans parquet files with pushdown filters.
    /// Example: create_ticks_view("ticks_all", "/data/parquet/ticks")
    pub fn create_ticks_view(&self, view_name: &str, ticks_root: &Path) -> Result<(), DuckError> {
        let root = ticks_root.to_string_lossy();
        // Partition-aware wildcard; columns must match parquet schema
        self.conn.execute_batch(&format!(
            r#"
            CREATE OR REPLACE VIEW {view} AS
            SELECT * FROM parquet_scan('{root}/**/*.parquet',
              hive_partitioning = 0
            );
            -- For common filters ensure sorted by key_ts_utc_us,key_tie if needed in result
        "#,
            view = view_name,
            root = root
        ))?;
        Ok(())
    }

    pub fn create_view(&self, view_name: &str, _topic: Topic) -> Result<(), DuckError> {
        // Legacy helper: we no longer construct filesystem-glob views here.
        // Create an empty view placeholder to keep API compatibility.
        self.conn.execute_batch(&format!(
            r#"
            CREATE OR REPLACE VIEW {view} AS SELECT 1 WHERE 1=0;
        "#,
            view = view_name,
        ))?;
        Ok(())
    }
}

/// Resolve or create provider_id
pub fn upsert_provider(conn: &Connection, provider_code: &str) -> Result<i64> {
    conn.execute(
        "insert or ignore into providers(provider_code) values (?)",
        params![provider_code],
    )?;
    let mut stmt = conn.prepare("select provider_id from providers where provider_code = ?")?;
    let id: i64 = stmt.query_row(params![provider_code], |r| r.get(0))?;
    Ok(id)
}

/// Resolve or create symbol_id for a provider
pub fn upsert_symbol(conn: &Connection, provider_id: i64, symbol: &str) -> Result<i64> {
    conn.execute(
        "insert or ignore into symbols(provider_id, symbol_text) values (?, ?)",
        params![provider_id, symbol],
    )?;
    let mut stmt =
        conn.prepare("select symbol_id from symbols where provider_id = ? and symbol_text = ?")?;
    let id: i64 = stmt.query_row(params![provider_id, symbol], |r| r.get(0))?;
    Ok(id)
}

/// Resolve or create dataset_id for (provider, symbol, topic, optional resolution override)
pub fn upsert_dataset(
    conn: &duckdb::Connection,
    provider_id: i64,
    symbol_id: i64,
    topic: Topic,
    res_override: Option<Resolution>,
) -> anyhow::Result<i64> {
    let kind_key = topic_to_kind_key(topic);
    let res_opt = res_override.or(topic_to_resolution(topic));
    let res_txt: Option<String> = res_opt.as_ref().map(|r| r.to_os_string());
    let res_key: String = resolution_key(res_opt);

    conn.execute(
        "INSERT INTO datasets (provider_id, symbol_id, kind, resolution, resolution_key)
         SELECT ?,?,?,?,?
         WHERE NOT EXISTS (
           SELECT 1 FROM datasets
            WHERE provider_id=? AND symbol_id=? AND kind=? AND resolution_key=?
         )",
        duckdb::params![
            provider_id,
            symbol_id,
            kind_key,
            res_txt,
            res_key,
            provider_id,
            symbol_id,
            kind_key,
            res_key
        ],
    )?;

    let id: i64 = conn.query_row(
        "SELECT dataset_id FROM datasets
          WHERE provider_id=? AND symbol_id=? AND kind=? AND resolution_key=?",
        duckdb::params![provider_id, symbol_id, kind_key, res_key],
        |r| r.get(0),
    )?;
    Ok(id)
}

pub fn create_partitions_schema(conn: &Connection) -> duckdb::Result<()> {
    conn.execute_batch(
        r#"
        -- Note: DuckDB 1.4 may not support PRAGMA foreign_keys ON; omit to avoid parser error.

        CREATE TABLE IF NOT EXISTS partitions (
            -- what dataset this partition belongs to (per provider/symbol/resolution)
            dataset_id   BIGINT NOT NULL,
            -- path to the physical object (parquet file, duckdb table chunk, etc.)
            path         TEXT    NOT NULL,
            -- 'parquet' | 'duckdb'
            storage      TEXT    NOT NULL,

            -- basic stats
            rows         BIGINT NOT NULL,
            bytes        BIGINT NOT NULL,

            -- time range covered by this partition (nanoseconds since unix epoch, UTC)
            min_ts_ns    BIGINT NOT NULL,
            max_ts_ns    BIGINT NOT NULL,

            -- optional monotonic sequence range, when feeds provide it
            min_seq      BIGINT,
            max_seq      BIGINT,

            -- optional day partition key; store as YYYY-MM-DD for readability
            day_key      TEXT,

            PRIMARY KEY (dataset_id, path)
        );

        -- helpful indexes for pruning
        CREATE INDEX IF NOT EXISTS idx_partitions_dataset_time
            ON partitions(dataset_id, min_ts_ns, max_ts_ns);

        CREATE INDEX IF NOT EXISTS idx_partitions_day
            ON partitions(dataset_id, day_key);
        "#,
    )
}

#[inline]
pub fn dt_to_ns(dt: chrono::DateTime<chrono::Utc>) -> i64 {
    dt.timestamp_nanos_opt().expect("ns fits i64")
}

// Convert ns -> DateTime only when needed; keep BIGINT ns in the DB.
pub fn upsert_partition(
    conn: &duckdb::Connection,
    dataset_id: i64,
    path: &str,
    storage: &str,
    rows: i64,
    bytes: i64,
    min_ts: chrono::DateTime<chrono::Utc>,
    max_ts: chrono::DateTime<chrono::Utc>,
    min_seq: Option<i64>,
    max_seq: Option<i64>,
    day_key: Option<chrono::NaiveDate>,
) -> anyhow::Result<()> {
    let min_ts_ns = dt_to_ns(min_ts);
    let max_ts_ns = dt_to_ns(max_ts);
    let day_key_txt: Option<String> = day_key.map(|d| d.to_string()); // "YYYY-MM-DD"

    // Try UPDATE first (merge semantics), then INSERT if no row was updated.
    let updated = conn.execute(
        r#"
        UPDATE partitions SET
          storage   = ?,
          rows      = ?,
          bytes     = ?,
          min_ts_ns = LEAST(min_ts_ns, ?),
          max_ts_ns = GREATEST(max_ts_ns, ?),
          min_seq   = CASE
                        WHEN min_seq IS NULL THEN ?
                        WHEN ? IS NULL        THEN min_seq
                        ELSE LEAST(min_seq, ?)
                      END,
          max_seq   = CASE
                        WHEN max_seq IS NULL THEN ?
                        WHEN ? IS NULL        THEN max_seq
                        ELSE GREATEST(max_seq, ?)
                      END,
          day_key   = COALESCE(day_key, ?)
        WHERE dataset_id = ? AND path = ?
        "#,
        duckdb::params![
            storage,
            rows,
            bytes,
            min_ts_ns,
            max_ts_ns,
            min_seq,
            min_seq,
            min_seq,
            max_seq,
            max_seq,
            max_seq,
            day_key_txt,
            dataset_id,
            path
        ],
    )?;

    if updated == 0 {
        conn.execute(
            r#"
            INSERT INTO partitions
              (dataset_id, path, storage, rows, bytes, min_ts_ns, max_ts_ns, min_seq, max_seq, day_key)
            VALUES (?,?,?,?,?,?,?,?,?,?)
            "#,
            duckdb::params![
                dataset_id, path, storage, rows, bytes,
                min_ts_ns, max_ts_ns, min_seq, max_seq, day_key_txt
            ],
        )?;
    }
    Ok(())
}

#[allow(unused)]
fn partition_paths_for_range(
    conn: &duckdb::Connection,
    dataset_id: i64,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<Vec<String>> {
    let start_ns = dt_to_ns(start);
    let end_ns = dt_to_ns(end);
    let mut q = conn.prepare(
        r#"
        SELECT path
          FROM partitions
         WHERE dataset_id = ?
           AND max_ts_ns >= ?
           AND min_ts_ns <  ?
         ORDER BY min_ts_ns ASC
        "#,
    )?;
    let mut rows = q.query(duckdb::params![dataset_id, start_ns, end_ns])?;
    let mut out = Vec::new();
    while let Some(r) = rows.next()? {
        out.push(r.get::<_, String>(0)?);
    }
    Ok(out)
}

/// Internal helper to resolve dataset_id from string keys.
pub fn resolve_dataset_id(
    conn: &duckdb::Connection,
    provider: &str,
    symbol: &str,
    topic: Topic,
) -> anyhow::Result<Option<i64>> {
    let kind_key = topic_to_kind_key(topic);
    let res_key = resolution_key(topic_to_resolution(topic));

    let mut stmt = conn.prepare(
        r#"
        SELECT d.dataset_id
          FROM datasets d
          JOIN providers p ON p.provider_id = d.provider_id
          JOIN symbols   s ON s.symbol_id   = d.symbol_id
         WHERE p.provider_code = ?
           AND s.symbol_text   = ?
           AND d.kind          = ?
           AND d.resolution_key= ?
        "#,
    )?;

    let mut rows = stmt.query(duckdb::params![provider, symbol, kind_key, res_key])?;
    if let Some(row) = rows.next()? {
        return Ok(Some(row.get::<_, i64>(0)?));
    }

    // Fallback: legacy rows may have empty resolution_key for candle topics
    if !res_key.is_empty() {
        let mut rows2 = stmt.query(duckdb::params![provider, symbol, kind_key, ""])?;
        if let Some(row) = rows2.next()? {
            return Ok(Some(row.get::<_, i64>(0)?));
        }
    }

    Ok(None)
}

#[inline]
fn ns_to_dt(ns: i64) -> std::result::Result<DateTime<Utc>, Error> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = (ns.rem_euclid(1_000_000_000)) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).ok_or_else(|| {
        duckdb::Error::FromSqlConversionFailure(
            8,
            duckdb::types::Type::BigInt,
            Box::new(std::io::Error::other("invalid nanos")),
        )
    })
}

pub fn earliest_available(
    conn: &Connection,
    provider: &ProviderKind,
    symbol: &Instrument,
    topic: Topic,
) -> Result<Option<SeqBound>> {
    let Some(dataset_id) = resolve_dataset_id(
        conn,
        &provider_kind_to_db_string(provider.clone()),
        &symbol.to_string(),
        topic,
    )?
    else {
        return Ok(None);
    };

    // earliest = smallest (min_ts_ns, min_seq)
    let mut stmt = conn.prepare(
        r#"
        SELECT min_ts_ns, min_seq
          FROM partitions
         WHERE dataset_id = ?
         ORDER BY min_ts_ns ASC, COALESCE(min_seq, 0) ASC
         LIMIT 1
        "#,
    )?;

    let row: Option<(i64, Option<i64>)> = stmt
        .query_row(params![dataset_id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;

    Ok(row.map(|(ts_ns, seq)| SeqBound {
        ts: ns_to_dt(ts_ns).unwrap(),
        seq,
    }))
}

pub fn latest_available(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    topic: Topic,
) -> Result<Option<SeqBound>> {
    let Some(dataset_id) = resolve_dataset_id(conn, provider, symbol, topic)? else {
        return Ok(None);
    };

    // latest = largest (max_ts_ns, max_seq)
    let mut stmt = conn.prepare(
        r#"
        SELECT max_ts_ns, max_seq
          FROM partitions
         WHERE dataset_id = ?
         ORDER BY max_ts_ns DESC, COALESCE(max_seq, 0) DESC
         LIMIT 1
        "#,
    )?;

    let row: Option<(i64, Option<i64>)> = stmt
        .query_row(params![dataset_id], |r| Ok((r.get(0)?, r.get(1)?)))
        .optional()?;

    Ok(row.map(|(ts_ns, seq)| SeqBound {
        ts: ns_to_dt(ts_ns).unwrap(),
        seq,
    }))
}

/// Bulk variant: fetch earliest for many symbols at once.
pub fn earliest_for_many(
    conn: &Connection,
    provider: &ProviderKind,
    symbols: &[&Instrument],
    topic: Topic,
) -> Result<Vec<(String, Option<SeqBound>)>> {
    let mut out = Vec::with_capacity(symbols.len());
    for sym in symbols {
        let v = earliest_available(conn, provider, sym, topic)?;
        out.push(((*sym).to_string(), v));
    }
    Ok(out)
}

/// Bulk variant: latest for many symbols.
pub fn latest_for_many(
    conn: &Connection,
    provider: &str,
    symbols: &[&str],
    topic: Topic,
) -> Result<Vec<(String, Option<SeqBound>)>> {
    let mut out = Vec::with_capacity(symbols.len());
    for sym in symbols {
        let v = latest_available(conn, provider, sym, topic)?;
        out.push(((*sym).to_string(), v));
    }
    Ok(out)
}

// Prune catalog entries pointing to missing parquet files. Safe to run at startup.
pub fn prune_missing_partitions(conn: &Connection) -> duckdb::Result<()> {
    use std::path::Path;

    // Gather all paths and filter those missing on the filesystem using Rust std APIs
    let mut stmt = conn.prepare("SELECT path FROM partitions")?;
    let mut rows = stmt.query([])?;
    let mut missing: Vec<String> = Vec::new();
    while let Some(r) = rows.next()? {
        let p: String = r.get(0)?;
        if !Path::new(&p).exists() {
            missing.push(p);
        }
    }

    if missing.is_empty() {
        return Ok(());
    }

    // Temporarily drop indexes to avoid index maintenance assertion during DELETE
    conn.execute_batch(
        r#"
        DROP INDEX IF EXISTS idx_partitions_dataset_time;
        DROP INDEX IF EXISTS idx_partitions_day;
        "#,
    )?;

    let tx = conn.unchecked_transaction()?;
    for p in missing {
        let _ = tx.execute("DELETE FROM partitions WHERE path = ?", duckdb::params![p])?;
    }
    tx.commit()?;

    // Recreate indexes after cleanup
    conn.execute_batch(
        r#"
        CREATE INDEX IF NOT EXISTS idx_partitions_dataset_time ON partitions(dataset_id, min_ts_ns, max_ts_ns);
        CREATE INDEX IF NOT EXISTS idx_partitions_day ON partitions(dataset_id, day_key);
        "#
    )?;

    Ok(())
}

// Scan all partition paths and remove catalog rows that point to unreadable parquet files.
// This uses the Rust parquet reader to avoid triggering DuckDB’s parquet reader on bad files.
pub fn quarantine_unreadable_partitions(conn: &Connection) -> anyhow::Result<usize> {
    use parquet::file::reader::SerializedFileReader;
    use std::fs::File;

    // Gather all paths currently in catalog
    let mut stmt = conn.prepare("SELECT path FROM partitions")?;
    let mut rows = stmt.query([])?;
    let mut bad: Vec<String> = Vec::new();
    while let Some(r) = rows.next()? {
        let p: String = r.get(0)?;
        // In addition to file_exists, attempt to read the parquet footer
        match File::open(&p) {
            Ok(file) => {
                let res = SerializedFileReader::new(file);
                if res.is_err() {
                    bad.push(p);
                }
            }
            Err(e) => {
                error!("Quarantines Partion: {e}");
                // Missing or unreadable
                bad.push(p);
            }
        }
    }

    let mut removed = 0usize;
    if !bad.is_empty() {
        // Drop indexes to avoid index maintenance assertion during DELETE
        conn.execute_batch(
            r#"
            DROP INDEX IF EXISTS idx_partitions_dataset_time;
            DROP INDEX IF EXISTS idx_partitions_day;
            "#,
        )?;

        let tx = conn.unchecked_transaction()?; // faster, we’re just deleting rows
        for p in bad {
            let n = tx.execute("DELETE FROM partitions WHERE path = ?", duckdb::params![p])?;
            removed += n as usize;
        }
        tx.commit()?;

        // Recreate indexes after cleanup
        conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_partitions_dataset_time ON partitions(dataset_id, min_ts_ns, max_ts_ns);
            CREATE INDEX IF NOT EXISTS idx_partitions_day ON partitions(dataset_id, day_key);
            "#
        )?;
    }
    Ok(removed)
}
