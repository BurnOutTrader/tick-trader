use anyhow::Result;
use chrono::{DateTime, Utc};
use duckdb::{Connection, params};

/// Open (or create) a DuckDB in-memory DB for ad-hoc Parquet ops.
/// (You can also pass a file-backed DB path if you prefer.)
pub fn connect_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory()?;
    Ok(conn)
}

/// Earliest timestamp (optional) for a parquet file (or set of files/glob).
/// `ts_col` is your canonical event time column (e.g. "time" or "ts_event").
pub fn earliest_ts(
    conn: &Connection,
    parquet_glob: &str,
    ts_col: &str,
) -> Result<Option<DateTime<Utc>>> {
    let sql = format!("SELECT MIN({}) AS min_ts FROM read_parquet(?);", ts_col);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![parquet_glob])?;
    if let Some(row) = rows.next()? {
        // DuckDB returns TIMESTAMP WITH TIME ZONE as RFC3339 text via `get::<_, String>(0)?`
        let s: Option<String> = row.get(0)?;
        Ok(s.and_then(|v| v.parse::<DateTime<Utc>>().ok()))
    } else {
        Ok(None)
    }
}

/// Latest timestamp (optional) for a parquet file (or set of files/glob).
pub fn latest_ts(
    conn: &Connection,
    parquet_glob: &str,
    ts_col: &str,
) -> Result<Option<DateTime<Utc>>> {
    let sql = format!("SELECT MAX({}) AS max_ts FROM read_parquet(?);", ts_col);
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![parquet_glob])?;
    if let Some(row) = rows.next()? {
        let s: Option<String> = row.get(0)?;
        Ok(s.and_then(|v| v.parse::<DateTime<Utc>>().ok()))
    } else {
        Ok(None)
    }
}

/// Get both (handy for planning incremental fetch windows).
pub fn min_max_ts(
    conn: &Connection,
    parquet_glob: &str,
    ts_col: &str,
) -> Result<(Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
    let sql = format!(
        "SELECT MIN({0}) AS min_ts, MAX({0}) AS max_ts FROM read_parquet(?);",
        ts_col
    );
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query(params![parquet_glob])?;
    if let Some(row) = rows.next()? {
        let min_s: Option<String> = row.get(0)?;
        let max_s: Option<String> = row.get(1)?;
        Ok((
            min_s.and_then(|v| v.parse::<DateTime<Utc>>().ok()),
            max_s.and_then(|v| v.parse::<DateTime<Utc>>().ok()),
        ))
    } else {
        Ok((None, None))
    }
}
