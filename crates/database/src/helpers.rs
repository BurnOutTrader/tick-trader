use duckdb::Connection;
use crate::catalog::{get_or_create_dataset_id, get_or_create_provider_id, get_or_create_symbol_id};

pub fn dataset_id_ticks(conn: &Connection, provider: &str, symbol: &str) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, provider)?;
    let sid = get_or_create_symbol_id(conn, pid, symbol)?;
    get_or_create_dataset_id(conn, pid, sid, DataKind::Tick, None)
}

pub fn dataset_id_bbo(conn: &Connection, provider: &str, symbol: &str) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, provider)?;
    let sid = get_or_create_symbol_id(conn, pid, symbol)?;
    get_or_create_dataset_id(conn, pid, sid, DataKind::Bbo, None)
}

pub fn dataset_id_books(conn: &Connection, provider: &str, symbol: &str) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, provider)?;
    let sid = get_or_create_symbol_id(conn, pid, symbol)?;
    get_or_create_dataset_id(conn, pid, sid, DataKind::BookL2, None)
}

pub fn dataset_id_candles(
    conn: &Connection,
    provider: &str,
    symbol: &str,
    res: Resolution,
) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, provider)?;
    let sid = get_or_create_symbol_id(conn, pid, symbol)?;
    get_or_create_dataset_id(conn, pid, sid, DataKind::Candle, Some(res))
}
