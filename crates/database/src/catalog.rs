//! Convenience helpers around the DuckDB catalog for listing providers, universes, and symbols,
//! and for ensuring dataset identities exist.

use anyhow::anyhow;
use duckdb::{params, Connection};
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;

use crate::duck::{Duck, DuckError};
use crate::models::{Provider, SymbolMeta, UniverseMember};
use crate::paths::{provider_kind_to_db_string, topic_to_db_string};

impl Duck {
    pub fn list_providers(&self) -> Result<Vec<Provider>, DuckError> {
        let mut stmt = self
            .conn()
            .prepare("SELECT provider, version FROM providers")?;
        let rows = stmt.query_map([], |r| {
            Ok(Provider {
                provider: r.get(0)?,
                version: r.get::<_, Option<String>>(1)?,
            })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    }

    pub fn list_universe(&self, universe: &str) -> Result<Vec<UniverseMember>, DuckError> {
        let mut stmt = self
            .conn()
            .prepare("SELECT universe, symbol_id, provider FROM universes WHERE universe = ?")?;
        let rows = stmt.query_map([universe], |r| {
            Ok(UniverseMember {
                universe: r.get(0)?,
                symbol_id: r.get(1)?,
                provider: r.get(2)?,
            })
        })?;
        Ok(rows.filter_map(Result::ok).collect())
    }

    pub fn get_symbol(&self, symbol_id: &str) -> Result<Option<SymbolMeta>, DuckError> {
        let mut stmt = self.conn().prepare(
            "SELECT symbol_id, security, exchange, currency, root, continuous_of FROM symbols WHERE symbol_id=?",
        )?;
        let mut rows = stmt.query([symbol_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(SymbolMeta {
                symbol_id: row.get(0)?,
                security: row.get(1)?,
                currency: row.get(3)?,
                root: row.get(4)?,
                continuous_of: row.get(5)?,
            }))
        } else {
            Ok(None)
        }
    }

    // expose &Connection for internal helpers
    pub(crate) fn conn(&self) -> &Connection {
        &self.conn
    }
}

#[inline]
fn topic_kind(topic: Topic) -> String {
    topic_to_db_string(topic)
}

/// Insert-if-missing, then return `provider_id`.
pub fn get_or_create_provider_id(conn: &Connection, provider_code: &str) -> anyhow::Result<i64> {
    conn.execute(
        "insert into providers(provider_code) values (?)
         on conflict(provider_code) do nothing",
        params![provider_code],
    )?;
    let id: i64 = conn.query_row(
        "select provider_id from providers where provider_code = ?",
        params![provider_code],
        |r| r.get(0),
    )?;
    Ok(id)
}

/// Insert-if-missing, then return `symbol_id` for (provider_id, symbol_text).
pub fn get_or_create_symbol_id(
    conn: &Connection,
    provider_id: i64,
    symbol_text: &str,
) -> anyhow::Result<i64> {
    conn.execute(
        "insert into symbols(provider_id, symbol_text) values (?, ?)
         on conflict(provider_id, symbol_text) do nothing",
        params![provider_id, symbol_text],
    )?;
    let id: i64 = conn.query_row(
        "select symbol_id from symbols where provider_id = ? and symbol_text = ?",
        params![provider_id, symbol_text],
        |r| r.get(0),
    )?;
    Ok(id)
}
fn provider_string(provider: &ProviderKind) -> String {
    match provider {
        ProviderKind::ProjectX(t) => "projectx".to_string(),
        ProviderKind::Rithmic(s) => "rithmic".to_string(),
    }
}

pub fn dataset_id(conn: &Connection, provider: &ProviderKind, instrument: &Instrument, topic: Topic) -> anyhow::Result<i64> {
    let pid = get_or_create_provider_id(conn, &provider_string(provider))?;
    let sid = get_or_create_symbol_id(conn, pid, &instrument.to_lowercase())?;
    get_or_create_dataset_id(conn, pid, sid, topic)
}

/// Insert-if-missing, then return `dataset_id` for (provider_id, symbol_id, kind, resolution_key).
/// New layout: `kind` encodes the topic (e.g., "ticks", "candles1m"); resolution columns unused.
pub fn get_or_create_dataset_id(
    conn: &Connection,
    provider_id: i64,
    symbol_id: i64,
    topic: Topic,
) -> anyhow::Result<i64> {
    let kind_s = topic_kind(topic);
    let res_txt: Option<String> = None;
    let res_key: String = String::new();
    conn.execute(
        "insert into datasets(provider_id, symbol_id, kind, resolution, resolution_key)
         values (?, ?, ?, ?, ?)
         on conflict(provider_id, symbol_id, kind, resolution_key) do nothing",
        params![provider_id, symbol_id, &kind_s, res_txt, res_key],
    )?;
    let id: i64 = conn.query_row(
        "select dataset_id from datasets
          where provider_id = ? and symbol_id = ? and kind = ? and resolution_key = ?",
        params![provider_id, symbol_id, &kind_s, ""],
        |r| r.get(0),
    )?;
    Ok(id)
}

// ---------- public entrypoints ----------
pub fn ensure_dataset(
    conn: &Connection,
    provider: ProviderKind,
    instrument: &Instrument,
    topic: Topic,
) -> anyhow::Result<i64> {
    let provider_id = ensure_provider_id(conn, provider_kind_to_db_string(provider).as_str())?;
    let symbol_id = ensure_symbol_id(conn, provider_id, instrument)?;
    ensure_dataset_row(conn, provider_id, symbol_id, topic)
}

// ---------- internal pieces ----------

fn ensure_provider_id(conn: &Connection, provider: &str) -> anyhow::Result<i64> {
    conn.execute(
        "INSERT INTO providers(provider_code) VALUES (?) ON CONFLICT(provider_code) DO NOTHING",
        params![provider],
    )?;

    conn.query_row(
        "SELECT provider_id FROM providers WHERE provider_code = ?",
        params![provider],
        |r| r.get::<_, i64>(0),
    )
    .map_err(|e| anyhow!("ensure_provider_id: {}", e))
}

fn ensure_symbol_id(conn: &Connection, provider_id: i64, instrument: &Instrument) -> anyhow::Result<i64> {
    let sym_txt = instrument.to_string();
    conn.execute(
        "INSERT INTO symbols(provider_id, symbol_text)
         SELECT ?, ?
         WHERE NOT EXISTS (
             SELECT 1 FROM symbols WHERE provider_id = ? AND symbol_text = ?
         )",
        params![provider_id, sym_txt, provider_id, sym_txt],
    )?;

    conn.query_row(
        "SELECT symbol_id FROM symbols WHERE provider_id = ? AND symbol_text = ?",
        params![provider_id, sym_txt],
        |r| r.get::<_, i64>(0),
    )
    .map_err(|e| anyhow!("ensure_symbol_id: {}", e))
}

fn ensure_dataset_row(
    conn: &Connection,
    provider_id: i64,
    instrument_id: i64,
    topic: Topic,
) -> anyhow::Result<i64> {
    let kind_s = topic_kind(topic);

    conn.execute(
        "INSERT INTO datasets(provider_id, symbol_id, kind, resolution, resolution_key)
         SELECT ?, ?, ?, NULL, ''
         WHERE NOT EXISTS (
             SELECT 1 FROM datasets
              WHERE provider_id = ? AND symbol_id = ? AND kind = ? AND resolution_key = ''
         )",
        params![provider_id, instrument_id, &kind_s, provider_id, instrument_id, &kind_s],
    )?;

    conn.query_row(
        "SELECT dataset_id FROM datasets
           WHERE provider_id=? AND symbol_id=? AND kind = ? AND resolution_key = ''",
        params![provider_id, instrument_id, &kind_s],
        |r| r.get::<_, i64>(0),
    )
    .map_err(|e| anyhow!("ensure_dataset_row: {}", e))
}
