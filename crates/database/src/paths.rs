//! Authoritative filesystem layout helpers for the database.
//!
//! Rules:
//! - Exactly one Parquet file per instrument per topic per month.
//! - Directory layout: provider/market_type/(root_symbol/)?instrument/topic/YYYY/
//! - Deterministic file name: instrument.topic.monthly.YYYYMM.parquet

use chrono::{Datelike, NaiveDate};
use std::path::{Path, PathBuf};
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::symbols::{Instrument, MarketType};

/// Map Topic to its canonical on-disk string used in paths and file names.
pub fn topic_to_db_string(topic: Topic) -> String {
    match topic {
        Topic::Ticks => "ticks".to_string(),
        Topic::Quotes => "quotes".to_string(),
        Topic::MBP10 => "depth".to_string(),
        Topic::Candles1s => "candles1s".to_string(),
        Topic::Candles1m => "candles1m".to_string(),
        Topic::Candles1h => "candles1h".to_string(),
        Topic::Candles1d => "candles1d".to_string(),
        _ => panic!("Invalid topic"), //todo remove these types from topic
    }
}

pub fn provider_kind_to_db_string(provider_kind: ProviderKind) -> String {
    match provider_kind {
        ProviderKind::ProjectX(_) => "projectx".to_string(),
        ProviderKind::Rithmic(_) => "rithmic".to_string(),
    }
}

#[allow(unreachable_patterns)]
/// provider/symbol/{kind} {res}/YYYY/MM/
pub fn partition_dir(
    data_root: &Path,
    provider: ProviderKind,
    market_type: MarketType,
    instrument: &Instrument,
    topic: Topic,
    year: u32,
) -> PathBuf {
    let base = data_root
        .join(provider_kind_to_db_string(provider))
        .join(market_type.to_string());

    let base = if market_type == MarketType::Futures {
        let root_symbol = extract_root(instrument);
        base.join(root_symbol).join(instrument.to_string())
    } else {
        base.join(instrument.to_string())
    };

    base.join(topic_to_db_string(topic))
        .join(format!("{:04}", year))
}

fn sanitize_string(sym: &str) -> String {
    sym.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub fn data_file_name(instrument: &Instrument, topic: Topic, date: NaiveDate) -> String {
    format!(
        "{}.{}.monthly.{:04}{:02}.parquet",
        sanitize_string(instrument.to_string().as_str()),
        topic_to_db_string(topic),
        date.year(),
        date.month()
    )
}

/// Convenience to ensure parent dirs exist.
pub fn ensure_parent_dirs(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}
