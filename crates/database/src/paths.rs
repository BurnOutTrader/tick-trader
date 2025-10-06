use tt_types::keys::Topic;
use chrono::{Datelike, NaiveDate};
use std::path::{Path, PathBuf};
use tt_types::providers::ProviderKind;
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::symbols::{Instrument, MarketType};

// Rules
// 1 file per instrument per topic per month

pub fn topic_to_db_string(topic: Topic) -> String {
    match topic {
        Topic::Ticks => "ticks".to_string(),
        Topic::Quotes => "quotes".to_string(),
        Topic::Depth => "depth".to_string(),
        Topic::Candles1s => "candles1s".to_string(),
        Topic::Candles1m => "candles1m".to_string(),
        Topic::Candles1h => "candles1h".to_string(),
        Topic::Candles1d => "candles1d".to_string(),
        _ => panic!("Invalid topic") //todo remove these types from topic
    }
}

pub fn privider_kind_to_db_string(provider_kind: ProviderKind) -> String {
    match provider_kind {
        ProviderKind::ProjectX(t) => format!("projectx_{}", t.to_id_segment()),
        ProviderKind::Rithmic(s) => format!("rithmic_{}", s.to_id_segment()),
    }
}

/// provider/symbol/{kind} {res}/YYYY/MM/
pub fn partition_dir(
    data_root: &Path,
    provider: ProviderKind,
    market_type: MarketType,
    instrument: Instrument,
    topic: Topic,
    day: NaiveDate,
) -> PathBuf {
    match market_type {
        MarketType::Futures => {
            let root_symbol = extract_root(&instrument);
            data_root
                .join(privider_kind_to_db_string(provider))
                .join(market_type.to_string())
                .join(root_symbol)
                .join(instrument.to_string())
                .join(topic_to_db_string(topic))
                .join(format!("{:04}", day.year()))
                .join(format!("{:02}", day.month()))
        }
        _ => {
            data_root
                .join(privider_kind_to_db_string(provider))
                .join(market_type.to_string())
                .join(instrument.to_string())
                .join(topic_to_db_string(topic))
                .join(format!("{:04}", day.year()))
                .join(format!("{:02}", day.month()))
        }
    }
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

pub fn monthly_file_name(symbol: &str, topic: Topic, year: i32, month: u32) -> String {
    format!(
        "{}.{}.monthly.{:04}{:02}.parquet",
        sanitize_string(symbol),
        topic_to_db_string(topic),
        year,
        month
    )
}

/// Convenience to ensure parent dirs exist.
pub fn ensure_parent_dirs(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}
