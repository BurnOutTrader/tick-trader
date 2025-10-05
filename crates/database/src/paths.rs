use crate::models::DataKind;
use chrono::{Datelike, NaiveDate};
use std::path::{Path, PathBuf};
use tt_types::base_data::Resolution;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::{Instrument, MarketType};

/// Logical dataset split knobs
#[derive(Debug, Clone, Copy)]
pub enum Partitioning {
    /// One file per day (ticks, quotes, 1s bars, etc.)
    Daily,
    /// One file per year (e.g., daily bars)
    Yearly,
    /// One file total (e.g., weekly bars)
    Single,
}

fn kind_dir(kind: DataKind) -> &'static str {
    match kind {
        DataKind::Tick => "tick",
        DataKind::Bbo => "bbo",
        DataKind::BookL2 => "bookl2",
        DataKind::Candle => "candle",
    }
}

pub fn get_partion(
    data_root: &Path,
    provider: &ProviderKind,
    market_type: MarketType,
    symbol: &Instrument,
    kind: DataKind,
    res: Option<Resolution>,
    day: NaiveDate,
) -> PathBuf {
    if let Some(res) = res {
        match res {
            | Resolution::Seconds(_)
            | Resolution::Minutes(_) |
            Resolution::Hours(_) | Resolution::Daily => {
                monthly_partition_dir(data_root, provider, market_type, symbol, kind, day.year())
            }
            Resolution::Weekly => {
                entire_history_partition_dir(data_root, provider, market_type, symbol, kind)
            }
        }
    } else {
        monthly_partition_dir(data_root, provider, market_type, symbol, kind, day.year())
    }
}
// Layout rules:
// - Intraday (Ticks/Seconds/Minutes/TickBars): one file per day, organized under YYYY/MM.
// - Hours (>= H1): one file per month.
// - Daily: one file per year.
// - Weekly: single file (entire history).

/// provider/symbol/{kind} {res}/YYYY/MM/
pub fn intraday_partition_dir(
    data_root: &Path,
    provider: &str,
    market_type: MarketType,
    symbol: &str,
    kind: DataKind,
    res: Resolution,
    day: NaiveDate,
) -> PathBuf {
    if res == Resolution::Weekly || res == Resolution::Daily {
        tracing::error!("Incorrect resolution for intraday");
    }
    data_root
        .join(provider)
        .join(market_type.to_string())
        .join(symbol)
        .join(format!("{} {}", kind_dir(kind), res.to_os_string()))
        .join(format!("{:04}", day.year()))
        .join(format!("{:02}", day.month()))
}

/// provider/symbol/{kind}/daily/YYYY/
pub fn monthly_partition_dir(
    data_root: &Path,
    provider: &ProviderKind,
    market_type: MarketType,
    symbol: &Instrument,
    kind: DataKind,
    year: i32,
) -> PathBuf {
    data_root
        .join(provider.to_string())
        .join(market_type.to_string())
        .join(symbol)
        .join(format!("{} {}", kind_dir(kind), "daily".to_string()))
        .join(format!("{:04}", year))
}

/// provider/symbol/{kind}/weekly/YYYY/Www/
pub fn entire_history_partition_dir(
    data_root: &Path,
    provider: &ProviderKind,
    market_type: MarketType,
    symbol: &Instrument,
    kind: DataKind,
) -> PathBuf {
    data_root
        .join(provider.to_string())
        .join(market_type.to_string())
        .join(symbol)
        .join(format!("{} weekly", kind_dir(kind)))
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

/// Intraday: 1 file per day
/// {symbol}.{kind}.{res}.{YYYYMMDD}.parquet
pub fn intraday_file_name(symbol: &str, kind: DataKind, res: Option<Resolution>, day: NaiveDate) -> String {
    if let Some(res) = res {
        format!(
            "{}.{}.{}.{}.parquet",
            sanitize_string(symbol),
            kind_dir(kind),
            res,
            day.format("%Y%m%d")
        )
    } else {
        format!(
            "{}.{}.{}.parquet",
            sanitize_string(symbol),
            kind_dir(kind),
            day.format("%Y%m%d")
        )
    }

}

/// Daily: 1 file per year
/// {symbol}.{kind}.daily.{YYYY}.parquet
pub fn daily_file_name(symbol: &str, kind: DataKind, year: i32) -> String {
    format!(
        "{}.{}.daily.{:04}.parquet",
        sanitize_string(symbol),
        kind_dir(kind),
        year
    )
}

/// Hours or above (>= H1): 1 file per month
/// {symbol}.{kind}.monthly.{YYYYMM}.parquet
pub fn monthly_file_name(symbol: &str, kind: DataKind, year: i32, month: u32) -> String {
    format!(
        "{}.{}.monthly.{:04}{:02}.parquet",
        sanitize_string(symbol),
        kind_dir(kind),
        year,
        month
    )
}

/// Weekly: 1 file per ISO week
/// {symbol}.{kind}.weekly.{YYYY}W{ww}.parquet
pub fn weekly_file_name(symbol: &str, kind: DataKind) -> String {
    format!(
        "{}.{}.weekly.parquet",
        sanitize_string(symbol),
        kind_dir(kind)
    )
}

/// Convenience to ensure parent dirs exist.
pub fn ensure_parent_dirs(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}
