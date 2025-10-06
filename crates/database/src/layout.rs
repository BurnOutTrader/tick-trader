// LEGACY: This module represents an older filesystem layout and query helper.
// Current persistence code uses paths.rs as the authoritative layout.
// Keep this for reference only; avoid using it in new code paths.
use chrono::{DateTime, Datelike, Utc};
use std::path::{Path, PathBuf};
use tt_types::base_data::Resolution;
use tt_types::keys::Topic;
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::symbols::Instrument;

/// Filesystem layout with resolution-aware partitioning.
/// Examples:
///   ticks (daily files):    {root}/parquet/ticks/{provider}/{symbol}/yyyy/mm/dd.snappy.parquet
///   candles daily (yearly): {root}/parquet/candles/{provider}/{symbol}/D/{yyyy}.zstd.parquet
///   candles weekly (one):   {root}/parquet/candles/{provider}/{symbol}/W/all.zstd.parquet
pub struct LakeLayout {
    pub root: std::path::PathBuf,
}
impl LakeLayout {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn tick_path(
        &self,
        provider: &str,
        symbol_id: &str,
        ts: DateTime<Utc>,
    ) -> std::path::PathBuf {
        self.root
            .join("parquet")
            .join("ticks")
            .join(provider)
            .join(symbol_id)
            .join(format!("{:04}", ts.year()))
            .join(format!("{:02}", ts.month()))
            .join(format!("{:02}.zstd.parquet", ts.day()))
    }

    pub fn candle_path(
        &self,
        provider: &str,
        symbol_id: &str,
        res: &Resolution,
        ts: DateTime<Utc>,
    ) -> std::path::PathBuf {
        match res {
            Resolution::Daily => self
                .root
                .join("parquet")
                .join("candles")
                .join(provider)
                .join(symbol_id)
                .join("D")
                .join(format!("{:04}.zstd.parquet", ts.year())),
            Resolution::Weekly => self
                .root
                .join("parquet")
                .join("candles")
                .join(provider)
                .join(symbol_id)
                .join("W")
                .join("all.zstd.parquet"),
            Resolution::Seconds(_)
            | Resolution::Minutes(_)
            | Resolution::Hours(_) => {
                // Keep them “daily” like ticks for write locality and compression
                self.root
                    .join("parquet")
                    .join("candles")
                    .join(provider)
                    .join(symbol_id)
                    .join(Self::res_dir(res))
                    .join(format!("{:04}", ts.year()))
                    .join(format!("{:02}", ts.month()))
                    .join(format!("{:02}.zstd.parquet", ts.day()))
            }
        }
    }

    pub fn bbo_path(
        &self,
        provider: &str,
        symbol_id: &str,
        ts: DateTime<Utc>,
    ) -> std::path::PathBuf {
        self.root
            .join("parquet")
            .join("bbo")
            .join(provider)
            .join(symbol_id)
            .join(format!("{:04}", ts.year()))
            .join(format!("{:02}", ts.month()))
            .join(format!("{:02}.zstd.parquet", ts.day()))
    }

    /// Directory token for a given resolution (S{n}, M{n}, H{n}, D, W)
    pub fn res_dir(res: &Resolution) -> String {
        match res {
            Resolution::Seconds(n) => format!("S{}", n),
            Resolution::Minutes(n) => format!("M{}", n),
            Resolution::Hours(n) => format!("H{}", n),
            Resolution::Daily => "D".into(),
            Resolution::Weekly => "W".into(),
        }
    }
}

/// We use Hive-style partitions so DuckDB can “discover” partition columns:
/// provider=..., kind=..., symbol=..., res=..., year=..., date=YYYY-MM-DD
///
/// Layout (examples):
/// ticks:   root/provider=Rithmic/kind=Tick/symbol=MNQ/exchange=CME/res=Ticks/date=2025-03-14/*.parquet
/// bbo:     root/provider=DB/kind=Bbo/symbol=MNQ/exchange=CME/res=Quote/date=2025-03-14/*.parquet
/// candles:
///   - Seconds/Minutes/Hours/TickBars: .../date=YYYY-MM-DD/*.parquet   (intraday = per-day files)
///   - Daily:                         .../year=2025/*.parquet          (per-year files)
///   - Weekly:                        .../*.parquet                    (single file total)
///
/// Notes:
/// - Keeping `exchange` and `res` as partitions helps pruning a lot.
/// - `symbol` here is *instrument key* (“MNQZ25” or continuous “MNQ”), your call.

fn res_str(res: &Resolution) -> String {
    use Resolution::*;
    match *res {
        Seconds(n) => format!("Seconds{n}"),
        Minutes(n) => format!("Minutes{n}"),
        Hours(n) => format!("Hours{n}"),
        Daily => "Daily".into(),
        Weekly => "Weekly".into(),
    }
}

pub struct Layout<'a> {
    pub root: &'a Path,
}

impl<'a> Layout<'a> {
    pub fn new(root: &'a Path) -> Self {
        Self { root }
    }

    /// Glob covering *all* files for a given (provider, kind, symbol, exchange, resolution).
    /// We don’t bake dates in; DuckDB will prune via partition filters.
    pub fn glob_for(
        &self,
        provider: &str,
        topic: Topic,
        instrument: &Instrument,
        exchange: &str,
        res: Resolution,
    ) -> String {
        let root_symbol = extract_root(&instrument);
        let mut p = PathBuf::from(self.root);
        p.push(format!("provider={provider}"));
        let kind = match topic {
            Topic::Ticks => "Tick",
            Topic::Quotes => "Bbo",
            Topic::Depth => "Depth",
            Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d => "Candle",
            _ => "Unknown",
        };
        p.push(format!("kind={kind}"));
        p.push(format!("symbol={root_symbol}"));
        p.push(format!("symbol={instrument}"));
        p.push(format!("exchange={exchange}"));
        p.push(format!("res={}", res_str(&res)));

        match (topic, res) {
            (Topic::Candles1d, Resolution::Daily) => {
                // per-year
                p.push("year=*");
                p.push("*.parquet");
            }
            (Topic::Candles1d, Resolution::Weekly) => {
                // single file (keep it flexible with a glob)
                p.push("*.parquet");
            }
            _ => {
                // per-day files (ticks, bbo, and intraday candles)
                p.push("date=*");
                p.push("*.parquet");
            }
        }
        p.to_string_lossy().into_owned()
    }
}
