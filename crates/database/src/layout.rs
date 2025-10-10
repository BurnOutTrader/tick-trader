// LEGACY: This module represents an older filesystem layout and query helper.
// Current persistence code uses paths.rs as the authoritative layout.
// Keep this for reference only; avoid using it in new code paths.
use crate::paths::provider_kind_to_db_string;
use std::path::{Path, PathBuf};
use tt_types::data::core::Exchange;
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::symbols::{Instrument, MarketType};

pub struct LakeLayout {
    pub root: std::path::PathBuf,
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
        provider: ProviderKind,
        topic: Topic,
        instrument: &Instrument,
        market_type: MarketType,
        exchange: Exchange,
    ) -> String {
        let provider = provider_kind_to_db_string(provider);

        let mut p = PathBuf::from(self.root);
        p.push(format!("provider={provider}"));
        p.push(format!("topic={topic}"));
        if market_type == MarketType::Futures {
            let root_symbol = extract_root(instrument);
            p.push(format!("symbol={root_symbol}"));
        }
        p.push(format!("symbol={instrument}"));
        p.push(format!("exchange={exchange}"));
        p.push("month=*");
        p.push("year=*");
        p.push("*.parquet");
        p.to_string_lossy().into_owned()
    }
}
