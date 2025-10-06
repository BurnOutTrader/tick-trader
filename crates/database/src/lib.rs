//! Tick Trader Database crate
//!
//! This crate provides a lightweight time-series data lake with:
//! - File-based storage in Parquet, organized by a monthly layout per instrument and topic.
//! - A small DuckDB catalog tracking datasets and partitions (paths, row counts, byte sizes, min/max timestamps, optional sequence ranges).
//! - High-level ingestion and persistence helpers that write compressed Parquet, merge batches deterministically, and upsert catalog entries.
//! - Query and maintenance helpers for discovering earliest/latest availability, pruning missing files, and quarantining unreadable partitions.
//!
//! Layout overview (see `paths`):
//! provider/market_type/(root_symbol/)?instrument/topic/YYYY/instrument.topic.monthly.YYYYMM.parquet
//!
//! Key modules:
//! - `paths`: Authoritative path building utilities and deterministic file naming.
//! - `ingest`: One-call batch ingestion APIs that group data by month and delegate to persistence.
//! - `perist`: Persistence functions that write Parquet with ZSTD, merge with existing data, and maintain the catalog.
//! - `duck`: Catalog schema, dataset/partition upsert helpers, availability queries, and maintenance.
//! - `parquet`: Parquet writers and fast stats readers used by persistence.
//! - `catalog`: Simple convenience helpers built around the `Duck` wrapper.
//! - `queries`: Legacy higher-level queries (gated behind the `queries` feature) pending full monthly refactor.
//!
//! To get started, create schemas via `init::create_identity_schema_if_needed` and `duck::create_partitions_schema`,
//! then call `ingest::*` to persist data. Use `duck::earliest_available` / `latest_available` to discover ranges.

pub mod append;
pub mod catalog;
pub mod duck;
pub mod duck_queries;
pub mod ingest;
pub mod init;
pub mod layout;
pub mod merge;
pub mod models;
pub mod parquet;
pub mod paths;
pub mod perist;
#[cfg(feature = "queries")]
pub mod queries;
#[cfg(feature = "queries")]
pub mod replay;
#[cfg(feature = "queries")]
pub mod update_historical;
