# Tick Trader Database

This document describes the database crate that powers Tick Trader’s on-disk history and the lightweight DuckDB catalog used to discover and maintain it.

## Goals

- Simple, durable file-based storage in Parquet
- Minimal catalog to track what exists and where
- Deterministic, human-readable paths and file names
- Ingestion APIs that are easy to use and safe to repeat
- Maintenance tools for pruning and quarantining broken files

## Layout (authoritative)

All path logic lives in `tt-database::paths`. The layout is:

```
<data_root>/
  {provider}/
    {market_type}/
      [root_symbol/]{instrument}/
        {topic}/
          {YYYY}/
            {instrument}.{topic}.monthly.{YYYY}{MM}.parquet
```

Notes:
- Futures include an extra folder level for the root symbol before the specific instrument.
- Exactly one Parquet file per instrument per topic per month.
- Topic strings are canonicalized by `paths::topic_to_db_string`.

## Catalog

We use a small DuckDB database as a catalog for fast discovery and maintenance. The schema is created via:

- `init::create_identity_schema_if_needed` (providers/symbols/…)
- `duck::create_partitions_schema` (datasets/partitions)

Important tables:
- `datasets(provider_id, symbol_id, kind, resolution, resolution_key)`
- `partitions(dataset_id, path, storage, rows, bytes, min_ts_ns, max_ts_ns, min_seq, max_seq, day_key)`

The `partitions` table holds per-file stats and supports range pruning without opening Parquet.

## Ingestion and persistence

- `ingest::*` functions accept batches of rows, group them by (year, month) based on timestamps, and call the corresponding persistence function.
- `perist::*_partition_zstd` functions write or merge a monthly Parquet file with ZSTD compression and upsert the `partitions` row with updated stats.
- Merging is performed by `append::append_merge_parquet`, which deduplicates by a stable composite key and writes a new file atomically.

## Query helpers

- `duck::earliest_available` / `duck::latest_available` return the earliest/latest timestamps across all partitions for a dataset.
- `duck::resolve_dataset_id` bridges human keys (provider/symbol/topic) to a `dataset_id` using a canonical mapping from Topic.

Legacy higher-level queries remain in `queries.rs` and are gated behind the `queries` feature until they are fully updated to the monthly layout.

## Maintenance

- `duck::prune_missing_partitions` removes catalog rows that point to files that no longer exist.
- `duck::quarantine_unreadable_partitions` attempts to read Parquet footers and removes catalog rows for unreadable files.

## Typical flow

1. Open a DuckDB connection (in-memory for tests or file-backed): `duck::connect(None)`.
2. Initialize schemas: `init::create_identity_schema_if_needed(&conn)` and `duck::create_partitions_schema(&conn)`.
3. Call `ingest_ticks` / `ingest_candles` / `ingest_bbo` with a `data_root` to persist data.
4. Query availability: `earliest_available` / `latest_available`.
5. Run maintenance periodically: prune/quarantine.

## Shared usage and configuration

Tick Trader uses DuckDB as an integrated catalog and metadata database. All historical and real-time data is stored in Parquet files, with DuckDB tracking and managing these files for fast discovery, pruning, and maintenance. Both the server and all strategies must use the same DB_PATH environment variable, ensuring they access the same DuckDB catalog and Parquet data. This is critical for correct operation, as mismatched DB_PATH values will result in inconsistent or missing data views.

- Set DB_PATH in your environment or .env file to the desired storage directory (default: ./storage).
- Both server and strategies must use the same DB_PATH value.

## Design choices

- One file per instrument/topic/month is a good balance between file count and update granularity.
- All path construction flows through `paths.rs`, so changing conventions in one place updates the system consistently.
- Catalog keeps only coarse stats (min/max ns, optional seq), enabling pushdown without heavy scans.

## Testing

See `crates/database/tests/db_catalog_tests.rs` for integration tests that validate dataset creation, partition registration, availability queries, and maintenance routines.
