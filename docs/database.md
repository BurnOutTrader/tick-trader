# Tick Trader Database (PostgreSQL)

This document describes the Postgres-backed database used by Tick Trader. We use sqlx for async access, and a small set of normalized tables tuned for high-rate appends and fast time-range queries.

Quick start (local Postgres via Docker):
1. cd pg && cp .env.example .env
2. chmod +x init/01-init.sh
3. docker compose up -d
4. Export DATABASE_URL (or rely on DB_PATH fallback described below)

Environment:
- DATABASE_URL: postgres://user:pass@host:port/tick_trader
- If not set, components synthesize a URL from DB_PATH like host:host_port:container_port (default 127.0.0.1:5432:5432) and pg/.env.example defaults.

Schema overview (see crates/database/src/schema.rs):
- instrument(id BIGSERIAL, sym TEXT UNIQUE)
- bars_1m(provider TEXT, symbol_id BIGINT, time_start TIMESTAMPTZ, time_end TIMESTAMPTZ, open/high/low/close NUMERIC(18,9), volume/ask_volume/bid_volume NUMERIC(18,9), resolution TEXT, PRIMARY KEY(provider,symbol_id,time_end))
- latest_bar_1m(provider TEXT, symbol_id BIGINT, time_start/time_end TIMESTAMPTZ, open/high/low/close NUMERIC(18,9), volume/ask_volume/bid_volume NUMERIC(18,9), resolution TEXT, PRIMARY KEY(provider,symbol_id))
- tick(ts_ns BIGINT, provider TEXT, symbol_id BIGINT, price NUMERIC(18,9), volume NUMERIC(18,9), side SMALLINT, venue_seq BIGINT, key_tie BIGINT, exec_id TEXT, PRIMARY KEY(provider,symbol_id,ts_ns,key_tie)); BRIN index on ts_ns
- bbo(ts_ns BIGINT, provider TEXT, symbol_id BIGINT, bid/bid_size/ask/ask_size NUMERIC(18,9), bid_orders/ask_orders INTEGER, venue_seq BIGINT, is_snapshot BOOLEAN, key_tie BIGINT, PRIMARY KEY(provider,symbol_id,ts_ns,key_tie)); BRIN index on ts_ns
- mbp10(provider TEXT, symbol_id BIGINT, ts_recv_ns BIGINT, ts_event_ns BIGINT, rtype/publisher_id/instrument_ref SMALLINT/INTEGER/INTEGER, action/side/depth SMALLINT, price/size NUMERIC(18,9), flags SMALLINT, ts_in_delta INTEGER, sequence BIGINT, book_* arrays, PRIMARY KEY(provider,symbol_id,ts_event_ns,sequence))
- series_extent(provider TEXT, symbol_id BIGINT, topic SMALLINT, earliest/latest TIMESTAMPTZ, PRIMARY KEY(provider,symbol_id,topic))
- kvp(ns TEXT, key TEXT, ts TIMESTAMPTZ, value JSONB, PRIMARY KEY(ns,key))

Provider mapping:
- ProviderKind is mapped to a stable short code via tt_database::paths::provider_kind_to_db_string (e.g., "projectx", "rithmic").

APIs (no separate row models; we map directly to domain types):
- Ingest: tt_database::ingest::{ingest_candles, ingest_ticks, ingest_bbo, ingest_mbp10}
- Queries: tt_database::queries::{latest_data_time, get_extent, get_range, get_symbols, latest_bars_1m}
- Schema/init: tt_database::schema::ensure_schema, get_or_create_instrument_id
- Connection: tt_database::init::{pool_from_env, init_db}

De-duplication & ordering:
- Candles: PRIMARY KEY(provider,symbol_id,time_end)
- Ticks/BBO: PRIMARY KEY(provider,symbol_id,ts_ns,key_tie) with key_tie as sequence tiebreaker when timestamps collide.
- MBP10: PRIMARY KEY(provider,symbol_id,ts_event_ns,sequence)

Recommended indexes:
- BRIN on tick.ts_ns and bbo.ts_ns for large append-only scans.
- Unique index on series_extent (already enforced by PK).

Roles and security (pg/.env.example):
- Server (writer) uses TT_WRITER; strategies can use TT_READER (read-only). You can also run both with the POSTGRES superuser locally.

Migration notes:
- Older docs referenced DuckDB + Parquet catalog. We now use PostgreSQL exclusively; strategy and server read/write directly via sqlx.

Testing:
- Integration tests live in crates/database/tests/db_tests.rs. Set DATABASE_URL and run `cargo test -p tt-database`.
