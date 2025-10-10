# 🚀 Tick Trader — current architecture overview

[![Status](https://img.shields.io/badge/Status-Experimental-orange)](#)
[![Rust](https://img.shields.io/badge/Rust-2024%20edition-b7410e?logo=rust)](https://www.rust-lang.org/)
[![Async](https://img.shields.io/badge/Async-Tokio-17a2b8?logo=tokio)](https://tokio.rs/)
[![rkyv](https://img.shields.io/crates/v/rkyv?label=rkyv&color=8e44ad)](https://crates.io/crates/rkyv)
[![rust_decimal](https://img.shields.io/crates/v/rust_decimal?label=rust_decimal&color=00aaff)](https://crates.io/crates/rust_decimal)
[![Transport](https://img.shields.io/badge/Transport-UDS%20%7C%20SHM-6c757d)](#)
[![Provider](https://img.shields.io/badge/Provider-ProjectX-8e44ad)](crates/adapters/projectx)
[![Provider](https://img.shields.io/badge/Provider-Rithmic%20(planned)-95a5a6)](#)
[![Provider](https://img.shields.io/badge/Provider-DataBento%20(planned)-95a5a6)](https://www.databento.com)
[![Columnar](https://img.shields.io/badge/Columnar-Arrow-5c6bc0)](https://arrow.apache.org/)
[![Catalog](https://img.shields.io/badge/Catalog-DuckDB-43b581)](https://duckdb.org/)


## 🔐 Environment and credentials (.env)

- The server and providers load .env automatically. ProviderSessionSpec::from_env scans all environment variables to construct credentials for multiple providers.
- ProjectX keys use the PX_ prefix:
  - `PX_{TENANT}_USERNAME, PX_{TENANT}_APIKEY, optional PX_{TENANT}_FIRM`
  - Example:
    - PX_TOPSTEP_USERNAME=alice
    - PX_TOPSTEP_APIKEY=xxxx
    - PX_TOPSTEP_FIRM=topstep
- Rithmic keys use the RITHMIC_ prefix (parsing supported, implementation TBD):
  - RITHMIC_{SYSTEM}_{USERNAME|APIKEY|PASSWORD|FCM_ID|IB_ID|USER_TYPE}
- Server address:
  - TT_BUS_ADDR defaults to /tmp/tick-trader.sock (macOS) or @tick-trader.sock (Linux abstract). Override in .env or env.
- Engine database path:
  - DB_PATH sets the directory for local database storage used by both the server and strategies (tt-engine). Both must use the same DB_PATH to ensure consistent access to the DuckDB catalog and Parquet data. Defaults to ./storage if not set. Example:
    - DB_PATH=./mydata
- Integrated database:
  - Tick Trader uses DuckDB as an integrated catalog and metadata database. DuckDB tracks and manages Parquet files, which are used for durable, efficient storage of all historical and real-time data. Both the server and strategies interact with the same DuckDB instance and Parquet files via the shared DB_PATH.

## ⚡ Quick start

1) Build the workspace:

```bash
cargo build
```

2) Start the server (separate terminal):

```bash
TT_BUS_ADDR=/tmp/tick-trader.sock cargo run -p tt-server
```

3) Prepare .env with your provider credentials (ProjectX example):

```env
PX_TOPSTEP_USERNAME=your_user
PX_TOPSTEP_APIKEY=your_key
PX_TOPSTEP_FIRM=topstep
TT_BUS_ADDR=/tmp/tick-trader.sock
```

4) Run the test strategy (client) in another terminal:

```bash
cargo run -p tt-engine --bin tt-engine-test_strategy
```

It connects over UDS for control and lossless streams. Hot market data (Ticks/Quotes/MBP10) is delivered via SHM snapshots once announced; the engine automatically starts per-(topic,key) SHM readers. If SHM is not available for a stream, the engine falls back to UDS delivery.


## 📚 Documentation index

- Architecture overview: [docs/architecture.md](docs/architecture.md)
- Wire protocol (rkyv frames): [docs/WIRE-v1.md](docs/WIRE-v1.md)
- Shared memory layout (SHM): [docs/SHM-layout.md](docs/SHM-layout.md)
- Database and persistence: [docs/database.md](docs/database.md)
- Strategies guide: [docs/strategies.md](docs/strategies.md)
- Advanced topics and notes: [docs/advanced.md](docs/advanced.md)

## 🧾 Wire serialization and alignment (current)

- We use rkyv for all wire frames (`WireMessage::{Request, Response}`) with length-delimited framing (max 8 MiB).
- Writers now produce `rkyv::AlignedVec` and then move it into a `Vec<u8>`/`bytes::Bytes` for transport. This ensures the producer side is always correctly aligned for rkyv.
- Readers must not assume alignment of the incoming slice. The client and router copy the frame bytes into an `AlignedVec` before calling `rkyv::from_bytes` to avoid "archive underaligned" errors.
- SHM snapshots also store rkyv bytes; see [docs/SHM-layout.md](docs/SHM-layout.md) for alignment notes when reading from shared memory.

## 🛣️ Roadmap: zero-copy rkyv in the engine

We plan to progressively enable zero-copy processing inside the client engine by:
- Retaining incoming frame buffers and accessing `rkyv::Archived<T>` views directly where safe, avoiding deserialize/copy for hot paths.
- Teaching the engine to prefer archived views for batches (e.g., MBP10, ticks, quotes) and only materialize owned structs when strategies need to mutate or persist.
- Exposing optional archived references in callbacks or via alternate channels, preserving the current owned-struct callbacks for ease of use.
- Coordinating this with SHM readers so strategies can memory-map aligned snapshots and read archived views without extra copies.

This will reduce allocations and CPU for high-rate feeds while keeping the current API stable during migration.


## 🤝 Providers

- Supported
  - ProjectX (Topstep): Market Data, Execution, Historical via `adapters/projectx`.
- Planned
  - Rithmic: Market Data, Execution, Historical (parsing in place; implementation planned).
  - DataBento: Market Data, Historical (planned adapter).


## 📦 Historical data, catalog, and storage

- Automatic download and cataloging:
  - The engine and server can automatically fetch historical data from the configured provider and persist it as Parquet, while maintaining a searchable DuckDB catalog.
  - Storage layout (from `DB_PATH`):
    - Parquet partitions live under `DB_PATH/market_data/`.
    - A catalog file `DB_PATH/market_data/catalog.duckdb` indexes all partitions (providers, symbols, datasets, time ranges).
  - The catalog is self-healing: on startup we create missing schema, prune entries for missing files, and quarantine unreadable/corrupt partitions.
- Live and historical consolidation:
  - Non standard duration Candles are produced via streaming consolidators (e.g., 1s/1m/1h/1d) so clients can subscribe to coarser data without subscribing to raw ticks.
    - This reduces over-the-wire volume and storage churn while keeping deterministic bar construction.
- Querying your data:
  - From strategies: use our database layer to query time windows or scan partitions directly.
  - From other tools/languages:
    - DuckDB catalog: DuckDB CLI and bindings for Python, R, Node.js, Java, etc.
    - Parquet files: widely supported via Arrow/Parquet ecosystems (Python: pandas/pyarrow/duckdb; Rust: polars/arrow2; R: arrow; Java/Scala: Spark; Go: parquet-go; Julia: Arrow.jl; Node.js: duckdb-wasm/arrow JS).
  - This means you can inspect, analyze, and model with the same files your engine writes—no proprietary format.
- The backtesting engine will accurately feed data into the system based on subscriptions and date ranges, there is no need for manual file handling.


## 🔑 Key-based subscription example

Example taken from the engine test binary (MNQZ25 via ProjectX Topstep):

```rust
use tt_types::keys::Topic;
use tt_types::wire::{Request, SubscribeKey};

// Send key-based subscribes; no FlowCredit needed (server manages backpressure).
let _ = req_tx.send(Request::SubscribeKey(SubscribeKey { topic: Topic::Ticks, key: key.clone(), latest_only: false, from_seq: 0 })).await;
let _ = req_tx.send(Request::SubscribeKey(SubscribeKey { topic: Topic::Depth, key: key.clone(), latest_only: false, from_seq: 0 })).await;
```

- SubscribeKey registers interest on (topic, key) and triggers Router → UpstreamManager::subscribe_md on first subscriber.
- No FlowCredit is needed: the server auto-manages credits/backpressure and continues delivering as long as the client keeps up.
- For lossless topics (Orders/Positions/AccountEvt), the Router tolerates transient backpressure and will only disconnect a client after sustained backlog (i.e., serious slowdown) beyond an internal threshold; otherwise it prefers to drop isolated batches for that client to keep the system healthy.


## 📬 Data delivery to strategies

- EngineRuntime delivers data to your Strategy via callbacks:
  - on_tick, on_quote, on_mbp10 (Depth/OrderBook), on_bar
  - on_orders_batch, on_positions_batch, on_account_delta_batch
  - on_subscribe/on_unsubscribe for control acknowledgments
- SHM-first for hot feeds:
  - Hot market data (Ticks, Quotes, MBP10) is produced into per-(topic,key) SHM snapshots by the provider adapter.
  - The Router emits Response::AnnounceShm for each (topic,key) once available.
  - The engine, upon AnnounceShm, spawns a lightweight polling task that reads snapshots from SHM and invokes your callbacks. No duplicate UDS messages are sent for these topics while SHM is active.
- UDS is still used for:
  - Control-plane (subscribe/unsubscribe acks, pings, discovery), orders/positions/account events (lossless), bars/candles, and any topic not backed by SHM.
- Fallback behavior:
  - If SHM is not announced for a subscribed hot stream, the engine continues to receive the corresponding UDS batches and dispatches callbacks as before.
- Strategy code does not change: you continue to implement the same callbacks; the engine selects the transport.


## 📈 Symbology quick reference

- Instrument = ROOT + month code + year code (e.g., "MNQ.Z25" or "NQ.Z25").
- Symbol = ROOT only (e.g., "MNQ").

```rust
use std::str::FromStr;
use tt_types::securities::symbols::Instrument;

let instrument: Instrument = Instrument::from_str("MNQ.Z25").unwrap();
let symbol: String = "MNQ".to_string();
```