# 🚀 Tick Trader — current architecture overview

[![Status](https://img.shields.io/badge/Status-Experimental-orange)](#)
[![Rust](https://img.shields.io/badge/Rust-2024%20edition-b7410e?logo=rust)](https://www.rust-lang.org/)
[![Async](https://img.shields.io/badge/Async-Tokio-17a2b8?logo=tokio)](https://tokio.rs/)
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

It connects over UDS, performs topic-level Subscribe for hot and account topics, and then requests a key-based market data stream.


## 📚 Documentation index

- Architecture overview: [docs/architecture.md](docs/architecture.md)
- Wire protocol (rkyv frames): [docs/WIRE-v1.md](docs/WIRE-v1.md)
- Shared memory layout (SHM): [docs/SHM-layout.md](docs/SHM-layout.md)
- Database and persistence: [docs/database.md](docs/database.md)
- Advanced topics and notes: [docs/advanced.md](docs/advanced.md)


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

- EngineRuntime sends Responses to your Strategy implementation via callbacks:
  - on_tick, on_quote, on_depth (OrderBook), on_bar
  - on_orders_batch, on_positions_batch, on_account_delta_batch
  - on_subscribe/on_unsubscribe for control acknowledgments
- Ensure your Strategy’s desired_topics returns the coarse topics you want; the engine handles the initial topic-level Subscribe automatically (no FlowCredit needed).




## 📈 Symbology quick reference

- Instrument = ROOT + month code + year code (e.g., "MNQZ5" or "MNQZ25").
- Symbol = ROOT only (e.g., "MNQ").

```rust
use std::str::FromStr;
use tt_types::securities::symbols::Instrument;

let instrument: Instrument = Instrument::from_str("MNQZ25").unwrap();
let symbol: String = "MNQ".to_string();
```