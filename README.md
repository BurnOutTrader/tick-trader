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
[![Buy Me A Coffee](https://img.shields.io/badge/Buy%20Me%20a%20Coffee-Sponsor-FFDD00?logo=buymeacoffee&logoColor=000)](https://buymeacoffee.com/BurnOutTrader)
[![Sponsor](https://img.shields.io/badge/Sponsor-GitHub-181717?logo=github&logoColor=white)](https://github.com/sponsors/BurnOutTrader)

> Note: This project is experimental and under active development. Interfaces may change and breaking updates can occur without notice. Use at your own risk.

> Note: The first build will take a while as I am using an integrated duck database as a stand in for an external connection later.

## Live trading status and near‑term roadmap

The engine can place live orders and is intended strictly for testing and evaluation. Do not use in production.

- Publish a reference “live test” strategy
- Surface EngineHandle helpers via top‑level exports
- Implement the backtesting engine
- Support external databases replacing the integrated DuckDB
- Warm up consolidators from live data plus the historical catalog
- Expand automated tests and CI coverage
- Adding rithmic + data bento at a later stage.


## ✨ Brief overview
- 💻 Platforms: macOS (tested) and Linux (experimental) are supported out of the box.
- 🧠 Strategies: run multiple strategies as separate processes/binaries.
- 🖧 Server: runs as a separate service; communicates over UDS and SHM; automatically starts/stops adapters (e.g., ProjectX) to minimize resource usage.
- 🔌 Adapters: multiple instances of the same adapter can run concurrently (e.g., ProjectX Tradeify + Topstep).
- 🏢 ProjectX tenants: not all firms are pre‑encoded in `ProjectXTenant` yet; adding a new tenant is straightforward and reuses the existing logic.
- 🚧 Status: work in progress — the architecture is largely stabilized, but features are still evolving.

### Strategies are straight forward
[Example Strategy](crates/a_strategies/my_strategies/orderbook_scalper)

Just implement the strategy trait
```rust

pub trait Strategy: Send + 'static {
    fn on_start(&mut self, _h: EngineHandle) {}
    fn on_stop(&mut self) {}

    fn on_tick(&mut self, _t: &Tick, _provider_kind: ProviderKind) {}
    fn on_quote(&mut self, _q: &Bbo, _provider_kind: ProviderKind) {}
    fn on_bar(&mut self, _b: &Candle, _provider_kind: ProviderKind) {}
    fn on_mbp10(&mut self, _d: &Mbp10, _provider_kind: ProviderKind) {}

    fn on_orders_batch(&mut self, _b: &OrdersBatch) {}
    fn on_positions_batch(&mut self, _b: &PositionsBatch) {}
    fn on_account_delta(&mut self, _accounts: &[AccountDelta]) {}

    fn on_trades_closed(&mut self, _trades: Vec<Trade>) {}

    fn on_subscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic, _success: bool) {}
    fn on_unsubscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic) {}

    fn accounts(&self) -> Vec<AccountKey> { Vec::new() }
}
```
Then in main
```rust
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tt_bus=info,tt_engine=info,projectx.ws=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
  
  let instrument = Instrument::from_str("MNQ.Z25").unwrap();
  let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;
    let mut engine = EngineRuntime::new(bus.clone());
    let account_name = AccountName::from_str("PRAC-V2-64").unwrap();
    let strategy = TotalLiveTestStrategy {
        _symbol: instrument.clone(),
        data_provider: provider,
        execution_provider: provider,
        account_name: account_name.clone(),
        subscribed: Vec::new(),
        last_order_type: OrderType::Market,
        engine: None,
        symbol_key: SymbolKey::new(instrument, )
    };
    let _handle = engine.start(strategy).await?;

    // Auto shutdown in 1000000 seconds
    sleep(Duration::from_secs(1000000)).await;

    let _ = engine.stop().await?;

    Ok(())
}
```

There are strategy helpers for queries, like portfolio, orders, positions etc, you access these via the engine handle
```rust
fn on_start(&mut self, _h: EngineHandle) {}
```

Your strategy should store this handle after starting 
```rust
pub struct TotalLiveTestStrategy {
    engine: Option<EngineHandle>,
}
impl Strategy for TotalLiveTestStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        // store handle
        self.engine = Some(h.clone());
    }

  fn on_tick(&mut self, t: &Tick, provider_kind: ProviderKind) {
      println!("{:?}", t);
    // then use the handle helper to place orders etc
    let _ = h.place_order(
      account.clone(),
      exec_key.clone(),
      tt_types::accounts::events::Side::Buy,
      1,
      OrderType::Market,
      None,
      None,
      None,
      Some("total_live_test".to_string()),
      None,
      None,
    );
  }
}
```

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


## 🌈 New Engine × Strategy model (2025-10)

Good news: your strategy logic is now single-threaded, lock-free on the hot path, and blissfully unaware of async plumbing. The engine does the heavy lifting so your code stays simple and fast.

Highlights
- 🧠 Strategy trait is synchronous (no async/await inside callbacks).
- 🧩 Engine owns the strategy by value and is the only task that calls it — deterministic and re-entrant.
- 📨 Non-blocking handle methods: fire-and-forget subscribe/place that enqueue commands instantly.
- ⚡ Instant getters: read portfolio state without awaiting (e.g., is_long/is_flat).
- 🔌 SHM workers never call your strategy; they push decoded events to the engine which updates marks and then calls you.

At a glance

```
[Providers] → Router → Engine intake mpsc ─→ (loop per message)
                                   │
                   [SHM workers] ──┘    1) update marks (PortfolioManager)
                                        2) strategy.on_*(&msg)
                                        3) drain command queue → bus I/O
```

Synchronous Strategy API

```rust
use tt_engine::engine::{Strategy, EngineHandle, DataTopic};
use tt_types::{keys::SymbolKey, providers::{ProviderKind, ProjectXTenant}};
use std::str::FromStr;

#[derive(Default)]
struct MyStrat { engine: Option<EngineHandle> }

impl Strategy for MyStrat {
    fn on_start(&mut self, h: EngineHandle) {
        self.engine = Some(h.clone());
        h.subscribe_now(
            DataTopic::MBP10,
            SymbolKey::new(
                tt_types::securities::symbols::Instrument::from_str("MNQ.Z25").unwrap(),
                ProviderKind::ProjectX(ProjectXTenant::Topstep),
            ),
        );
    }

    fn on_mbp10(&mut self, ob: &tt_types::data::mbp10::Mbp10, _pk: ProviderKind) {
        if let Some(h) = &self.engine {
            if h.is_flat(&ob.instrument) {
                // decide → h.place_now(order_spec)
            }
        }
    }
}
```

Fire-and-forget commands + instant reads
- subscribe_now(topic, key)
- unsubscribe_now(topic, key)
- place_now(place_spec) → returns EngineUuid (tag added into custom_tag for correlation)
- is_long/is_short/is_flat(&Instrument)

Async is still available (where it’s cold path)
- Need discovery? Use handle.list_instruments().await.
- Need account snapshots? EngineRuntime keeps last_* caches with async getters for tooling/UI.

How we abstracted async away from your strategy
- The engine runs a single task that receives messages, updates portfolio marks, invokes your sync callbacks, then drains the command queue for any fire-and-forget requests.
- SHM readers decode bytes and send compact events into the same intake mpsc; they never call the strategy directly.
- Because the engine updates marks before calling you, instant getters reflect the latest state in the same tick.

Want an async-style strategy anyway? You can.
- Pattern: send from sync callbacks into an async channel that your own background task consumes.

```rust
use tokio::sync::mpsc;

struct AsyncishStrat {
    tx: mpsc::UnboundedSender<tt_types::data::mbp10::Mbp10>,
}

impl Strategy for AsyncishStrat {
    fn on_start(&mut self, _h: EngineHandle) {}
    fn on_mbp10(&mut self, d: &tt_types::data::mbp10::Mbp10, _pk: ProviderKind) {
        let _ = self.tx.send(d.clone()); // offload to your async worker
    }
}

// elsewhere: spawn the consumer task that can .await, debounce, do I/O, etc.
```

Migration notes (from older async Strategy)
- Remove async from Strategy methods; they now take &T or &mut self by reference with no awaits.
- EngineRuntime::start takes your strategy by value: engine.start(MyStrat::default()).await?
- Replace any direct bus calls from callbacks with handle.subscribe_now/place_now.

Why this is faster and simpler
- Zero locks in the hot path, no await in callbacks, deterministic single-threaded strategy execution.
- Bounded command queue provides backpressure isolation — your decisions enqueue instantly; the engine handles I/O.

More details: see docs/strategies.md and docs/advanced.md.
