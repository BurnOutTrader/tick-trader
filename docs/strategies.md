# Strategies — building on the Engine

This guide shows how to write strategies on top of the engine, what helper APIs are available today, and what’s planned to make strategy development fast and safe.

- Current capabilities: Strategy callbacks, subscription helpers, instruments discovery, account control, orders, and portfolio helpers.
- Planned capabilities: historical requests and queries, richer order tooling (OCO/brackets), risk guards, and convenience utilities.

---

## Strategy trait: lifecycle and callbacks

Implement the Strategy trait to receive data and lifecycle events. The trait is synchronous: no async/await inside callbacks. The engine is the only task that calls your strategy, in a single-threaded, deterministic loop.

Key callbacks (subset):
- Lifecycle: on_start(&mut self, EngineHandle), on_stop(&mut self)
- Market data: on_tick(&mut self, &Tick, ProviderKind), on_quote(&mut self, &Bbo, ProviderKind), on_bar(&mut self, &Candle, ProviderKind), on_mbp10(&mut self, &Mbp10, ProviderKind)
- Portfolio: on_orders_batch(&mut self, &OrdersBatch), on_positions_batch(&mut self, &PositionsBatch), on_account_delta(&mut self, &[AccountDelta])
- Control: on_subscribe(&mut self, Instrument, DataTopic, bool), on_unsubscribe(&mut self, Instrument, DataTopic)

Minimal skeleton (sync):

```rust
use tt_engine::engine::{Strategy, EngineHandle, DataTopic};
use tt_types::{keys::SymbolKey, providers::{ProviderKind, ProjectXTenant}};
use std::str::FromStr;

#[derive(Default)]
struct MyStrategy { engine: Option<EngineHandle> }

impl Strategy for MyStrategy {
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

    fn on_mbp10(&mut self, d: &tt_types::data::mbp10::Mbp10, _pk: ProviderKind) {
        if let Some(h) = &self.engine {
            if h.is_flat(&d.instrument) {
                // decide → h.place_now(order_spec)
            }
        }
    }
}
```

---

## How data arrives (SHM vs UDS)

- You do not need to choose between SHM and UDS in your strategy. The engine selects the transport.
- Hot feeds (Ticks, Quotes, MBP10):
  - Provider adapters write snapshots to SHM and the Router emits Response::AnnounceShm.
  - The engine starts a per-(topic,key) SHM reader upon AnnounceShm and invokes your callbacks from SHM data.
  - While SHM is active, the system does not send duplicate UDS batches for these topics.
- Other topics and control:
  - Orders/Positions/Account events, bars/candles, discovery, pings, and subscribe acks are carried over UDS.
- Fallback:
  - If SHM is not announced for a hot stream, the engine consumes UDS batches for that stream and your callbacks still fire.

## EngineRuntime helpers (current)

The `EngineRuntime` exposes a set of async helper methods to interact with the server over UDS via the client message bus.

Subscriptions
- `subscribe_key(topic, key)` / `unsubscribe_key(topic, key)`: start/stop a specific stream.
- `subscribe_symbol(topic, key)` / `unsubscribe_symbol(topic, key)`: equivalent convenience APIs.
- Vendor securities auto-refresh: the engine will automatically refresh the vendor’s instruments list hourly the first time you subscribe for that provider.

Instruments and reference data
- `list_instruments(provider, pattern: Option<String>) -> Vec<Instrument>`: quick filtered list (short timeout).
- `get_instruments_map(provider) -> Vec<(Instrument, FuturesContractWire)>`: contracts map (futures example).
- `get_account_info(provider) -> AccountInfoResponse`: account metadata snapshot (corr_id matched).

Orders
- `place_order(spec: PlaceOrder)`
- `cancel_order(spec: CancelOrder)`
- `replace_order(spec: ReplaceOrder)`

Accounts and portfolio
- Activate streams: `activate_account_interest(account_key)` / `deactivate_account_interest(account_key)`
- Bulk init: `initialize_accounts(iter)` / `initialize_account_names(provider, names)`
- Cached snapshots: `last_orders()`, `last_positions()`, `last_accounts()`
- Helpers:
  - `find_position_delta(instrument)`
  - `orders_for_instrument(instrument)`
  - `is_long/is_short/is_flat(instrument)`

Correlation helper
- `request_with_corr(|corr_id| -> Request) -> oneshot::Receiver<Response>`: send a correlated Request and await a single Response. This is the basis for custom request/response patterns.

Start/stop
- `start(strategy: S)` where S: Strategy — engine owns your strategy by value; it invokes on_start synchronously, then runs the engine loop.
- `stop()` cancels the task and detaches.

Fire-and-forget vs async helpers
- From callbacks, use EngineHandle::{subscribe_now, unsubscribe_now, place_now} to enqueue commands instantly.
- For cold paths (discovery, tooling), async helpers remain: list_instruments().await, subscribe_key().await, etc.

---

## Example: subscribe to MNQ ticks and place an order

```rust
use std::str::FromStr;
use std::time::Duration;
use tracing::level_filters::LevelFilter;
use tt_bus::ClientMessageBus;
use tt_engine::engine::{EngineRuntime, Strategy, EngineHandle, DataTopic};
use tt_types::keys::SymbolKey;
use tt_types::providers::{ProviderKind, ProjectXTenant};

#[derive(Default)]
struct MyStrategy { engine: Option<EngineHandle> }

impl Strategy for MyStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        // Keep the handle for later and subscribe immediately (non-blocking)
        self.engine = Some(h.clone());
        h.subscribe_now(
            DataTopic::Ticks,
            SymbolKey::new(
                tt_types::securities::symbols::Instrument::from_str("MNQ.Z25").unwrap(),
                ProviderKind::ProjectX(ProjectXTenant::Topstep),
            ),
        );
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(LevelFilter::INFO).init();

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?; // returns Arc<ClientMessageBus>

    let mut engine = EngineRuntime::new(bus.clone());
    let strategy = MyStrategy::default();
    let _handle = engine.start(strategy).await?;

    tokio::time::sleep(Duration::from_secs(10)).await;
    engine.stop().await?;
    Ok(())
}
```

---

## Historical data and catalog (current vs planned)

Current
- The server side (providers + download manager + database) can fetch historical data and catalog it as Parquet with DuckDB metadata.
- Strategies benefit passively: when you subscribe to bars (e.g., Candles1m), the system consolidates live data and you can query the same Parquet/DuckDB from external tools.

Planned engine-side helpers
- Historical update request:
  - `request_historical_update(req: HistoricalRequest) -> TaskHandle` (non-blocking; deduplicates identical inflight requests and returns a stable id; engine can `wait()` or poll status).
- Historical pull:
  - `fetch_historical(req: HistoricalRequest) -> Vec<HistoryEvent>` or a streaming channel for large windows.
- Catalog queries:
  - `query_candles(symbol, res, start, end) -> Vec<CandleRow>`
  - `query_ticks(symbol, start, end) -> Vec<TickRow>`
  - These will use the engine’s existing DuckDB connection under the hood.

Why: so strategies can warm caches, run pre-trade checks, and do hybrid live+historical studies without leaving the engine.

---

## Data consolidation (bandwidth and determinism)

- Live consolidators produce 1s/1m/1h/1d candles from tick/quote streams so you can subscribe only to the granularity you need.
- Batching and consolidation reduce transport volume and storage churn while keeping deterministic bar construction.

---

## Best practices

- Prefer `subscribe_key` to get only what you need; unsubscribe when done to free upstream.
- Use `list_instruments`/`get_instruments_map` to validate symbols before subscribing or placing orders.
- Call `activate_account_interest` on startup to receive orders/positions/account events.
- Use correlation (`request_with_corr`) for one-shot server queries; add a timeout on the receiver.
- Keep `on_*` callbacks light; offload heavy logic to background tasks if needed.

---

## Roadmap (selected)

- Historical helpers (request/pull) and catalog query APIs directly from strategies.
- Richer order primitives: OCO, bracket orders, synthetic stops, order templates.
- Risk and safety: max position/risk checks, per-instrument throttles, circuit breakers.
- Symbol services: unified resolution (aliases, continuous futures), session calendars.
- Observability: per-stream metrics exposure, easy logging scopes.

---

## Troubleshooting

- No data after subscribe: ensure the provider credentials are loaded and the instrument exists (use `list_instruments`).
- Missing portfolio events: call `activate_account_interest` with your account key(s).
- Slow or missing response to a one-shot request: increase your timeout around `request_with_corr()` and check server logs.



---

## Zero-copy roadmap for strategies

We are planning to introduce optional zero-copy data paths in the engine to reduce allocations and improve latency on high-rate feeds:

- Archived views: The engine will retain incoming rkyv frames and expose `rkyv::Archived<T>` views internally, avoiding deserialize/copy where possible.
- Dual APIs during transition: Strategy callbacks will continue to receive owned Rust structs for simplicity. Advanced users will be able to opt into alternate handlers or pull archived references for ultra-low latency paths.
- SHM integration: For hot feeds with SHM snapshots, we will provide helpers to read aligned, memory-mapped snapshots and work with archived views without extra copies.
- Safety and ergonomics: The existing owned-struct API remains supported; zero-copy will be additive and opt-in.

Benefits: fewer allocations, less memcpy, lower CPU for tick/quote/depth bursts.



---

## 🌟 2025-10 Update: Strategies are now synchronous

Your strategy runs on a single engine task. No async/await inside callbacks; no locks on the hot path. The engine performs all async work around your code.

Key changes
- Trait is sync. Methods take borrowed data: &Tick, &Bbo, &Candle, &Mbp10.
- Engine owns your strategy by value: engine.start(MyStrat::default()).await?
- Non-blocking helpers via EngineHandle: subscribe_now, unsubscribe_now, place_now.
- Instant getters via EngineHandle: is_long/is_short/is_flat(&Instrument).

Sync trait skeleton

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

    fn on_mbp10(&mut self, d: &tt_types::data::mbp10::Mbp10, _pk: ProviderKind) {
        if let Some(h) = &self.engine {
            if h.is_flat(&d.instrument) {
                // decide: h.place_now(order)
            }
        }
    }
}
```

How the engine abstracts async away
- Engine loop: parse → update portfolio marks → call strategy.on_* → drain commands queue (bus I/O happens here).
- SHM tasks decode frames and push events into the engine intake; they do not call your strategy.
- Ordering guarantee: marks are updated before your callback, so getters reflect the latest tick.

Async-style patterns you can use
- Offload from callbacks into an async channel; process in your own task.

```rust
use tokio::sync::mpsc;

#[derive(Default)]
struct AsyncishStrat { tx: mpsc::UnboundedSender<tt_types::data::mbp10::Mbp10> }

impl Strategy for AsyncishStrat {
    fn on_mbp10(&mut self, d: &tt_types::data::mbp10::Mbp10, _pk: ProviderKind) {
        let _ = self.tx.send(d.clone());
    }
}
```

Migration guide (from older async Strategy)
- Remove async/await from Strategy methods; switch to &T arguments.
- Replace direct bus I/O inside callbacks with EngineHandle fire-and-forget helpers.
- EngineRuntime::start now takes your strategy by value, not Arc/Mutex.

Tips
- Keep callbacks light; enqueue commands and let the engine perform I/O.
- Size bursts: the internal command queue is bounded (4096 by default). For ultra-bursty strategies, consider pacing your enqueues.
- Use handle.list_instruments().await or other async helpers on cold paths (e.g., startup, discovery).
