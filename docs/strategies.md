# Strategies — building on the Engine

This guide shows how to write strategies on top of the engine, what helper APIs are available today, and what’s planned to make strategy development fast and safe.

- Current capabilities: Strategy callbacks, subscription helpers, instruments discovery, account control, orders, and portfolio helpers.
- Planned capabilities: historical requests and queries, richer order tooling (OCO/brackets), risk guards, and convenience utilities.

---

## Strategy trait: lifecycle and callbacks

Implement the Strategy trait to receive data and lifecycle events. The trait is synchronous: no async/await inside callbacks. The engine is the only task that calls your strategy, in a single-threaded, deterministic loop.

Key callbacks (as implemented today):
- Lifecycle: on_start(&mut self, EngineHandle), on_stop(&mut self)
- Market data: on_tick(&mut self, &Tick, ProviderKind), on_quote(&mut self, &Bbo, ProviderKind), on_bar(&mut self, &Candle, ProviderKind), on_mbp10(&mut self, &Mbp10, ProviderKind)
- Portfolio: on_orders_batch(&mut self, &OrdersBatch), on_positions_batch(&mut self, &PositionsBatch), on_account_delta(&mut self, &[AccountDelta])
- Trades: on_trades_closed(&mut self, Vec<Trade>)
- Control: on_subscribe(&mut self, Instrument, DataTopic, bool), on_unsubscribe(&mut self, Instrument, DataTopic)

Minimal skeleton (sync):

```rust
use tt_engine::traits::Strategy;
use tt_engine::handle::EngineHandle;
use tt_engine::models::DataTopic;
use tt_types::{keys::SymbolKey, providers::{ProviderKind, ProjectXTenant}};
use std::str::FromStr;

#[derive(Default)]
struct MyStrategy { engine: Option<EngineHandle> }

impl Strategy for MyStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        self.engine = Some(h.clone());
        h.subscribe_now(
            DataTopic::Ticks,
            SymbolKey::new(
                tt_types::securities::symbols::Instrument::from_str("MNQ.Z25").unwrap(),
                ProviderKind::ProjectX(ProjectXTenant::Topstep),
            ),
        );
    }

    fn on_mbp10(&mut self, d: &tt_types::data::mbp10::Mbp10, pk: ProviderKind) {
        if let Some(h) = &self.engine {
            if h.is_flat_delta(&d.instrument) {
                // decide → h.place_now(order_spec)
            }
        }
    }
}
```

---
## Engine runtime and helpers (current)

The runtime and handle expose synchronous helper methods. Use EngineHandle inside callbacks for low-latency fire-and-forget actions; use EngineRuntime outside callbacks (setup, tools, discovery, batch operations).

EngineRuntime (synchronous; returns Results)
- Subscriptions
  - `subscribe_key(data_topic: DataTopic, key: SymbolKey) -> anyhow::Result<()>`
  - `unsubscribe_key(data_topic: DataTopic, key: SymbolKey) -> anyhow::Result<()>`
  - `subscribe_symbol(topic: Topic, key: SymbolKey) -> anyhow::Result<()>`
  - `unsubscribe_symbol(topic: Topic, key: SymbolKey) -> anyhow::Result<()>`
  - Vendor securities auto-refresh: first subscribe per provider triggers an hourly background refresh of instruments.
- Instruments and reference data
  - `list_instruments(provider: ProviderKind, pattern: Option<String>) -> anyhow::Result<Vec<Instrument>>`
  - `get_instruments_map(provider: ProviderKind) -> anyhow::Result<Vec<FuturesContract>>`
  - `get_account_info(provider: ProviderKind) -> anyhow::Result<tt_types::wire::AccountInfoResponse>`
- Orders
  - `send_order_for_execution(spec: PlaceOrder) -> anyhow::Result<()>`
  - `cancel_order(spec: CancelOrder) -> anyhow::Result<()>`
  - `replace_order(spec: ReplaceOrder) -> anyhow::Result<()>`
  - Convenience: `place_order_with(account_name, key, side, qty, r#type, limit_price, stop_price, trail_price, custom_tag, stop_loss, take_profit) -> anyhow::Result<()>`
- Accounts and portfolio
  - Activate streams: `activate_account_interest(key: AccountKey) -> anyhow::Result<()>`
  - Deactivate: `deactivate_account_interest(key: AccountKey) -> anyhow::Result<()>`
  - Bulk init: `initialize_accounts(iter: impl IntoIterator<Item = AccountKey>) -> anyhow::Result<()>`
  - Bulk init (names): `initialize_account_names(provider: ProviderKind, names: impl IntoIterator<Item = AccountName>) -> anyhow::Result<()>`
  - Cached snapshots: `last_orders() -> Option<OrdersBatch>`, `last_positions() -> Option<PositionsBatch>`, `last_accounts() -> Option<AccountDeltaBatch>`
  - Helpers: `find_position_delta(&Instrument) -> Option<PositionDelta>`, `orders_for_instrument(&Instrument) -> Vec<OrderUpdate>`
  - Position booleans (delta across accounts): `is_long_delta/is_short_delta/is_flat_delta(&Instrument) -> bool`
- Other
  - `request_with_corr(|corr_id| -> Request) -> tokio::sync::oneshot::Receiver<Response>`: correlated single-response helper.
  - Historical latest refresh for a key: `update_historical_latest_by_key(provider: ProviderKind, topic: Topic, instrument: Instrument) -> anyhow::Result<()>`
- Lifecycle
  - `start(strategy: S) -> anyhow::Result<EngineHandle>` where `S: Strategy` — invokes on_start synchronously, runs engine loop.
  - `stop() -> anyhow::Result<()>` — stops engine, detaches tasks.

EngineHandle (low-latency from callbacks)
- Consolidators
  - `add_consolidator(from: DataTopic, for_key: SymbolKey, cons: Box<dyn Consolidator + Send>)`
  - `remove_consolidator(from: DataTopic, for_key: SymbolKey)`
- Fire-and-forget commands
  - `subscribe_now(topic: DataTopic, key: SymbolKey)`
  - `unsubscribe_now(topic: DataTopic, key: SymbolKey)`
  - `place_now(spec: PlaceOrder) -> EngineUuid`
  - Convenience: `place_order(account_name, key, side, qty, r#type, limit_price, stop_price, trail_price, custom_tag, stop_loss, take_profit) -> anyhow::Result<EngineUuid>`
- Instant portfolio queries (thread-safe; internal RwLocks)
  - `is_long/is_short/is_flat(&AccountKey, &Instrument) -> bool`  // account-specific
  - `is_long_delta/is_short_delta/is_flat_delta(&Instrument) -> bool`  // aggregated delta across accounts
  - `open_orders_for_instrument(&Instrument) -> Vec<OrderUpdate>`
  - `find_position_delta(&Instrument) -> Option<PositionDelta>`
- Reference data cache
  - `securities_for(provider: ProviderKind) -> Vec<Instrument>`

Notes
- All helpers above are synchronous. Use EngineRuntime for operations that return Results; EngineHandle methods that enqueue work never block.
- Snapshot getters are consistent and cheap due to internal RwLock-protected caches.

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

## Historical data (server-managed) and Postgres queries

Current
- The server side (providers + download manager + database) fetches historical data and writes directly to PostgreSQL using tt_database::ingest::*.
- Strategies benefit passively: when you subscribe to bars (e.g., Candles1m), the system consolidates live data; you can also query Postgres directly via tt_database::queries::* for time windows.

Planned engine-side helpers
- Historical update request:
  - `request_historical_update(req: HistoricalRequest) -> TaskHandle` (non-blocking; deduplicates identical inflight requests and returns a stable id; engine can `wait()` or poll status).
- Historical pull:
  - `fetch_historical(req: HistoricalRequest) -> Vec<HistoryEvent>` or a streaming channel for large windows.
- Query helpers (strategy side):
  - `query_candles(symbol, res, start, end)`
  - `query_ticks(symbol, start, end)`
  - These will delegate to tt_database::queries over sqlx.

Why: so strategies can warm caches, run pre-trade checks, and do hybrid live+historical studies without leaving the engine.

---

## Data consolidation (bandwidth and determinism)

- Live consolidators produce 1s/1m/1h/1d candles from tick/quote streams so you can subscribe only to the granularity you need.
- Batching and consolidation reduce transport volume and storage churn while keeping deterministic bar construction.

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

## 🌟 2025-10 Update: Strategies are now synchronous

Your strategy runs on a single engine task. No async/await inside callbacks; no locks on the hot path. The engine performs all async work around your code.

Key changes
- Trait is sync. Methods take borrowed data: &Tick, &Bbo, &Candle, &Mbp10.
- Engine owns your strategy by value: engine.start(MyStrat::default())
- Non-blocking helpers via EngineHandle: subscribe_now, unsubscribe_now, place_now.
- Instant getters via EngineHandle: is_long/is_short/is_flat(&AccountKey, &Instrument) and delta variants is_long_delta/is_short_delta/is_flat_delta(&Instrument).

Sync trait skeleton

```rust
use tt_engine::traits::Strategy;
use tt_engine::handle::EngineHandle;
use tt_engine::models::DataTopic;
use tt_types::{keys::SymbolKey, providers::{ProviderKind, ProjectXTenant}};
use std::str::FromStr;

#[derive(Default)]
struct MyStrat { engine: Option<EngineHandle> }

impl Strategy for MyStrat {
    fn on_start(&mut self, h: EngineHandle) {
        self.engine = Some(h.clone());
        h.subscribe_now(
            DataTopic::Ticks,
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
