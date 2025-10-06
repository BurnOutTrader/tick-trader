# Strategies — building on the Engine

This guide shows how to write strategies on top of the engine, what helper APIs are available today, and what’s planned to make strategy development fast and safe.

- Current capabilities: Strategy callbacks, subscription helpers, instruments discovery, account control, orders, and portfolio helpers.
- Planned capabilities: historical requests and queries, richer order tooling (OCO/brackets), risk guards, and convenience utilities.

---

## Strategy trait: lifecycle and callbacks

Implement the `Strategy` trait to receive data and lifecycle events. You pick your topics in `desired_topics`, and the engine subscribes to those coarse topics when it starts.

Key callbacks (subset):
- `desired_topics(&self) -> HashSet<Topic>`: which coarse topics you need (e.g., Ticks, Quotes, Depth, Candles1m).
- Lifecycle: `on_start`, `on_stop`.
- Market data: `on_tick(Tick)`, `on_quote(Bbo)`, `on_bar(Candle)`, `on_depth(OrderBook)`.
- Portfolio: `on_orders_batch(OrdersBatch)`, `on_positions_batch(PositionsBatch)`, `on_account_delta_batch(AccountDeltaBatch)`.
- Control: `on_subscribe(instrument, topic, success)`, `on_unsubscribe(instrument, topic)`.

Minimal skeleton:

```rust
use std::collections::HashSet;
use std::sync::Arc;
use tt_types::keys::Topic;
use tt_engine::engine::{EngineRuntime, Strategy};

struct MyStrategy;

#[async_trait::async_trait]
impl Strategy for MyStrategy {
    fn desired_topics(&self) -> HashSet<Topic> {
        [Topic::Ticks, Topic::Quotes, Topic::Candles1m].into_iter().collect()
    }
    async fn on_start(&self) {}
    async fn on_tick(&self, _t: tt_types::base_data::Tick) {}
    async fn on_quote(&self, _q: tt_types::base_data::Bbo) {}
    async fn on_bar(&self, _b: tt_types::base_data::Candle) {}
}
```

---

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
- `start(strategy: Arc<impl Strategy>)` attaches the engine to the bus, subscribes `desired_topics`, and begins dispatching callbacks.
- `stop()` cancels the task and detaches.

---

## Example: subscribe to MNQ ticks and place an order

```rust
use std::sync::Arc;
use tt_bus::ClientMessageBus;
use tt_engine::engine::{EngineRuntime, Strategy};
use tt_types::keys::{Topic, SymbolKey};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let bus = Arc::new(ClientMessageBus::connect("/tmp/tick-trader.sock").await?);
    let mut rt = EngineRuntime::new(bus);
    let strat = Arc::new(MyStrategy);
    rt.start(strat.clone()).await?;

    let key = SymbolKey { provider: ProviderKind::ProjectX("Topstep".into()), instrument: Instrument::from_str("MNQZ25").unwrap() };
    rt.subscribe_key(Topic::Ticks, key.clone()).await?;

    // ... later, submit an order (example shape only)
    // rt.place_order(tt_types::wire::PlaceOrder { /* fields */ }).await?;

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

