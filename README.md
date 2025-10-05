# 🚀 Tick Trader — current architecture overview

[![Status](https://img.shields.io/badge/Status-Experimental-orange)](#)
[![Rust](https://img.shields.io/badge/Rust-2024%20edition-b7410e?logo=rust)](https://www.rust-lang.org/)
[![Async](https://img.shields.io/badge/Async-Tokio-17a2b8?logo=tokio)](https://tokio.rs/)
[![Serialization](https://img.shields.io/badge/Serialization-rkyv-5c6bc0)](https://github.com/rkyv/rkyv)
[![Transport](https://img.shields.io/badge/Transport-UDS%20%7C%20SHM-6c757d)](#)

This document explains how the current system works end-to-end: the standalone Router (server side), the ProviderManager/Workers, the ProjectX adapter, the wire protocol (rkyv), and the engine test strategy that connects over a Unix Domain Socket (UDS).

If you just want to try it quickly, see the Quick start section.

- Server process: tt-server (crates/server)
- Router (authoritative hub): tt-bus::router (crates/bus)
- Providers and manager: tt-providers (crates/providers) and adapter crates (e.g., adapters/projectx)
- Client engine: tt-engine (crates/engine) with a demo strategy binary (tt-engine-test_strategy)
- Wire protocol and data types: tt-types (crates/tt-types)


## 🧭 Control plane and data plane (current state)

- Control plane (implemented):
  - Request/Response messages (rkyv-serialized) over length-delimited frames on a UDS.
  - Clients send Subscribe (topic-level), SubscribeKey (topic+symbol/provider), UnsubscribeKey, UnsubscribeAll, Ping, Kick, InstrumentsRequest. No client credits are needed; the server manages backpressure automatically.
  - Router replies with Pong, SubscribeResponse, and fans out data Responses.
  - Kick allows server-initiated or client-initiated disconnection with full cleanup.

- Data plane (implemented):
  - Hot feeds: Ticks, Quotes, and Depth/OrderBook use high-priority fanout paths. In addition to framed rkyv, these streams have working shared-memory (SHM) snapshots per (Topic, Key) written by providers and announced to clients via AnnounceShm.
  - Lossless streams (Orders, Positions, Account events) continue to use framed rkyv fanout. The Router tolerates transient backpressure and only disconnects a client after sustained backlog (serious slowdown) beyond an internal threshold to avoid divergence.
  - Bars remain on framed rkyv.


## 🧩 Key components

### 🚌 Router (server hub)
Location: crates/bus/src/router.rs, exported as tt_bus::Router.

Responsibilities:
- Owns client sessions and fanout queues.
- Subscription indices:
  - Coarse topic-level interest (Topic).
  - Key-based interest per (Topic, SymbolKey).
- Sharding: internal maps are sharded by a stable hash of (topic, key); configure shard count with Router::new(N).
- Safety: frame length capped to 8 MiB; heartbeat Ping/Pong; clean detach (UnsubscribeAll on disconnect); optional forced kick for backpressure.
- Upstream integration: calls an UpstreamManager trait on first-subscribe/last-unsubscribe to start/stop upstream streams.

Selected APIs:
- attach_client(UnixStream): attaches a new client connection.
- set_backend(Arc<dyn UpstreamManager>): provides the link to providers via a manager.
- publish_* methods used by providers to emit data (tick/quote/bar/orderbook/etc.).

### 🔌 UpstreamManager and ProviderManager
Location: crates/bus/src/router.rs (trait) and crates/providers/src/manager.rs (implementation).

- UpstreamManager: async trait with subscribe_md/unsubscribe_md for market data streams.
- ProviderManager implements UpstreamManager and manages provider pairs and worker shards per ProviderKind.
- Uses only dyn traits (MarketDataProvider and ExecutionProvider). No concrete SDK types appear in the server or router code.
- Worker model: an InprocessWorker wraps a MD provider and refcounts interest per (topic, key), calling provider.subscribe_md on first subscribe and provider.unsubscribe_md on last.
- ProviderManager ensures the MD side is connected (connect_to_market) the first time a provider pair is created.

### 🔷 ProjectX adapter
Location: crates/adapters/projectx

- PXClient implements both MarketDataProvider and ExecutionProvider traits.
- Connects to ProjectX HTTP and WebSocket hubs and publishes data into the Router via tt_bus::Router publish_* methods.
- Market events currently supported:
  - GatewayTrade → Response::TickBatch
  - GatewayQuote → Response::QuoteBatch
  - GatewayDepth → Response::OrderBookBatch (OrderBook snapshots via rkyv)
  - User/account/order/position updates → corresponding batches

### 🖥️ Server main
Location: crates/server/src/main.rs

- Loads .env and reads TT_BUS_ADDR for the UDS path. On Linux, abstract namespace like "@tick-trader.sock" is supported via a custom bind helper.
- Creates Router::new(8) (8 shards by default) and registers a ProviderManager backend.
- Accept loop: for each UDS connection, spawns Router::attach_client.
- The server does not hold concrete provider types; providers publish directly to the Router.

### ⚙️ Engine (single strategy)
Location: crates/engine

- ClientMessageBus is an in-process bus for a single strategy. It maintains local topic-level interest, forwards Requests to the server, and fans in Responses to the strategy loop.
- EngineRuntime wires a Strategy implementation and executes callbacks for data and account events.
- The demo strategy binary connects over UDS and issues both topic-level and key-based subscriptions.


## 📡 Wire protocol (tt-types)
Location: crates/tt-types/src/wire.rs

Requests (subset):
- Subscribe { topic, latest_only, from_seq }
- SubscribeKey { topic, key, latest_only, from_seq }
- UnsubscribeKey { topic, key }
- UnsubscribeAll { reason }
- Ping { ts_ns }
- Kick { reason }
- InstrumentsRequest { provider, pattern, corr_id }

Responses (subset):
- Pong { ts_ns }
- SubscribeResponse { topic, instrument, success }
- TickBatch | QuoteBatch | BarBatch | OrderBookBatch | VendorData
- OrdersBatch | PositionsBatch | AccountDeltaBatch
- InstrumentsResponse | InstrumentsMapResponse

All messages are archived with rkyv and sent as length-delimited frames (tokio-util codec).


## 🔐 Environment and credentials (.env)

- The server and providers load .env automatically. ProviderSessionSpec::from_env scans all environment variables to construct credentials for multiple providers.
- ProjectX keys use the PX_ prefix:
  - PX_{TENANT}_USERNAME, PX_{TENANT}_APIKEY, optional PX_{TENANT}_FIRM
  - Example:
    - PX_TOPSTEP_USERNAME=alice
    - PX_TOPSTEP_APIKEY=xxxx
    - PX_TOPSTEP_FIRM=topstep
- Rithmic keys use the RITHMIC_ prefix (parsing supported, implementation TBD):
  - RITHMIC_{SYSTEM}_{USERNAME|APIKEY|PASSWORD|FCM_ID|IB_ID|USER_TYPE}
- Server address:
  - TT_BUS_ADDR defaults to /tmp/tick-trader.sock (macOS) or @tick-trader.sock (Linux abstract). Override in .env or env.


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


## 🗺️ Current limitations and planned work

- SHM data plane for Ticks/Quotes/Depth: Implemented. Providers write rkyv snapshots into SHM using a seqlock header via tt-shm and the Router announces streams with AnnounceShm (cached for late subscribers). Client-side zero-copy readers are a TODO (engine still consumes framed rkyv; sample SHM reader helpers to be added).
- Full out-of-process ProviderWorker processes: Scaffolding added (providers::ipc) with a minimal command protocol; current workers are in-process. Next step: spawn/process management and UDS/TCP transport.
- Remove legacy Request variants once all clients use key-based messages.


## 📈 Symbology quick reference

- Instrument = ROOT + month code + year code (e.g., "MNQZ5" or "MNQZ25").
- Symbol = ROOT only (e.g., "MNQ").

```rust
use std::str::FromStr;
use tt_types::securities::symbols::Instrument;

let instrument: Instrument = Instrument::from_str("MNQZ25").unwrap();
let symbol: String = "MNQ".to_string();
```