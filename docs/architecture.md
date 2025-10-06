# Tick Trader Architecture

This document explains how the system works end-to-end: the standalone Router (server side), the ProviderManager and workers, the ProjectX adapter, the wire protocol (rkyv), and the demo engine strategy that connects over a Unix Domain Socket (UDS).

If you just want to run it, jump to Quick start.

- Server process: `tt-server` (crates/server)
- Router (authoritative hub): `tt-bus::router` (crates/bus)
- Providers and manager: `tt-providers` (crates/providers) and adapter crates (for example, `adapters/projectx`)
- Client engine: `tt-engine` (crates/engine) with demo binary `tt-engine-test_strategy`
- Wire protocol and data types: `tt-types` (crates/types)

## 🗺️ Current limitations and planned work
- SHM data plane for Ticks/Quotes/Depth: Implemented. Providers write rkyv snapshots into SHM using a seqlock header via tt-shm and the Router announces streams with AnnounceShm (cached for late subscribers). Client-side zero-copy readers are a TODO (engine still consumes framed rkyv; sample SHM reader helpers to be added).
- Full out-of-process ProviderWorker processes: Scaffolding added (providers::ipc) with a minimal command protocol; current workers are in-process. Next step: spawn/process management and UDS/TCP transport.
- Remove legacy Request variants once all clients use key-based messages.

---

## Table of contents

- System overview
- Control plane and data plane
- Key components
  - Router (server hub)
  - UpstreamManager and ProviderManager
  - ProjectX adapter
  - Server main
  - Engine (single strategy)
- Wire protocol (tt-types)
- Data flow: end-to-end at a glance
- Quick start
- Glossary

---

## System overview

Tick Trader is split into a server that hosts a Router and provider integrations, and one or more clients (engines/strategies) that connect via UDS. The Router is the authoritative hub that fans out data to subscribed clients and notifies providers (via the ProviderManager) when streams should be started/stopped.

- The server is stateless with respect to provider SDK types: it only depends on provider traits.
- Providers publish ticks/quotes/depth/bars and account data into the Router.
- Clients subscribe to topics and specific keys and receive rkyv-serialized messages over framed UDS.

---

## 🧭 Control plane and data plane (current state)

Control plane (implemented):
- Request/Response messages (rkyv-serialized) over length-delimited frames on a UDS.
- Clients send Subscribe (topic-level), SubscribeKey (topic+symbol/provider), UnsubscribeKey, UnsubscribeAll, Ping, Kick, InstrumentsRequest. No client credits are needed; the server manages backpressure automatically.
- Router replies with Pong, SubscribeResponse, and fans out data responses.
- Kick allows server-initiated or client-initiated disconnection with full cleanup.

Data plane (implemented):
- Hot feeds: Ticks, Quotes, and Depth/OrderBook use high-priority fanout paths. In addition to framed rkyv, these streams have working shared-memory (SHM) snapshots per (Topic, Key) written by providers and announced to clients via AnnounceShm.
- Lossless streams (Orders, Positions, Account events) continue to use framed rkyv fanout. The Router tolerates transient backpressure and only disconnects a client after sustained backlog beyond an internal threshold to avoid divergence.
- Bars remain on framed rkyv.

---

## 🧩 Key components

### 🚌 Router (server hub)
Location: `crates/bus/src/router.rs`, exported as `tt_bus::Router`.

Responsibilities:
- Owns client sessions and fanout queues.
- Subscription indices:
  - Coarse topic-level interest (Topic).
  - Key-based interest per (Topic, SymbolKey).
- Sharding: internal maps are sharded by a stable hash of (topic, key); configure shard count with `Router::new(N)`.
- Safety: frame length capped to 8 MiB; heartbeat Ping/Pong; clean detach (UnsubscribeAll on disconnect); optional forced kick for backpressure.
- Upstream integration: calls an `UpstreamManager` on first-subscribe/last-unsubscribe to start/stop upstream streams.

Selected APIs:
- `attach_client(UnixStream)`: attach a new client connection.
- `set_backend(Arc<dyn UpstreamManager>)`: link to providers via a manager.
- `publish_*` methods used by providers to emit data (tick/quote/bar/orderbook/etc.).

### 🔌 UpstreamManager and ProviderManager
Location: trait in `crates/bus/src/router.rs`; implementation in `crates/providers/src/manager.rs`.

- `UpstreamManager`: async trait with `subscribe_md` / `unsubscribe_md` for market data streams, plus execution and account operations.
- `ProviderManager` implements `UpstreamManager` and manages provider pairs and worker shards per `ProviderKind`.
- Uses only dyn traits (`MarketDataProvider`, `ExecutionProvider`, `HistoricalDataProvider`). No concrete SDK types appear in server/router code.
- Worker model: an `InprocessWorker` wraps a MD provider and refcounts interest per (topic, key), calling `provider.subscribe_md` on first subscribe and `provider.unsubscribe_md` on last.
- ProviderManager ensures the MD side is connected (`connect_to_market`) the first time a provider pair is created.

### 🔷 ProjectX adapter
Location: `crates/adapters/projectx`

- `PXClient` implements both `MarketDataProvider` and `ExecutionProvider` traits (and historical via `HistoricalDataProvider`).
- Connects to ProjectX HTTP and WebSocket hubs and publishes data into the Router via `tt_bus::Router` `publish_*` methods.
- Market events currently supported:
  - GatewayTrade → `Response::TickBatch`
  - GatewayQuote → `Response::QuoteBatch`
  - GatewayDepth → `Response::OrderBookBatch` (OrderBook snapshots via rkyv)
  - User/account/order/position updates → corresponding batches

### 🖥️ Server main
Location: `crates/server/src/main.rs`

- Loads `.env` and reads `TT_BUS_ADDR` for the UDS path. On Linux, abstract namespace like `@tick-trader.sock` is supported via a custom bind helper.
- Creates `Router::new(8)` (8 shards by default) and registers a `ProviderManager` backend.
- Accept loop: for each UDS connection, spawns `Router::attach_client`.
- The server does not hold concrete provider types; providers publish directly to the Router.

### ⚙️ Engine (single strategy)
Location: `crates/engine`

- `ClientMessageBus` is an in-process bus for a single strategy. It maintains local topic-level interest, forwards Requests to the server, and fans in Responses to the strategy loop.
- `EngineRuntime` wires a `Strategy` implementation and executes callbacks for data and account events.
- The demo strategy binary connects over UDS and issues both topic-level and key-based subscriptions.

---

## 📡 Wire protocol (tt-types)
Location: `crates/types/src/wire.rs`

Requests (subset):
- `Subscribe { topic, latest_only, from_seq }`
- `SubscribeKey { topic, key, latest_only, from_seq }`
- `UnsubscribeKey { topic, key }`
- `UnsubscribeAll { reason }`
- `Ping { ts_ns }`
- `Kick { reason }`
- `InstrumentsRequest { provider, pattern, corr_id }`

Responses (subset):
- `Pong { ts_ns }`
- `SubscribeResponse { topic, instrument, success }`
- `TickBatch | QuoteBatch | BarBatch | OrderBookBatch | VendorData`
- `OrdersBatch | PositionsBatch | AccountDeltaBatch`
- `InstrumentsResponse | InstrumentsMapResponse`

All messages are archived with rkyv and sent as length-delimited frames (`tokio-util` codec). SHM announcements are sent via `Response::AnnounceShm` for eligible topics/keys.

---

## 🔄 Data flow: end-to-end at a glance

1) Client connects over UDS to the server and sends `SubscribeKey(topic, key)`.
2) Router records the subscription and, on first subscriber for that (topic,key), calls `UpstreamManager::subscribe_md`.
3) Provider worker subscribes upstream (via adapter) and starts publishing data using `Router::publish_*`.
4) Router fans out framed responses to all current subscribers of the (topic,key). For hot feeds, a SHM snapshot may also be announced with `AnnounceShm`.
5) On `UnsubscribeKey` (and the set becomes empty), Router calls `UpstreamManager::unsubscribe_md` and the provider stops the stream.

---

## 🚀 Quick start

- Build the workspace and run the server (`tt-server`).
- In a separate terminal, run the demo engine strategy (`tt-engine-test_strategy`) and observe subscriptions and data flowing over UDS.
- Optional: use `InstrumentsRequest` to enumerate instruments exposed by a provider.

Environment tips:
- `TT_BUS_ADDR` controls the UDS path the server binds and clients connect to.
- On Linux, an abstract path (for example, `@tick-trader.sock`) is supported.
- See the repository README for detailed commands and env var examples.

---

## 📘 Glossary

- Router: Central hub for client sessions, subscriptions, and data fanout.
- Topic: A coarse data channel (Ticks, Quotes, Depth, Candles1m, etc.).
- Key: A (Topic, SymbolKey) pair identifying a specific instrument stream.
- ProviderKind: Vendor/source identifier; used to select the right adapter.
- SHM: Shared memory snapshot for hot feeds; announced via `AnnounceShm`.
- rkyv: Zero-copy serialization used for wire messages.

---

Last updated: 2025-10-06
