# 🔌 Wire Protocol v1 (current)

[![Framing](https://img.shields.io/badge/Framing-Length--delimited-6c757d)](#)
[![Serialization](https://img.shields.io/badge/Serialization-rkyv-5c6bc0)](https://github.com/rkyv/rkyv)
[![MaxFrame](https://img.shields.io/badge/Max%20Frame-8%20MiB-orange)](#)
[![Planes](https://img.shields.io/badge/Planes-Control%20%26%20Data-blue)](#)

This document summarizes the current control-plane and data-plane messages between clients (engine/strategy), the Router, and provider workers.

> Status: evolving. Key-based subscriptions are the default. Flow credits are abolished; the Router manages throughput and tolerant backpressure.

## ✉️ Envelopes

All frames are rkyv-serialized `WireMessage::{Request, Response}` using length-delimited framing (max 8 MiB).

Alignment and safety:
- Writers serialize into `rkyv::AlignedVec` and then convert to `Vec<u8>`/`bytes::Bytes` for the transport codec.
- Readers copy the incoming frame into an `AlignedVec` before `rkyv::from_bytes` to avoid underaligned archives (UDS framing does not guarantee alignment).
- This keeps the network transport simple while preserving rkyv's alignment guarantees locally.

## 📥 Requests (client → Router)

| Message                 | Fields                                   | Notes                                                        |
|-------------------------|-------------------------------------------|--------------------------------------------------------------|
| `Subscribe`             | `topic`, `latest_only`, `from_seq`        | Coarse topic-level interest                                  |
| `SubscribeKey`          | `topic`, `key`, `latest_only`, `from_seq` | Precise per-(topic, key)                                     |
| `UnsubscribeKey`        | `topic`, `key`                             | Stop interest for the specific key                            |
| `SubscribeAccount`      | `key`                                      | Subscribe all execution streams (orders/positions/account) for account |
| `UnsubscribeAccount`    | `key`                                      | Unsubscribe all execution streams for account                 |
| `UnsubscribeAll`        | —                                         | Clean detach (Router also performs cleanup on disconnect)     |
| `Ping`                  | `ts_ns`                                   | Heartbeat                                                    |
| `Kick`                  | `reason: Option<String>`                  | Client-initiated disconnect                                  |
| `InstrumentsRequest`    | `provider`, `pattern`, `corr_id`          | Discovery                                                    |
| `PlaceOrder`            | `account_id`, `key`, `side`, `qty`, `type`, optional prices, `custom_tag`, optional `stop_loss`/`take_profit` | Typed order placement |
| `CancelOrder`           | `account_id`, `provider_order_id?`, `client_order_id?` | Typed cancel                                   |
| `ReplaceOrder`          | `account_id`, ids, `new_qty?`, `new_limit_price?`, `new_stop_price?`, `new_trail_price?` | Typed modify |

## 📤 Responses (Router → client)

| Message               | Fields                                                                   | Purpose/Notes                                                                   |
|-----------------------|---------------------------------------------------------------------------|----------------------------------------------------------------------------------|
| `TickBatch`           | `topic`, `seq`, `ticks`                                                   | Hot feed batch                                                                   |
| `QuoteBatch`          | `topic`, `seq`, `quotes`                                                  | Hot feed batch                                                                   |
| `OrderBookBatch`      | `topic`, `seq`, `books`                                                   | Depth snapshots/updates                                                          |
| `BarBatch`            | `topic`, `seq`, `bars`                                                    | OHLC bars                                                                        |
| `OrdersBatch`         | `topic`, `seq`, `orders`                                                  | Lossless                                                                         |
| `PositionsBatch`      | `topic`, `seq`, `positions`                                               | Lossless                                                                         |
| `AccountDeltaBatch`   | `topic`, `seq`, `deltas`                                                  | Lossless                                                                         |
| `AnnounceShm`         | `topic`, `key`, `name`, `layout_ver`, `size`                              | SHM snapshot announcement (cached for late subscribers)                          |
| `InstrumentsResponse` | `corr_id`, `instruments`                                                  | Discovery                                                                        |
| `Pong`                | `ts_ns`                                                                   | Heartbeat                                                                        |
| `SubscribeResponse`   | `topic`, `instrument`, `success`                                          | Ack for subscribe requests                                                       |

Notes:
- Sequences are per-topic monotonic counters. Optionally, set `TT_ROUTER_USE_TS_SEQ=1` to use timestamp-based sequences with a monotonic guard.
- Lossless topics (Orders/Positions/AccountEvt) use tolerant backpressure: Router drops transiently for slow clients and only disconnects after sustained failures.

## 🔑 Key-based subscriptions

The Router maintains a sharded index of `(topic, key) → {subscribers}`. First-subscribe triggers upstream subscription via `UpstreamManager::subscribe_md`, last-unsubscribe triggers `unsubscribe_md`. If SHM is used for a stream, Router caches and replays the latest `AnnounceShm` to new subscribers.

## 🧮 Shared memory (SHM)

Hot feeds (Ticks/Quotes/Depth) can be published via SHM snapshots. Router emits `AnnounceShm` on first sub and caches it for late subscribers. See `docs/SHM-layout.md` for details.

## ⚙️ Router configuration

Environment variables influence Router behavior:
- `TT_ROUTER_SHARDS` (default 8): number of sharded indices.
- `TT_ROUTER_CLIENT_CHAN` (default 1024): per-client outbound queue capacity.
- `TT_ROUTER_LOSSLESS_KICK` (default 2048): sustained send-failure threshold before disconnecting a client on lossless topics.
- `TT_ROUTER_USE_TS_SEQ` (default off): if set, use timestamp-based sequences with monotonic guard.
