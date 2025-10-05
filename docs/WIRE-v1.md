# 🔌 Wire Protocol v1 (current)

[![Framing](https://img.shields.io/badge/Framing-Length--delimited-6c757d)](#)
[![Serialization](https://img.shields.io/badge/Serialization-rkyv-5c6bc0)](https://github.com/rkyv/rkyv)
[![MaxFrame](https://img.shields.io/badge/Max%20Frame-8%20MiB-orange)](#)
[![Planes](https://img.shields.io/badge/Planes-Control%20%26%20Data-blue)](#)

This document summarizes the current control-plane and data-plane messages between clients (engine/strategy), the Router, and provider workers.

> Status: evolving; legacy variants are still present in code but clients should prefer key-based requests. Credits are abolished; Router manages backpressure.

## ✉️ Envelopes

All frames are rkyv-serialized `WireMessage::{Request, Response}` using length-delimited framing (max 8 MiB).

## 📥 Requests (client → Router)

| Message                 | Fields                                   | Notes                                   |
|-------------------------|-------------------------------------------|-----------------------------------------|
| `Subscribe`             | `topic`, `latest_only`, `from_seq`        | Coarse topic-level interest              |
| `SubscribeKey`          | `topic`, `key`, `latest_only`, `from_seq` | Precise per-(topic, key)                 |
| `UnsubscribeKey`        | `topic`, `key`                             | Stop interest for the specific key       |
| `UnsubscribeAll`        | —                                         | Clean detach                             |
| `Ping`                  | `ts_ns`                                   | Heartbeat                                |
| `Kick`                  | `reason: Option<String>`                  | Client-initiated disconnect              |
| `InstrumentsRequest`    | `provider`, `pattern`, `corr_id`          | Discovery                                |
| `ProviderCmd` (future)  | `…`                                       | Worker control (reserved)                |

## 📤 Responses (Router → client)

| Message               | Fields                             | Purpose/Notes                                                                   |
|-----------------------|-------------------------------------|----------------------------------------------------------------------------------|
| `TickBatch`           | `topic`, `seq`, `ticks`             | Hot feed batch                                                                   |
| `QuoteBatch`          | `topic`, `seq`, `quotes`            | Hot feed batch                                                                   |
| `OrderBookBatch`      | `topic`, `seq`, `books`             | Depth snapshots/updates                                                          |
| `BarBatch`            | `topic`, `seq`, `bars`              | OHLC bars                                                                        |
| `OrdersBatch`         | `topic`, `seq`, `orders`            | Lossless                                                                         |
| `PositionsBatch`      | `topic`, `seq`, `positions`         | Lossless                                                                         |
| `AccountDeltaBatch`   | `topic`, `seq`, `deltas`            | Lossless                                                                         |
| `AnnounceShm`         | `topic`, `key`, `name`, `layout_ver`, `size` | SHM snapshot announcement (cached for late subscribers)                      |
| `InstrumentsResponse` | `corr_id`, `instruments`            | Discovery                                                                        |
| `Pong`                | `ts_ns`                             | Heartbeat                                                                        |
| `SubscribeResponse`   | `topic`, `success`                  | Ack for subscribe requests                                                       |

Notes:
- Sequences are per-topic monotonic counters (or timestamp-based if `TT_ROUTER_USE_TS_SEQ` is set); ordering is preserved per topic.
- Lossless topics (Orders/Positions/AccountEvt) observe tolerant backpressure; Router drops transiently and only disconnects after sustained failures.

## 🔑 Key-based subscriptions

The Router maintains a sharded index of `(topic, key) → {subscribers}`. First-subscribe triggers upstream subscription via `UpstreamManager::subscribe_md`, last-unsubscribe triggers `unsubscribe_md` and SHM teardown (if any).

## 🧮 Shared memory (SHM)

Hot feeds (Ticks/Quotes/Depth) can be published via SHM snapshots. Router emits `AnnounceShm` on first sub and caches it for late subscribers. See `docs/SHM-layout.md` for details.
