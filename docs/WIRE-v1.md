# Wire Protocol v1 (current)

This document summarizes the current control-plane and data-plane messages between clients (engine/strategy), the Router, and provider workers.

Status: evolving; legacy variants are still present in code but clients should prefer key-based requests. Credits are abolished; Router manages backpressure.

## Envelopes

All frames are rkyv-serialized `WireMessage::{Request, Response}` using length-delimited framing (max 8 MiB).

### Requests (client → Router)
- Subscribe { topic, latest_only, from_seq }  — coarse topic-level interest
- SubscribeKey { topic, key, latest_only, from_seq }  — precise per-(topic,key)
- UnsubscribeKey { topic, key }
- UnsubscribeAll { }
- Ping { ts_ns }
- Kick { reason: Option<String> }  — client-initiated disconnect
- InstrumentsRequest { provider, pattern, corr_id }

Future (worker control):
- ProviderCmd { … }

### Responses (Router → client)
- TickBatch { topic, seq, ticks }
- QuoteBatch { topic, seq, quotes }
- OrderBookBatch { topic, seq, books }
- BarBatch { topic, seq, bars }
- OrdersBatch { topic, seq, orders }
- PositionsBatch { topic, seq, positions }
- AccountDeltaBatch { topic, seq, deltas }
- AnnounceShm { topic, key, name, layout_ver, size }
- InstrumentsResponse { corr_id, instruments }
- Pong { ts_ns }
- SubscribeResponse { topic, success }

Notes:
- Sequences are per-topic monotonic counters (or timestamp-based if TT_ROUTER_USE_TS_SEQ is set); ordering is preserved per topic.
- Lossless topics (Orders/Positions/AccountEvt) observe tolerant backpressure; Router drops transiently and only disconnects after sustained failures.

## Key-based subscriptions

The Router maintains a sharded index of (topic,key) → {subscribers}. First-subscribe triggers upstream subscription via `UpstreamManager::subscribe_md`, last-unsubscribe triggers `unsubscribe_md` and SHM teardown (if any).

## Shared memory (SHM)

Hot feeds (Ticks/Quotes/Depth) can be published via SHM snapshots. Router emits `AnnounceShm` on first sub and caches it for late subscribers. See SHM-layout.md for details.
