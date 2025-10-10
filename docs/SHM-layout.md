# 📦 SHM Layout and Policy

[![Endianness](https://img.shields.io/badge/Endian-Little--endian-6c757d)](#)
[![Transport](https://img.shields.io/badge/Transport-SHM-blue)](#)
[![Snapshots](https://img.shields.io/badge/Snapshots-rkyv-5c6bc0)](https://github.com/rkyv/rkyv)
[![Access](https://img.shields.io/badge/Access-Seqlock-green)](#)

This document defines the shared memory (SHM) segment format and lifecycle used for hot market data feeds (Ticks, Quotes, Depth/OrderBook).

Policy summary (current):
- Providers/adapters write hot feed snapshots to SHM per (topic,key).
- Router emits Response::AnnounceShm for each (topic,key) when available and caches it for late subscribers.
- Engine prefers SHM for hot feeds: upon AnnounceShm it spawns a polling reader per (topic,key) and does NOT consume duplicate UDS data for those topics while SHM is active.
- UDS remains for control-plane and lossless/account topics, and as fallback if SHM is not announced.

## 🏷️ Segment naming

Segments are file-backed objects created under /dev/shm (or /tmp as a fallback) with names:

```text
ttshm.{Topic}.{Provider}.{Instrument}
```

Example:

```text
ttshm.Depth-ProjectX(TOPSTEP).MNQZ5
```

## 🧱 Header format (little-endian)

| Offset | Size | Field    | Details                                     |
|-------:|-----:|----------|---------------------------------------------|
|      0 |    4 | magic    | ASCII "TSHM"                                |
|      4 |    4 | version  | 1                                           |
|      8 |    4 | capacity | Payload capacity in bytes                   |
|     12 |    4 | seq      | Seqlock sequence counter (odd = writing)    |
|     16 |    4 | len      | Length in bytes of the current payload      |
|     20 |  ... | payload  | rkyv-serialized snapshot bytes              |

## 🔧 Alignment and payload format

- Payload bytes are rkyv-serialized snapshots (e.g., Tick, Bbo, Mbp10).
- Writers serialize into `rkyv::AlignedVec` and write the resulting bytes; this ensures producer-side alignment.
- Readers must ensure alignment before calling `rkyv::from_bytes`:
  - For a simple read, copy the payload into `AlignedVec` and then call `from_bytes`.
  - For memory-mapped snapshots, map with alignment and only then read `Archived<T>` views directly. We plan to provide helper APIs for safe, zero-copy reads from SHM.

## 🔁 Seqlock protocol

✍️ Writers:
- Read `seq`, set it to the next odd value (enter), Store-Release fence.
- Write `len` and payload.
- Store-Release fence, then set `seq` to next even value (exit).

👀 Readers:
- Read `seq`; if odd, retry.
- Read `len` and payload; memory-fence Acquire; re-read `seq`; if changed or odd, retry.

✅ This guarantees readers never observe torn writes.

## 🔐 Permissions

On Unix platforms, segments are created with `0600` permissions.

## ♻️ Lifecycle

- On first subscription for `(topic, key)`, the provider worker may create the SHM segment, write snapshots, and Router emits `AnnounceShm` (cached for late subscribers).
- On last-unsubscribe, the provider worker should stop writing; Router or worker may call `remove_snapshot` to unlink.

## 📏 Sizes

Default snapshot capacities (tunable in code):
- Ticks/Quotes: 64 KiB
- Depth: 256 KiB
