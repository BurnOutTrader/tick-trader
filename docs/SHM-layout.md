# SHM Layout and Policy

This document defines the shared memory (SHM) segment format and lifecycle used for hot market data feeds (Ticks, Quotes, Depth/OrderBook).

## Segment naming

Segments are file-backed objects created under /dev/shm (or /tmp as a fallback) with names:

  ttshm.{Topic}.{Provider}.{Instrument}

Example: `ttshm.Depth-ProjectX(TOPSTEP).MNQZ5`

## Header format (little-endian)

Offset  Size  Field
0       4     magic = "TSHM"
4       4     version = 1
8       4     capacity (bytes) of the payload region
12      4     seq (seqlock sequence)
16      4     len (bytes) of current payload
20      ...   payload bytes (rkyv-serialized snapshot)

## Seqlock protocol

Writers:
- Read seq, set it to the next odd value (enter), Store-Release fence.
- Write len and payload.
- Store-Release fence, then set seq to next even value (exit).

Readers:
- Read seq; if odd, retry.
- Read len and payload; memory-fence Acquire; re-read seq; if changed or odd, retry.

This guarantees readers never observe torn writes.

## Permissions

On Unix platforms, segments are created with 0600 permissions.

## Lifecycle

- On first subscription for (topic,key), the provider worker may create the SHM segment, write snapshots, and Router emits AnnounceShm (cached for late subscribers).
- On last-unsubscribe, the provider worker should stop writing; Router or worker may call remove_snapshot to unlink.

## Sizes

Default snapshot capacities (tunable in code):
- Ticks/Quotes: 64 KiB
- Depth: 256 KiB

