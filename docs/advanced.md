## ⚙️ Router configuration

The Router reads these environment variables at startup:
- TT_ROUTER_SHARDS (default 8): number of shards for key-based indices.
- TT_ROUTER_CLIENT_CHAN (default 1024): per-client outbound queue capacity.
- TT_ROUTER_LOSSLESS_KICK (default 2048): sustained failure threshold before disconnecting a client on lossless streams.
- TT_ROUTER_USE_TS_SEQ (default off): if set, use timestamp-based sequences with a monotonic guard.




---

## 🧬 Engine internals (2025-10)

Hot loop ordering
1. Receive Response (from UDS or SHM-fed mpsc)
2. Update portfolio marks via PortfolioManager::update_apply_last_price
3. Call strategy.on_*(&msg) synchronously
4. Drain command queue (crossbeam::ArrayQueue<Command>) and perform async bus I/O

Command queue
- Type: crossbeam::queue::ArrayQueue<Command>; bounded (default 4096) for backpressure isolation.
- Enqueued by statics helpers (subscriptions::subscribe/unsubscribe, order_placement::place_order) from strategy callbacks.
- Drained between messages and also once immediately after on_start to send early subscribes.

SHM workers
- Workers are spawned on Response::AnnounceShm(topic,key).
- They read aligned rkyv snapshots, decode into concrete structs, and push Response::{Tick,Quote,Mbp10} into the engine intake mpsc.
- They never call the strategy; only the engine task does.

Backpressure hints
- Size cmd_q for a few milliseconds of your worst burst (orders/subs). If full, pushes are dropped by design; log if you need visibility.
- UDS client channels are sized to tolerate brief stalls on lossless streams; sustained backlog triggers a kick to protect the system.

Consistency guarantees
- Marks before callbacks: getters like is_long/is_flat see fresh state corresponding to the current event.
- Single-threaded strategy: the engine is the sole caller; avoid cross-thread mutation of your strategy state.

Optional async helpers
- Discovery: use EngineRuntime::list_instruments(provider, pattern).await (or provider-side tooling) with short timeouts.
- Snapshots: EngineRuntime::last_orders/last_positions/last_accounts for tooling and dashboards.

ASCII map

```
Providers → Router → Client mpsc → Engine loop ─┬─ update marks
                                               ├─ strategy.on_*
                             SHM snapshot ─────┘  (sync, no await)
                                               └─ drain cmd_q → bus I/O (async)
```
