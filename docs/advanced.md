## ⚙️ Router configuration

The Router reads these environment variables at startup:
- TT_ROUTER_SHARDS (default 8): number of shards for key-based indices.
- TT_ROUTER_CLIENT_CHAN (default 1024): per-client outbound queue capacity.
- TT_ROUTER_LOSSLESS_KICK (default 2048): sustained failure threshold before disconnecting a client on lossless streams.
- TT_ROUTER_USE_TS_SEQ (default off): if set, use timestamp-based sequences with a monotonic guard.

