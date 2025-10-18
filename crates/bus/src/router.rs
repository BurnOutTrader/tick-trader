#[cfg(feature = "server")]
use crate::metrics::METRICS;
use ahash::AHashMap;
use anyhow::Result;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{error, info, trace, warn};
use tt_database::init::{Connection, init_db};
use tt_types::data::core::{DateTime, Utc};
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::wire::Bytes as WireBytes;
use tt_types::wire::{Request, Response, WireMessage};

const LOSSLESS_BP_KICK_THRESHOLD: u32 = 2048;

#[derive(Clone, Debug)]
pub struct RouterConfig {
    pub shards: usize,
    pub client_chan_capacity: usize,
    pub lossless_kick_threshold: u32,
    // warmup window after SubscribeKey during which we avoid early drops by awaiting send
    pub warmup_ms: u64,
    // per-message awaited send timeout during warmup
    pub warmup_send_ms: u64,
}

impl Default for RouterConfig {
    fn default() -> Self {
        // Defaults chosen to be safe and broadly applicable; can be overridden via env or caller
        let shards = std::env::var("TT_ROUTER_SHARDS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8);
        let client_chan_capacity = std::env::var("TT_ROUTER_CLIENT_CHAN")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(1024);
        let lossless_kick_threshold = std::env::var("TT_ROUTER_LOSSLESS_KICK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(LOSSLESS_BP_KICK_THRESHOLD);
        let warmup_ms = std::env::var("TT_ROUTER_WARMUP_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(2000);
        let warmup_send_ms = std::env::var("TT_ROUTER_WARMUP_SEND_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        Self {
            shards: shards.max(1),
            client_chan_capacity,
            lossless_kick_threshold,
            warmup_ms,
            warmup_send_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubId(pub(crate) u64);

#[derive(Debug, Clone)]
pub struct Subscriber {
    pub id: SubId,
    pub topics: HashSet<Topic>,
}

#[derive(Debug, Clone)]
pub struct SubscriberMeta {
    pub id: SubId,
    // control-plane topics currently subscribed (coarse)
    pub topics: HashSet<Topic>,
}

#[derive(Clone)]
pub struct Router {
    subscribers: Arc<DashMap<SubId, mpsc::Sender<Response>>>,
    meta: Arc<DashMap<SubId, SubscriberMeta>>,
    next_id: Arc<AtomicU64>,
    // sequences per topic
    seq_by_topic: Arc<DashMap<Topic, u64>>,

    // Config
    cfg: RouterConfig,

    // Sharded indices for key-based routing
    shards: usize,
    sub_index: Vec<DashMap<(Topic, SymbolKey), HashSet<SubId>>>,
    // Cached SHM announcements per (topic,key) to deliver to late subscribers
    shm_announces: Arc<DashMap<(Topic, SymbolKey), tt_types::wire::AnnounceShm>>,
    // Optional upstream manager to inform providers on first/last subscription
    backend: Arc<std::sync::RwLock<Option<Arc<dyn UpstreamManager>>>>,
    // per-client kick channel to forcefully disconnect slow/unhealthy clients
    kickers: Arc<DashMap<SubId, oneshot::Sender<()>>>,
    // backpressure counters per client (for lossless topics)
    backpressure: Arc<DashMap<SubId, u32>>,
    // warmup tracking: for a brief window after SubscribeKey, use an await send to avoid early drops
    recent_key_subs: Arc<DashMap<(Topic, SymbolKey, SubId), std::time::Instant>>,
    // Account-interest tracking for execution streams
    account_subs: Arc<DashMap<AccountKey, HashSet<SubId>>>,
    accounts_by_client: Arc<DashMap<SubId, HashSet<AccountKey>>>,
    connection: Connection,
}

#[async_trait::async_trait]
pub trait UpstreamManager: Send + Sync {
    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()>;
    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()>;
    // Execution path: account stream subscriptions
    async fn subscribe_account(&self, key: tt_types::keys::AccountKey) -> Result<()>;
    async fn unsubscribe_account(&self, key: tt_types::keys::AccountKey) -> Result<()>;
    // Orders API
    async fn place_order(&self, spec: tt_types::wire::PlaceOrder) -> Result<()>;
    async fn cancel_order(&self, spec: tt_types::wire::CancelOrder) -> Result<()>;
    async fn replace_order(&self, spec: tt_types::wire::ReplaceOrder) -> Result<()>;
    // Startup account info
    async fn get_account_info(
        &self,
        provider: tt_types::providers::ProviderKind,
    ) -> Result<tt_types::wire::AccountInfoResponse>;
    // Instruments listing
    async fn get_instruments(
        &self,
        provider: tt_types::providers::ProviderKind,
        pattern: Option<String>,
    ) -> Result<Vec<tt_types::securities::symbols::Instrument>>;
    // Full contracts map
    async fn get_securities(
        &self,
        provider: tt_types::providers::ProviderKind,
    ) -> Result<Vec<FuturesContract>>;

    async fn update_historical_latest_by_key(
        &self,
        provider: tt_types::providers::ProviderKind,
        topic: Topic,
        instrument: tt_types::securities::symbols::Instrument,
    ) -> anyhow::Result<Option<DateTime<Utc>>>;
}

impl Router {
    pub fn new(shards: usize) -> Self {
        let connection = init_db().unwrap();
        let mut cfg = RouterConfig::default();
        if shards > 0 {
            cfg.shards = shards;
        }
        Self::new_with_config(cfg, connection)
    }

    pub fn new_with_config(cfg: RouterConfig, connection: Connection) -> Self {
        let shards = cfg.shards.max(1);
        let mut sub_index = Vec::with_capacity(shards);
        for _ in 0..shards {
            sub_index.push(DashMap::new());
        }
        Self {
            subscribers: Arc::new(DashMap::new()),
            meta: Arc::new(DashMap::new()),
            next_id: Arc::new(AtomicU64::new(1)),
            seq_by_topic: Arc::new(DashMap::new()),
            cfg,
            shards,
            sub_index,
            shm_announces: Arc::new(DashMap::new()),
            backend: Arc::new(std::sync::RwLock::new(None)),
            kickers: Arc::new(DashMap::new()),
            backpressure: Arc::new(DashMap::new()),
            recent_key_subs: Arc::new(DashMap::new()),
            account_subs: Arc::new(DashMap::new()),
            accounts_by_client: Arc::new(DashMap::new()),
            connection,
        }
    }

    // Backend manager used to forward first-sub/last-unsub upstream controls
    pub fn set_backend(&self, mgr: Arc<dyn UpstreamManager>) {
        let mut g = self.backend.write().unwrap();
        *g = Some(mgr);
    }

    pub async fn attach_client(&self, sock: UnixStream) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<Response>(self.cfg.client_chan_capacity);
        let id = SubId(self.next_id.fetch_add(1, Ordering::SeqCst));
        self.subscribers.insert(id.clone(), tx);
        self.meta.insert(
            id.clone(),
            SubscriberMeta {
                id: id.clone(),
                topics: HashSet::new(),
            },
        );

        let (read_half, write_half) = sock.into_split();
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(8 * 1024 * 1024)
            .new_codec();
        let mut framed_reader = FramedRead::new(read_half, codec.clone());
        let mut framed_writer = FramedWrite::new(write_half, codec);

        // writer task: drain per-subscriber queue
        let subs = self.subscribers.clone();
        let writer = tokio::spawn(async move {
            while let Some(resp) = rx.recv().await {
                let wire = WireMessage::Response(resp);
                let aligned = wire.to_aligned_bytes();
                let vec: Vec<u8> = aligned.into();
                if let Err(_e) = framed_writer.send(bytes::Bytes::from(vec)).await {
                    break;
                }
            }
            // on writer exit, nothing specific to clean here; reader will handle detach
            drop(subs);
        });

        // reader loop: support external kick to forcefully disconnect
        let (kick_tx, mut kick_rx) = oneshot::channel::<()>();
        self.kickers.insert(id.clone(), kick_tx);
        loop {
            tokio::select! {
                biased;
                _ = &mut kick_rx => {
                    // forced disconnect
                    break;
                }
                maybe_item = framed_reader.next() => {
                    let Some(item) = maybe_item else {
                        info!(sub_id = %id.0, "router: client disconnected (EOF)");
                        break
                    };
                    match item {
                        Ok(bytes) => {
                            match WireMessage::from_bytes(bytes.as_ref()) {
                                Ok(WireMessage::Request(req)) => {
                                    self.handle_request(&id, req).await?;
                                }
                                Ok(_) => { /* ignore unexpected direction */ }
                                Err(e) => {
                                    warn!(sub_id = %id.0, error = %e, "router: decode error; skipping frame");
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            warn!(sub_id = %id.0, error = %e, "router: read error; disconnecting");
                            break;
                        }
                    }
                }
            }
        }
        // Clean detach: unsubscribe-all and drop sender
        self.unsubscribe_all(&id).await;
        self.subscribers.remove(&id);
        self.kickers.remove(&id);
        self.backpressure.remove(&id);
        let _ = writer.await;
        Ok(())
    }

    async fn handle_request(&self, id: &SubId, req: Request) -> Result<()> {
        match req {
            Request::Subscribe(s) => {
                // Track topic; credits are server-managed (abolished for clients)
                if let Some(mut meta) = self.meta.get_mut(id) {
                    meta.topics.insert(s.topic());
                }
                info!(sub_id = %id.0, topic = ?s.topic(), "router.subscribe");
            }
            Request::SubscribeKey(s) => {
                let topic0 = s.topic;
                let key0 = s.key.clone();
                let instrument0 = s.key.instrument.clone();
                let shard = self.shard_idx(topic0, &key0);
                let key_tuple = (topic0, key0.clone());
                let (is_first, subs_count) = {
                    let mut entry = self.sub_index[shard].entry(key_tuple.clone()).or_default();
                    let was_empty = entry.is_empty();
                    entry.insert(id.clone());
                    (was_empty, entry.len())
                };
                // mark this (topic,key,sub) as recently subscribed to allow warmup delivery
                self.recent_key_subs.insert(
                    (topic0, key0.clone(), id.clone()),
                    std::time::Instant::now(),
                );
                info!(sub_id = %id.0, topic = ?topic0, key = %format!("{}@{:?}", instrument0, key0.provider), shard = shard, subs = subs_count, "router.subscribe_key");
                if is_first {
                    if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                        let router = self.clone();
                        let id2 = id.clone();
                        let key_for_spawn = key0.clone();
                        let instrument_for_resp = instrument0.clone();
                        tokio::spawn(async move {
                            let ok = mgr.subscribe_md(topic0, &key_for_spawn).await.is_ok();
                            let _ = router
                                .send_to(
                                    &id2,
                                    Response::SubscribeResponse {
                                        topic: topic0,
                                        instrument: instrument_for_resp,
                                        success: ok,
                                    },
                                )
                                .await;
                        });
                    } else {
                        // No backend; consider success as true for local subscriptions
                        error!("router.subscribe_key: no backend manager");
                        let _ = self
                            .send_to(
                                id,
                                Response::SubscribeResponse {
                                    topic: topic0,
                                    instrument: instrument0.clone(),
                                    success: false,
                                },
                            )
                            .await;
                    }
                } else {
                    // Not first subscriber; upstream already active. Acknowledge success to caller.
                    let _ = self
                        .send_to(
                            id,
                            Response::SubscribeResponse {
                                topic: topic0,
                                instrument: instrument0.clone(),
                                success: true,
                            },
                        )
                        .await;
                }
                // If there is a prior SHM announcement for this (topic,key), send it to the new subscriber now.
                if let Some(ann) = self.shm_announces.get(&(topic0, key0.clone())) {
                    let _ = self
                        .send_to(id, Response::AnnounceShm(ann.value().clone()))
                        .await;
                }
            }
            Request::UnsubscribeKey(u) => {
                let shard = self.shard_idx(u.topic, &u.key);
                let key1 = u.key.clone();
                let mut last = false;
                if let Some(mut set) = self.sub_index[shard].get_mut(&(u.topic, key1)) {
                    set.remove(id);
                    if set.is_empty() {
                        last = true;
                    }
                    if set.is_empty() {
                        drop(set);
                        let _ = self.sub_index[shard].remove(&(u.topic, u.key.clone()));
                    }
                }
                if last && let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    let topic = u.topic;
                    let key = u.key.clone();
                    tokio::spawn(async move {
                        let _ = mgr.unsubscribe_md(topic, &key).await;
                    });
                }
            }
            Request::Ping(p) => {
                let _ = self
                    .send_to(id, Response::Pong(tt_types::wire::Pong { ts_ns: p.ts_ns }))
                    .await;
            }
            Request::UnsubscribeAll(_u) => {
                info!(sub_id = %id.0, "router.unsubscribe_all");
                self.unsubscribe_all(id).await;
            }
            Request::Kick(k) => {
                let reason = k.reason.as_deref().unwrap_or("client requested");
                info!(sub_id = %id.0, reason, "router.kick_request");
                self.kick_client(id, reason);
            }
            Request::PlaceOrder(spec) => {
                if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    tokio::spawn(async move {
                        let _ = mgr.place_order(spec).await;
                    });
                } else {
                    warn!(sub_id = %id.0, "router.place_order: no backend manager");
                }
            }
            Request::CancelOrder(spec) => {
                if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    tokio::spawn(async move {
                        let _ = mgr.cancel_order(spec).await;
                    });
                } else {
                    warn!(sub_id = %id.0, "router.cancel_order: no backend manager");
                }
            }
            Request::ReplaceOrder(spec) => {
                if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    tokio::spawn(async move {
                        let _ = mgr.replace_order(spec).await;
                    });
                } else {
                    warn!(sub_id = %id.0, "router.replace_order: no backend manager");
                }
            }
            Request::SubscribeAccount(sa) => {
                // Mark coarse interest in account-related topics for this client
                if let Some(mut meta) = self.meta.get_mut(id) {
                    meta.topics.insert(Topic::Orders);
                    meta.topics.insert(Topic::Positions);
                    meta.topics.insert(Topic::AccountEvt);
                    meta.topics.insert(Topic::Fills);
                }
                let key = sa.key.clone();
                // Track per-client accounts
                {
                    let mut set = self.accounts_by_client.entry(id.clone()).or_default();
                    set.insert(key.clone());
                }
                // Track global subscribers for this account; call backend only when first appears
                let is_first = {
                    let mut entry = self.account_subs.entry(key.clone()).or_default();
                    let was_empty = entry.is_empty();
                    entry.insert(id.clone());
                    was_empty
                };
                info!(sub_id = %id.0, account = %format!("{:?}", key), first = is_first, "router.subscribe_account");
                if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    if is_first {
                        let key2 = key.clone();
                        tokio::spawn(async move {
                            let _ = mgr.subscribe_account(key2).await;
                        });
                    }
                } else {
                    warn!(sub_id = %id.0, "router.subscribe_account: no backend manager");
                }
            }
            Request::UnsubscribeAccount(ua) => {
                let key = ua.key.clone();
                // Remove from this client's account set and, if empty, drop coarse topics
                let mut client_empty = false;
                if let Some(mut set) = self.accounts_by_client.get_mut(id) {
                    set.remove(&key);
                    client_empty = set.is_empty();
                }
                if client_empty && let Some(mut meta) = self.meta.get_mut(id) {
                    meta.topics.remove(&Topic::Orders);
                    meta.topics.remove(&Topic::Positions);
                    meta.topics.remove(&Topic::AccountEvt);
                }
                // Update global subscribers set for this account
                let mut last = false;
                if let Some(mut set) = self.account_subs.get_mut(&key) {
                    set.remove(id);
                    if set.is_empty() {
                        last = true;
                        drop(set);
                        let _ = self.account_subs.remove(&key);
                    }
                }
                info!(sub_id = %id.0, account = %format!("{:?}", key), last = last, "router.unsubscribe_account");
                if last {
                    if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                        let key2 = key.clone();
                        tokio::spawn(async move {
                            let _ = mgr.unsubscribe_account(key2).await;
                        });
                    } else {
                        warn!(sub_id = %id.0, "router.unsubscribe_account: no backend manager");
                    }
                }
            }
            Request::AccountInfoRequest(req_ai) => {
                if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    let prov = req_ai.provider;
                    let corr = req_ai.corr_id;
                    let router = self.clone();
                    let id2 = id.clone();
                    tokio::spawn(async move {
                        match mgr.get_account_info(prov).await {
                            Ok(mut resp) => {
                                resp.corr_id = corr;
                                let _ = router
                                    .send_to(&id2, Response::AccountInfoResponse(resp))
                                    .await;
                            }
                            Err(e) => {
                                warn!(sub_id = %id2.0, error = %e, "router.account_info: backend error");
                                let empty = tt_types::wire::AccountInfoResponse {
                                    provider: prov,
                                    corr_id: corr,
                                    accounts: Vec::new(),
                                };
                                let _ = router
                                    .send_to(&id2, Response::AccountInfoResponse(empty))
                                    .await;
                            }
                        }
                    });
                } else {
                    warn!(sub_id = %id.0, "router.account_info: no backend manager");
                    let empty = tt_types::wire::AccountInfoResponse {
                        provider: req_ai.provider,
                        corr_id: req_ai.corr_id,
                        accounts: Vec::new(),
                    };
                    let _ = self.send_to(id, Response::AccountInfoResponse(empty)).await;
                }
            }
            Request::InstrumentsRequest(req_inst) => {
                info!(sub_id = %id.0, provider = ?req_inst.provider, "router.instruments_request");
                if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    let prov = req_inst.provider;
                    let patt = req_inst.pattern.clone();
                    let corr = req_inst.corr_id;
                    let router = self.clone();
                    let id2 = id.clone();
                    tokio::spawn(async move {
                        match mgr.get_instruments(prov, patt).await {
                            Ok(list) => {
                                let resp = tt_types::wire::InstrumentsResponse {
                                    provider: prov,
                                    instruments: list,
                                    corr_id: corr,
                                };
                                let _ = router
                                    .send_to(&id2, Response::InstrumentsResponse(resp))
                                    .await;
                            }
                            Err(e) => {
                                warn!(sub_id = %id2.0, error = %e, "router.instruments_request: backend error");
                                let resp = tt_types::wire::InstrumentsResponse {
                                    provider: prov,
                                    instruments: Vec::new(),
                                    corr_id: corr,
                                };
                                let _ = router
                                    .send_to(&id2, Response::InstrumentsResponse(resp))
                                    .await;
                            }
                        }
                    });
                } else {
                    warn!(sub_id = %id.0, "router.instruments_request: no backend manager");
                    let resp = tt_types::wire::InstrumentsResponse {
                        provider: req_inst.provider,
                        instruments: Vec::new(),
                        corr_id: req_inst.corr_id,
                    };
                    let _ = self.send_to(id, Response::InstrumentsResponse(resp)).await;
                }
            }
            Request::DbUpdateKeyLatest(req) => {
                info!(sub_id = %id.0, provider = ?req.provider, instrument=%req.instrument.to_string(), topic=?req.topic, "router.db_update_key_latest");
                if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    let router = self.clone();
                    let id2 = id.clone();
                    tokio::spawn(async move {
                        let corr = req.corr_id;
                        let prov = req.provider;
                        let instr = req.instrument.clone();
                        let topic = req.topic;
                        let res = mgr
                            .update_historical_latest_by_key(prov, topic, instr.clone())
                            .await;
                        let (success, error_msg,latest_time) = match res {
                            Ok(t) => (true, None, t),
                            Err(e) => (false, Some(e.to_string()), None),
                        };
                        let _ = router
                            .send_to(
                                &id2,
                                Response::DbUpdateComplete {
                                    provider: prov,
                                    instrument: instr,
                                    topic,
                                    corr_id: corr,
                                    success,
                                    error_msg,
                                    latest_time
                                },
                            )
                            .await;
                    });
                } else {
                    warn!(sub_id = %id.0, "router.db_update_key_latest: no backend manager");
                    let _ = self
                        .send_to(
                            id,
                            Response::DbUpdateComplete {
                                provider: req.provider,
                                instrument: req.instrument,
                                topic: req.topic,
                                corr_id: req.corr_id,
                                success: false,
                                error_msg: Some("no backend manager".into()),
                                latest_time: None
                            },
                        )
                        .await;
                }
            }
            Request::InstrumentsMapRequest(req_map) => {
                info!(sub_id = %id.0, provider = ?req_map.provider, "router.instruments_map_request");
                if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                    let prov = req_map.provider;
                    let corr = req_map.corr_id;
                    let router = self.clone();
                    let id2 = id.clone();
                    let connection = self.connection.clone();
                    tokio::spawn(async move {
                        match mgr.get_securities(prov).await {
                            Ok(pairs) => {
                                let resp = tt_types::wire::ContractsResponse {
                                    provider: format!("{:?}", prov),
                                    contracts: pairs.clone(),
                                    corr_id: corr,
                                };
                                let _ = router
                                    .send_to(&id2, Response::InstrumentsMapResponse(resp))
                                    .await;

                                let mut map = AHashMap::new();
                                for c in pairs {
                                    map.insert(c.instrument.clone(), c);
                                }
                                // Ensure schema exists before we try to persist contracts (idempotent)
                                if let Err(e) =
                                    tt_database::schema::ensure_schema(&connection).await
                                {
                                    error!(
                                        "Error ensuring schema before contract map ingest: {}",
                                        e
                                    );
                                }
                                if let Err(e) = tt_database::ingest::ingest_contracts_map(
                                    &connection,
                                    prov,
                                    map,
                                )
                                .await
                                {
                                    error!("Error injecting contract map: {}", e);
                                }
                            }
                            Err(e) => {
                                warn!(sub_id = %id2.0, error = %e, "router.instruments_map_request: backend error");
                                let resp = tt_types::wire::ContractsResponse {
                                    provider: format!("{:?}", prov),
                                    contracts: Vec::new(),
                                    corr_id: corr,
                                };
                                let _ = router
                                    .send_to(&id2, Response::InstrumentsMapResponse(resp))
                                    .await;
                            }
                        }
                    });
                } else {
                    warn!(sub_id = %id.0, "router.instruments_map_request: no backend manager");
                    let resp = tt_types::wire::ContractsResponse {
                        provider: format!("{:?}", req_map.provider),
                        contracts: Vec::new(),
                        corr_id: req_map.corr_id,
                    };
                    let _ = self
                        .send_to(id, Response::InstrumentsMapResponse(resp))
                        .await;
                }
            }
            #[allow(unreachable_patterns)]
            _ => unreachable!(),
        }
        Ok(())
    }

    async fn unsubscribe_all(&self, id: &SubId) {
        // Clear coarse topic interests
        if let Some(mut meta) = self.meta.get_mut(id) {
            meta.topics.clear();
        }
        // Clear backpressure counter for this client
        self.backpressure.remove(id);

        // Handle account-interest cleanup: remove this client from all accounts it tracked
        let accounts: Option<HashSet<AccountKey>> =
            self.accounts_by_client.remove(id).map(|(_k, v)| v);
        if let Some(accs) = accounts {
            // For each account, remove client from global set and call backend if last
            let mut to_unsub: Vec<AccountKey> = Vec::new();
            for key in accs.iter() {
                if let Some(mut set) = self.account_subs.get_mut(key) {
                    set.remove(id);
                    if set.is_empty() {
                        to_unsub.push(key.clone());
                        drop(set);
                        let _ = self.account_subs.remove(key);
                    }
                }
            }
            if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                for key in to_unsub {
                    tokio::spawn({
                        let mgr = mgr.clone();
                        async move {
                            let _ = mgr.unsubscribe_account(key).await;
                        }
                    });
                }
            }
        }

        // Remove from key-based indices and credits across all shards
        for shard in 0..self.shards {
            // Track which (topic,key) become empty after removal to inform upstream
            let mut became_empty: Vec<(Topic, SymbolKey)> = Vec::new();
            // Remove sub from all sets
            for mut entry in self.sub_index[shard].iter_mut() {
                if entry.value_mut().remove(id) && entry.value().is_empty() {
                    became_empty.push(entry.key().clone());
                }
            }
            // Drop any empty sets to keep maps tidy
            let empty_keys: Vec<_> = self.sub_index[shard]
                .iter()
                .filter(|e| e.value().is_empty())
                .map(|e| e.key().clone())
                .collect();
            for k in empty_keys {
                let _ = self.sub_index[shard].remove(&k);
            }

            // Inform upstream for streams that closed due to this detach
            if let Some(mgr) = self.backend.read().unwrap().as_ref().cloned() {
                for (topic, key) in became_empty {
                    tokio::spawn({
                        let mgr = mgr.clone();
                        async move {
                            let _ = mgr.unsubscribe_md(topic, &key).await;
                        }
                    });
                }
            }
        }
    }

    fn shard_idx(&self, topic: Topic, key: &SymbolKey) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        topic.hash(&mut h);
        key.hash(&mut h);
        (h.finish() as usize) % self.shards
    }

    pub async fn send_to(&self, id: &SubId, resp: Response) -> Result<()> {
        if let Some(tx) = self.subscribers.get(id).map(|r| r.value().clone()) {
            let _ = tx.send(resp).await;
        }
        Ok(())
    }

    fn kick_client(&self, id: &SubId, reason: &str) {
        warn!(sub_id = %id.0, reason, "router.kick_client");
        if let Some((_k, tx)) = self.kickers.remove(id) {
            let _ = tx.send(());
        }
    }

    pub async fn publish_for_topic<F>(&self, topic: Topic, build: F) -> Result<()>
    where
        F: FnOnce(u64) -> Response,
    {
        // assign sequence
        let seq = {
            let mut e = self.seq_by_topic.entry(topic).or_insert(0);
            *e += 1;
            *e
        };
        let resp = build(seq);

        // Collect eligible recipients
        let mut to_send: Vec<(SubId, mpsc::Sender<Response>)> = Vec::new();
        for entry in self.subscribers.iter() {
            let sid = entry.key().clone();
            let tx = entry.value().clone();
            if let Some(meta) = self.meta.get(&sid) {
                if !meta.topics.contains(&topic) {
                    continue;
                }
                // credits are server-managed; no gating here
                to_send.push((sid, tx));
            }
        }
        let mut sent = 0usize;
        let mut dropped = 0usize;
        let is_lossless = matches!(
            topic,
            Topic::Orders | Topic::Positions | Topic::AccountEvt | Topic::Fills
        );
        for (sid, tx) in to_send {
            if let Some(_meta) = self.meta.get_mut(&sid) {
                if tx.try_send(resp.clone()).is_ok() {
                    sent += 1;
                    // On successful delivery, reset backpressure counter
                    let _ = self.backpressure.remove(&sid);
                } else if is_lossless {
                    // Tolerate transient backpressure: increment a counter and only kick
                    // the client if sustained failures exceed a high threshold.
                    #[allow(unused_assignments)]
                    let mut count = 0u32;
                    if let Some(mut e) = self.backpressure.get_mut(&sid) {
                        *e += 1;
                        count = *e;
                    } else {
                        self.backpressure.insert(sid.clone(), 1);
                        count = 1;
                    }
                    if count > self.cfg.lossless_kick_threshold {
                        self.kick_client(&sid, "lossless backpressure threshold exceeded");
                    } else {
                        // best-effort: drop this batch for this client
                        dropped += 1;
                        trace!(sub_id = %sid.0, count, "router: dropping lossless batch due to transient backpressure");
                    }
                } else {
                    // lossy topics: drop silently
                    dropped += 1;
                }
            }
        }
        trace!(?topic, seq, sent, dropped, "router.publish_for_topic");
        #[cfg(feature = "server")]
        {
            METRICS.inc_sent(sent as u64);
            METRICS.inc_dropped(dropped as u64);
        }
        Ok(())
    }

    pub async fn publish_for_key<F>(&self, topic: Topic, key: &SymbolKey, build: F) -> Result<()>
    where
        F: FnOnce(u64) -> Response,
    {
        // assign sequence using per-topic counter to keep ordering per topic
        let seq = {
            let mut e = self.seq_by_topic.entry(topic).or_insert(0);
            *e += 1;
            *e
        };
        let resp = build(seq);
        let shard = self.shard_idx(topic, key);
        let mut to_send: Vec<(SubId, mpsc::Sender<Response>)> = Vec::new();
        let mut seen: HashSet<u64> = HashSet::new();
        // 1) Key-specific subscribers
        if let Some(set) = self.sub_index[shard].get(&(topic, key.clone())) {
            for sid in set.iter() {
                if let Some(tx) = self.subscribers.get(sid).map(|r| r.value().clone()) {
                    to_send.push((sid.clone(), tx));
                    seen.insert(sid.0);
                }
            }
        }
        // 2) Topic-wide subscribers (wildcard for all keys)
        for entry in self.subscribers.iter() {
            let sid = entry.key().clone();
            if seen.contains(&sid.0) {
                continue;
            }
            if let Some(meta) = self.meta.get(&sid) {
                if !meta.topics.contains(&topic) {
                    continue;
                }
                to_send.push((sid.clone(), entry.value().clone()));
                seen.insert(sid.0);
            }
        }
        let mut sent = 0usize;
        let mut dropped = 0usize;
        let candidates = to_send.len();
        for (sid, tx) in to_send {
            // Warmup path: right after a SubscribeKey for this (topic,key,sub), try an awaited send
            // for a very short period to avoid dropping the very first updates before the client
            // has fully started draining.
            let mut delivered = false;
            if let Some(ts) = self.recent_key_subs.get(&(topic, key.clone(), sid.clone()))
                && ts.elapsed() <= std::time::Duration::from_millis(self.cfg.warmup_ms)
            {
                // Small timeout to avoid blocking the publisher pipeline
                match tokio::time::timeout(
                    std::time::Duration::from_millis(self.cfg.warmup_send_ms),
                    tx.send(resp.clone()),
                )
                .await
                {
                    Ok(Ok(())) => {
                        sent += 1;
                        let _ = self.backpressure.remove(&sid);
                        delivered = true;
                    }
                    _ => { /* fall through to try_send */ }
                }
            }
            if !delivered {
                if tx.try_send(resp.clone()).is_ok() {
                    sent += 1;
                    // reset backpressure counter on success
                    let _ = self.backpressure.remove(&sid);
                } else {
                    dropped += 1;
                }
            }
        }
        if candidates == 0 {
            // Truly no subscribers matched this key. Suppress warning if there are no active subscribers
            // at all (e.g., clients have sent Request::Kick and disconnected).
            if self.subscribers.is_empty() {
                trace!(?topic, key = %format!("{}@{:?}", key.instrument, key.provider), seq, "router.publish_for_key: no subscribers (router idle); suppressing warn");
            } else {
                let mut keys_in_shard: Vec<String> = Vec::new();
                for entry in self.sub_index[shard].iter() {
                    let (t, k) = entry.key();
                    if *t == topic {
                        keys_in_shard.push(format!("{}@{:?}", k.instrument, k.provider));
                    }
                }
                warn!(?topic, key = %format!("{}@{:?}", key.instrument, key.provider), seq, present_keys = ?keys_in_shard, "router.publish_for_key: no subscribers matched for key");
            }
        } else if sent == 0 {
            // There were subscribers, but all sends failed (likely backpressure). Treat according to topic.
            let is_lossless = matches!(
                topic,
                Topic::Orders | Topic::Positions | Topic::AccountEvt | Topic::Fills
            );
            if is_lossless {
                warn!(?topic, key = %format!("{}@{:?}", key.instrument, key.provider), seq, dropped, candidates, "router.publish_for_key: delivery failed for all recipients (lossless topic) – applying backpressure policy");
            } else {
                warn!(?topic, key = %format!("{}@{:?}", key.instrument, key.provider), seq, dropped, candidates, "router.publish_for_key: dropped for all recipients due to backpressure (lossy topic)");
            }
        }
        #[cfg(feature = "server")]
        {
            METRICS.inc_sent(sent as u64);
            METRICS.inc_dropped(dropped as u64);
        }
        Ok(())
    }

    /// Store and announce a Shared Memory stream for (topic,key).
    /// Sends AnnounceShm to all current subscribers of this key and caches it for future subscribers.
    pub async fn announce_shm_for_key(&self, ann: tt_types::wire::AnnounceShm) -> Result<()> {
        let topic = ann.topic;
        let key = ann.key.clone();
        self.shm_announces.insert((topic, key.clone()), ann.clone());
        // fan out to current key subscribers
        let shard = self.shard_idx(topic, &key);
        if let Some(set) = self.sub_index[shard].get(&(topic, key.clone())) {
            for sid in set.iter() {
                let _ = self.send_to(sid, Response::AnnounceShm(ann.clone())).await;
            }
        }
        Ok(())
    }
}

impl Router {
    pub async fn publish_tick_batch(&self, mut batch: tt_types::wire::TickBatch) -> Result<()> {
        self.publish_for_topic(Topic::Ticks, |seq| {
            batch.seq = seq;
            Response::TickBatch(batch)
        })
        .await
    }

    pub async fn publish_tick(
        &self,
        t: tt_types::data::core::Tick,
        provider_kind: ProviderKind,
    ) -> Result<()> {
        self.publish_for_topic(Topic::Ticks, |seq| {
            Response::TickBatch(tt_types::wire::TickBatch {
                topic: Topic::Ticks,
                seq,
                ticks: vec![t],
                provider_kind,
            })
        })
        .await
    }

    pub async fn publish_quote_batch(
        &self,
        mut batch: tt_types::wire::QuoteBatch,
        _provider_kind: ProviderKind,
    ) -> Result<()> {
        self.publish_for_topic(Topic::Quotes, |seq| {
            batch.seq = seq;
            Response::QuoteBatch(batch)
        })
        .await
    }

    pub async fn publish_quote(
        &self,
        q: tt_types::data::core::Bbo,
        provider_kind: ProviderKind,
    ) -> Result<()> {
        self.publish_for_topic(Topic::Quotes, |seq| {
            Response::QuoteBatch(tt_types::wire::QuoteBatch {
                topic: Topic::Quotes,
                seq,
                quotes: vec![q],
                provider_kind,
            })
        })
        .await
    }

    pub async fn publish_bar_batch(&self, mut batch: tt_types::wire::BarsBatch) -> Result<()> {
        let topic = match batch.topic {
            Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d => {
                batch.topic
            }
            _ => Topic::Candles1m,
        };
        self.publish_for_topic(topic, |seq| {
            batch.seq = seq;
            Response::BarBatch(batch)
        })
        .await
    }

    pub async fn publish_orderbook_batch(
        &self,
        mut batch: tt_types::wire::MBP10Batch,
    ) -> Result<()> {
        self.publish_for_topic(Topic::MBP10, |seq| {
            batch.seq = seq;
            Response::MBP10Batch(batch)
        })
        .await
    }

    pub async fn publish_mbp10(
        &self,
        event: tt_types::data::mbp10::Mbp10,
        provider_kind: ProviderKind,
    ) -> Result<()> {
        self.publish_for_topic(Topic::MBP10, |seq| {
            Response::MBP10Batch(tt_types::wire::MBP10Batch {
                topic: Topic::MBP10,
                seq,
                event,
                provider_kind,
            })
        })
        .await
    }

    /// Key-based fanout for a single OrderBook snapshot. Preferred for precise routing.
    pub async fn publish_mbp10_for_key(
        &self,
        key: &tt_types::keys::SymbolKey,
        provider_kind: ProviderKind,
        event: tt_types::data::mbp10::Mbp10,
    ) -> Result<()> {
        let topic = Topic::MBP10;
        self.publish_for_key(topic, key, |seq| {
            Response::MBP10Batch(tt_types::wire::MBP10Batch {
                topic,
                seq,
                event,
                provider_kind,
            })
        })
        .await
    }

    /// Key-based fanout for a batch of OrderBooks (all for the same key/stream).
    pub async fn publish_orderbook_batch_for_key(
        &self,
        key: &tt_types::keys::SymbolKey,
        mut batch: tt_types::wire::MBP10Batch,
    ) -> Result<()> {
        let topic = Topic::MBP10;
        self.publish_for_key(topic, key, |seq| {
            batch.seq = seq;
            Response::MBP10Batch(batch)
        })
        .await
    }

    pub async fn publish_vendor_data(&self, mut vd: tt_types::wire::VendorData) -> Result<()> {
        let topic = vd.topic;
        self.publish_for_topic(topic, |seq| {
            vd.seq = seq;
            Response::VendorData(vd)
        })
        .await
    }

    pub async fn publish_orders_batch(&self, mut batch: tt_types::wire::OrdersBatch) -> Result<()> {
        self.publish_for_topic(Topic::Orders, |seq| {
            batch.seq = seq;
            Response::OrdersBatch(batch)
        })
        .await
    }

    pub async fn publish_positions_batch(
        &self,
        mut batch: tt_types::wire::PositionsBatch,
    ) -> Result<()> {
        self.publish_for_topic(Topic::Positions, |seq| {
            batch.seq = seq;
            Response::PositionsBatch(batch)
        })
        .await
    }

    pub async fn publish_account_delta_batch(
        &self,
        mut batch: tt_types::wire::AccountDeltaBatch,
    ) -> Result<()> {
        self.publish_for_topic(Topic::AccountEvt, |seq| {
            batch.seq = seq;
            Response::AccountDeltaBatch(batch)
        })
        .await
    }

    pub async fn publish_closed_trades(&self, trades: Vec<tt_types::wire::Trade>) -> Result<()> {
        // Use Fills topic to gate delivery to account subscribers
        self.publish_for_topic(Topic::Fills, |_seq| Response::ClosedTrades(trades.clone()))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tokio::time::{Duration, timeout};
    use tt_types::data::core::Decimal;
    use tt_types::providers::{ProjectXTenant, ProviderKind};
    use tt_types::securities::symbols::Instrument;

    fn make_router_for_tests() -> Router {
        Router::new(4)
    }

    #[tokio::test]
    async fn publish_for_key_delivers_after_subscribe_warmup() {
        let r = make_router_for_tests();
        // Register a client without sockets
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Response>(1);
        let id = SubId(1);
        r.subscribers.insert(id.clone(), tx);
        r.meta.insert(
            id.clone(),
            SubscriberMeta {
                id: id.clone(),
                topics: HashSet::new(),
            },
        );

        // Subscribe to key
        let key = SymbolKey::new(
            Instrument::from_str("MNQ.Z25").unwrap(),
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
        );
        // Also mark coarse topic interest to allow wildcard delivery checks if needed
        let _ = r
            .handle_request(
                &id,
                Request::SubscribeKey(tt_types::wire::SubscribeKey {
                    topic: Topic::MBP10,
                    key: key.clone(),
                    latest_only: false,
                    from_seq: 0,
                }),
            )
            .await;
        // Drain SubscribeResponse if any to keep channel free
        let _ = timeout(Duration::from_millis(100), rx.recv()).await; // ignore content

        // Spawn publisher; it should use warmup awaited send if receiver isn't draining yet
        let r2 = r.clone();
        let key2 = key.clone();
        let publish = tokio::spawn(async move {
            let now_ns = tt_types::data::mbp10::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(0) as u64;
            let event = tt_types::data::mbp10::make_mbp10(
                key2.instrument.clone(),
                now_ns,
                now_ns,
                10,
                0,
                0,
                tt_types::data::mbp10::Action::Modify.into(),
                tt_types::data::mbp10::BookSide::None.into(),
                0,
                0,
                Decimal::from(0u32),
                tt_types::data::mbp10::Flags::F_MBP,
                0,
                0,
                None,
            )
            .expect("build mbp10");
            let _ = r2
                .publish_mbp10_for_key(&key2, ProviderKind::ProjectX(ProjectXTenant::Demo), event)
                .await;
        });

        // After a short delay, start receiving; the awaited send should complete
        tokio::time::sleep(Duration::from_millis(50)).await;
        let got = timeout(Duration::from_millis(500), async {
            loop {
                if let Some(resp) = rx.recv().await {
                    if let Response::MBP10Batch(batch) = resp {
                        break Some(batch);
                    }
                } else {
                    break None;
                }
            }
        })
        .await
        .expect("recv did not complete in time");
        assert!(got.is_some(), "expected MBP10 delivery after warmup");
        let _ = publish.await;
    }

    #[tokio::test]
    async fn key_publish_reaches_topic_wildcard_subscriber() {
        let r = make_router_for_tests();
        // Two subscribers: one topic-wide, one specific key
        let (tx_topic, mut rx_topic) = tokio::sync::mpsc::channel::<Response>(4);
        let topic_id = SubId(2);
        r.subscribers.insert(topic_id.clone(), tx_topic);
        r.meta.insert(
            topic_id.clone(),
            SubscriberMeta {
                id: topic_id.clone(),
                topics: HashSet::new(),
            },
        );
        // Subscribe to topic (coarse)
        let _ = r
            .handle_request(
                &topic_id,
                Request::Subscribe(tt_types::wire::Subscribe {
                    topic: Topic::MBP10,
                    latest_only: false,
                    from_seq: 0,
                }),
            )
            .await;

        let (tx_key, mut rx_key) = tokio::sync::mpsc::channel::<Response>(4);
        let key_id = SubId(3);
        r.subscribers.insert(key_id.clone(), tx_key);
        r.meta.insert(
            key_id.clone(),
            SubscriberMeta {
                id: key_id.clone(),
                topics: HashSet::new(),
            },
        );

        let key = SymbolKey::new(
            Instrument::from_str("MNQ.Z25").unwrap(),
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
        );
        let _ = r
            .handle_request(
                &key_id,
                Request::SubscribeKey(tt_types::wire::SubscribeKey {
                    topic: Topic::MBP10,
                    key: key.clone(),
                    latest_only: false,
                    from_seq: 0,
                }),
            )
            .await;
        // Drain initial control frames
        let _ = tokio::time::timeout(Duration::from_millis(100), rx_topic.recv()).await;
        let _ = tokio::time::timeout(Duration::from_millis(100), rx_key.recv()).await;

        // Publish for key
        let now_ns = tt_types::data::mbp10::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(0) as u64;
        let event = tt_types::data::mbp10::make_mbp10(
            key.instrument.clone(),
            now_ns,
            now_ns,
            10,
            0,
            0,
            tt_types::data::mbp10::Action::Modify.into(),
            tt_types::data::mbp10::BookSide::None.into(),
            0,
            0,
            Decimal::from(0u32),
            tt_types::data::mbp10::Flags::F_MBP,
            0,
            0,
            None,
        )
        .expect("build mbp10");
        let _ = r
            .publish_mbp10_for_key(&key, ProviderKind::ProjectX(ProjectXTenant::Demo), event)
            .await;

        // Expect both subscribers to get MBP10
        let got_topic = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if let Some(resp) = rx_topic.recv().await
                    && matches!(resp, Response::MBP10Batch(_))
                {
                    break true;
                }
            }
        })
        .await
        .unwrap_or(false);
        let got_key = tokio::time::timeout(Duration::from_millis(300), async {
            loop {
                if let Some(resp) = rx_key.recv().await
                    && matches!(resp, Response::MBP10Batch(_))
                {
                    break true;
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            got_topic && got_key,
            "both topic-wide and key subscriber must receive MBP10"
        );
    }
}
