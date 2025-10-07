use crate::models::{
    BarRec, DepthDeltaRec, EngineConfig, InterestEntry, StreamKey, StreamMetrics, SubState, TickRec,
};
use async_trait::async_trait;
use dashmap::DashMap;
use duckdb::Connection;
use std::collections::{HashMap, HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::info;
use tt_bus::{ClientMessageBus, ClientSubId};
use tt_database::init::init_db;
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::server_side::traits::{MarketDataProvider, ProbeStatus, ProviderParams};
use tt_types::wire::{
    AccountDeltaBatch, BarBatch, OrdersBatch, PositionsBatch, QuoteBatch, Request, Response,
    Subscribe, TickBatch,
};

#[derive(Default)]
pub struct Caches {
    pub ticks: HashMap<StreamKey, VecDeque<TickRec>>, // bounded by secs window
    pub bars: HashMap<StreamKey, VecDeque<BarRec>>,   // sized per resolution
    pub depth_ring: HashMap<StreamKey, VecDeque<DepthDeltaRec>>, // ~2–5s ring
}

pub struct Engine<P: MarketDataProvider + 'static> {
    provider: Arc<P>,
    cfg: EngineConfig,
    inner: Arc<Mutex<EngineInner>>,
}

struct EngineInner {
    interest: HashMap<StreamKey, InterestEntry>,
    metrics: HashMap<StreamKey, StreamMetrics>,
    database: Connection,
    caches: Caches,
}

impl<P: MarketDataProvider + 'static> Engine<P> {
    pub fn new(provider: Arc<P>, cfg: EngineConfig) -> anyhow::Result<Self> {
        let database = init_db(Path::new(&cfg.db_path))?;
        Ok(Self {
            provider,
            cfg,
            inner: Arc::new(Mutex::new(EngineInner {
                interest: HashMap::new(),
                metrics: HashMap::new(),
                caches: Caches::default(),
                database,
            })),
        })
    }

    pub async fn interest_delta(
        &self,
        topic: Topic,
        key: SymbolKey,
        delta: i32,
        params: Option<ProviderParams>,
    ) -> anyhow::Result<()> {
        let sk = StreamKey {
            topic,
            key: key.clone(),
        };
        let mut inner = self.inner.lock().await;
        let ent = inner.interest.entry(sk.clone()).or_insert(InterestEntry {
            downstream_count: 0,
            state: SubState::Unsubscribed,
            params: params.clone(),
            last_upstream_data_at: None,
        });
        // Update params if provided (persist for resubscribe)
        if let Some(p) = params {
            ent.params = Some(p);
        }
        if delta > 0 {
            let prev = ent.downstream_count;
            ent.downstream_count = ent.downstream_count.saturating_add(delta as u32);
            if prev == 0 {
                drop(inner);
                info!(?sk, "upstream subscribe start");
                // idempotency: only call if not already Subscribed/Subscribing
                let _ = self.provider.subscribe_md(topic, &key).await;
                let mut inner2 = self.inner.lock().await;
                if let Some(ent2) = inner2.interest.get_mut(&sk) {
                    ent2.state = SubState::Subscribed;
                }
            }
        } else if delta < 0 {
            let prev = ent.downstream_count;
            ent.downstream_count = ent.downstream_count.saturating_sub((-delta) as u32);
            if prev > 0 && ent.downstream_count == 0 {
                drop(inner);
                info!(?sk, "upstream unsubscribe start (downstream=0)");
                self.provider.unsubscribe_md(topic, &key).await?;
                let mut inner2 = self.inner.lock().await;
                if let Some(ent2) = inner2.interest.get_mut(&sk) {
                    ent2.state = SubState::Unsubscribed;
                }
            }
        }
        Ok(())
    }

    pub async fn probe_stream(&self, topic: Topic, _key: &SymbolKey) -> ProbeStatus {
        self.provider.supports(topic); // hint; no-op for now
        // delegate to provider if it has a probe (not in trait for streams), so return Ok(0)
        ProbeStatus::Ok(0)
    }

    // Provider → Engine data callbacks (skeletons for caches)
    pub async fn on_tick_batch(
        &self,
        topic: Topic,
        key: &SymbolKey,
        now: Instant,
        batch_bytes: usize,
    ) {
        let mut inner = self.inner.lock().await;
        let sk = StreamKey {
            topic,
            key: key.clone(),
        };
        let m = inner.metrics.entry(sk.clone()).or_default();
        m.frames += 1;
        m.bytes += batch_bytes as u64;
        m.last_upstream_data_at = Some(now);
        let q = inner.caches.ticks.entry(sk).or_default();
        q.push_back(TickRec {
            ts_ns: now.elapsed().as_nanos() as i64,
            bytes: batch_bytes,
        });
        // Evict by time window
        let window = Duration::from_secs(self.cfg.ticks_replay_secs);
        let nowi = Instant::now();
        while let Some(_front) = q.front() {
            // Here we used now elapsed placeholder; in real we would compare to message ts
            if nowi.duration_since(now) > window {
                q.pop_front();
            } else {
                break;
            }
        }
    }

    pub async fn on_depth_delta(&self, topic: Topic, key: &SymbolKey, now: Instant, bytes: usize) {
        let mut inner = self.inner.lock().await;
        let sk = StreamKey {
            topic,
            key: key.clone(),
        };
        let m = inner.metrics.entry(sk.clone()).or_default();
        m.frames += 1;
        m.bytes += bytes as u64;
        m.last_upstream_data_at = Some(now);
        let ring = inner.caches.depth_ring.entry(sk).or_default();
        ring.push_back(DepthDeltaRec {
            ts_ns: now.elapsed().as_nanos() as i64,
            bytes,
        });
        // Cap by approximate time: keep last N entries within depth_ring_secs (simplified)
        while ring.len() > 1024 {
            ring.pop_front();
        }
    }
}

#[async_trait]
pub trait Strategy: Send + Sync + 'static {
    fn desired_topics(&self) -> HashSet<Topic>;
    async fn on_start(&self) {}
    async fn on_stop(&self) {}
    async fn on_tick(&self, _t: Tick) {}
    async fn on_quote(&self, _q: Bbo) {}
    async fn on_bar(&self, _b: Candle) {}
    async fn on_depth(&self, _d: Mbp10) {}
    async fn on_orders_batch(&self, _b: OrdersBatch) {}
    async fn on_positions_batch(&self, _b: PositionsBatch) {}
    async fn on_account_delta_batch(&self, _b: AccountDeltaBatch) {}
    async fn on_subscribe(&self, _instrument: Instrument, _topic: Topic, _success: bool) {}
    async fn on_unsubscribe(&self, _instrument: Instrument, _topic: Topic) {}
}

struct EngineAccountsState {
    last_orders: Option<OrdersBatch>,
    last_positions: Option<PositionsBatch>,
    last_accounts: Option<AccountDeltaBatch>,
}

pub struct EngineRuntime {
    bus: Arc<ClientMessageBus>,
    sub_id: Option<ClientSubId>,
    rx: Option<mpsc::Receiver<Response>>,
    task: Option<tokio::task::JoinHandle<()>>,
    state: Arc<Mutex<EngineAccountsState>>,
    // Correlated request/response callbacks (engine-local)
    next_corr_id: Arc<AtomicU64>,
    pending: Arc<DashMap<u64, oneshot::Sender<Response>>>,
    // Vendor securities cache and watchers
    securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
    watching_providers: Arc<DashMap<ProviderKind, ()>>,
}

impl EngineRuntime {
    /// Send a correlated request that expects a single Response back from the server.
    /// The engine maintains a DashMap of pending callbacks keyed by corr_id.
    pub async fn request_with_corr<F>(&self, make: F) -> oneshot::Receiver<Response>
    where
        F: FnOnce(u64) -> Request,
    {
        let corr_id = self.next_corr_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(corr_id, tx);
        // Forward to server via client bus using our own sub_id
        let req = make(corr_id);
        let _ = self
            .bus
            .handle_request(&self.sub_id.as_ref().expect("engine started"), req)
            .await;
        rx
    }

    pub async fn list_instruments(
        &self,
        provider: ProviderKind,
        pattern: Option<String>,
    ) -> anyhow::Result<Vec<Instrument>> {
        use tokio::time::timeout;
        use tt_types::wire::{InstrumentsRequest, Response as WireResp};
        let rx = self
            .request_with_corr(|corr_id| {
                Request::InstrumentsRequest(InstrumentsRequest {
                    provider,
                    pattern,
                    corr_id,
                })
            })
            .await;
        // Wait briefly for the server to respond; if unsupported, return empty
        match timeout(Duration::from_millis(750), rx).await {
            Ok(Ok(WireResp::InstrumentsResponse(ir))) => Ok(ir.instruments),
            Ok(Ok(_other)) => Ok(vec![]),
            _ => Ok(vec![]),
        }
    }

    pub async fn get_instruments_map(
        &self,
        provider: ProviderKind,
    ) -> anyhow::Result<Vec<(Instrument, tt_types::wire::FuturesContractWire)>> {
        use tokio::time::timeout;
        use tt_types::wire::{InstrumentsMapRequest, Response as WireResp};
        let rx = self
            .request_with_corr(|corr_id| {
                Request::InstrumentsMapRequest(InstrumentsMapRequest { provider, corr_id })
            })
            .await;
        match timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(WireResp::InstrumentsMapResponse(imr))) => Ok(imr.instruments),
            Ok(Ok(_other)) => Ok(vec![]),
            _ => Ok(vec![]),
        }
    }

    pub async fn get_account_info(
        &self,
        provider: ProviderKind,
    ) -> anyhow::Result<tt_types::wire::AccountInfoResponse> {
        use tokio::time::timeout;
        use tt_types::wire::{AccountInfoRequest, Response as WireResp};
        let rx = self
            .request_with_corr(|corr_id| {
                Request::AccountInfoRequest(AccountInfoRequest { provider, corr_id })
            })
            .await;
        match timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(WireResp::AccountInfoResponse(air))) => Ok(air),
            Ok(Ok(_other)) => Err(anyhow::anyhow!(
                "unexpected response for AccountInfoRequest"
            )),
            _ => Err(anyhow::anyhow!("timeout waiting for AccountInfoResponse")),
        }
    }

    pub async fn subscribe_symbol(&self, topic: Topic, key: SymbolKey) -> anyhow::Result<()> {
        // On first subscribe for this provider, start vendor securities refresh (hourly)
        self.ensure_vendor_securities_watch(key.provider).await;
        // Forward to server
        let _ = self
            .bus
            .handle_request(
                &self.sub_id.as_ref().expect("engine started"),
                Request::SubscribeKey(tt_types::wire::SubscribeKey {
                    topic,
                    key,
                    latest_only: false,
                    from_seq: 0,
                }),
            )
            .await?;
        Ok(())
    }
    pub async fn unsubscribe_symbol(&self, topic: Topic, key: SymbolKey) -> anyhow::Result<()> {
        let _ = self
            .bus
            .handle_request(
                &self.sub_id.as_ref().expect("engine started"),
                Request::UnsubscribeKey(tt_types::wire::UnsubscribeKey { topic, key }),
            )
            .await?;
        Ok(())
    }

    // Convenience: subscribe/unsubscribe by key without exposing sender
    pub async fn subscribe_key(&self, topic: Topic, key: SymbolKey) -> anyhow::Result<()> {
        // Ensure vendor securities refresh is active for this provider
        self.ensure_vendor_securities_watch(key.provider).await;
        let _ = self
            .bus
            .handle_request(
                &self.sub_id.as_ref().expect("engine started"),
                Request::SubscribeKey(tt_types::wire::SubscribeKey {
                    topic,
                    key,
                    latest_only: false,
                    from_seq: 0,
                }),
            )
            .await?;
        Ok(())
    }

    pub async fn unsubscribe_key(&self, topic: Topic, key: SymbolKey) -> anyhow::Result<()> {
        self.unsubscribe_symbol(topic, key).await
    }

    // Orders API helpers
    pub async fn place_order(&self, spec: tt_types::wire::PlaceOrder) -> anyhow::Result<()> {
        let _ = self
            .bus
            .handle_request(
                &self.sub_id.as_ref().expect("engine started"),
                tt_types::wire::Request::PlaceOrder(spec),
            )
            .await?;
        Ok(())
    }

    pub async fn cancel_order(&self, spec: tt_types::wire::CancelOrder) -> anyhow::Result<()> {
        let _ = self
            .bus
            .handle_request(
                &self.sub_id.as_ref().expect("engine started"),
                tt_types::wire::Request::CancelOrder(spec),
            )
            .await?;
        Ok(())
    }

    pub async fn replace_order(&self, spec: tt_types::wire::ReplaceOrder) -> anyhow::Result<()> {
        let _ = self
            .bus
            .handle_request(
                &self.sub_id.as_ref().expect("engine started"),
                tt_types::wire::Request::ReplaceOrder(spec),
            )
            .await?;
        Ok(())
    }

    // Account interest: auto-subscribe all execution streams for an account
    pub async fn activate_account_interest(
        &self,
        key: tt_types::keys::AccountKey,
    ) -> anyhow::Result<()> {
        let _ = self
            .bus
            .handle_request(
                &self.sub_id.as_ref().expect("engine started"),
                tt_types::wire::Request::SubscribeAccount(tt_types::wire::SubscribeAccount { key }),
            )
            .await?;
        Ok(())
    }

    pub async fn deactivate_account_interest(
        &self,
        key: tt_types::keys::AccountKey,
    ) -> anyhow::Result<()> {
        let _ = self
            .bus
            .handle_request(
                &self.sub_id.as_ref().expect("engine started"),
                tt_types::wire::Request::UnsubscribeAccount(tt_types::wire::UnsubscribeAccount {
                    key,
                }),
            )
            .await?;
        Ok(())
    }

    /// Initialize one or more accounts at engine startup by subscribing to all
    /// account-related streams (orders, positions, account events). This is a
    /// convenience wrapper around `activate_account_interest`.
    pub async fn initialize_accounts<I>(&self, accounts: I) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = AccountKey>,
    {
        for key in accounts {
            self.activate_account_interest(key).await?;
        }
        Ok(())
    }

    /// Convenience: initialize by account names for a given provider kind.
    pub async fn initialize_account_names<I>(
        &self,
        provider: ProviderKind,
        names: I,
    ) -> anyhow::Result<()>
    where
        I: IntoIterator<Item = tt_types::accounts::account::AccountName>,
    {
        let keys = names.into_iter().map(|account_name| AccountKey {
            provider,
            account_name,
        });
        self.initialize_accounts(keys).await
    }

    // Portfolio helpers
    pub async fn last_orders(&self) -> Option<tt_types::wire::OrdersBatch> {
        let st = self.state.lock().await;
        st.last_orders.clone()
    }
    pub async fn last_positions(&self) -> Option<tt_types::wire::PositionsBatch> {
        let st = self.state.lock().await;
        st.last_positions.clone()
    }
    pub async fn last_accounts(&self) -> Option<tt_types::wire::AccountDeltaBatch> {
        let st = self.state.lock().await;
        st.last_accounts.clone()
    }
    pub async fn find_position_delta(
        &self,
        instrument: &tt_types::securities::symbols::Instrument,
    ) -> Option<tt_types::accounts::events::PositionDelta> {
        let st = self.state.lock().await;
        st.last_positions.as_ref().and_then(|pb| {
            pb.positions
                .iter()
                .find(|p| &p.instrument == instrument)
                .cloned()
        })
    }
    pub async fn orders_for_instrument(
        &self,
        instrument: &tt_types::securities::symbols::Instrument,
    ) -> Vec<tt_types::accounts::events::OrderUpdate> {
        let st = self.state.lock().await;
        st.last_orders
            .as_ref()
            .map(|ob| {
                ob.orders
                    .iter()
                    .filter(|o| &o.instrument == instrument)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Return cached securities list for a provider (may be empty if not fetched yet)
    pub fn securities_for(&self, provider: ProviderKind) -> Vec<Instrument> {
        self.securities_by_provider
            .get(&provider)
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }

    /// Ensure that we fetch instruments from the vendor now and refresh every hour.
    async fn ensure_vendor_securities_watch(&self, provider: ProviderKind) {
        if self.watching_providers.contains_key(&provider) {
            return;
        }
        self.watching_providers.insert(provider, ());
        // Immediate fetch using the client bus correlation helper
        let bus = self.bus.clone();
        let sec_map = self.securities_by_provider.clone();
        // Helper closure to perform one fetch
        async fn fetch_into(
            bus: Arc<ClientMessageBus>,
            provider: ProviderKind,
            sec_map: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
        ) {
            use tokio::time::timeout;
            use tt_types::wire::{InstrumentsRequest, Request as WireReq, Response as WireResp};
            // Use bus-level correlation to avoid depending on EngineRuntime internals
            let rx = bus
                .request_with_corr(|corr_id| {
                    WireReq::InstrumentsRequest(InstrumentsRequest {
                        provider,
                        pattern: None,
                        corr_id,
                    })
                })
                .await;
            if let Ok(Ok(WireResp::InstrumentsResponse(ir))) =
                timeout(Duration::from_secs(3), rx).await
            {
                sec_map.insert(provider, ir.instruments);
            }
        }
        fetch_into(bus.clone(), provider, sec_map.clone()).await;
        // Spawn hourly refresh
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tick.tick().await;
                fetch_into(bus.clone(), provider, sec_map.clone()).await;
            }
        });
    }

    // Position helpers (account currently ignored; model aggregates by instrument)
    pub async fn is_long(
        &self,
        _account: Option<tt_types::accounts::account::AccountName>,
        instrument: &Instrument,
    ) -> bool {
        let st = self.state.lock().await;
        if let Some(pb) = &st.last_positions {
            if let Some(p) = pb.positions.iter().find(|p| &p.instrument == instrument) {
                return p.net_qty_after > 0;
            }
        }
        false
    }
    pub async fn is_short(
        &self,
        _account: Option<tt_types::accounts::account::AccountName>,
        instrument: &Instrument,
    ) -> bool {
        let st = self.state.lock().await;
        if let Some(pb) = &st.last_positions {
            if let Some(p) = pb.positions.iter().find(|p| &p.instrument == instrument) {
                return p.net_qty_after < 0;
            }
        }
        false
    }
    pub async fn is_flat(
        &self,
        _account: Option<tt_types::accounts::account::AccountName>,
        instrument: &Instrument,
    ) -> bool {
        let st = self.state.lock().await;
        if let Some(pb) = &st.last_positions {
            if let Some(p) = pb.positions.iter().find(|p| &p.instrument == instrument) {
                return p.net_qty_after == 0;
            }
        }
        true
    }

    pub fn new(bus: Arc<ClientMessageBus>) -> Self {
        Self {
            bus,
            sub_id: None,
            rx: None,
            task: None,
            state: Arc::new(Mutex::new(EngineAccountsState {
                last_orders: None,
                last_positions: None,
                last_accounts: None,
            })),
            next_corr_id: Arc::new(AtomicU64::new(1)),
            pending: Arc::new(DashMap::new()),
            securities_by_provider: Arc::new(DashMap::new()),
            watching_providers: Arc::new(DashMap::new()),
        }
    }

    pub async fn start<S: Strategy>(&mut self, strategy: Arc<S>) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel::<Response>(1024);
        let sub_id = self.bus.add_client(tx).await;
        self.sub_id = Some(sub_id.clone());
        self.rx = Some(rx);
        for topic in strategy.desired_topics().into_iter() {
            let sub = Subscribe {
                topic,
                latest_only: false,
                from_seq: 0,
            };
            self.bus
                .handle_request(&sub_id, Request::Subscribe(sub))
                .await?;
        }
        let mut rx = self.rx.take().expect("rx present after start");
        let state = self.state.clone();
        let pending = self.pending.clone();
        strategy.on_start().await;
        let handle = tokio::spawn(async move {
            while let Some(resp) = rx.recv().await {
                // Fulfill engine-local correlated callbacks first
                match &resp {
                    Response::InstrumentsResponse(ir) => {
                        if let Some((_k, tx)) = pending.remove(&ir.corr_id) {
                            let _ = tx.send(resp.clone());
                            continue;
                        }
                    }
                    Response::InstrumentsMapResponse(imr) => {
                        if let Some((_k, tx)) = pending.remove(&imr.corr_id) {
                            let _ = tx.send(resp.clone());
                            continue;
                        }
                    }
                    _ => {}
                }
                match resp {
                    Response::TickBatch(TickBatch { ticks, .. }) => {
                        for t in ticks {
                            strategy.on_tick(t).await;
                        }
                    }
                    Response::QuoteBatch(QuoteBatch { quotes, .. }) => {
                        for q in quotes {
                            strategy.on_quote(q).await;
                        }
                    }
                    Response::BarBatch(BarBatch { bars, .. }) => {
                        for b in bars {
                            strategy.on_bar(b).await;
                        }
                    }
                    Response::MBP10(ob) => {
                        for book in ob.books {
                            strategy.on_depth(book).await;
                        }
                    }
                    Response::OrdersBatch(ob) => {
                        // Update engine account state cache then notify strategy
                        // Clone minimal to move into async call
                        {
                            let mut st = state.lock().await;
                            st.last_orders = Some(ob.clone());
                        }
                        strategy.on_orders_batch(ob.clone()).await;
                    }
                    Response::PositionsBatch(pb) => {
                        {
                            let mut st = state.lock().await;
                            st.last_positions = Some(pb.clone());
                        }
                        strategy.on_positions_batch(pb.clone()).await;
                    }
                    Response::AccountDeltaBatch(ab) => {
                        {
                            let mut st = state.lock().await;
                            st.last_accounts = Some(ab.clone());
                        }
                        strategy.on_account_delta_batch(ab.clone()).await;
                    }
                    Response::Pong(_)
                    | Response::InstrumentsResponse(_)
                    | Response::InstrumentsMapResponse(_)
                    | Response::VendorData(_)
                    | Response::Tick(_)
                    | Response::Quote(_)
                    | Response::Bar(_)
                    | Response::AnnounceShm(_) => {}
                    Response::SubscribeResponse {
                        topic,
                        instrument,
                        success,
                    } => {
                        strategy.on_subscribe(instrument, topic, success).await;
                    }
                    Response::UnsubscribeResponse { topic, instrument } => {
                        strategy.on_unsubscribe(instrument, topic).await;
                    }
                    Response::AccountInfoResponse(air) => {
                        if let Some((_k, tx)) = pending.remove(&air.corr_id) {
                            let _ = tx.send(Response::AccountInfoResponse(air.clone()));
                        }
                    }
                }
            }
            let _ = strategy.on_stop().await;
        });
        self.task = Some(handle);
        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(handle) = self.task.take() {
            handle.abort();
        }
        self.rx.take();
    }

    // Account state getters
    pub async fn last_orders_batch(&self) -> Option<OrdersBatch> {
        let st = self.state.lock().await;
        st.last_orders.clone()
    }
    pub async fn last_positions_batch(&self) -> Option<PositionsBatch> {
        let st = self.state.lock().await;
        st.last_positions.clone()
    }
    pub async fn last_account_delta_batch(&self) -> Option<AccountDeltaBatch> {
        let st = self.state.lock().await;
        st.last_accounts.clone()
    }
}
