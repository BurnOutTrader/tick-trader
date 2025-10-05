use async_trait::async_trait;
use provider::traits::{MarketDataProvider, ProbeStatus, ProviderParams};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::info;
use tt_types::keys::{SymbolKey, Topic};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubState {
    Unsubscribed,
    Subscribing,
    Subscribed,
    Unsubscribing,
    Error,
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    // health thresholds (defaults; env override)
    pub ticks_warn_ms: u64,
    pub ticks_alert_ms: u64,
    pub depth_warn_ms: u64,
    pub depth_alert_ms: u64,
    pub bars1s_warn_ms: u64,
    pub bars1s_alert_ms: u64,
    pub bars1m_warn_ms: u64,
    pub bars1m_alert_ms: u64,
    pub orders_warn_ms: u64,
    pub orders_alert_ms: u64,
    // replay windows
    pub ticks_replay_secs: u64,
    pub bars1s_window_secs: u64,
    pub bars1m_window_secs: u64,
    // depth/mbo ring duration (approx)
    pub depth_ring_secs: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        fn env_u64(k: &str, d: u64) -> u64 {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        }
        Self {
            ticks_warn_ms: env_u64("TT_TICKS_WARN_MS", 100),
            ticks_alert_ms: env_u64("TT_TICKS_ALERT_MS", 500),
            depth_warn_ms: env_u64("TT_DEPTH_WARN_MS", 100),
            depth_alert_ms: env_u64("TT_DEPTH_ALERT_MS", 500),
            bars1s_warn_ms: env_u64("TT_BARS1S_WARN_MS", 2000),
            bars1s_alert_ms: env_u64("TT_BARS1S_ALERT_MS", 5000),
            bars1m_warn_ms: env_u64("TT_BARS1M_WARN_MS", 120_000),
            bars1m_alert_ms: env_u64("TT_BARS1M_ALERT_MS", 300_000),
            orders_warn_ms: env_u64("TT_ORDERS_WARN_MS", 5000),
            orders_alert_ms: env_u64("TT_ORDERS_ALERT_MS", 10_000),
            ticks_replay_secs: env_u64("TT_TICKS_REPLAY_SECS", 60),
            bars1s_window_secs: env_u64("TT_BARS1S_WINDOW_SECS", 600),
            bars1m_window_secs: env_u64("TT_BARS1M_WINDOW_SECS", 3600),
            depth_ring_secs: env_u64("TT_DEPTH_RING_SECS", 5),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamKey {
    pub topic: Topic,
    pub key: SymbolKey,
}

#[derive(Debug, Clone)]
pub struct InterestEntry {
    pub downstream_count: u32,
    pub state: SubState,
    pub params: Option<ProviderParams>,
    pub last_upstream_data_at: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
pub struct StreamMetrics {
    pub frames: u64,
    pub bytes: u64,
    pub drops: u64,
    pub credit_stalls: u64,
    pub last_upstream_data_at: Option<Instant>,
    pub last_downstream_send_at: Option<Instant>,
}

// Replay cache skeletons
#[derive(Debug, Clone)]
pub struct TickRec {
    pub ts_ns: i64,
    pub bytes: usize,
}
#[derive(Debug, Clone)]
pub struct BarRec {
    pub ts_ns: i64,
    pub bytes: usize,
}
#[derive(Debug, Clone)]
pub struct DepthDeltaRec {
    pub ts_ns: i64,
    pub bytes: usize,
}

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
    caches: Caches,
}

impl<P: MarketDataProvider + 'static> Engine<P> {
    pub fn new(provider: Arc<P>, cfg: EngineConfig) -> Self {
        Self {
            provider,
            cfg,
            inner: Arc::new(Mutex::new(EngineInner {
                interest: HashMap::new(),
                metrics: HashMap::new(),
                caches: Caches::default(),
            })),
        }
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

// -------- Strategy runtime over the MessageBus --------
use tokio::sync::mpsc;
use tt_bus::{ClientMessageBus, ClientSubId};
use tt_types::base_data::{Bbo, Candle, OrderBook, Tick};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{
    AccountDeltaBatch, BarBatch, OrdersBatch, PositionsBatch, QuoteBatch, Request, Response,
    Subscribe, TickBatch,
};

#[async_trait]
pub trait Strategy: Send + Sync + 'static {
    fn desired_topics(&self) -> HashSet<Topic>;
    async fn on_start(&self) {}
    async fn on_stop(&self) {}
    async fn on_tick(&self, _t: Tick) {}
    async fn on_quote(&self, _q: Bbo) {}
    async fn on_bar(&self, _b: Candle) {}
    async fn on_depth(&self, _d: OrderBook) {}
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
}

impl EngineRuntime {
    pub async fn list_instruments(
        &self,
        provider: ProviderKind,
        pattern: Option<String>,
    ) -> anyhow::Result<Vec<Instrument>> {
        use tokio::time::timeout;
        use tt_types::wire::{InstrumentsRequest, Response as WireResp};
        let rx = self
            .bus
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
    pub async fn subscribe_symbol(&self, topic: Topic, key: SymbolKey) -> anyhow::Result<()> {
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
        strategy.on_start().await;
        let handle = tokio::spawn(async move {
            while let Some(resp) = rx.recv().await {
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
                    Response::OrderBookBatch(ob) => {
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
