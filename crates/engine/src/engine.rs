use crate::models::{
    BarRec, DepthDeltaRec, EngineConfig, InterestEntry, StreamKey, StreamMetrics, SubState, TickRec,
};
use crate::portfolio::PortfolioManager;
use anyhow::anyhow;
use dashmap::DashMap;
use duckdb::Connection;
use rust_decimal::prelude::ToPrimitive;
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::trace;
use tracing::{error, info};
use tt_bus::{ClientMessageBus, ClientSubId};
use tt_database::init::init_db;
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::{AccountDelta, PositionSide};
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::Mbp10;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::Instrument;
use tt_types::server_side::traits::{MarketDataProvider, ProbeStatus, ProviderParams};
use tt_types::wire::{
    AccountDeltaBatch, BarBatch, Kick, OrdersBatch, PositionsBatch, QuoteBatch, Request, Response,
    TickBatch, Trade,
};
use chrono::Utc;
// SHM snapshots
use tt_shm;
// Bytes trait for rkyv deserialization of individual items
use crossbeam::queue::ArrayQueue;
use tt_types::wire::Bytes as WireBytes;

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
    #[allow(dead_code)]
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

pub trait Strategy: Send + 'static {
    fn on_start(&mut self, _h: EngineHandle) {}
    fn on_stop(&mut self) {}

    fn on_tick(&mut self, _t: &Tick, _provider_kind: ProviderKind) {}
    fn on_quote(&mut self, _q: &Bbo, _provider_kind: ProviderKind) {}
    fn on_bar(&mut self, _b: &Candle, _provider_kind: ProviderKind) {}
    fn on_mbp10(&mut self, _d: &Mbp10, _provider_kind: ProviderKind) {}

    fn on_orders_batch(&mut self, _b: &OrdersBatch) {}
    fn on_positions_batch(&mut self, _b: &PositionsBatch) {}
    fn on_account_delta(&mut self, _accounts: &[AccountDelta]) {}

    fn on_trades_closed(&mut self, _trades: Vec<Trade>) {}

    fn on_subscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic, _success: bool) {}
    fn on_unsubscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic) {}

    fn accounts(&self) -> Vec<AccountKey> {
        Vec::new()
    }
}

#[derive(Clone, Debug, Copy)]
pub enum DataTopic {
    Ticks,
    Quotes,
    MBP10,
    Candles1s,
    Candles1m,
    Candles1h,
    Candles1d,
}

impl DataTopic {
    pub(crate) fn to_topic_or_err(&self) -> anyhow::Result<Topic> {
        match self {
            DataTopic::Ticks => Ok(Topic::Ticks),
            DataTopic::Quotes => Ok(Topic::Quotes),
            DataTopic::MBP10 => Ok(Topic::MBP10),
            DataTopic::Candles1s => Ok(Topic::Candles1s),
            DataTopic::Candles1m => Ok(Topic::Candles1m),
            DataTopic::Candles1h => Ok(Topic::Candles1h),
            DataTopic::Candles1d => Ok(Topic::Candles1d),
        }
    }

    pub fn from(topic: Topic) -> Self {
        match topic {
            Topic::Ticks => DataTopic::Ticks,
            Topic::Quotes => DataTopic::Quotes,
            Topic::MBP10 => DataTopic::MBP10,
            Topic::Candles1s => DataTopic::Candles1s,
            Topic::Candles1m => DataTopic::Candles1m,
            Topic::Candles1h => DataTopic::Candles1h,
            Topic::Candles1d => DataTopic::Candles1d,
            _ => unimplemented!("Topic: {} not added to engine handling", topic),
        }
    }
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
    // Active SHM polling tasks keyed by (topic,key)
    shm_tasks: Arc<DashMap<(Topic, SymbolKey), tokio::task::JoinHandle<()>>>,
    portfolio_manager: Arc<PortfolioManager>,
    // Bounded command queue drained by engine loop
    cmd_q: Arc<ArrayQueue<Command>>,
    // Map our EngineUuid -> provider's order id string (populated from order updates)
    provider_order_ids: Arc<DashMap<EngineUuid, String>>,
}

// Shared view used by EngineHandle
pub(crate) struct EngineRuntimeShared {
    bus: Arc<ClientMessageBus>,
    sub_id: ClientSubId,
    next_corr_id: Arc<AtomicU64>,
    pending: Arc<DashMap<u64, oneshot::Sender<Response>>>,
    securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
    watching_providers: Arc<DashMap<ProviderKind, ()>>,
    state: Arc<Mutex<EngineAccountsState>>,
    portfolio_manager: Arc<PortfolioManager>,
    // Map our EngineUuid -> provider's order id string (populated from order updates)
    provider_order_ids: Arc<DashMap<EngineUuid, String>>,
}

// Non-blocking commands enqueued by the strategy/handle and drained by the engine task
enum Command {
    Subscribe { topic: Topic, key: SymbolKey },
    Unsubscribe { topic: Topic, key: SymbolKey },
    Place(tt_types::wire::PlaceOrder),
    Cancel(tt_types::wire::CancelOrder),
    Replace(tt_types::wire::ReplaceOrder),
}

#[derive(Clone)]
pub struct EngineHandle {
    inner: Arc<EngineRuntimeShared>,
    cmd_q: Arc<ArrayQueue<Command>>,
}

impl EngineHandle {
    // === FIRE-AND-FORGET ===
    #[inline]
    pub fn subscribe_now(&self, topic: DataTopic, key: SymbolKey) {
        let _ = self.cmd_q.push(Command::Subscribe {
            topic: topic.to_topic_or_err().unwrap(),
            key,
        });
    }
    #[inline]
    pub fn unsubscribe_now(&self, topic: DataTopic, key: SymbolKey) {
        let _ = self.cmd_q.push(Command::Unsubscribe {
            topic: topic.to_topic_or_err().unwrap(),
            key,
        });
    }
    #[inline]
    pub fn place_now(&self, mut spec: tt_types::wire::PlaceOrder) -> EngineUuid {
        let id = EngineUuid::new();
        spec.custom_tag = Some(EngineUuid::append_engine_tag(spec.custom_tag.take(), id));
        let _ = self.cmd_q.push(Command::Place(spec));
        id
    }

    // === SYNC GETTERS (instant) ===
    #[inline]
    pub fn is_long(&self, instr: &Instrument) -> bool {
        self.inner.portfolio_manager.is_long(instr)
    }
    #[inline]
    pub fn is_short(&self, instr: &Instrument) -> bool {
        self.inner.portfolio_manager.is_short(instr)
    }
    #[inline]
    pub fn is_flat(&self, instr: &Instrument) -> bool {
        self.inner.portfolio_manager.is_flat(instr)
    }
    #[inline]
    pub fn open_orders_for_instrument(
        &self,
        instr: &Instrument,
    ) -> Vec<tt_types::accounts::events::OrderUpdate> {
        self.inner
            .portfolio_manager
            .open_orders_for_instrument(instr)
    }

    async fn request_with_corr<F>(&self, make: F) -> oneshot::Receiver<Response>
    where
        F: FnOnce(u64) -> Request,
    {
        let corr_id = self.inner.next_corr_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.inner.pending.insert(corr_id, tx);
        let req = make(corr_id);
        let _ = self.inner.bus.handle_request(&self.inner.sub_id, req).await;
        rx
    }

    // Market data
    pub async fn subscribe_key(&self, data_topic: DataTopic, key: SymbolKey) -> anyhow::Result<()> {
        let topic = data_topic.to_topic_or_err()?;
        // Ensure vendor watch is running for this provider
        self.ensure_vendor_securities_watch(key.provider).await;
        let _ = self
            .inner
            .bus
            .handle_request(
                &self.inner.sub_id,
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

    pub async fn unsubscribe_key(
        &self,
        data_topic: DataTopic,
        key: SymbolKey,
    ) -> anyhow::Result<()> {
        let topic = data_topic.to_topic_or_err()?;
        let _ = self
            .inner
            .bus
            .handle_request(
                &self.inner.sub_id,
                Request::UnsubscribeKey(tt_types::wire::UnsubscribeKey { topic, key }),
            )
            .await?;
        Ok(())
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
        match timeout(Duration::from_millis(750), rx).await {
            Ok(Ok(WireResp::InstrumentsResponse(ir))) => Ok(ir.instruments),
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => Ok(vec![]),
        }
    }

    pub async fn get_instruments_map(
        &self,
        provider: ProviderKind,
    ) -> anyhow::Result<Vec<FuturesContract>> {
        use tokio::time::timeout;
        use tt_types::wire::{InstrumentsMapRequest, Response as WireResp};
        let rx = self
            .request_with_corr(|corr_id| {
                Request::InstrumentsMapRequest(InstrumentsMapRequest { provider, corr_id })
            })
            .await;
        match timeout(Duration::from_secs(2), rx).await {
            Ok(Ok(WireResp::InstrumentsMapResponse(imr))) => Ok(imr.instruments),
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => Ok(vec![]),
        }
    }

    // Fire-and-forget: enqueue cancel; engine task performs I/O
    pub fn cancel_order(
        &self,
        _provider_kind: ProviderKind,
        account_name: AccountName,
        order_id: EngineUuid,
    ) -> anyhow::Result<()> {
        // Look up provider order ID from map populated by order updates
        if let Some(pid) = self.inner.provider_order_ids.get(&order_id) {
            let spec = tt_types::wire::CancelOrder {
                account_name,
                provider_order_id: pid.value().clone(),
            };
            let _ = self.cmd_q.push(Command::Cancel(spec));
            Ok(())
        } else {
            Err(anyhow!(
                "provider_order_id not known for engine order {}",
                order_id
            ))
        }
    }
    // Fire-and-forget: enqueue replace; engine task performs I/O
    pub fn replace_order(
        &self,
        _provider_kind: ProviderKind,
        account_name: AccountName,
        incoming: tt_types::wire::ReplaceOrder,
        order_id: EngineUuid,
    ) -> anyhow::Result<()> {
        if let Some(pid) = self.inner.provider_order_ids.get(&order_id) {
            let spec = tt_types::wire::ReplaceOrder {
                account_name,
                provider_order_id: pid.value().clone(),
                new_qty: incoming.new_qty,
                new_limit_price: incoming.new_limit_price,
                new_stop_price: incoming.new_stop_price,
                new_trail_price: incoming.new_trail_price,
            };
            let _ = self.cmd_q.push(Command::Replace(spec));
            Ok(())
        } else {
            Err(anyhow!(
                "provider_order_id not known for engine order {}",
                order_id
            ))
        }
    }
    /// Convenience: construct a `PlaceOrder` from parameters and enqueue it.
    /// This keeps strategies from having to allocate and own a full `PlaceOrder` struct.
    /// Do not use +oId: as part of your order's custom tag, it is reserved for internal order management
    pub fn place_order(
        &self,
        account_name: tt_types::accounts::account::AccountName,
        key: tt_types::keys::SymbolKey,
        side: tt_types::accounts::events::Side,
        qty: i64,
        r#type: tt_types::wire::OrderType,
        limit_price: Option<rust_decimal::Decimal>,
        stop_price: Option<rust_decimal::Decimal>,
        trail_price: Option<rust_decimal::Decimal>,
        custom_tag: Option<String>,
        stop_loss: Option<tt_types::wire::BracketWire>,
        take_profit: Option<tt_types::wire::BracketWire>,
    ) -> anyhow::Result<EngineUuid> {
        let engine_uuid = EngineUuid::new();
        let tag = EngineUuid::append_engine_tag(custom_tag, engine_uuid);
        let spec = tt_types::wire::PlaceOrder {
            account_name,
            key,
            side,
            qty,
            r#type,
            limit_price: limit_price.and_then(|d| d.to_f64()),
            stop_price: stop_price.and_then(|d| d.to_f64()),
            trail_price: trail_price.and_then(|d| d.to_f64()),
            custom_tag: Some(tag),
            stop_loss,
            take_profit,
        };
        // Fire-and-forget: enqueue; engine task will send to execution provider
        let _ = self.cmd_q.push(Command::Place(spec));
        Ok(engine_uuid)
    }

    // Accounts/positions (cached snapshots)
    pub async fn last_orders(&self) -> Option<OrdersBatch> {
        let st = self.inner.state.lock().await;
        st.last_orders.clone()
    }
    pub async fn last_positions(&self) -> Option<PositionsBatch> {
        let st = self.inner.state.lock().await;
        st.last_positions.clone()
    }
    pub async fn last_accounts(&self) -> Option<AccountDeltaBatch> {
        let st = self.inner.state.lock().await;
        st.last_accounts.clone()
    }
    pub async fn orders_for_instrument(
        &self,
        instr: &Instrument,
    ) -> Vec<tt_types::accounts::events::OrderUpdate> {
        let st = self.inner.state.lock().await;
        st.last_orders
            .as_ref()
            .map(|ob| {
                ob.orders
                    .iter()
                    .filter(|o| &o.instrument == instr)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
    pub async fn find_position_delta(
        &self,
        instr: &Instrument,
    ) -> Option<tt_types::accounts::events::PositionDelta> {
        let st = self.inner.state.lock().await;
        st.last_positions.as_ref().and_then(|pb| {
            pb.positions
                .iter()
                .find(|p| &p.instrument == instr)
                .cloned()
        })
    }
    pub async fn position_state(
        &self,
        _account_name: &AccountName,
        _instr: &Instrument,
    ) -> PositionSide {
        todo!()
    }
    pub async fn position_state_delta(&self, _instr: &Instrument) -> PositionSide {
        todo!()
    }

    pub async fn is_long_delta(&self, instr: &Instrument) -> bool {
        self.find_position_delta(instr)
            .await
            .map(|p| p.net_qty > rust_decimal::Decimal::ZERO)
            .unwrap_or(false)
    }
    pub async fn is_short_delta(&self, instr: &Instrument) -> bool {
        self.find_position_delta(instr)
            .await
            .map(|p| p.net_qty < rust_decimal::Decimal::ZERO)
            .unwrap_or(false)
    }
    pub async fn is_flat_delta(&self, instr: &Instrument) -> bool {
        self.find_position_delta(instr)
            .await
            .map(|p| p.net_qty == rust_decimal::Decimal::ZERO)
            .unwrap_or(true)
    }

    // Reference data cache access
    pub fn securities_for(&self, provider: ProviderKind) -> Vec<Instrument> {
        self.inner
            .securities_by_provider
            .get(&provider)
            .map(|v| v.value().clone())
            .unwrap_or_default()
    }

    async fn ensure_vendor_securities_watch(&self, provider: ProviderKind) {
        if self.inner.watching_providers.contains_key(&provider) {
            return;
        }
        self.inner.watching_providers.insert(provider, ());
        // Immediate fetch using the client bus correlation helper
        let bus = self.inner.bus.clone();
        let sec_map = self.inner.securities_by_provider.clone();

        async fn fetch_into(
            bus: Arc<ClientMessageBus>,
            provider: ProviderKind,
            sec_map: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
        ) {
            use tokio::time::timeout;
            use tt_types::wire::{InstrumentsRequest, Request as WireReq, Response as WireResp};
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
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(3600));
            loop {
                tick.tick().await;
                fetch_into(bus.clone(), provider, sec_map.clone()).await;
            }
        });
    }
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
    ) -> anyhow::Result<Vec<FuturesContract>> {
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
    pub async fn subscribe_key(&self, data_topic: DataTopic, key: SymbolKey) -> anyhow::Result<()> {
        // Ensure vendor securities refresh is active for this provider
        let topic = data_topic.to_topic_or_err()?;
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

    pub async fn unsubscribe_key(
        &self,
        data_topic: DataTopic,
        key: SymbolKey,
    ) -> anyhow::Result<()> {
        let topic = data_topic.to_topic_or_err()?;
        self.unsubscribe_symbol(topic, key).await
    }

    // Orders API helpers
    pub async fn send_order_for_execution(
        &self,
        spec: tt_types::wire::PlaceOrder,
    ) -> anyhow::Result<()> {
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

    /// Convenience: construct and send a `PlaceOrder` from individual parameters.
    pub async fn place_order_with(
        &self,
        account_name: tt_types::accounts::account::AccountName,
        key: tt_types::keys::SymbolKey,
        side: tt_types::accounts::events::Side,
        qty: i64,
        r#type: tt_types::wire::OrderType,
        limit_price: Option<rust_decimal::Decimal>,
        stop_price: Option<rust_decimal::Decimal>,
        trail_price: Option<rust_decimal::Decimal>,
        custom_tag: Option<String>,
        stop_loss: Option<tt_types::wire::BracketWire>,
        take_profit: Option<tt_types::wire::BracketWire>,
    ) -> anyhow::Result<()> {
        let spec = tt_types::wire::PlaceOrder {
            account_name,
            key,
            side,
            qty,
            r#type,
            limit_price: limit_price.and_then(|d| d.to_f64()),
            stop_price: stop_price.and_then(|d| d.to_f64()),
            trail_price: trail_price.and_then(|d| d.to_f64()),
            custom_tag,
            stop_loss,
            take_profit,
        };
        self.send_order_for_execution(spec).await
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
                return p.side == PositionSide::Long;
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
                return p.side == PositionSide::Short;
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
            if let Some(_) = pb.positions.iter().find(|p| &p.instrument == instrument) {
                return false;
            }
        }
        true
    }

    pub fn new(bus: Arc<ClientMessageBus>) -> Self {
        let instruments = Arc::new(DashMap::new());
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
            securities_by_provider: instruments.clone(),
            watching_providers: Arc::new(DashMap::new()),
            shm_tasks: Arc::new(DashMap::new()),
            portfolio_manager: Arc::new(PortfolioManager::new(instruments)),
            cmd_q: Arc::new(ArrayQueue::new(4096)),
            provider_order_ids: Arc::new(DashMap::new()),
        }
    }

    pub async fn start<S: Strategy>(&mut self, mut strategy: S) -> anyhow::Result<EngineHandle> {
        let (tx, rx) = mpsc::channel::<Response>(2048);
        let tx_internal = tx.clone();
        let sub_id = self.bus.add_client(tx).await;
        self.sub_id = Some(sub_id.clone());
        self.rx = Some(rx);
        let rx = self.rx.take().expect("rx present after start");
        let state = self.state.clone();
        let pending = self.pending.clone();
        // Build handle and pass to strategy
        let shared = EngineRuntimeShared {
            bus: self.bus.clone(),
            sub_id: sub_id.clone(),
            next_corr_id: self.next_corr_id.clone(),
            pending: self.pending.clone(),
            securities_by_provider: self.securities_by_provider.clone(),
            watching_providers: self.watching_providers.clone(),
            state: self.state.clone(),
            portfolio_manager: self.portfolio_manager.clone(),
            provider_order_ids: self.provider_order_ids.clone(),
        };
        let handle = EngineHandle {
            inner: Arc::new(shared),
            cmd_q: self.cmd_q.clone(),
        };
        // Start processing task loop
        let pm = self.portfolio_manager.clone();
        let shm_tasks = self.shm_tasks.clone();
        let bus_for_task = self.bus.clone();
        let sub_id_for_task = sub_id.clone();
        let securities_watch_for_task = self.watching_providers.clone();
        let handle_inner_for_task = handle.inner.clone();
        let securities_by_provider_for_task = handle_inner_for_task.securities_by_provider.clone();
        let cmd_q_for_task = self.cmd_q.clone();
        // Call on_start before moving strategy
        info!("engine: invoking strategy.on_start");
        strategy.on_start(handle.clone());
        info!("engine: strategy.on_start returned");
        // Auto-subscribe to account streams declared by the strategy, if any.
        // We lock briefly to fetch the list, then drop before awaiting network calls.
        let accounts_to_init: Vec<AccountKey> = strategy.accounts();
        if !accounts_to_init.is_empty() {
            info!(
                "engine: initializing {} account(s) for strategy",
                accounts_to_init.len()
            );
            self.initialize_accounts(accounts_to_init).await?;
        }
        // Move strategy into the spawned task after on_start and accounts have been called
        let mut strategy_for_task = strategy;
        let handle_task = tokio::spawn(async move {
            let mut rx = rx;
            // Initial drain to process any commands enqueued during on_start (e.g., subscribe_now)
            Self::drain_commands_for_task(
                cmd_q_for_task.clone(),
                bus_for_task.clone(),
                sub_id_for_task.clone(),
                securities_watch_for_task.clone(),
                securities_by_provider_for_task.clone(),
            )
            .await;
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
                    Response::AccountInfoResponse(air) => {
                        if let Some((_k, tx)) = pending.remove(&air.corr_id) {
                            let _ = tx.send(Response::AccountInfoResponse(air.clone()));
                            continue;
                        }
                    }
                    _ => {}
                }
                match resp {
                    Response::TickBatch(TickBatch {
                        ticks,
                        provider_kind,
                        ..
                    }) => {
                        let pk = provider_kind.clone();
                        for t in ticks {
                            // Update portfolio marks then deliver to strategy
                            pm.update_apply_last_price(pk.clone(), &t.instrument, t.price);
                            strategy_for_task.on_tick(&t, provider_kind.clone());
                        }
                    }
                    Response::QuoteBatch(QuoteBatch {
                        quotes,
                        provider_kind,
                        ..
                    }) => {
                        let pk = provider_kind.clone();
                        for q in quotes {
                            // Use mid-price as mark
                            let mid = (q.bid + q.ask) / rust_decimal::Decimal::from(2);
                            pm.update_apply_last_price(pk.clone(), &q.instrument, mid);
                            trace!(instrument = %q.instrument, provider = ?provider_kind, "strategy.on_quote");
                            strategy_for_task.on_quote(&q, provider_kind.clone());
                        }
                    }
                    Response::BarBatch(BarBatch {
                        bars,
                        provider_kind,
                        ..
                    }) => {
                        let pk = provider_kind.clone();
                        for b in bars {
                            // Use close as mark
                            pm.update_apply_last_price(pk.clone(), &b.instrument, b.close);
                            strategy_for_task.on_bar(&b, provider_kind.clone());
                        }
                    }
                    Response::MBP10Batch(ob) => {
                        // Derive a mark from MBP10: prefer mid from level 0 if present, else event.price
                        let ev = &ob.event;
                        let mark = if let Some(book) = &ev.book {
                            if let (Some(b0), Some(a0)) = (book.bid_px.get(0), book.ask_px.get(0)) {
                                (*b0 + *a0) / rust_decimal::Decimal::from(2)
                            } else {
                                ev.price
                            }
                        } else {
                            ev.price
                        };
                        pm.update_apply_last_price(ob.provider_kind.clone(), &ev.instrument, mark);
                        strategy_for_task.on_mbp10(&ob.event, ob.provider_kind);
                    }
                    Response::OrdersBatch(ob) => {
                        {
                            // Update portfolio open orders view
                            pm.apply_orders_batch(ob.clone());
                        }
                        {
                            let mut st = state.lock().await;
                            st.last_orders = Some(ob.clone());
                        }
                        {
                            // Populate provider order ID map from updates
                            for o in ob.orders.iter() {
                                if let Some(pid) = &o.provider_order_id {
                                    handle_inner_for_task
                                        .provider_order_ids
                                        .insert(o.order_id, pid.0.clone());
                                }
                            }
                        }
                        {
                            strategy_for_task.on_orders_batch(&ob);
                        }
                    }
                    Response::PositionsBatch(mut pb) => {
                        {
                            let mut st = state.lock().await;
                            // Adjust open_pnl via portfolio manager's last prices before delivering
                            let adj = pm.adjust_positions_batch_open_pnl(pb.clone());
                            st.last_positions = Some(adj.clone());
                        }
                        {
                            // Deliver adjusted positions batch to strategy (by-ref, sync)
                            let st = state.lock().await;
                            for p in pb.positions.iter_mut() {
                                if p.open_pnl == rust_decimal::Decimal::ZERO {
                                    if let Some(ep) = st.last_positions.as_ref() {
                                        for ep in ep.positions.iter() {
                                            if p.provider_kind == ep.provider_kind
                                                && p.instrument == ep.instrument
                                                && p.side == ep.side
                                            {
                                                p.open_pnl = ep.open_pnl;
                                            }
                                        }
                                    }
                                }
                            }
                            strategy_for_task.on_positions_batch(&pb);
                        }
                    }
                    Response::AccountDeltaBatch(mut ab) => {
                        {
                            // Update portfolio manager with latest account deltas
                            pm.apply_account_delta_batch(ab.clone());
                        }
                        {
                            // If provider returned zeros for both fields, compute from our portfolio view
                            for a in ab.accounts.iter_mut() {
                                if a.day_realized_pnl.is_zero() && a.open_pnl.is_zero() {
                                    let open = pm.account_open_pnl_sum(&a.provider_kind, &a.name);
                                    let day = pm.account_day_realized_pnl_utc(
                                        &a.provider_kind,
                                        &a.name,
                                        Utc::now(),
                                    );
                                    a.open_pnl = open;
                                    a.day_realized_pnl = day;
                                }
                            }
                        }
                        {
                            let mut st = state.lock().await;
                            st.last_accounts = Some(ab.clone());
                        }
                        {
                            strategy_for_task.on_account_delta(&ab.accounts);
                        }
                    }
                    Response::ClosedTrades(t) => {
                        // Record closed trades for realized PnL computation
                        pm.apply_closed_trades(t.clone());
                        strategy_for_task.on_trades_closed(t);
                    }
                    Response::Tick { tick, provider_kind } => {
                        pm.update_apply_last_price(provider_kind.clone(), &tick.instrument, tick.price);
                        strategy_for_task.on_tick(&tick, provider_kind);
                    }
                    Response::Quote { bbo, provider_kind } => {
                        let mid = (bbo.bid + bbo.ask) / rust_decimal::Decimal::from(2);
                        pm.update_apply_last_price(provider_kind.clone(), &bbo.instrument, mid);
                        strategy_for_task.on_quote(&bbo, provider_kind);
                    }
                    Response::Bar { candle, provider_kind } => {
                        pm.update_apply_last_price(provider_kind.clone(), &candle.instrument, candle.close);
                        strategy_for_task.on_bar(&candle, provider_kind);
                    }
                    Response::Mbp10 { mbp10, provider_kind } => {
                        pm.update_apply_last_price(provider_kind.clone(), &mbp10.instrument, mbp10.price);
                        strategy_for_task.on_mbp10(&mbp10, provider_kind);
                    }
                    Response::AnnounceShm(ann) => {
                        use tokio::task::JoinHandle;
                        let topic = ann.topic;
                        let key = ann.key.clone();
                        if shm_tasks.get(&(topic, key.clone())).is_none() {
                            let tx_shm = tx_internal.clone();
                            let value = key.clone();
                            let handle: JoinHandle<()> = tokio::spawn(async move {
                                let mut last_seq: u32 = 0;
                                loop {
                                    if let Some(reader) = tt_shm::ensure_reader(topic, &value) {
                                        if let Some((seq, buf)) = reader.read_with_seq() {
                                            if seq != last_seq {
                                                last_seq = seq;
                                                let maybe_resp = match topic {
                                                    Topic::Quotes => Bbo::from_bytes(&buf)
                                                        .ok()
                                                        .map(|bbo| Response::Quote { bbo, provider_kind: key.provider }),
                                                    Topic::Ticks => Tick::from_bytes(&buf)
                                                        .ok()
                                                        .map(|t| Response::Tick { tick: t, provider_kind: key.provider }),
                                                    Topic::MBP10 => Mbp10::from_bytes(&buf)
                                                        .ok()
                                                        .map(|m| Response::Mbp10 { mbp10: m, provider_kind: key.provider }),
                                                    _ => None,
                                                };
                                                if let Some(resp) = maybe_resp {
                                                    if tx_shm.try_send(resp).is_err() {
                                                        error!("tx shm task is panicked");
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    tokio::time::sleep(Duration::from_millis(5)).await;
                                }
                            });
                            shm_tasks.insert((topic, key), handle);
                        }
                    }
                    Response::SubscribeResponse { topic, instrument, success } => {
                        let data_topic = DataTopic::from(topic);
                        strategy_for_task.on_subscribe(instrument, data_topic, success);
                    }
                    Response::UnsubscribeResponse { topic, instrument } => {
                        // Stop any SHM polling task(s) for this topic/instrument
                        let to_remove: Vec<(Topic, SymbolKey)> = shm_tasks
                            .iter()
                            .filter_map(|e| {
                                let (t, k) = (e.key().0, e.key().1.clone());
                                if t == topic && k.instrument == instrument {
                                    Some((t, k))
                                } else {
                                    None
                                }
                            })
                            .collect();
                        for (tk, kk) in to_remove {
                            if let Some((_, handle)) = shm_tasks.remove(&(tk, kk)) {
                                handle.abort();
                            }
                        }
                        let data_topic = DataTopic::from(topic);
                        strategy_for_task.on_unsubscribe(instrument, data_topic);
                    }
                    Response::Pong(_)
                    | Response::InstrumentsResponse(_)
                    | Response::InstrumentsMapResponse(_)
                    | Response::VendorData(_)
                    | Response::AccountInfoResponse(_) => {}
                }
                // Flush any commands the strategy enqueued during this message
                Self::drain_commands_for_task(
                    cmd_q_for_task.clone(),
                    bus_for_task.clone(),
                    sub_id_for_task.clone(),
                    securities_watch_for_task.clone(),
                    securities_by_provider_for_task.clone(),
                )
                .await;
            }

            strategy_for_task.on_stop();
        });
        self.task = Some(handle_task);
        Ok(handle)
    }

    async fn drain_commands_for_task(
        cmd_q: Arc<ArrayQueue<Command>>,
        bus: Arc<ClientMessageBus>,
        sub_id: ClientSubId,
        securities_watch: Arc<DashMap<ProviderKind, ()>>,
        securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
    ) {
        use tt_types::wire::{SubscribeKey, UnsubscribeKey};

        // Local helper to ensure vendor securities watch is running
        async fn ensure_vendor(
            bus: Arc<ClientMessageBus>,
            provider: ProviderKind,
            watch: Arc<DashMap<ProviderKind, ()>>,
            sec_map: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
        ) {
            if watch.get(&provider).is_some() {
                return;
            }
            watch.insert(provider, ());
            // immediate fetch
            let bus2 = bus.clone();
            let sec_map2 = sec_map.clone();
            async fn fetch_into(
                bus: Arc<ClientMessageBus>,
                provider: ProviderKind,
                sec_map: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
            ) {
                use tokio::time::timeout;
                use tt_types::wire::{
                    InstrumentsRequest, Request as WireReq, Response as WireResp,
                };
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
            fetch_into(bus2.clone(), provider, sec_map2.clone()).await;
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(3600));
                loop {
                    tick.tick().await;
                    fetch_into(bus2.clone(), provider, sec_map2.clone()).await;
                }
            });
        }

        while let Some(cmd) = cmd_q.pop() {
            match cmd {
                Command::Subscribe { topic, key } => {
                    ensure_vendor(
                        bus.clone(),
                        key.provider,
                        securities_watch.clone(),
                        securities_by_provider.clone(),
                    )
                    .await;
                    let _ = bus
                        .handle_request(
                            &sub_id,
                            Request::SubscribeKey(SubscribeKey {
                                topic,
                                key,
                                latest_only: false,
                                from_seq: 0,
                            }),
                        )
                        .await;
                }
                Command::Unsubscribe { topic, key } => {
                    let _ = bus
                        .handle_request(
                            &sub_id,
                            Request::UnsubscribeKey(UnsubscribeKey { topic, key }),
                        )
                        .await;
                }
                Command::Place(spec) => {
                    let _ = bus.handle_request(&sub_id, Request::PlaceOrder(spec)).await;
                }
                Command::Cancel(spec) => {
                    let _ = bus
                        .handle_request(&sub_id, Request::CancelOrder(spec))
                        .await;
                }
                Command::Replace(spec) => {
                    let _ = bus
                        .handle_request(&sub_id, Request::ReplaceOrder(spec))
                        .await;
                }
            }
        }
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        info!("engine: stopping");
        let _ = self.bus.handle_request(
            &self.sub_id.as_ref().expect("engine started"),
            tt_types::wire::Request::Kick(Kick {
                reason: Some("Shutting Down".to_string()),
            }),
        );
        if let Some(handle) = self.task.take() {
            handle.abort();
        }
        for task in self.shm_tasks.iter() {
            task.abort();
        }
        self.rx.take();
        info!("engine: shutdown complete");
        Ok(())
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
