use crate::backtest::backtest_clock::BacktestClock;
use crate::engine::EngineAccountsState;
use crate::handle::EngineHandle;
use crate::models::{Command, DataTopic};
use crate::portfolio::PortfolioManager;
use crate::traits::Strategy;
use crossbeam::queue::ArrayQueue;
use dashmap::DashMap;
use smallvec::SmallVec;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::{Notify, mpsc, oneshot};
use tracing::info;
use tt_bus::{ClientMessageBus, ClientSubId};
use tt_types::accounts::events::PositionSide;
use tt_types::consolidators::ConsolidatorKey;
use tt_types::data::core::Candle;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{
    AccountDeltaBatch, BarBatch, Kick, OrdersBatch, PositionsBatch, QuoteBatch, Request, Response,
    TickBatch,
};

// Shared view used by EngineHandle
pub(crate) struct EngineRuntimeShared {
    pub(crate) bus: Arc<ClientMessageBus>,
    pub(crate) sub_id: ClientSubId,
    pub(crate) next_corr_id: Arc<AtomicU64>,
    pub(crate) pending: Arc<DashMap<u64, oneshot::Sender<Response>>>,
    pub(crate) securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
    pub(crate) watching_providers: Arc<DashMap<ProviderKind, ()>>,
    pub(crate) state: Arc<EngineAccountsState>,
    pub(crate) portfolio_manager: Arc<PortfolioManager>,
    // Map our EngineUuid -> provider's order id string (populated from order updates)
    pub(crate) provider_order_ids: Arc<DashMap<EngineUuid, String>>,
    // Map our EngineUuid -> account key (account name + provider), recorded at place_order time
    pub(crate) engine_order_accounts: Arc<DashMap<EngineUuid, AccountKey>>,
    // Pending cancels: keep the full spec until provider order id is known
    pub(crate) pending_cancels: Arc<DashMap<EngineUuid, tt_types::wire::CancelOrder>>,
    // Pending replaces: keep the full spec until provider order id is known
    pub(crate) pending_replaces: Arc<DashMap<EngineUuid, tt_types::wire::ReplaceOrder>>,
    // Consolidators shared with handle for registration
    pub(crate) consolidators:
        Arc<DashMap<ConsolidatorKey, Box<dyn tt_types::consolidators::Consolidator + Send>>>,
    // When true, disable live-only behaviors (SHM, Kick, vendor timers)
    pub(crate) backtest_mode: bool,
    // Deterministic simulation clock (present in backtest mode)
    #[allow(dead_code)]
    pub(crate) backtest_clock: Option<Arc<BacktestClock>>,
}

impl EngineRuntimeShared {
    #[inline]
    pub fn now_ns(&self) -> u64 {
        if let Some(ref bt) = self.backtest_clock {
            bt.now_ns()
        } else {
            // system time in ns
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            now.as_secs()
                .saturating_mul(1_000_000_000)
                .saturating_add(now.subsec_nanos() as u64)
        }
    }
    #[inline]
    pub fn now_dt(&self) -> chrono::DateTime<chrono::Utc> {
        if let Some(ref bt) = self.backtest_clock {
            bt.now_dt()
        } else {
            chrono::Utc::now()
        }
    }
}

pub struct EngineRuntime {
    backtest_mode: bool,
    backtest_notify: Option<Arc<Notify>>,
    backtest_clock: Option<Arc<BacktestClock>>,
    bus: Arc<ClientMessageBus>,
    sub_id: Option<ClientSubId>,
    rx: Option<mpsc::Receiver<Response>>,
    task: Option<tokio::task::JoinHandle<()>>,
    state: Arc<EngineAccountsState>,
    // Correlated request/response callbacks (engine-local)
    next_corr_id: Arc<AtomicU64>,
    pending: Arc<DashMap<u64, oneshot::Sender<Response>>>,
    // Vendor securities cache and watchers
    securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>,
    watching_providers: Arc<DashMap<ProviderKind, ()>>,
    // Active SHM polling tasks keyed by (topic,key)
    shm_tasks: Arc<DashMap<(Topic, SymbolKey), tokio::task::JoinHandle<()>>>,
    // Keys for which SHM has been disabled due to fatal errors; fall back to UDS frames per-strategy
    shm_blacklist: Arc<DashMap<(Topic, SymbolKey), ()>>,
    portfolio_manager: Arc<PortfolioManager>,
    // Bounded command queue drained by engine loop
    cmd_q: Arc<ArrayQueue<Command>>,
    // Map our EngineUuid -> provider's order id string (populated from order updates)
    provider_order_ids: Arc<DashMap<EngineUuid, String>>,
    // Map our EngineUuid -> account key (account name + provider), recorded at place_order time
    engine_order_accounts: Arc<DashMap<EngineUuid, AccountKey>>,
    // Pending cancels: keep the full spec until provider order id is known
    pending_cancels: Arc<DashMap<EngineUuid, tt_types::wire::CancelOrder>>,
    // Pending replaces: keep the full spec until provider order id is known
    pending_replaces: Arc<DashMap<EngineUuid, tt_types::wire::ReplaceOrder>>,
    slow_spin_ns: Option<u64>,
    consolidators:
        Arc<DashMap<ConsolidatorKey, Box<dyn tt_types::consolidators::Consolidator + Send>>>,
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
        let req = make(corr_id);
        self.bus
            .handle_request(self.sub_id.as_ref().expect("engine started"), req)
            .await
            .ok();
        rx
    }

    pub async fn list_instruments(
        &self,
        provider: ProviderKind,
        pattern: Option<String>,
    ) -> anyhow::Result<Vec<Instrument>> {
        use std::time::Duration;
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
        use std::time::Duration;
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
        use std::time::Duration;
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
        if !self.backtest_mode {
            self.ensure_vendor_securities_watch(key.provider).await;
        }
        // Forward to server
        self.bus
            .handle_request(
                self.sub_id.as_ref().expect("engine started"),
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
        self.bus
            .handle_request(
                self.sub_id.as_ref().expect("engine started"),
                Request::UnsubscribeKey(tt_types::wire::UnsubscribeKey { topic, key }),
            )
            .await?;
        Ok(())
    }

    // Convenience: subscribe/unsubscribe by key without exposing sender
    pub async fn subscribe_key(&self, data_topic: DataTopic, key: SymbolKey) -> anyhow::Result<()> {
        // Ensure vendor securities refresh is active for this provider
        let topic = data_topic.to_topic_or_err()?;
        if !self.backtest_mode {
            self.ensure_vendor_securities_watch(key.provider).await;
        }
        self.bus
            .handle_request(
                self.sub_id.as_ref().expect("engine started"),
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
        self.bus
            .handle_request(
                self.sub_id.as_ref().expect("engine started"),
                tt_types::wire::Request::PlaceOrder(spec),
            )
            .await?;
        Ok(())
    }

    pub async fn cancel_order(&self, spec: tt_types::wire::CancelOrder) -> anyhow::Result<()> {
        self.bus
            .handle_request(
                self.sub_id.as_ref().expect("engine started"),
                tt_types::wire::Request::CancelOrder(spec),
            )
            .await?;
        Ok(())
    }

    pub async fn replace_order(&self, spec: tt_types::wire::ReplaceOrder) -> anyhow::Result<()> {
        self.bus
            .handle_request(
                self.sub_id.as_ref().expect("engine started"),
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
        self.bus
            .handle_request(
                self.sub_id.as_ref().expect("engine started"),
                tt_types::wire::Request::SubscribeAccount(tt_types::wire::SubscribeAccount { key }),
            )
            .await?;
        Ok(())
    }

    pub async fn deactivate_account_interest(
        &self,
        key: tt_types::keys::AccountKey,
    ) -> anyhow::Result<()> {
        self.bus
            .handle_request(
                self.sub_id.as_ref().expect("engine started"),
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
    pub fn last_orders(&self) -> Option<tt_types::wire::OrdersBatch> {
        self.state
            .last_orders
            .read()
            .expect("orders lock poisoned")
            .clone()
    }
    pub fn last_positions(&self) -> Option<tt_types::wire::PositionsBatch> {
        self.state
            .last_positions
            .read()
            .expect("positions lock poisoned")
            .clone()
    }
    pub fn last_accounts(&self) -> Option<tt_types::wire::AccountDeltaBatch> {
        self.state
            .last_accounts
            .read()
            .expect("accounts lock poisoned")
            .clone()
    }
    pub fn find_position_delta(
        &self,
        instrument: &tt_types::securities::symbols::Instrument,
    ) -> Option<tt_types::accounts::events::PositionDelta> {
        let guard = self
            .state
            .last_positions
            .read()
            .expect("positions lock poisoned");
        guard.as_ref().and_then(|pb| {
            pb.positions
                .iter()
                .find(|p| &p.instrument == instrument)
                .cloned()
        })
    }
    pub fn orders_for_instrument(
        &self,
        instrument: &tt_types::securities::symbols::Instrument,
    ) -> Vec<tt_types::accounts::events::OrderUpdate> {
        let guard = self.state.last_orders.read().expect("orders lock poisoned");
        guard
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
        if self.backtest_mode {
            return;
        }
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
            use std::time::Duration;
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
    pub fn is_long(
        &self,
        _account: Option<tt_types::accounts::account::AccountName>,
        instrument: &Instrument,
    ) -> bool {
        let guard = self
            .state
            .last_positions
            .read()
            .expect("positions lock poisoned");
        if let Some(pb) = &*guard
            && let Some(p) = pb.positions.iter().find(|p| &p.instrument == instrument)
        {
            return p.side == PositionSide::Long;
        }
        false
    }
    pub fn is_short(
        &self,
        _account: Option<tt_types::accounts::account::AccountName>,
        instrument: &Instrument,
    ) -> bool {
        let guard = self
            .state
            .last_positions
            .read()
            .expect("positions lock poisoned");
        if let Some(pb) = &*guard
            && let Some(p) = pb.positions.iter().find(|p| &p.instrument == instrument)
        {
            return p.side == PositionSide::Short;
        }
        false
    }
    pub fn is_flat(
        &self,
        _account: Option<tt_types::accounts::account::AccountName>,
        instrument: &Instrument,
    ) -> bool {
        let guard = self
            .state
            .last_positions
            .read()
            .expect("positions lock poisoned");
        if let Some(pb) = &*guard
            && pb.positions.iter().any(|p| &p.instrument == instrument)
        {
            return false;
        }
        true
    }

    /// Create a new EngineRuntime bound to a ClientMessageBus.
    ///
    /// Parameters:
    /// - bus: Connected ClientMessageBus used for all requests and streaming responses.
    /// - slow_spin: Optional nanos to sleep when polling SHM without new data (to avoid tight loops).
    pub fn new(bus: Arc<ClientMessageBus>, slow_spin: Option<u64>) -> Self {
        let instruments = Arc::new(DashMap::new());
        Self {
            backtest_mode: false,
            backtest_notify: None,
            backtest_clock: None,
            bus,
            sub_id: None,
            rx: None,
            task: None,
            state: Arc::new(EngineAccountsState {
                last_orders: Arc::new(RwLock::new(None)),
                last_positions: Arc::new(RwLock::new(None)),
                last_accounts: Arc::new(RwLock::new(None)),
            }),
            next_corr_id: Arc::new(AtomicU64::new(1)),
            pending: Arc::new(DashMap::new()),
            securities_by_provider: instruments.clone(),
            watching_providers: Arc::new(DashMap::new()),
            shm_tasks: Arc::new(DashMap::new()),
            shm_blacklist: Arc::new(DashMap::new()),
            portfolio_manager: Arc::new(PortfolioManager::new(instruments)),
            cmd_q: Arc::new(ArrayQueue::new(4096)),
            provider_order_ids: Arc::new(DashMap::new()),
            engine_order_accounts: Arc::new(DashMap::new()),
            pending_cancels: Arc::new(DashMap::new()),
            pending_replaces: Arc::new(DashMap::new()),
            slow_spin_ns: slow_spin,
            consolidators: Arc::new(DashMap::new()),
        }
    }

    /// Construct an EngineRuntime in Backtest mode.
    /// Live-only features (SHM, Kick, vendor watch timers) are disabled.
    pub fn new_backtest(
        bus: Arc<ClientMessageBus>,
        slow_spin: Option<u64>,
        backtest_notify: Option<Arc<Notify>>,
    ) -> Self {
        let mut rt = Self::new(bus, slow_spin);
        rt.backtest_mode = true;
        rt.backtest_clock = Some(Arc::new(BacktestClock::new(0)));
        rt.backtest_notify = backtest_notify;
        rt
    }

    /// Start the engine processing loop and hand an EngineHandle to the strategy.
    ///
    /// Parameters:
    /// - strategy: Your Strategy implementation; on_start will be invoked with an EngineHandle.
    ///
    /// Returns: EngineHandle for issuing subscriptions and orders from your strategy.
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
            engine_order_accounts: self.engine_order_accounts.clone(),
            pending_cancels: self.pending_cancels.clone(),
            pending_replaces: self.pending_replaces.clone(),
            consolidators: self.consolidators.clone(),
            backtest_mode: self.backtest_mode,
            backtest_clock: self.backtest_clock.clone(),
        };
        let handle = EngineHandle {
            inner: Arc::new(shared),
            cmd_q: self.cmd_q.clone(),
        };
        // Start processing task loop
        let pm = self.portfolio_manager.clone();
        let shm_tasks = self.shm_tasks.clone();
        let shm_blacklist_for_task = self.shm_blacklist.clone();
        let bus_for_task = self.bus.clone();
        let sub_id_for_task = sub_id.clone();
        let securities_watch_for_task = self.watching_providers.clone();
        let handle_inner_for_task = handle.inner.clone();
        let securities_by_provider_for_task = handle_inner_for_task.securities_by_provider.clone();
        let cmd_q_for_task = self.cmd_q.clone();
        let backtest_notify_for_task = self.backtest_notify.clone();
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
        let slow_spin_ns = self.slow_spin_ns;
        let handle_task = tokio::spawn(async move {
            let mut rx = rx;
            // Track last time we emitted snapshots in backtest mode
            let mut last_pos_emit_bt: Option<chrono::DateTime<chrono::Utc>> = None;
            // Track last logical backtest time announced by orchestrator
            let mut last_bt_now: Option<chrono::DateTime<chrono::Utc>> = None;
            // Determine snapshot cadence from latency model //todo[latency] not sure how i will handle acual models yet. maybe overkill??
            let pos_refresh_every = Duration::from_millis(500);
            // Initial drain to process any commands enqueued during on_start (e.g., subscribe_now)
            Self::drain_commands_for_task(
                cmd_q_for_task.clone(),
                bus_for_task.clone(),
                sub_id_for_task.clone(),
                securities_watch_for_task.clone(),
                securities_by_provider_for_task.clone(),
                handle_inner_for_task.backtest_mode,
            )
            .await;

            // Create a time ticker to drive consolidators' on_time about 50ms after each whole second
            let mut ticker = {
                use tokio::time::{Instant, MissedTickBehavior, interval_at};
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let sub_ns = now.subsec_nanos();
                let target_ns: u32 = 50_000_000; // 50ms after the whole second
                let wait_ns: u64 = if sub_ns <= target_ns {
                    (target_ns - sub_ns) as u64
                } else {
                    1_000_000_000u64 - (sub_ns as u64 - target_ns as u64)
                };
                let start = Instant::now() + Duration::from_nanos(wait_ns);
                let mut iv = interval_at(start, Duration::from_secs(1));
                iv.set_missed_tick_behavior(MissedTickBehavior::Skip);
                iv
            };

            loop {
                tokio::select! {
                    biased;
                    // Drive time-based consolidation first if due
                    _ = ticker.tick() => {
                        if !handle_inner_for_task.backtest_mode {
                            let now = handle_inner_for_task.now_dt();
                            // 1) Walk consolidators mutably and collect outputs.
                            if !handle_inner_for_task.consolidators.is_empty() {
                                let mut outs: SmallVec<[(ProviderKind, Candle); 16]> = SmallVec::new();
                                for mut entry in handle_inner_for_task.consolidators.iter_mut() {
                                    let provider = entry.key().provider;        // read key (cheap)
                                    if let Some(tt_types::consolidators::ConsolidatedOut::Candle(c)) =
                                        entry.value_mut().on_time(now)
                                    {
                                        outs.push((provider, c));
                                    }
                                } // all MapRefMut guards dropped here; no locks held
                                for (prov, c) in outs {
                                    strategy_for_task.on_bar(&c, prov);
                                }
                            }
                        }
                    },
                    resp_opt = rx.recv() => {
                        if let Some(resp) = resp_opt {
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
                                Response::DbUpdateComplete { corr_id, .. } => {
                                    let cid: u64 = *corr_id;
                                    if let Some((_k, tx)) = pending.remove(&cid) {
                                        let _ = tx.send(resp.clone());
                                        continue;
                                    }
                                }
                                _ => {}
                            }
                match resp {
                    Response::BacktestTimeUpdated { now } => {
                        if handle_inner_for_task.backtest_mode && let Some(prev_now) = last_bt_now && prev_now == now { continue; }
                        // Record logical time for other emissions
                        last_bt_now = Some(now);
                        // 1) Drive time-based consolidators using orchestrator-provided logical time
                        if !handle_inner_for_task.consolidators.is_empty() {
                            let mut outs: SmallVec<[(ProviderKind, Candle); 16]> = SmallVec::new();
                            for mut entry in handle_inner_for_task.consolidators.iter_mut() {
                                let provider = entry.key().provider;
                                if let Some(tt_types::consolidators::ConsolidatedOut::Candle(c)) = entry.value_mut().on_time(now) {
                                    outs.push((provider, c));
                                }
                            }
                            for (prov, c) in outs {
                                strategy_for_task.on_bar(&c, prov);
                            }
                        }
                        // 2) Throttle account snapshots using model-defined interval (emit even when flat)
                        {
                            use chrono::Duration as ChronoDuration;
                            let interval = ChronoDuration::from_std(pos_refresh_every).unwrap_or_else(|_| ChronoDuration::seconds(1));
                            let should_emit = match  last_pos_emit_bt {
                                None => { last_pos_emit_bt = Some(now); true },
                                Some(prev) => {
                                    if now - prev >= interval {
                                        last_pos_emit_bt = Some(now);
                                        true
                                    } else { false }
                                }
                            };
                            if should_emit {
                                // Accounts snapshot only (positions are emitted on structural changes only)
                                let acct_snap = pm.accounts_snapshot(now);
                                if !acct_snap.accounts.is_empty() {
                                    let _ = tx_internal.try_send(Response::AccountDeltaBatch(acct_snap));
                                }
                            }
                        }
                    },
                    Response::BacktestCompleted { end: _ } => {
                        // Graceful shutdown: stop strategy and terminate engine task
                        strategy_for_task.on_stop();
                        return;
                    },
                    Response::WarmupComplete{ .. } => {
                       strategy_for_task.on_warmup_complete();
                       if handle_inner_for_task.backtest_mode {
                           let now_bt = last_bt_now.unwrap_or_else(chrono::Utc::now);
                           let ab = pm.accounts_snapshot(now_bt);
                           if !ab.accounts.is_empty() {
                               let _ = tx_internal.try_send(Response::AccountDeltaBatch(ab));
                           }
                       }
                    },
                    Response::TickBatch(TickBatch {
                        ticks,
                        provider_kind,
                        ..
                    }) => {
                        for t in ticks {
                            // Update portfolio marks then deliver to strategy
                            pm.update_apply_last_price(provider_kind, &t.instrument, t.price, handle_inner_for_task.now_dt());
                            strategy_for_task.on_tick(&t, provider_kind);
                            // Drive any consolidators registered for ticks on this key
                            let key = ConsolidatorKey::new(t.instrument.clone(), provider_kind, Topic::Ticks);
                            if let Some(mut cons) = handle_inner_for_task
                                .consolidators
                                .get_mut(&key)
                                && let Some(out) = cons.on_tick(&t)
                                && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                            {
                                strategy_for_task.on_bar(c, provider_kind);
                            }
                        }
                    }
                    Response::QuoteBatch(QuoteBatch {
                        quotes,
                        provider_kind,
                        ..
                    }) => {
                        for q in quotes {
                            // Drive consolidators for quotes (BBO)
                            pm.update_apply_last_price(provider_kind, &q.instrument, q.bid, handle_inner_for_task.now_dt());
                            strategy_for_task.on_quote(&q, provider_kind);
                            let key = ConsolidatorKey::new(q.instrument.clone(), provider_kind, Topic::Quotes);
                            if let Some(mut cons) = handle_inner_for_task
                                .consolidators
                                .get_mut(&key)
                                && let Some(out) = cons.on_bbo(&q)
                                && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                            {
                                strategy_for_task.on_bar(c, provider_kind);
                            }
                        }
                    }
                    Response::BarBatch(BarBatch {
                        bars,
                        provider_kind,
                        ..
                    }) => {
                        for b in bars {
                            // Use close as mark
                            pm.update_apply_last_price(provider_kind, &b.instrument, b.close, handle_inner_for_task.now_dt());
                            strategy_for_task.on_bar(&b, provider_kind);
                            // Drive consolidators for incoming candles (candle-to-candle)
                            let tpc = match b.resolution {
                                tt_types::data::models::Resolution::Seconds(1) => Topic::Candles1s,
                                tt_types::data::models::Resolution::Minutes(1) => Topic::Candles1m,
                                tt_types::data::models::Resolution::Hours(1) => Topic::Candles1h,
                                tt_types::data::models::Resolution::Daily => Topic::Candles1d,
                                tt_types::data::models::Resolution::Weekly => Topic::Candles1d,
                                _ => Topic::Candles1d,
                            };
                            let key = ConsolidatorKey::new(b.instrument.clone(), provider_kind, tpc);
                            if let Some(mut cons) =
                                handle_inner_for_task.consolidators.get_mut(&key)
                                && let Some(out) = cons.on_candle(&b)
                                && let tt_types::consolidators::ConsolidatedOut::Candle(ref c) = out
                            {
                                strategy_for_task.on_bar(c, provider_kind);
                            }
                        }
                    }
                    Response::MBP10Batch(ob) => {
                        //todo, the way we apply Mpd10 will not actually be this way, it will be broken down by types.
                         // trades and bbo can build bars from the single feed.
                        let resp2 =
                            pm.process_response(tt_types::wire::Response::MBP10Batch(ob.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::MBP10Batch(ob2) = resp2 {
                            strategy_for_task.on_mbp10(&ob2.event, ob2.provider_kind);
                        }
                    }
                    Response::OrdersBatch(ob) => {
                        let resp2 =
                            pm.process_response(tt_types::wire::Response::OrdersBatch(ob.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::OrdersBatch(ob2) = resp2 {
                            {
                                let mut g =
                                    state.last_orders.write().expect("orders lock poisoned");
                                *g = Some(ob2.clone());
                            }
                            {
                                for o in ob2.orders.iter() {
                                    if let Some(pid) = &o.provider_order_id {
                                        handle_inner_for_task
                                            .provider_order_ids
                                            .insert(o.order_id, pid.0.clone());
                                        // If there is a pending cancel for this engine order, dispatch it now
                                        if let Some((_, mut spec)) = handle_inner_for_task.pending_cancels.remove(&o.order_id) {
                                            // fill provider order id and send
                                            spec.provider_order_id = pid.0.clone();
                                            let _ = bus_for_task
                                                .handle_request(&sub_id_for_task, Request::CancelOrder(spec))
                                                .await;
                                        }
                                        // If there is a pending replace, dispatch it now
                                        if let Some((_, mut spec)) = handle_inner_for_task.pending_replaces.remove(&o.order_id) {
                                            // fill provider order id and send
                                            spec.provider_order_id = pid.0.clone();
                                            let _ = bus_for_task
                                                .handle_request(&sub_id_for_task, Request::ReplaceOrder(spec))
                                                .await;
                                        }
                                    }
                                    if o.state.is_eol() {
                                        handle_inner_for_task.engine_order_accounts.remove(&o.order_id);
                                        handle_inner_for_task.provider_order_ids.remove(&o.order_id);
                                    }

                                }
                            }
                            strategy_for_task.on_orders_batch(&ob2);
                        }
                    }
                    Response::PositionsBatch(pb) => {
                        let resp2 = pm
                            .process_response(tt_types::wire::Response::PositionsBatch(pb.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::PositionsBatch(pb2) = resp2 {
                            {
                                let mut g = state
                                    .last_positions
                                    .write()
                                    .expect("positions lock poisoned");
                                *g = Some(pb2.clone());
                            }
                            strategy_for_task.on_positions_batch(&pb2);
                            // In backtest mode, whenever positions structurally change, emit an account snapshot too.
                            if handle_inner_for_task.backtest_mode {
                                let now_bt = last_bt_now.unwrap_or_else(chrono::Utc::now);
                                let ab = pm.accounts_snapshot(now_bt);
                                if !ab.accounts.is_empty() {
                                    let _ = tx_internal.try_send(Response::AccountDeltaBatch(ab));
                                }
                            }
                        }
                    }
                    Response::AccountDeltaBatch(ab) => {
                        let resp2 = pm.process_response(tt_types::wire::Response::AccountDeltaBatch(ab.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::AccountDeltaBatch(ab2) = resp2 {
                            {
                                let mut g =
                                    state.last_accounts.write().expect("accounts lock poisoned");
                                *g = Some(ab2.clone());
                            }
                            strategy_for_task.on_account_delta(&ab2.accounts);
                        }
                    }
                    Response::ClosedTrades(t) => {
                        let resp2 =
                            pm.process_response(tt_types::wire::Response::ClosedTrades(t.clone()), handle_inner_for_task.now_dt());
                        if let tt_types::wire::Response::ClosedTrades(t2) = resp2 {
                            strategy_for_task.on_trades_closed(t2);
                        }
                    }
                    Response::Tick {
                        tick,
                        provider_kind,
                    } => {
                        pm.update_apply_last_price(provider_kind, &tick.instrument, tick.price, handle_inner_for_task.now_dt());
                        strategy_for_task.on_tick(&tick, provider_kind);
                    }
                    Response::Quote { bbo, provider_kind } => {
                        let mid = (bbo.bid + bbo.ask) / rust_decimal::Decimal::from(2);
                        pm.update_apply_last_price(provider_kind, &bbo.instrument, mid, handle_inner_for_task.now_dt());
                        strategy_for_task.on_quote(&bbo, provider_kind);
                    }
                    Response::Bar {
                        candle,
                        provider_kind,
                    } => {
                        pm.update_apply_last_price(provider_kind, &candle.instrument, candle.close, handle_inner_for_task.now_dt());
                        strategy_for_task.on_bar(&candle, provider_kind);
                    }
                    Response::Mbp10 {
                        mbp10,
                        provider_kind,
                    } => {
                        //todo, the way we apply Mpd10 will not actually be this way, it will be broken down by types.
                        // trades and bbo can build bars from the single feed.
                        pm.update_apply_last_price(provider_kind, &mbp10.instrument, mbp10.price, handle_inner_for_task.now_dt());
                        strategy_for_task.on_mbp10(&mbp10, provider_kind);
                    }
                    Response::AnnounceShm(ann) => {
                        if handle_inner_for_task.backtest_mode { continue; }
                        use tokio::task::JoinHandle;
                        use tracing::error;
                        use tt_types::data::core::{Bbo, Tick};
                        use tt_types::data::mbp10::Mbp10;
                        use tt_types::wire::Bytes;
                        let topic = ann.topic;
                        let key = ann.key.clone();
                        // If this (topic,key) was blacklisted due to SHM errors, skip spawning reader
                        if shm_blacklist_for_task.get(&(topic, key.clone())).is_some() {
                            info!(?topic, ?key, "SHM disabled for this stream; staying on UDS");
                        } else if shm_tasks.get(&(topic, key.clone())).is_none() {
                            let tx_shm = tx_internal.clone();
                            let value = key.clone();
                            let bus = bus_for_task.clone();
                            let sub_id = sub_id_for_task.clone();
                            let shm_blacklist = shm_blacklist_for_task.clone();

                            let handle: JoinHandle<()> = tokio::spawn(async move {
                                let mut last_seq: u32 = 0;
                                let mut consecutive_failures: u32 = 0;
                                loop {
                                    let mut progressed = false;
                                    #[allow(clippy::collapsible_if)]
                                    if let Some(reader) = tt_shm::ensure_reader(topic, &value) {
                                        if let Some((seq, buf)) = reader.read_with_seq() {
                                            if seq != last_seq {
                                                progressed = true;
                                                last_seq = seq;
                                                let maybe_resp = match topic {
                                                    Topic::Quotes => Bbo::from_bytes(&buf)
                                                        .ok()
                                                        .map(|bbo| Response::Quote {
                                                            bbo,
                                                            provider_kind: key.provider,
                                                        }),
                                                    Topic::Ticks => Tick::from_bytes(&buf)
                                                        .ok()
                                                        .map(|t| Response::Tick {
                                                            tick: t,
                                                            provider_kind: key.provider,
                                                        }),
                                                    Topic::MBP10 => Mbp10::from_bytes(&buf)
                                                        .ok()
                                                        .map(|m| Response::Mbp10 {
                                                            mbp10: m,
                                                            provider_kind: key.provider,
                                                        }),
                                                    _ => None,
                                                };
                                                if let Some(resp) = maybe_resp {
                                                    consecutive_failures = 0;
                                                    if tx_shm.is_closed() {
                                                        error!(
                                                            "tx shm channel closed; terminating SHM task"
                                                        );
                                                        break;
                                                    }
                                                    if let Err(e) = tx_shm.send(resp).await {
                                                        error!(
                                                            "tx shm send failed: {e}; terminating SHM task"
                                                        );
                                                        break;
                                                    }
                                                } else {
                                                    // Parse failure or unsupported topic buffer
                                                    consecutive_failures =
                                                        consecutive_failures.saturating_add(1);
                                                    if consecutive_failures >= 8 {
                                                        // Mark this (topic,key) as SHM-failed and request framed fallback
                                                        info!(?topic, key = ?value, fails = consecutive_failures, "SHM decode failures; falling back to UDS for this stream");
                                                        shm_blacklist
                                                            .insert((topic, value.clone()), ());
                                                        // Ask server to (re)subscribe; providers may resume framed publishing when SHM is disabled client-side
                                                        let _ = bus
                                                            .handle_request(
                                                                &sub_id,
                                                                Request::SubscribeKey(
                                                                    tt_types::wire::SubscribeKey {
                                                                        topic,
                                                                        key: value.clone(),
                                                                        latest_only: false,
                                                                        from_seq: 0,
                                                                    },
                                                                ),
                                                            )
                                                            .await;
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                    }

                                    // If no progress (no reader, no new seq, or decode produced no message),
                                    // yield/sleep so task can be aborted and we avoid busy-spin.
                                    if !progressed {
                                        if let Some(ns) = slow_spin_ns {
                                            tokio::time::sleep(Duration::from_nanos(ns)).await;
                                        } else {
                                            tokio::task::yield_now().await;
                                        }
                                    }
                                }
                            });
                            shm_tasks.insert((topic, key), handle);
                        }
                    }
                    Response::SubscribeResponse {
                        topic,
                        instrument,
                        success,
                    } => {
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
                    Response::DbUpdateComplete { .. } => {}
                }
                // After processing a message in backtest mode, notify feeder so it can proceed to the next one.
                if handle_inner_for_task.backtest_mode && let Some(notify) = &backtest_notify_for_task {
                    notify.notify_one();
                }
                // Flush any commands the strategy enqueued during this message
                Self::drain_commands_for_task(
                    cmd_q_for_task.clone(),
                    bus_for_task.clone(),
                    sub_id_for_task.clone(),
                    securities_watch_for_task.clone(),
                    securities_by_provider_for_task.clone(),
                    handle_inner_for_task.backtest_mode,
                )
                .await;
                        } else { break; }
                    }
                }
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
        backtest_mode: bool,
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
                    if !backtest_mode {
                        ensure_vendor(
                            bus.clone(),
                            key.provider,
                            securities_watch.clone(),
                            securities_by_provider.clone(),
                        )
                        .await;
                    }
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

    #[allow(dead_code)]
    /// Trigger a historical DB update for the latest data for a given provider/topic/instrument.
    /// This sends a DbUpdateKeyLatest request and blocks until the DbUpdateComplete response arrives.
    /// Returns Ok(()) when the update completed successfully; otherwise returns Err with the server error message.
    fn update_historical_latest_by_key(
        &self,
        provider: ProviderKind,
        topic: Topic,
        instrument: Instrument,
    ) -> anyhow::Result<()> {
        use anyhow::anyhow;
        use tt_types::wire::{DbUpdateKeyLatest, Response as WireResp};
        let fut = async {
            let instr_clone = instrument.clone();
            let rx = self
                .request_with_corr(|corr_id| {
                    Request::DbUpdateKeyLatest(DbUpdateKeyLatest {
                        provider,
                        instrument: instr_clone,
                        topic,
                        corr_id,
                    })
                })
                .await;
            match rx.await {
                Ok(WireResp::DbUpdateComplete {
                    success, error_msg, ..
                }) => {
                    if success {
                        Ok(())
                    } else {
                        Err(anyhow!(
                            error_msg.unwrap_or_else(|| "historical update failed".to_string())
                        ))
                    }
                }
                Ok(other) => Err(anyhow!(format!("unexpected response: {:?}", other))),
                Err(_canceled) => Err(anyhow!("engine response channel closed")),
            }
        };
        // Execute the async flow in a blocking manner
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // Ensure we don't block the runtime reactor; run in a blocking section
            tokio::task::block_in_place(|| handle.block_on(fut))
        } else {
            let rt = tokio::runtime::Runtime::new().map_err(|e| anyhow!(e))?;
            rt.block_on(fut)
        }
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        info!("engine: stopping");
        // Send Kick to bus in live modes; skip in backtest
        if !self.backtest_mode {
            self.bus
                .handle_request(
                    self.sub_id.as_ref().expect("engine started"),
                    tt_types::wire::Request::Kick(Kick {
                        reason: Some("Shutting Down".to_string()),
                    }),
                )
                .await
                .ok();
        }
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
    pub fn last_orders_batch(&self) -> Option<OrdersBatch> {
        self.state
            .last_orders
            .read()
            .expect("orders lock poisoned")
            .clone()
    }
    pub fn last_positions_batch(&self) -> Option<PositionsBatch> {
        self.state
            .last_positions
            .read()
            .expect("positions lock poisoned")
            .clone()
    }
    pub fn last_account_delta_batch(&self) -> Option<AccountDeltaBatch> {
        self.state
            .last_accounts
            .read()
            .expect("accounts lock poisoned")
            .clone()
    }
}

#[cfg(test)]
mod engine_shm_tests {
    use crate::runtime::EngineRuntime;
    use crate::traits::Strategy;
    use std::str::FromStr;
    use tokio::time::{Duration, sleep};
    use tt_bus::ClientMessageBus;
    use tt_types::keys::{SymbolKey, Topic};
    use tt_types::providers::ProviderKind;
    use tt_types::securities::symbols::Instrument;
    use tt_types::wire::Response;

    struct NopStrategy;
    impl Strategy for NopStrategy {}

    // When an AnnounceShm arrives for a non-blacklisted (topic,key), the engine should spawn an SHM reader task.
    #[tokio::test]
    async fn shm_reader_is_spawned_on_announce_when_not_blacklisted() {
        let (req_tx, mut _req_rx) = tokio::sync::mpsc::channel::<tt_types::wire::Request>(8);
        let bus = ClientMessageBus::new_with_transport(req_tx);

        let mut rt = EngineRuntime::new(bus.clone(), Some(1));
        let _handle = rt.start(NopStrategy).await.expect("engine start");

        let key = SymbolKey::new(
            Instrument::from_str("TST.Z25").unwrap(),
            ProviderKind::ProjectX(tt_types::providers::ProjectXTenant::Topstep),
        );
        let ann = tt_types::wire::AnnounceShm {
            topic: Topic::MBP10,
            key: key.clone(),
            name: "test.mbp10".to_string(),
            layout_ver: 1,
            size: 4096,
        };

        // Deliver AnnounceShm to the engine via in-memory bus routing.
        let _ = bus.route_response(Response::AnnounceShm(ann)).await;

        // Give the engine loop a short moment to spawn the task.
        sleep(Duration::from_millis(20)).await;

        assert!(
            rt.shm_tasks.get(&(Topic::MBP10, key.clone())).is_some(),
            "expected SHM task to be spawned for non-blacklisted stream"
        );
        let _ = tokio::time::timeout(Duration::from_secs(2), rt.stop())
            .await
            .expect("engine stop should not hang");
    }

    //todo fix test make sure it works
    // If the (topic,key) is blacklisted due to previous SHM errors, AnnounceShm should not spawn a reader task.
    #[tokio::test]
    async fn shm_reader_is_skipped_when_blacklisted() {
        let (req_tx, mut _req_rx) = tokio::sync::mpsc::channel::<tt_types::wire::Request>(8);
        let bus = ClientMessageBus::new_with_transport(req_tx);

        let mut rt = EngineRuntime::new(bus.clone(), Some(1));
        let key = SymbolKey::new(
            Instrument::from_str("TST.Z25").unwrap(),
            ProviderKind::ProjectX(tt_types::providers::ProjectXTenant::Topstep),
        );
        // Pre-mark the stream as SHM-disabled
        rt.shm_blacklist.insert((Topic::MBP10, key.clone()), ());

        let _handle = rt.start(NopStrategy).await.expect("engine start");

        let ann = tt_types::wire::AnnounceShm {
            topic: Topic::MBP10,
            key: key.clone(),
            name: "test.mbp10".to_string(),
            layout_ver: 1,
            size: 4096,
        };
        let _ = bus.route_response(Response::AnnounceShm(ann)).await;

        // Give the engine loop a short moment; it should NOT spawn a task for blacklisted key.
        sleep(Duration::from_millis(20)).await;

        assert!(
            rt.shm_tasks.get(&(Topic::MBP10, key.clone())).is_none(),
            "SHM task should not be spawned for blacklisted stream"
        );

        let _ = tokio::time::timeout(Duration::from_secs(2), rt.stop())
            .await
            .expect("engine stop should not hang");
    }
}
