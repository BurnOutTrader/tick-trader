use crate::download_manager::DownloadManager;
use crate::worker::ProviderWorker;
use anyhow::Result;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::sync::Arc;
use tracing::error;
use tt_bus::Router;
use tt_bus::UpstreamManager;
use tt_types::accounts::events::OrderUpdate;
use tt_types::accounts::order::OrderState;
use tt_types::history::HistoricalRangeRequest;
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::server_side::traits::{
    ExecutionProvider, HistoricalDataProvider, MarketDataProvider, ProviderSessionSpec,
};
use tt_types::wire::OrdersBatch;
// Added for unified wait semantics and diagnostics
use tokio::time::{Duration as TokioDuration, timeout};
use tt_database::init::init_db;
use tt_database::queries::latest_data_time;

/// Minimal ProviderManager that ensures a provider pair exists for a given ProviderKind.
/// For this initial pass, it uses the ProjectX in-process adapter and returns dyn traits.
pub struct ProviderManager {
    md: DashMap<ProviderKind, Arc<dyn MarketDataProvider>>,
    ex: DashMap<ProviderKind, Arc<dyn ExecutionProvider>>,
    hist: DashMap<ProviderKind, Arc<dyn HistoricalDataProvider>>,
    // Pool of workers per provider kind, indexed by shard id
    workers: DashMap<(ProviderKind, usize), Arc<crate::worker::InprocessWorker>>,
    shards: usize,
    // Upstream wiring requirements
    bus: Arc<Router>,
    session: ProviderSessionSpec,
    download_manager: DownloadManager,
}

#[async_trait::async_trait]
impl UpstreamManager for ProviderManager {
    async fn subscribe_md(
        &self,
        topic: tt_types::keys::Topic,
        key: &tt_types::keys::SymbolKey,
    ) -> Result<()> {
        let kind = key.provider;
        // Ensure provider pair and workers exist for this provider kind
        let _ = self.ensure_clients(kind).await?;
        // Delegate to the appropriate worker based on shard
        self.subscribe_md(topic, key).await
    }
    async fn unsubscribe_md(
        &self,
        topic: tt_types::keys::Topic,
        key: &tt_types::keys::SymbolKey,
    ) -> Result<()> {
        let kind = key.provider;
        // If workers exist, delegate; otherwise no-op
        let _ = self.ensure_clients(kind).await?; // ensure present to have a worker map; returns immediately if already exists
        self.unsubscribe_md(topic, key).await
    }

    async fn subscribe_account(&self, key: tt_types::keys::AccountKey) -> Result<()> {
        let kind = key.provider;
        self.ensure_clients(kind).await?;
        let ex = self
            .ex
            .get(&kind)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("execution provider missing"))?;
        // Subscribe all relevant execution streams for this account
        ex.subscribe_account_events(&key).await?;
        ex.subscribe_positions(&key).await?;
        ex.subscribe_order_updates(&key).await?;
        Ok(())
    }

    async fn unsubscribe_account(&self, key: tt_types::keys::AccountKey) -> Result<()> {
        let kind = key.provider;
        self.ensure_clients(kind).await?;
        let ex = self
            .ex
            .get(&kind)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("execution provider missing"))?;
        // Unsubscribe all relevant streams
        let _ = ex.unsubscribe_order_updates(&key).await; // best-effort
        let _ = ex.unsubscribe_positions(&key).await;
        let _ = ex.unsubscribe_account_events(&key).await;
        Ok(())
    }

    async fn place_order(&self, spec: tt_types::wire::PlaceOrder) -> Result<()> {
        // Ensure execution provider for this key's provider kind
        let kind = spec.account_key.provider;
        self.ensure_clients(kind).await?;
        let ex = self
            .ex
            .get(&kind)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("execution provider missing"))?;
        let r = ex.place_order(spec.clone()).await;
        if !r.ok {
            let update = OrderUpdate {
                account_name: spec.account_key.account_name,
                instrument: spec.instrument,
                provider_kind: spec.account_key.provider,
                provider_order_id: None,
                order_id: spec.order_id,
                state: OrderState::Rejected,
                leaves: 0,
                cum_qty: 0,
                side: spec.side,
                avg_fill_px: Default::default(),
                tag: spec.custom_tag,
                time: Utc::now(),
                msg: r.message,
            };
            let batch = OrdersBatch {
                topic: Topic::Orders,
                seq: 0,
                orders: vec![update],
            };
            if let Err(e) = self.bus.publish_orders_batch(batch).await {
                error!(target: "manager", "failed to publish OrderUpdate: {:?}", e);
            }
        }

        Ok(())
    }

    async fn cancel_order(&self, spec: tt_types::wire::CancelOrder) -> Result<()> {
        let kind = if let Some(p) = self.ex.iter().next().map(|e| *e.key()) {
            p
        } else {
            return Err(anyhow::anyhow!("no providers"));
        };
        self.ensure_clients(kind).await?;
        let ex = self
            .ex
            .get(&kind)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("execution provider missing"))?;
        let _ = ex.cancel_order(spec).await;
        Ok(())
    }

    async fn replace_order(&self, spec: tt_types::wire::ReplaceOrder) -> Result<()> {
        let kind = if let Some(p) = self.ex.iter().next().map(|e| *e.key()) {
            p
        } else {
            return Err(anyhow::anyhow!("no providers"));
        };
        self.ensure_clients(kind).await?;
        let ex = self
            .ex
            .get(&kind)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("execution provider missing"))?;
        let _ = ex.replace_order(spec).await;
        Ok(())
    }

    async fn get_account_info(
        &self,
        provider: ProviderKind,
    ) -> Result<tt_types::wire::AccountInfoResponse> {
        // Ensure provider exists; connect execution side if needed
        self.ensure_clients(provider).await?;
        let ex = self
            .ex
            .get(&provider)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("execution provider missing"))?;
        // Query provider for account snapshots and map to wire summaries
        let snaps = ex.list_accounts().await.unwrap_or_default();
        let mut accounts = Vec::with_capacity(snaps.len());
        for s in snaps {
            accounts.push(tt_types::wire::AccountSummaryWire {
                account_id: s.id,
                account_name: s.name.clone(),
                provider,
            });
        }
        Ok(tt_types::wire::AccountInfoResponse {
            provider,
            corr_id: 0,
            accounts,
        })
    }

    async fn get_instruments(
        &self,
        provider: ProviderKind,
        pattern: Option<String>,
    ) -> Result<Vec<tt_types::securities::symbols::Instrument>> {
        // Ensure provider exists and ask the MD side for instruments (if supported by the trait impl)
        self.ensure_clients(provider).await?;
        if let Some(md) = self.md.get(&provider) {
            let list = md.list_instruments(pattern).await.unwrap_or_default();
            return Ok(list);
        }
        Ok(Vec::new())
    }

    async fn get_securities(&self, provider: ProviderKind) -> Result<Vec<FuturesContract>> {
        self.ensure_clients(provider).await?;
        if let Some(md) = self.md.get(&provider) {
            let vec = md.instruments().await.unwrap_or_default();
            return Ok(vec);
        }
        Ok(Vec::new())
    }

    async fn update_historical_latest_by_key(
        &self,
        provider: ProviderKind,
        topic: tt_types::keys::Topic,
        instrument: tt_types::securities::symbols::Instrument,
    ) -> anyhow::Result<Option<DateTime<Utc>>> {
        // Ensure clients for this provider exist
        self.ensure_clients(provider).await?;
        // Find historical client
        let client = if let Some(c) = self.hist.get(&provider) {
            c.value().clone()
        } else {
            return Err(anyhow::anyhow!(
                "Unsupported historical data provider: {:?}",
                provider
            ));
        };
        if !client.supports(topic) {
            return Err(anyhow::anyhow!(
                "Unsupported topic: {:?} for historical data provider: {:?}",
                provider,
                topic
            ));
        }

        // Diagnostics: capture latest before
        let db = init_db()?;
        let before_latest = latest_data_time(&db, provider, &instrument, topic).await?;
        tracing::info!(caller="ProviderManager.update_historical_latest_by_key", provider=?provider, instrument=%instrument, topic=?topic, before_latest=?before_latest, "starting historical latest update");

        let now = chrono::Utc::now();
        let req = HistoricalRangeRequest {
            provider_kind: provider,
            topic,
            instrument: instrument.clone(),
            // The download manager computes the actual start time from DB latest; these are placeholders
            start: now - chrono::Duration::days(1),
            end: now,
        };
        let dm = &self.download_manager;
        // Use a unified wait-for-completion with timeout to prevent hangs
        const TIMEOUT_SECS: u64 = 300;
        if let Some(handle) = dm.request_update(client.clone(), req.clone()).await? {
            tracing::info!(task_id=%handle.id(), provider=?provider, instrument=%req.instrument, topic=?req.topic, "historical latest update already inflight; waiting for completion");
            let wait_res = timeout(TokioDuration::from_secs(TIMEOUT_SECS), handle.wait()).await;
            match wait_res {
                Ok(res) => res?,
                Err(_) => {
                    tracing::warn!(provider=?provider, instrument=%req.instrument, topic=?req.topic, timeout_secs=TIMEOUT_SECS, "historical latest update wait timed out");
                    return Err(anyhow::anyhow!("historical update wait timed out"));
                }
            }
        } else {
            let handle = dm.start_update(client.clone(), req.clone()).await?;
            tracing::info!(task_id=%handle.id(), provider=?provider, instrument=%req.instrument, topic=?req.topic, "historical latest update started; waiting for completion");
            let wait_res = timeout(TokioDuration::from_secs(TIMEOUT_SECS), handle.wait()).await;
            match wait_res {
                Ok(res) => res?,
                Err(_) => {
                    tracing::warn!(provider=?provider, instrument=%req.instrument, topic=?req.topic, timeout_secs=TIMEOUT_SECS, "historical latest update wait timed out");
                    return Err(anyhow::anyhow!("historical update wait timed out"));
                }
            }
        }

        // Diagnostics: capture latest after
        let after_latest = latest_data_time(&db, provider, &instrument, topic).await?;
        tracing::info!(provider=?provider, instrument=%instrument, topic=?topic, before_latest=?before_latest, after_latest=?after_latest, advanced=?after_latest.as_ref().zip(before_latest).map(|(a,b)| *a > b).unwrap_or(after_latest.is_some() && before_latest.is_none()), "historical latest update finished");
        Ok(after_latest)
    }
}

#[allow(dead_code)]
fn type_i_from(t: tt_types::wire::OrderType) -> i32 {
    match t {
        tt_types::wire::OrderType::Limit => 1,
        tt_types::wire::OrderType::Market => 2,
        tt_types::wire::OrderType::Stop => 4,
        tt_types::wire::OrderType::TrailingStop => 5,
        tt_types::wire::OrderType::JoinBid => 6,
        tt_types::wire::OrderType::JoinAsk => 7,
        tt_types::wire::OrderType::StopLimit => 4,
    }
}

impl ProviderManager {
    pub async fn new_async(router: Arc<Router>) -> anyhow::Result<ProviderManager> {
        let session = ProviderSessionSpec::from_env();
        let download_manager = DownloadManager::new_async().await?;
        Ok(Self {
            md: DashMap::new(),
            ex: DashMap::new(),
            hist: DashMap::new(),
            download_manager,
            workers: DashMap::new(),
            shards: 8,
            bus: router,
            session,
        })
    }
    pub async fn with_shards_async(
        router: Arc<Router>,
        shards: usize,
    ) -> anyhow::Result<ProviderManager> {
        let session = ProviderSessionSpec::from_env();
        let download_manager = DownloadManager::new_async().await?;
        Ok(Self {
            md: DashMap::new(),
            ex: DashMap::new(),
            hist: DashMap::new(),
            download_manager,
            workers: DashMap::new(),
            shards,
            bus: router,
            session,
        })
    }

    pub async fn ensure_clients(&self, kind: ProviderKind) -> anyhow::Result<()> {
        match kind {
            ProviderKind::ProjectX(_) => {
                if let (Some(_m), Some(_e), Some(_h)) =
                    (self.md.get(&kind), self.ex.get(&kind), self.hist.get(&kind))
                {
                    return Ok(());
                }
                let (md, ex, hist) = projectx::factory::create_provider_pair(
                    kind,
                    self.session.clone(),
                    self.bus.clone(),
                )
                .await?;
                // Connect market side once on first creation so that subsequent subscribes succeed
                if let Err(e) = md.connect_to_market(kind, self.session.clone()).await {
                    tracing::error!(provider=?kind, error=%e, "ProviderManager: connect_to_market failed");
                    return Err(e);
                }
                // Connect execution side as well so account streams and order APIs are ready
                if let Err(e) = ex.connect_to_broker(kind, self.session.clone()).await {
                    tracing::error!(provider=?kind, error=%e, "ProviderManager: connect_to_broker failed");
                    return Err(e);
                }
                // Build workers (shards) that share the same underlying MD provider
                for shard in 0..self.shards {
                    let w = Arc::new(crate::worker::InprocessWorker::new(
                        md.clone(),
                        self.bus.clone(),
                    ));
                    self.workers.insert((kind, shard), w);
                }
                self.md.insert(kind, md.clone());
                self.ex.insert(kind, ex.clone());
                self.hist.insert(kind, hist.clone());
                Ok(())
            }
            ProviderKind::Rithmic(_) => anyhow::bail!("Rithmic not implemented"),
        }
    }

    /// Optionally allow updating the session (e.g., after AuthCredentials)
    pub fn set_session(&mut self, session: ProviderSessionSpec) {
        self.session = session;
    }

    fn shard_of(&self, topic: tt_types::keys::Topic, key: &tt_types::keys::SymbolKey) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        topic.hash(&mut h);
        key.hash(&mut h);
        (h.finish() as usize) % self.shards
    }

    pub async fn subscribe_md(
        &self,
        topic: tt_types::keys::Topic,
        key: &tt_types::keys::SymbolKey,
    ) -> Result<()> {
        let kind = key.provider;
        let shard = self.shard_of(topic, key);
        if let Some(w) = self.workers.get(&(kind, shard)) {
            w.subscribe_md(topic, key).await
        } else {
            anyhow::bail!("worker not initialized for provider {kind:?}")
        }
    }

    pub async fn unsubscribe_md(
        &self,
        topic: tt_types::keys::Topic,
        key: &tt_types::keys::SymbolKey,
    ) -> Result<()> {
        let kind = key.provider;
        let shard = self.shard_of(topic, key);
        if let Some(w) = self.workers.get(&(kind, shard)) {
            w.unsubscribe_md(topic, key).await
        } else {
            Ok(())
        }
    }
}
