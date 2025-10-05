use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use provider::traits::{MarketDataProvider, ExecutionProvider, ProviderSessionSpec};
use tt_types::providers::ProviderKind;
use tracing::info;
use tt_bus::Router;
use crate::worker::ProviderWorker;
use tt_bus::UpstreamManager;

/// Minimal ProviderManager that ensures a provider pair exists for a given ProviderKind.
/// For this initial pass, it uses the ProjectX in-process adapter and returns dyn traits.
pub struct ProviderManager {
    md: DashMap<ProviderKind, Arc<dyn MarketDataProvider>>,
    ex: DashMap<ProviderKind, Arc<dyn ExecutionProvider>>,
    // Pool of workers per provider kind, indexed by shard id
    workers: DashMap<(ProviderKind, usize), Arc<crate::worker::InprocessWorker>>,
    shards: usize,
    // Upstream wiring requirements
    bus: Arc<Router>,
    session: ProviderSessionSpec,
}

#[async_trait::async_trait]
impl UpstreamManager for ProviderManager {
    async fn subscribe_md(&self, topic: tt_types::keys::Topic, key: &tt_types::keys::SymbolKey) -> Result<()> {
        let kind = key.provider;
        // Ensure provider pair and workers exist for this provider kind
        let _ = self.ensure_pair(kind).await?;
        // Delegate to the appropriate worker based on shard
        self.subscribe_md(topic, key).await
    }
    async fn unsubscribe_md(&self, topic: tt_types::keys::Topic, key: &tt_types::keys::SymbolKey) -> Result<()> {
        let kind = key.provider;
        // If workers exist, delegate; otherwise no-op
        let _ = self.ensure_pair(kind).await?; // ensure present to have a worker map; returns immediately if already exists
        self.unsubscribe_md(topic, key).await
    }
}

impl ProviderManager {
    pub fn new(router: Arc<Router>) -> Self {
        let session = ProviderSessionSpec::from_env();
        Self { md: DashMap::new(), ex: DashMap::new(), workers: DashMap::new(), shards: 4, bus: router, session }
    }
    pub fn with_shards(router: Arc<Router>, shards: usize) -> Self {
        let session = ProviderSessionSpec::from_env();
        Self { md: DashMap::new(), ex: DashMap::new(), workers: DashMap::new(), shards: shards.max(1), bus: router, session }
    }
    pub fn with_router_and_session(router: Arc<Router>, shards: usize, session: ProviderSessionSpec) -> Self {
        Self { md: DashMap::new(), ex: DashMap::new(), workers: DashMap::new(), shards: shards.max(1), bus: router, session }
    }

    pub async fn ensure_pair(
        &self,
        kind: ProviderKind,
    ) -> Result<(Arc<dyn MarketDataProvider>, Arc<dyn ExecutionProvider>)> {
        if let (Some(m), Some(e)) = (self.md.get(&kind), self.ex.get(&kind)) {
            return Ok((m.clone(), e.clone()));
        }
        match kind {
            ProviderKind::ProjectX(_) => {
                let (md, ex) = adapters_projectx::factory::create_provider_pair(kind, self.session.clone(), self.bus.clone()).await?;
                // Build workers (shards) that share the same underlying MD provider
                for shard in 0..self.shards {
                    let w = Arc::new(crate::worker::InprocessWorker::new(md.clone()));
                    self.workers.insert((kind, shard), w);
                }
                self.md.insert(kind, md.clone());
                self.ex.insert(kind, ex.clone());
                Ok((md, ex))
            }
            ProviderKind::Rithmic(_) => anyhow::bail!("Rithmic not implemented"),
        }
    }

    /// Optionally allow updating the session (e.g., after AuthCredentials)
    pub fn set_session(&mut self, session: ProviderSessionSpec) { self.session = session; }

    fn shard_of(&self, topic: tt_types::keys::Topic, key: &tt_types::keys::SymbolKey) -> usize {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        topic.hash(&mut h);
        key.hash(&mut h);
        (h.finish() as usize) % self.shards
    }

    pub async fn subscribe_md(&self, topic: tt_types::keys::Topic, key: &tt_types::keys::SymbolKey) -> Result<()> {
        let kind = key.provider;
        let shard = self.shard_of(topic, key);
        if let Some(w) = self.workers.get(&(kind, shard)) {
            w.subscribe_md(topic, key).await
        } else {
            anyhow::bail!("worker not initialized for provider {kind:?}")
        }
    }

    pub async fn unsubscribe_md(&self, topic: tt_types::keys::Topic, key: &tt_types::keys::SymbolKey) -> Result<()> {
        let kind = key.provider;
        let shard = self.shard_of(topic, key);
        if let Some(w) = self.workers.get(&(kind, shard)) {
            w.unsubscribe_md(topic, key).await
        } else {
            Ok(())
        }
    }
}
