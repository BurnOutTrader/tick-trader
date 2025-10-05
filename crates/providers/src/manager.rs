use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use provider::traits::{MarketDataProvider, ExecutionProvider, ProviderSessionSpec};
use tt_types::providers::ProviderKind;
use tracing::info;
use tt_bus::ServerMessageBus;
use crate::worker::ProviderWorker;

/// Minimal ProviderManager that ensures a provider pair exists for a given ProviderKind.
/// For this initial pass, it uses the ProjectX in-process adapter and returns dyn traits.
#[derive(Default)]
pub struct ProviderManager {
    md: DashMap<ProviderKind, Arc<dyn MarketDataProvider>>,
    ex: DashMap<ProviderKind, Arc<dyn ExecutionProvider>>,
    // Pool of workers per provider kind, indexed by shard id
    workers: DashMap<(ProviderKind, usize), Arc<crate::worker::InprocessWorker>>,
    shards: usize,
}

impl ProviderManager {
    pub fn new() -> Self { Self { md: DashMap::new(), ex: DashMap::new(), workers: DashMap::new(), shards: 4 } }
    pub fn with_shards(shards: usize) -> Self { Self { md: DashMap::new(), ex: DashMap::new(), workers: DashMap::new(), shards: shards.max(1) } }

    pub async fn ensure_pair(
        &self,
        kind: ProviderKind,
        session: ProviderSessionSpec,
        bus: Arc<ServerMessageBus>,
    ) -> Result<(Arc<dyn MarketDataProvider>, Arc<dyn ExecutionProvider>)> {
        if let (Some(m), Some(e)) = (self.md.get(&kind), self.ex.get(&kind)) {
            return Ok((m.clone(), e.clone()));
        }
        match kind {
            ProviderKind::ProjectX(_) => {
                let (md, ex) = adapters_projectx::factory::create_provider_pair(kind, session, bus).await?;
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
