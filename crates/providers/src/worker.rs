use anyhow::Result;
use async_trait::async_trait;
use dashmap::DashMap;
use std::sync::Arc;
use provider::traits::MarketDataProvider;
use tt_types::keys::{SymbolKey, Topic};

#[async_trait]
pub trait ProviderWorker: Send + Sync {
    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()>;
    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()>;
}

pub struct InprocessWorker {
    md: Arc<dyn MarketDataProvider>,
    interest: DashMap<(Topic, SymbolKey), usize>,
    router: std::sync::Arc<tt_bus::Router>,
}

impl InprocessWorker {
    pub fn new(md: Arc<dyn MarketDataProvider>, router: std::sync::Arc<tt_bus::Router>) -> Self {
        Self { md, interest: DashMap::new(), router }
    }
}

#[async_trait]
impl ProviderWorker for InprocessWorker {
    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()> {
        let mut e = self.interest.entry((topic, key.clone())).or_insert(0);
        if *e == 0 {
            // On first local subscriber, ensure upstream and announce SHM for hot snapshots
            self.md.subscribe_md(topic, key).await?;
            if matches!(topic, Topic::Quotes | Topic::Depth | Topic::Ticks) {
                let name = tt_shm::suggest_name(topic, key);
                let size = if matches!(topic, Topic::Depth) {
                    tt_shm::DEFAULT_DEPTH_SNAPSHOT_SIZE
                } else if matches!(topic, Topic::Quotes) {
                    tt_shm::DEFAULT_QUOTE_SNAPSHOT_SIZE
                } else {
                    tt_shm::DEFAULT_TICK_SNAPSHOT_SIZE
                };
                let ann = tt_types::wire::AnnounceShm { topic, key: key.clone(), name, layout_ver: 1, size: size as u64 };
                let _ = self.router.announce_shm_for_key(ann).await;
            }
        }
        *e += 1;
        Ok(())
    }

    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()> {
        if let Some(mut e) = self.interest.get_mut(&(topic, key.clone())) {
            if *e > 0 { *e -= 1; }
            if *e == 0 {
                drop(e);
                self.interest.remove(&(topic, key.clone()));
                self.md.unsubscribe_md(topic, key).await?;
                // On last local unsubscribe, remove SHM snapshot for hot topics
                if matches!(topic, Topic::Quotes | Topic::Depth | Topic::Ticks) {
                    tt_shm::remove_snapshot(topic, key);
                }
            }
        }
        Ok(())
    }
}
