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
}

impl InprocessWorker {
    pub fn new(md: Arc<dyn MarketDataProvider>) -> Self {
        Self { md, interest: DashMap::new() }
    }
}

#[async_trait]
impl ProviderWorker for InprocessWorker {
    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey) -> Result<()> {
        let mut e = self.interest.entry((topic, key.clone())).or_insert(0);
        if *e == 0 {
            self.md.subscribe_md(topic, key).await?;
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
            }
        }
        Ok(())
    }
}
