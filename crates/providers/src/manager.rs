use anyhow::Result;
use dashmap::DashMap;
use std::sync::Arc;
use provider::traits::{MarketDataProvider, ExecutionProvider, ProviderSessionSpec};
use tt_types::providers::ProviderKind;
use tracing::info;
use tt_bus::ServerMessageBus;

/// Minimal ProviderManager that ensures a provider pair exists for a given ProviderKind.
/// For this initial pass, it uses the ProjectX in-process adapter and returns dyn traits.
#[derive(Default)]
pub struct ProviderManager {
    md: DashMap<ProviderKind, Arc<dyn MarketDataProvider>>,
    ex: DashMap<ProviderKind, Arc<dyn ExecutionProvider>>,
}

impl ProviderManager {
    pub fn new() -> Self { Self::default() }

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
                self.md.insert(kind, md.clone());
                self.ex.insert(kind, ex.clone());
                Ok((md, ex))
            }
            ProviderKind::Rithmic(_) => anyhow::bail!("Rithmic not implemented"),
        }
    }
}
