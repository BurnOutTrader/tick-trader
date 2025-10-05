use std::sync::Arc;
use provider::traits::{MarketDataProvider, ExecutionProvider, ProviderSessionSpec};
use tt_types::providers::ProviderKind;
use tt_bus::ServerMessageBus;

use crate::client::PXClient;

// Build a ProjectX provider and return as trait objects for both MD and EX roles.
pub async fn create_provider_pair(
    kind: ProviderKind,
    session: ProviderSessionSpec,
    bus: Arc<ServerMessageBus>,
) -> anyhow::Result<(Arc<dyn MarketDataProvider>, Arc<dyn ExecutionProvider>)> {
    let p = PXClient::new_from_session(kind, session, bus).await?;
    let arc = Arc::new(p);
    // Coerce Arc<PXClient> to both trait object Arcs
    let md: Arc<dyn MarketDataProvider> = arc.clone();
    let ex: Arc<dyn ExecutionProvider> = arc;
    Ok((md, ex))
}
