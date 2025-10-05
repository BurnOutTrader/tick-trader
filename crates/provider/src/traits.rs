use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Instant;
use std::sync::Arc;
use ahash::AHashMap;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Degraded(String),
}

#[derive(Debug, Clone)]
pub struct ProviderSessionSpec {
    // Opaque to bus/engine; provider-specific
    pub provider_kind: ProviderKind,
    pub creds: HashMap<String, String>,
    pub endpoints: Vec<String>,
    pub env: Option<String>,
    pub rate_limits: HashMap<String, String>,
}
impl Default for ProviderSessionSpec {
    fn default() -> Self {
        Self {
            provider_kind: ProviderKind::ProjectX(tt_types::providers::ProjectXTenant::Demo),
            creds: HashMap::new(),
            endpoints: vec![],
            env: None,
            rate_limits: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DisconnectReason {
    ClientRequested,
    Retry(String),
    Fatal(String),
}

#[derive(Debug, Clone)]
pub struct ConnectResult {
    pub ok: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ProbeStatus {
    Ok(i64),          // last_provider_ts (ns)
    Stalled(Instant), // since
    Error(String),
}

#[derive(Debug, Clone)]
pub enum ProviderEvent {
    Disconnected(String),
    ReauthRequired,
    Throttled { until: Instant },
    Relogin,
    Warning(String),
}

pub type ProviderParams = HashMap<String, String>;

#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    // Identity
    fn id(&self) -> ProviderKind; // ProviderId (interned elsewhere)
    fn supports(&self, topic: Topic) -> bool;

    // Lifecycle
    async fn connect_to_market(&self, session: ProviderSessionSpec) -> anyhow::Result<()>;
    async fn disconnect(&self, reason: DisconnectReason);
    async fn connection_state(&self) -> ConnectionState;

    // Symbol management
    async fn subscribe_md(
        &self,
        topic: Topic,
        key: &SymbolKey,
        params: Option<&ProviderParams>,
    ) -> anyhow::Result<()>;
    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> anyhow::Result<()>;
    async fn active_md_subscriptions(&self) -> AHashMap<Topic, Vec<SymbolKey>>;
    async fn auto_update(&self) -> anyhow::Result<()>;

    // Push callbacks (Engine will call into Bus using these rows of data)
    // In traits, we expose function hooks providers call on Engine; here we just declare signatures
    // The concrete Engine will implement these trait-object callbacks.
}

#[async_trait]
pub trait ExecutionProvider: Send + Sync {
    // Identity & lifecycle
    fn id(&self) -> ProviderKind;
    async fn connect_to_broker(&self, session: ProviderSessionSpec) -> anyhow::Result<()>;
    async fn disconnect(&self, reason: DisconnectReason);
    async fn connection_state(&self) -> ConnectionState;

    // Account + positions
    async fn subscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn unsubscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn subscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn unsubscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn active_account_subscriptions(&self) -> Vec<AccountKey>;

    // Orders/Fills stream
    async fn subscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn unsubscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()>;

    // Order API
    async fn place(&self, order_cmd: HashMap<String, String>) -> CommandAck;
    async fn cancel(&self, order_id: String) -> CommandAck;
    async fn replace(&self, order_replace_cmd: HashMap<String, String>) -> CommandAck;
    async fn auto_update(&self) -> anyhow::Result<()>;
}

#[derive(Debug, Clone)]
pub struct CommandAck {
    pub ok: bool,
    pub message: Option<String>,
}



