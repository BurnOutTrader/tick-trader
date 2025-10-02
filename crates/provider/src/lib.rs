use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Instant;
use tt_types::keys::{AccountKey, SymbolKey, Topic};

#[derive(Debug, Clone)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Degraded(String),
}

#[derive(Debug, Clone)]
pub struct ProviderSessionSpec {
    // Opaque to bus/engine; provider-specific
    pub creds: HashMap<String, String>,
    pub endpoints: Vec<String>,
    pub env: Option<String>,
    pub rate_limits: HashMap<String, String>,
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
pub struct SubscribeResult {
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

// Callback types
#[derive(Debug, Clone)]
pub struct Tick {
    pub ts_ns: i64,
    pub bid_px: i64,
    pub ask_px: i64,
    pub bid_sz: i32,
    pub ask_sz: i32,
}
#[derive(Debug, Clone)]
pub struct Quote {
    pub ts_ns: i64,
    pub bid_px: i64,
    pub ask_px: i64,
    pub bid_sz: i32,
    pub ask_sz: i32,
}
#[derive(Debug, Clone)]
pub struct DepthDelta {
    pub price: i64,
    pub size: i64,
    pub side: i8,
}
#[derive(Debug, Clone)]
pub struct DepthLevel {
    pub price: i64,
    pub size: i64,
    pub side: i8,
}
#[derive(Debug, Clone)]
pub struct MboEvent {
    pub ts_ns: i64,
    pub order_id: u64,
    pub kind: i32,
}
#[derive(Debug, Clone)]
pub struct Bar {
    pub ts_ns: i64,
    pub open: i64,
    pub high: i64,
    pub low: i64,
    pub close: i64,
    pub volume: i64,
}

#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    // Identity
    fn id(&self) -> u16; // ProviderId (interned elsewhere)
    fn name(&self) -> &'static str;
    fn supports(&self, topic: Topic) -> bool;

    // Lifecycle
    async fn connect(&self, session: ProviderSessionSpec) -> ConnectResult;
    async fn disconnect(&self, reason: DisconnectReason);
    fn connection_state(&self) -> ConnectionState;
    fn last_heartbeat_at(&self) -> Instant;

    // Symbol management
    async fn subscribe_md(
        &self,
        topic: Topic,
        key: &SymbolKey,
        params: Option<&ProviderParams>,
    ) -> SubscribeResult;
    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey);
    fn active_md_subscriptions(&self) -> Vec<(Topic, SymbolKey)>;

    // Push callbacks (Engine will call into Bus using these rows of data)
    // In traits, we expose function hooks providers call on Engine; here we just declare signatures
    // The concrete Engine will implement these trait-object callbacks.
}

#[async_trait]
pub trait ExecutionProvider: Send + Sync {
    // Identity & lifecycle
    fn id(&self) -> u16;
    fn name(&self) -> &'static str;
    async fn connect(&self, session: ProviderSessionSpec) -> ConnectResult;
    async fn disconnect(&self, reason: DisconnectReason);
    fn connection_state(&self) -> ConnectionState;
    fn last_heartbeat_at(&self) -> Instant;

    // Account + positions
    async fn subscribe_account_events(&self, account_key: &AccountKey) -> SubscribeResult;
    async fn unsubscribe_account_events(&self, account_key: &AccountKey);
    async fn subscribe_positions(&self, account_key: &AccountKey) -> SubscribeResult;
    async fn unsubscribe_positions(&self, account_key: &AccountKey);
    fn active_account_subscriptions(&self) -> Vec<AccountKey>;

    // Orders/Fills stream
    async fn subscribe_order_flow(&self, account_key: &AccountKey) -> SubscribeResult;
    async fn unsubscribe_order_flow(&self, account_key: &AccountKey);

    // Order API
    async fn place(&self, order_cmd: HashMap<String, String>) -> CommandAck;
    async fn cancel(&self, order_id: String) -> CommandAck;
    async fn replace(&self, order_replace_cmd: HashMap<String, String>) -> CommandAck;
}

#[derive(Debug, Clone)]
pub struct CommandAck {
    pub ok: bool,
    pub message: Option<String>,
}
