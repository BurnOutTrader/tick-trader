use crate::accounts::account::AccountSnapShot;
use crate::data::core::{DateTime, Utc};
use crate::history::{HistoricalRequest, HistoryEvent};
use crate::keys::{AccountKey, SymbolKey, Topic};
use crate::providers::{ProjectXTenant, ProviderKind, RithmicSystem};
use crate::securities::security::FuturesContract;
use crate::securities::symbols::Instrument;
use crate::wire::{CancelOrder, PlaceOrder, ReplaceOrder};
use ahash::AHashMap;
use async_trait::async_trait;
use dotenvy::dotenv;
use std::collections::HashMap;
use std::time::Instant;

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
    pub user_names: HashMap<ProviderKind, String>,
    pub passwords: HashMap<ProviderKind, String>,
    pub api_keys: HashMap<ProviderKind, String>,
    pub other: HashMap<(ProviderKind, String), String>,
}
impl ProviderSessionSpec {
    pub fn from_env() -> Self {
        // Ensure .env is loaded for any binary or test that didn't call dotenv explicitly
        let _ = dotenvy::dotenv();
        dotenv().ok();

        let mut user_names: HashMap<ProviderKind, String> = HashMap::new();
        let mut passwords: HashMap<ProviderKind, String> = HashMap::new();
        let mut api_keys: HashMap<ProviderKind, String> = HashMap::new();
        let mut other: HashMap<(ProviderKind, String), String> = HashMap::new();

        // Iterate all environment variables and extract credentials for PX_* and RITHMIC_* patterns.
        for (key, value) in std::env::vars() {
            // Normalize helper: collapse to uppercase and remove surrounding whitespace.
            let key_trim = key.trim().to_string();
            if let Some(rest) = key_trim.strip_prefix("PX_") {
                // Expect: PX_{TENANT}_{CRED}
                // Split only into 2 parts after the prefix to allow underscores in CRED.
                let mut parts = rest.splitn(2, '_');
                let tenant_str = match parts.next() {
                    Some(s) if !s.is_empty() => s,
                    _ => continue,
                };
                let cred_key_raw = match parts.next() {
                    Some(s) if !s.is_empty() => s,
                    _ => continue,
                };

                let tenant = ProjectXTenant::from_env_string(tenant_str);
                let kind = ProviderKind::ProjectX(tenant);

                // Normalize credential key: uppercase and remove non-alphanum underscores normalization
                let norm = cred_key_raw.to_ascii_uppercase().replace('-', "_");
                let norm = norm.as_str();

                match norm {
                    // common variants
                    "USERNAME" => {
                        user_names.insert(kind, value);
                    }
                    "APIKEY" | "API_KEY" => {
                        api_keys.insert(kind, value);
                    }
                    "PASSWORD" => {
                        passwords.insert(kind, value);
                    }
                    // PX specific optional
                    "FIRM" => {
                        other.insert((kind, "FIRM".to_string()), value);
                    }
                    // anything else goes to other
                    other_key => {
                        other.insert((kind, other_key.to_string()), value);
                    }
                }
            } else if let Some(rest) = key_trim.strip_prefix("RITHMIC_") {
                // Expect: RITHMIC_{SYSTEM}_{CRED}
                let mut parts = rest.splitn(2, '_');
                let system_str = match parts.next() {
                    Some(s) if !s.is_empty() => s,
                    _ => continue,
                };
                let cred_key_raw = match parts.next() {
                    Some(s) if !s.is_empty() => s,
                    _ => continue,
                };

                // Try to decode using provided helper; skip if unknown system.
                if let Some(system) = RithmicSystem::from_env_string(system_str) {
                    let kind = ProviderKind::Rithmic(system);
                    let norm = cred_key_raw.to_ascii_uppercase().replace('-', "_");
                    let norm = norm.as_str();

                    match norm {
                        "USERNAME" => {
                            user_names.insert(kind, value);
                        }
                        "APIKEY" | "API_KEY" => {
                            api_keys.insert(kind, value);
                        }
                        "PASSWORD" => {
                            passwords.insert(kind, value);
                        }
                        // Rithmic specific extras go into 'other'
                        "FCM_ID" | "IB_ID" | "USER_TYPE" => {
                            other.insert((kind, norm.to_string()), value);
                        }
                        other_key => {
                            other.insert((kind, other_key.to_string()), value);
                        }
                    }
                } else {
                    // Unknown system; ignore this env var.
                    continue;
                }
            }
        }

        Self {
            user_names,
            passwords,
            api_keys,
            other,
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
    async fn connect_to_market(
        &self,
        kind: ProviderKind,
        session: ProviderSessionSpec,
    ) -> anyhow::Result<()>;
    async fn disconnect(&self, reason: DisconnectReason);
    async fn connection_state(&self) -> ConnectionState;

    // Symbol management
    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey) -> anyhow::Result<()>;
    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> anyhow::Result<()>;
    async fn active_md_subscriptions(&self) -> AHashMap<Topic, Vec<SymbolKey>>;
    /// Optional: list instruments available on this market data provider. Default empty.
    async fn list_instruments(&self, _pattern: Option<String>) -> anyhow::Result<Vec<Instrument>> {
        Ok(Vec::new())
    }
    /// Optional: full instruments map (Instrument -> FuturesContract). Default empty.
    async fn instruments_map(
        &self,
    ) -> anyhow::Result<ahash::AHashMap<Instrument, FuturesContract>> {
        Ok(ahash::AHashMap::new())
    }
    async fn auto_update(&self) -> anyhow::Result<()>;

    // Push callbacks (Engine will call into Bus using these rows of data)
    // In traits, we expose function hooks providers call on Engine; here we just declare signatures
    // The concrete Engine will implement these trait-object callbacks.
}

#[async_trait]
pub trait ExecutionProvider: Send + Sync {
    // Identity & lifecycle
    fn id(&self) -> ProviderKind;
    async fn connect_to_broker(
        &self,
        kind: ProviderKind,
        session: ProviderSessionSpec,
    ) -> anyhow::Result<()>;
    async fn disconnect(&self, reason: DisconnectReason);
    async fn connection_state(&self) -> ConnectionState;

    // Account + positions
    async fn subscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn unsubscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn subscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn unsubscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn active_account_subscriptions(&self) -> Vec<AccountKey>;
    /// Optional: list accounts available on this execution provider. Default empty.
    async fn list_accounts(&self) -> anyhow::Result<Vec<AccountSnapShot>> {
        Ok(Vec::new())
    }

    // Orders/Fills stream
    async fn subscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()>;
    async fn unsubscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()>;

    // Order API (typed)
    async fn place_order(&self, spec: PlaceOrder) -> CommandAck;
    async fn cancel_order(&self, spec: CancelOrder) -> CommandAck;
    async fn replace_order(&self, spec: ReplaceOrder) -> CommandAck;
    async fn auto_update(&self) -> anyhow::Result<()>;
}

/// Providers implement this to supply historical data.
///
/// Contract:
/// - `fetch()` returns immediately with a handle and an `mpsc::Receiver<HistoryEvent>`.
/// - The provider streams results (possibly chunked) to the receiver.
/// - Cancellation via `handle.cancel()` should stop the stream ASAP.
#[async_trait::async_trait]
pub trait HistoricalDataProvider: Send + Sync {
    fn name(&self) -> ProviderKind;

    /// Optional: connect/login; should be idempotent.
    async fn ensure_connected(&self) -> anyhow::Result<()>;

    /// The job of this function is to return any data within the period, for the specifications,
    /// how your function does that doesnt matter, as long as you return all data available for the period
    async fn fetch(&self, req: HistoricalRequest) -> anyhow::Result<Vec<HistoryEvent>>;

    /// Feature flags help the router pick/shape requests.
    fn supports(&self, _topic: Topic) -> bool {
        true
    }

    async fn earliest_available(
        &self,
        instrument: Instrument,
        topic: Topic,
    ) -> anyhow::Result<Option<DateTime<Utc>>>;
}

#[derive(Debug, Clone)]
pub struct CommandAck {
    pub ok: bool,
    pub message: Option<String>,
}
