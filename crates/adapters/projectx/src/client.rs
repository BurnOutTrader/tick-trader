use crate::http::client::PxHttpClient;
use crate::http::credentials::PxCredential;
use crate::http::error::PxError;
use crate::websocket::client::PxWebSocketClient;
use ahash::AHashMap;
use async_trait::async_trait;
use provider::traits::{
    CommandAck, ConnectionState, DisconnectReason, ExecutionProvider, MarketDataProvider,
    ProviderParams, ProviderSessionSpec,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tt_bus::MessageBus;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::futures_helpers::{extract_month_year, extract_root};
use tt_types::securities::symbols::Instrument;

pub struct PXClient {
    pub provider_kind: ProviderKind,
    http: Arc<PxHttpClient>,
    websocket: Arc<PxWebSocketClient>,
    http_connection_state: Arc<RwLock<ConnectionState>>,
    account_subscriptions: Arc<RwLock<Vec<AccountKey>>>,
}

impl PXClient {
    pub async fn instruments_map_snapshot(
        &self,
    ) -> AHashMap<
        tt_types::securities::symbols::Instrument,
        tt_types::securities::security::FuturesContract,
    > {
        self.http.instruments_snapshot().await
    }
    #[allow(dead_code)]
    pub async fn new_from_session(
        kind: ProviderKind,
        session: ProviderSessionSpec,
        bus: Arc<MessageBus>,
    ) -> anyhow::Result<Self> {
        let user_name = session
            .user_names
            .get(&kind)
            .expect(">&user_name credential to be set in the session credentials");
        let api_key = session.api_keys.get(&kind).expect(
            "PXClient requires a 'api_key' credential to be set in the session credentials",
        );
        let firm = match kind {
            ProviderKind::ProjectX(firm) => firm,
            _ => anyhow::bail!("PXClient requires a ProjectX provider kind"),
        };
        let px_credentials = PxCredential::new(firm, user_name.clone(), api_key.clone());
        let http = PxHttpClient::new(px_credentials, None, None, None, None)?;
        let base = http.inner.rtc_base();
        let token = http.inner.token_string().await;
        let websocket = PxWebSocketClient::new(base, token, firm, bus);
        Ok(Self {
            provider_kind: kind,
            http: Arc::new(http),
            websocket: Arc::new(websocket),
            http_connection_state: Arc::new(RwLock::new(ConnectionState::Disconnected)),
            account_subscriptions: Arc::new(RwLock::new(vec![])),
        })
    }

    async fn state(&self) -> ConnectionState {
        let http_state = self.http_connection_state.read().await;
        if *http_state == ConnectionState::Disconnected
            || self.websocket.is_user_connected() == false
            || self.websocket.is_market_connected() == false
        {
            return ConnectionState::Disconnected;
        }
        ConnectionState::Connected
    }

    async fn connect_all(&self) -> anyhow::Result<()> {
        let mut conn_state = self.http_connection_state.write().await;
        if *conn_state == ConnectionState::Disconnected {
            *conn_state = ConnectionState::Connecting;

            match self.http.start().await {
                Ok(_) => {
                    *conn_state = ConnectionState::Connected;
                }
                Err(PxError::Other(e)) => {
                    *conn_state = ConnectionState::Disconnected;
                    return Err(e);
                }
                Err(e) => {
                    log::error!("Error starting HTTP client: {:?}", e);
                    *conn_state = ConnectionState::Disconnected;
                }
            }
        }
        if self.websocket.is_market_connected() == false {
            *conn_state = ConnectionState::Connecting;
            match self.websocket.connect_market().await {
                Ok(conn) => {}
                Err(e) => {
                    log::error!("Error starting WebSocket client: {:?}", e);
                    return Err(e);
                }
            };
        }
        if self.websocket.is_user_connected() == false {
            match self.websocket.connect_user().await {
                Ok(conn) => {}
                Err(e) => {
                    log::error!("Error starting WebSocket client: {:?}", e);
                }
            }
        }
        Ok(())
    }
}

fn parse_symbol_key(key: SymbolKey) -> anyhow::Result<String> {
    let instrument = key.instrument; // already an Instrument
    let root = extract_root(&instrument);
    Ok(match extract_month_year(&instrument) {
        None => format!("CON.F.US.{root}"),
        Some((month, year)) => format!("CON.F.US.{root}.{month}{year}"),
    })
}

#[async_trait]
impl MarketDataProvider for PXClient {
    fn id(&self) -> ProviderKind {
        self.provider_kind
    }

    fn supports(&self, topic: Topic) -> bool {
        match topic {
            Topic::Ticks | Topic::Quotes | Topic::Depth => true,
            _ => false,
        }
    }

    async fn connect_to_market(
        &self,
        kind: ProviderKind,
        _session: ProviderSessionSpec,
    ) -> anyhow::Result<()> {
        self.connect_all().await
    }

    async fn disconnect(&self, reason: DisconnectReason) {
        self.websocket.kill().await;
        self.http.kill()
    }

    async fn connection_state(&self) -> ConnectionState {
        self.state().await
    }

    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey) -> anyhow::Result<()> {
        let instrument = parse_symbol_key(key.clone())?;
        match topic {
            Topic::Ticks => {
                self.websocket
                    .subscribe_contract_ticks(instrument.as_str())
                    .await
            }
            Topic::Quotes => {
                self.websocket
                    .subscribe_contract_quotes(instrument.as_str())
                    .await
            }
            Topic::Depth => {
                self.websocket
                    .subscribe_contract_market_depth(instrument.as_str())
                    .await
            }
            _ => anyhow::bail!("Unsupported topic: {:?}", topic),
        }
    }

    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> anyhow::Result<()> {
        let instrument = parse_symbol_key(key.clone())?;
        match topic {
            Topic::Ticks => {
                self.websocket
                    .unsubscribe_contract_ticks(instrument.as_str())
                    .await
            }
            Topic::Quotes => {
                self.websocket
                    .unsubscribe_contract_quotes(instrument.as_str())
                    .await
            }
            Topic::Depth => {
                self.websocket
                    .unsubscribe_contract_market_depth(instrument.as_str())
                    .await
            }
            _ => anyhow::bail!("Unsupported topic: {:?}", topic),
        }
    }

    async fn active_md_subscriptions(&self) -> AHashMap<Topic, Vec<SymbolKey>> {
        // Collect current active subscriptions from the websocket client which tracks
        // contract ids per topic internally.
        fn symbol_from_contract_id(firm: ProjectXTenant, instrument: &str) -> Option<SymbolKey> {
            let provider = ProviderKind::ProjectX(firm);
            let instrument = Instrument::from_str(instrument).ok()?;
            Some(SymbolKey {
                instrument,
                provider,
            })
        }
        let firm = match self.provider_kind {
            ProviderKind::ProjectX(firm) => firm,
            _ => return AHashMap::new(),
        };

        let ticks = {
            let g = self.websocket.active_contract_ids_ticks().await;
            g.iter()
                .filter_map(|s| symbol_from_contract_id(firm, s))
                .collect::<Vec<_>>()
        };
        let quotes = {
            let g = self.websocket.active_contract_ids_quotes().await;
            g.iter()
                .filter_map(|s| symbol_from_contract_id(firm, s))
                .collect::<Vec<_>>()
        };
        let depth = {
            let g = self.websocket.active_contract_ids_depth().await;
            g.iter()
                .filter_map(|s| symbol_from_contract_id(firm, s))
                .collect::<Vec<_>>()
        };

        let mut map = AHashMap::new();
        map.insert(Topic::Ticks, ticks);
        map.insert(Topic::Quotes, quotes);
        map.insert(Topic::Depth, depth);
        map
    }

    async fn auto_update(&self) -> anyhow::Result<()> {
        self.http.auto_update().await
    }
}

#[async_trait]
impl ExecutionProvider for PXClient {
    fn id(&self) -> ProviderKind {
        self.provider_kind
    }

    async fn connect_to_broker(
        &self,
        kind: ProviderKind,
        session: ProviderSessionSpec,
    ) -> anyhow::Result<()> {
        self.connect_all().await
    }

    async fn disconnect(&self, reason: DisconnectReason) {
        self.websocket.kill().await;
        self.http.kill();
        log::info!("Disconnected from ProjectX: {:?}", reason);
    }

    async fn connection_state(&self) -> ConnectionState {
        self.state().await
    }

    async fn subscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        match self.websocket.subscribe_user_account(id).await {
            Ok(_) => {
                let mut lock = self.account_subscriptions.write().await;
                lock.push(account_key.clone());
                Ok(())
            }
            Err(_) => Err(anyhow::anyhow!(
                "Failed to subscribe to account events: {}",
                account_key.account_name
            )),
        }
    }

    async fn unsubscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        let mut lock = self.account_subscriptions.write().await;
        let index = lock.iter().position(|k| k == account_key);
        self.websocket.unsubscribe_user_account(id).await
    }

    async fn subscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        self.websocket.subscribe_account_positions(id).await
    }

    async fn unsubscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        self.websocket.unsubscribe_account_positions(id).await
    }

    async fn active_account_subscriptions(&self) -> Vec<AccountKey> {
        let lock = self.account_subscriptions.read().await;
        lock.clone()
    }

    async fn subscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        self.websocket.subscribe_account_orders(id).await
    }

    async fn unsubscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self
            .http
            .account_id(account_key.account_name.clone())
            .await?;
        self.websocket.unsubscribe_account_orders(id).await
    }

    async fn place(&self, order_cmd: HashMap<String, String>) -> CommandAck {
        todo!()
    }

    async fn cancel(&self, order_id: String) -> CommandAck {
        todo!()
    }

    async fn replace(&self, order_replace_cmd: HashMap<String, String>) -> CommandAck {
        todo!()
    }

    async fn auto_update(&self) -> anyhow::Result<()> {
        self.http.auto_update().await
    }
}
