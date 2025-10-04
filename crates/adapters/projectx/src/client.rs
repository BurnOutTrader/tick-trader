use std::collections::HashMap;
use std::sync::Arc;
use ahash::AHashMap;
use async_trait::async_trait;
use tokio::sync::RwLock;
use provider::traits::{CommandAck, ConnectionState, DisconnectReason, ExecutionProvider, MarketDataProvider, ProviderParams, ProviderSessionSpec};
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::securities::futures_helpers::{extract_month_year, extract_root};
use tt_types::securities::symbols::Instrument;
use crate::http::client::PxHttpClient;
use crate::http::credentials::PxCredential;
use crate::http::error::PxError;
use crate::websocket::client::PxWebSocketClient;
use tokio::sync::watch;
use tt_bus::MessageBus;

pub struct PXClient {
    http: PxHttpClient,
    websocket: PxWebSocketClient,
    http_connection_state: RwLock<ConnectionState>,
    id: String,
    account_subscriptions: Arc<RwLock<Vec<AccountKey>>>,
}

impl PXClient {
    async fn new_from_session(session: ProviderSessionSpec, bus: Arc<MessageBus>) -> anyhow::Result<Self> {
        let firm = session.creds.get("firm").expect(
            "PXClient requires a 'firm' credential to be set in the session credentials",
        );
        let user_name = session.creds.get("user_name").expect(">&user_name credential to be set in the session credentials");
        let api_key = session.creds.get("api_key").expect(
            "PXClient requires a 'api_key' credential to be set in the session credentials",
        );
        let px_credentials = PxCredential::new(firm.clone(), user_name.clone(), api_key.clone());
        let http = PxHttpClient::new(px_credentials, None, None, None, None, bus.clone())?;
        let base = http.inner.rtc_base();
        let token = http.inner.token_string().await;
        let websocket = PxWebSocketClient::new(base, token, firm.clone(), bus);
        let id = format!("ProjectX:{}", firm);
        Ok(Self {
            id,
            http,
            websocket,
            http_connection_state: RwLock::new(ConnectionState::Disconnected),
            account_subscriptions: Arc::new(RwLock::new(vec![])),
        })
    }

    async fn state(&self) -> ConnectionState {
        let http_state = self.http_connection_state.read().await;
        if *http_state == ConnectionState::Disconnected || self.websocket.is_user_connected() == false || self.websocket.is_market_connected() == false {
            return ConnectionState::Disconnected
        }
        ConnectionState::Connected
    }

    async fn connect_all(&self) -> anyhow::Result<()>  {
        let mut conn_state = self.http_connection_state.write().await;
        if *conn_state == ConnectionState::Disconnected {
            * conn_state = ConnectionState::Connecting;

            // Wire token updates from HTTP to WebSocket via watch channel
            let (tx, rx) = watch::channel(String::new());
            // Start watching for token updates on the websocket side
            self.websocket.watch_token_updates(rx).await;

            match self.http.start(tx).await {
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
                Ok(conn) => {},
                Err(e) => {
                    log::error!("Error starting WebSocket client: {:?}", e);
                    return Err(e);
                }
            };
        }
        if self.websocket.is_user_connected() == false
        {
            match self.websocket.connect_user().await {
                Ok(conn) => {},
                Err(e) => {
                    log::error!("Error starting WebSocket client: {:?}", e);
                }
            }
        }
        Ok(())
    }
}

fn parse_symbol_key(key: SymbolKey) -> anyhow::Result<String> {
    match Instrument::try_from(key.instrument.as_str()) {
        Ok(instrument) => {
            let root = extract_root(&instrument);
            match extract_month_year(&instrument) {
                None => Ok(format!("CON.F.US.{root}")),
                Some((month, year)) => Ok(format!("CON.F.US.{root}.{month}{year}")),
            }
        }
        Err(e) => anyhow::bail!("Failed to parse symbol key: {}", e),
    }
}

#[async_trait]
impl MarketDataProvider for PXClient {
    fn id(&self) -> &str {
        self.id.as_str()
    }

    fn supports(&self, topic: Topic) -> bool {
        match topic {
            Topic::Ticks | Topic::Quotes | Topic::Depth => true,
            _ => false,
        }
    }

    async fn connect(&self, _session: ProviderSessionSpec) -> anyhow::Result<()> {
        self.connect_all().await
    }

    async fn disconnect(&self, reason: DisconnectReason) {
        self.websocket.kill().await;
        self.http.kill()
    }

    async fn connection_state(&self) -> ConnectionState {
        self.state().await
    }

    async fn subscribe_md(&self, topic: Topic, key: &SymbolKey, params: Option<&ProviderParams>) -> anyhow::Result<()> {
        let instrument = parse_symbol_key(key.clone())?;
        match topic {
            Topic::Ticks => self.websocket.subscribe_contract_ticks(instrument.as_str()).await,
            Topic::Quotes => self.websocket.subscribe_contract_quotes(instrument.as_str()).await,
            Topic::Depth => self.websocket.subscribe_contract_market_depth(instrument.as_str()).await,
            _ => anyhow::bail!("Unsupported topic: {:?}", topic),
        }
    }

    async fn unsubscribe_md(&self, topic: Topic, key: &SymbolKey) -> anyhow::Result<()> {
        let instrument = parse_symbol_key(key.clone())?;
        match topic {
            Topic::Ticks => self.websocket.unsubscribe_contract_ticks(instrument.as_str()).await,
            Topic::Quotes => self.websocket.unsubscribe_contract_quotes(instrument.as_str()).await,
            Topic::Depth => self.websocket.unsubscribe_contract_market_depth(instrument.as_str()).await,
            _ => anyhow::bail!("Unsupported topic: {:?}", topic),
        }
    }

    fn active_md_subscriptions(&self) -> AHashMap<Topic, Vec<SymbolKey>> {
        let mut map = AHashMap::new();
        // Minimal implementation: we currently track contract IDs internally.
        // Returning empty vectors until a reverse mapping to SymbolKey is defined.

        map.insert(Topic::Ticks, vec![]);
        map.insert(Topic::Quotes, vec![]);
        map.insert(Topic::Depth, vec![]);
        map
    }
}

#[async_trait]
impl ExecutionProvider for PXClient {
    fn id(&self) -> &str {
        self.id.as_str()
    }

    async fn connect(&self, session: ProviderSessionSpec) -> anyhow::Result<()> {
        self.connect_all().await
    }

    async fn disconnect(&self, reason: DisconnectReason) {
        todo!()
    }

    async fn connection_state(&self) -> ConnectionState {
        self.state().await
    }

    async fn subscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.http.account_id(account_key.account_name.clone()).await?;
        match self.websocket.subscribe_user_account(id).await {
            Ok(_) => {
                let mut lock = self.account_subscriptions.write().await;
                lock.push(account_key.clone());
                Ok(())
            }
            Err(_) => {
                Err(anyhow::anyhow!("Failed to subscribe to account events: {}", account_key.account_name))
            }
        }
    }

    async fn unsubscribe_account_events(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.http.account_id(account_key.account_name.clone()).await?;
        let mut lock = self.account_subscriptions.write().await;
        let index = lock.iter().position(|k| k == account_key);
        self.websocket.unsubscribe_user_account(id).await
    }

    async fn subscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.http.account_id(account_key.account_name.clone()).await?;
        self.websocket.subscribe_account_positions(id).await
    }

    async fn unsubscribe_positions(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.http.account_id(account_key.account_name.clone()).await?;
        self.websocket.unsubscribe_account_positions(id).await
    }

    async fn active_account_subscriptions(&self) -> Vec<AccountKey> {
        let lock = self.account_subscriptions.read().await;
        lock.clone()
    }

    async fn subscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.http.account_id(account_key.account_name.clone()).await?;
        self.websocket.subscribe_account_orders(id).await
    }

    async fn unsubscribe_order_updates(&self, account_key: &AccountKey) -> anyhow::Result<()> {
        let id = self.http.account_id(account_key.account_name.clone()).await?;
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
}
