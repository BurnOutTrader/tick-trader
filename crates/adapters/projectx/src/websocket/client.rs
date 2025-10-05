use crate::websocket::base_client::{WebSocketClient, WebSocketConfig};
use crate::websocket::models;
use crate::websocket::models::{
    GatewayQuote, GatewayTrade, GatewayUserAccount, GatewayUserOrder, GatewayUserPosition,
    GatewayUserTrade,
};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal_macros::dec;
use serde_json::Value;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info};
use tt_bus::Router;
use tt_types::keys::Topic;
use tt_types::accounts::account::{AccountName, AccountSnapShot};
use tt_types::accounts::events::{
    AccountDelta, ClientOrderId, OrderUpdate, PositionDelta, ProviderOrderId,
};
use tt_types::base_data::{Price, Side, Tick, Volume, OrderBook, BookLevel};
use tt_types::providers::ProjectXTenant;
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{AccountDeltaBatch, OrdersBatch, PositionsBatch};

#[allow(unused)]
/// Realtime client scaffold for ProjectX hubs
///
/// This client documents the API surface and subscriptions for both user and
/// market hubs. It will acquire a JWT token from the HTTP client and use it as
/// a bearer for SignalR access tokens.
#[derive(Clone)]
pub struct PxWebSocketClient {
    bus: Arc<Router>, 
    firm: ProjectXTenant,
    /// Base URL for the websocket service, e.g. `https://rtc.tradeify.projectx.com`
    pub base_url: String,
    /// Optional bearer token used for hub access
    pub bearer_token: Arc<RwLock<Option<String>>>,

    // ---- Connection state ----
    /// Indicates whether the user hub is connected
    user_connected: Arc<AtomicBool>,
    /// Indicates whether the market hub is connected
    market_connected: Arc<AtomicBool>,
    /// Stop signal shared across background tasks
    signal: Arc<AtomicBool>,
    /// Monotonic counter for tracking outbound requests/messages
    request_id_counter: Arc<AtomicU64>,

    // ---- User hub subscriptions ----
    /// Whether the broadcast "accounts" stream is subscribed
    user_accounts_subscribed: Arc<AtomicBool>,
    account_subs: Arc<RwLock<Vec<i64>>>,
    /// Tracked account IDs for orders subscription
    user_orders_subs: Arc<RwLock<Vec<i64>>>,
    /// Tracked account IDs for positions subscription
    user_positions_subs: Arc<RwLock<Vec<i64>>>,
    /// Tracked account IDs for trades subscription
    user_trades_subs: Arc<RwLock<Vec<i64>>>,

    /// Tracked contract IDs for quotes subscription
    pub(crate) market_contract_quotes_subs: Arc<RwLock<Vec<String>>>,
    /// Tracked contract IDs for depth subscription
    pub(crate) market_contract_depth_subs: Arc<RwLock<Vec<String>>>,
    /// Tracked contract IDs for trades subscription
    pub(crate) market_contract_tick_subs: Arc<RwLock<Vec<String>>>,

    market_contract_bars_subs: Arc<RwLock<Vec<String>>>,

    // ---- Live connections ----
    user_ws: Arc<tokio::sync::Mutex<Option<WebSocketClient>>>,
    user_reader_task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    market_ws: Arc<tokio::sync::Mutex<Option<WebSocketClient>>>,
    market_reader_task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,

    // ---- Message processing offload (per hub) ----
    user_msg_tx: mpsc::Sender<Value>,
    user_msg_task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    market_msg_tx: mpsc::Sender<Value>,
    market_msg_task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,

    precisions: Arc<DashMap<String, u8>>,

    // ---- Positions ----
    positions: Arc<DashMap<Instrument, GatewayUserPosition>>,

    // ---- Orders ----
    pending_orders: Arc<DashMap<Instrument, GatewayUserOrder>>,

    trades: Arc<DashMap<Instrument, Vec<GatewayUserTrade>>>,
}

pub fn parse_px_instrument(input: &str) -> String {
    let mut parts: Vec<&str> = input.split('.').collect();
    if parts.len() >= 2 {
        let expiry = parts.pop().unwrap();
        let root = parts.pop().unwrap();
        format!("{}{}", root, expiry)
    } else {
        input.to_string()
    }
}

impl PxWebSocketClient {
    /// Create a new websocket client with optional bearer token
    /// token is a clone or the Arc<RwLock<Option<String>>> managed by the http client.
    /// If an update is made to the token, it will automatically be copied by the websocket client by way of arc.
    pub fn new(
        base_url: impl Into<String>,
        bearer_token: Arc<RwLock<Option<String>>>,
        firm: ProjectXTenant,
        bus: Arc<Router>,
    ) -> Self {
        // Dedicated per-hub message queues to avoid head-of-line blocking
        let (user_tx, mut user_rx) = mpsc::channel::<Value>(4096);
        let (market_tx, mut market_rx) = mpsc::channel::<Value>(4096);
        let client = Self {
            bus,
            firm,
            base_url: base_url.into(),
            bearer_token,
            user_connected: Arc::new(AtomicBool::new(false)),
            market_connected: Arc::new(AtomicBool::new(false)),
            signal: Arc::new(AtomicBool::new(false)),
            request_id_counter: Arc::new(AtomicU64::new(1)),
            user_accounts_subscribed: Arc::new(AtomicBool::new(false)),
            account_subs: Arc::new(RwLock::new(Vec::new())),
            user_orders_subs: Arc::new(RwLock::new(Vec::new())),
            user_positions_subs: Arc::new(RwLock::new(Vec::new())),
            user_trades_subs: Arc::new(RwLock::new(Vec::new())),
            market_contract_quotes_subs: Arc::new(RwLock::new(Vec::new())),
            market_contract_depth_subs: Arc::new(RwLock::new(Vec::new())),
            market_contract_tick_subs: Arc::new(RwLock::new(Vec::new())),
            market_contract_bars_subs: Arc::new(RwLock::new(Vec::new())),
            user_ws: Arc::new(tokio::sync::Mutex::new(None)),
            user_reader_task: Arc::new(tokio::sync::Mutex::new(None)),
            market_ws: Arc::new(tokio::sync::Mutex::new(None)),
            market_reader_task: Arc::new(tokio::sync::Mutex::new(None)),
            user_msg_tx: user_tx.clone(),
            user_msg_task: Arc::new(tokio::sync::Mutex::new(None)),
            market_msg_tx: market_tx.clone(),
            market_msg_task: Arc::new(tokio::sync::Mutex::new(None)),
            precisions: Arc::new(DashMap::new()),
            positions: Arc::new(Default::default()),
            pending_orders: Arc::new(Default::default()),
            trades: Arc::new(DashMap::new()),
        };
        // Spawn background processors per hub to keep hot path free
        let user_worker = client.clone();
        let user_task = tokio::spawn(async move {
            while let Some(val) = user_rx.recv().await {
                user_worker.handle_signalr_message(val).await;
            }
        });
        let market_worker = client.clone();
        let market_task = tokio::spawn(async move {
            while let Some(val) = market_rx.recv().await {
                market_worker.handle_signalr_message(val).await;
            }
        });
        // Save task handles
        tokio::spawn({
            let client_inner = client.clone();
            async move {
                let mut g = client_inner.user_msg_task.lock().await;
                *g = Some(user_task);
            }
        });
        tokio::spawn({
            let client_inner = client.clone();
            async move {
                let mut g = client_inner.market_msg_task.lock().await;
                *g = Some(market_task);
            }
        });
        client
    }

    /// Returns the full URL for the user hub (ws/wss scheme)
    pub async fn user_hub_url(&self) -> String {
        let mut base = self.base_url.trim_end_matches('/').to_string();
        // Ensure websocket scheme for tungstenite
        if base.starts_with("https://") {
            base = base.replacen("https://", "wss://", 1);
        } else if base.starts_with("http://") {
            base = base.replacen("http://", "ws://", 1);
        }
        let token = self.bearer_token.read().await.clone();
        if let Some(token) = token {
            format!("{}/hubs/user?access_token={}", base, token)
        } else {
            format!("{}/hubs/user", base)
        }
    }

    /// Returns the full URL for the market hub (ws/wss scheme)
    pub async fn market_hub_url(&self) -> String {
        let mut base = self.base_url.trim_end_matches('/').to_string();
        // Ensure websocket scheme for tungstenite
        if base.starts_with("https://") {
            base = base.replacen("https://", "wss://", 1);
        } else if base.starts_with("http://") {
            base = base.replacen("http://", "ws://", 1);
        }
        let token = self.bearer_token.read().await.clone();
        if let Some(token) = token {
            format!("{}/hubs/market?access_token={}", base, token)
        } else {
            format!("{}/hubs/market", base)
        }
    }

    pub async fn kill(&self) {
        // Set stop signal and abort background tasks; ensure writer tasks are stopped.
        self.signal.store(true, Ordering::SeqCst);

        // Abort user/market message processing tasks
        {
            let mut g = self.user_msg_task.lock().await;
            if let Some(handle) = g.take() {
                handle.abort();
            }
        }
        {
            let mut g = self.market_msg_task.lock().await;
            if let Some(handle) = g.take() {
                handle.abort();
            }
        }

        // Abort user/market reader tasks (socket readers)
        {
            let mut g = self.user_reader_task.lock().await;
            if let Some(handle) = g.take() {
                handle.abort();
            }
        }
        {
            let mut g = self.market_reader_task.lock().await;
            if let Some(handle) = g.take() {
                handle.abort();
            }
        }

        // Stop websocket writer tasks & drop clients
        {
            let mut g = self.user_ws.lock().await;
            if let Some(client) = g.take() {
                client.kill().await;
            }
        }
        {
            let mut g = self.market_ws.lock().await;
            if let Some(client) = g.take() {
                client.kill().await;
            }
        }

        // Mark disconnected
        self.user_connected.store(false, Ordering::SeqCst);
        self.market_connected.store(false, Ordering::SeqCst);
    }

    /// Connect to the user hub
    pub async fn connect_user(&self) -> anyhow::Result<()> {
        let url = self.user_hub_url().await;
        let mut headers = vec![(
            "User-Agent".to_string(),
            "nautilus-projectx/1.0".to_string(),
        )];
        let token_lock = self.bearer_token.read().await.clone();
        if let Some(token) = token_lock {
            headers.push(("Authorization".to_string(), format!("Bearer {}", token)));
        }
        let config = WebSocketConfig {
            url,
            headers,
            heartbeat: None,
            heartbeat_msg: None,
            ping_handler: None,
            reconnect_timeout_ms: Some(10_000),
            reconnect_delay_initial_ms: Some(2_000),
            reconnect_delay_max_ms: Some(30_000),
            reconnect_backoff_factor: Some(1.5),
            reconnect_jitter_ms: Some(100),
        };

        let (mut reader, client) = WebSocketClient::connect_stream(config, vec![], None, None)
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect user websocket: {e}"))?;

        // Save client handle
        {
            let mut guard = self.user_ws.lock().await;
            *guard = Some(client);
        }

        // Perform SignalR handshake: {"protocol":"json","version":1}\x1e
        self.send_user_text(format!(
            "{}\u{001e}",
            serde_json::json!({"protocol":"json","version":1}).to_string()
        ))
        .await?;

        // Spawn reader task to process SignalR frames
        let this = self.clone();
        let task = tokio::spawn(async move {
            while let Some(msg) = reader.next().await {
                match msg {
                    Ok(Message::Text(txt)) => {
                        for frame in txt.split('\u{001e}') {
                            if frame.is_empty() {
                                continue;
                            }
                            if let Ok(val) = serde_json::from_str::<serde_json::Value>(frame) {
                                let _ = this.user_msg_tx.try_send(val);
                            }
                        }
                    }
                    Ok(Message::Binary(bin)) => {
                        if let Ok(txt) = String::from_utf8(bin.to_vec()) {
                            for frame in txt.split('\u{001e}') {
                                if frame.is_empty() {
                                    continue;
                                }
                                if let Ok(val) = serde_json::from_str::<serde_json::Value>(frame) {
                                    let _ = this.user_msg_tx.try_send(val);
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(_)) => {}
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(_)) => {
                        break;
                    }
                    Ok(Message::Frame(_)) => {}
                    Err(err) => {
                        log::warn!("user reader error: {}", err);
                        break;
                    }
                }
            }
        });
        {
            let mut g = self.user_reader_task.lock().await;
            *g = Some(task);
        }

        // Re-send any tracked subscriptions after (re)connecting to the user hub
        // If we previously subscribed to the broadcast Accounts stream, re-subscribe.
        if self.user_accounts_subscribed.load(Ordering::SeqCst) {
            let _ = self.invoke_user("SubscribeAccounts", vec![]).await;
        }
        // Re-invoke per-account subscriptions for any accounts we have tracked.
        let tracked_accounts: Vec<i64> = {
            let guard = self.account_subs.read().await;
            guard.iter().filter_map(|s| Some(s.clone())).collect()
        };
        for account_id in tracked_accounts {
            let id_val = Value::from(account_id);
            let _ = self
                .invoke_user("SubscribeOrders", vec![id_val.clone()])
                .await;
            let _ = self
                .invoke_user("SubscribePositions", vec![id_val.clone()])
                .await;
            let _ = self
                .invoke_user("SubscribeTrades", vec![id_val.clone()])
                .await;
        }

        self.user_connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Connect to the market hub
    pub async fn connect_market(&self) -> anyhow::Result<()> {
        let url = self.market_hub_url().await;
        let mut headers = vec![(
            "User-Agent".to_string(),
            "nautilus-projectx/1.0".to_string(),
        )];
        let token_lock = self.bearer_token.read().await.clone();
        if let Some(token) = token_lock {
            headers.push(("Authorization".to_string(), format!("Bearer {}", token)));
        }
        let config = WebSocketConfig {
            url,
            headers,
            heartbeat: None,
            heartbeat_msg: None,
            ping_handler: None,
            reconnect_timeout_ms: Some(10_000),
            reconnect_delay_initial_ms: Some(2_000),
            reconnect_delay_max_ms: Some(30_000),
            reconnect_backoff_factor: Some(1.5),
            reconnect_jitter_ms: Some(100),
        };

        let (mut reader, client) = WebSocketClient::connect_stream(config, vec![], None, None)
            .await
            .map_err(|e| anyhow::anyhow!("failed to connect market websocket: {e}"))?;

        // Save client handle
        {
            let mut guard = self.market_ws.lock().await;
            *guard = Some(client);
        }

        // Perform SignalR handshake: {"protocol":"json","version":1}\x1e
        self.send_market_text(format!(
            "{}\u{001e}",
            serde_json::json!({"protocol":"json","version":1}).to_string()
        ))
        .await?;

        // Spawn reader task to process SignalR frames
        let this = self.clone();
        let task = tokio::spawn(async move {
            while let Some(msg) = reader.next().await {
                match msg {
                    Ok(Message::Text(txt)) => {
                        for frame in txt.split('\u{001e}') {
                            if frame.is_empty() {
                                continue;
                            }
                            if let Ok(val) = serde_json::from_str::<serde_json::Value>(frame) {
                                let _ = this.market_msg_tx.try_send(val);
                            }
                        }
                    }
                    Ok(Message::Binary(bin)) => {
                        // Treat as UTF-8 if possible (some servers may send text as bytes)
                        if let Ok(txt) = String::from_utf8(bin.to_vec()) {
                            for frame in txt.split('\u{001e}') {
                                if frame.is_empty() {
                                    continue;
                                }
                                if let Ok(val) = serde_json::from_str::<serde_json::Value>(frame) {
                                    let _ = this.market_msg_tx.try_send(val);
                                }
                            }
                        }
                    }
                    Ok(Message::Ping(_)) => {}
                    Ok(Message::Pong(_)) => {}
                    Ok(Message::Close(_)) => {
                        break;
                    }
                    Ok(Message::Frame(_)) => {}
                    Err(err) => {
                        log::error!("market reader error: {}", err);
                        break;
                    }
                }
            }
        });
        {
            let mut g = self.market_reader_task.lock().await;
            *g = Some(task);
        }

        // Re-send any tracked market subscriptions after (re)connecting
        // Contract-based subscriptions have explicit invocation methods; re-invoke them.
        let contract_quotes: Vec<String> = {
            let g = self.market_contract_quotes_subs.read().await;
            g.clone()
        };
        for cid in contract_quotes {
            let _ = self
                .invoke_market("SubscribeContractQuotes", vec![Value::String(cid)])
                .await;
        }
        let contract_trades: Vec<String> = {
            let g = self.market_contract_tick_subs.read().await;
            g.clone()
        };
        for cid in contract_trades {
            let _ = self
                .invoke_market("SubscribeContractTrades", vec![Value::String(cid)])
                .await;
        }
        let contract_depth: Vec<String> = {
            let g = self.market_contract_depth_subs.read().await;
            g.clone()
        };
        for cid in contract_depth {
            let _ = self
                .invoke_market("SubscribeContractMarketDepth", vec![Value::String(cid)])
                .await;
        }
        let contract_bars: Vec<String> = {
            let g = self.market_contract_bars_subs.read().await;
            g.clone()
        };
        for cid in contract_bars {
            let _ = self
                .invoke_market("SubscribeContractBars", vec![Value::String(cid)])
                .await;
        }
        // Note: symbol-level maps (market_quotes_subs/market_trades_subs/market_depth_subs)
        // do not have explicit invoke methods in this client, so we only re-track them.

        self.market_connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn send_market_text(&self, data: String) -> anyhow::Result<()> {
        let guard = self.market_ws.lock().await;
        if let Some(client) = guard.as_ref() {
            match client.send_text(data, None).await {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!(format!("failed to send market text: {e}"))),
            }
        } else {
            Err(anyhow::anyhow!("market websocket not connected"))
        }
    }

    async fn send_user_text(&self, data: String) -> anyhow::Result<()> {
        let guard = self.user_ws.lock().await;
        if let Some(client) = guard.as_ref() {
            match client.send_text(data, None).await {
                Ok(_) => Ok(()),
                Err(e) => Err(anyhow::anyhow!(format!("failed to send user text: {e}"))),
            }
        } else {
            Err(anyhow::anyhow!("user websocket not connected"))
        }
    }

    async fn handle_signalr_message(&self, val: Value) {
        if let Some(t) = val.get("type").and_then(|v| v.as_i64()) {
            match t {
                1 => {
                    if let Some(target) = val.get("target").and_then(|v| v.as_str()) {
                        match target {
                            // Market hub events
                            "GatewayQuote" => {
                                // SignalR Invocation typically passes [contractId, data] or just [data]
                                // Extract the data object and deserialize into GatewayQuote, then convert to Nautilus QuoteTick
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let data_val = if args.len() >= 2 {
                                        &args[1]
                                    } else {
                                        args.get(0).unwrap_or(&Value::Null)
                                    };
                                    if let Ok(px_quote) =
                                        serde_json::from_value::<GatewayQuote>(data_val.to_owned())
                                    {
                                        if let Ok(instrument) =
                                            Instrument::from_str(&parse_px_instrument(px_quote.symbol.as_str()))
                                        {
                                            let symbol = extract_root(&instrument);
                                            let bid = Price::from_f64(px_quote.best_bid)
                                                .unwrap_or_default();
                                            let ask = Price::from_f64(px_quote.best_ask)
                                                .unwrap_or_default();
                                            let bid_size = Volume::from(0);
                                            let ask_size = Volume::from(0);
                                            // prefer timestamp then last_updated
                                            let time = DateTime::<Utc>::from_str(
                                                px_quote.timestamp.as_str(),
                                            )
                                            .or_else(|_| {
                                                DateTime::<Utc>::from_str(
                                                    px_quote.last_updated.as_str(),
                                                )
                                            })
                                            .unwrap_or_else(|_| Utc::now());
                                            let bbo = tt_types::base_data::Bbo {
                                                symbol,
                                                instrument: instrument.clone(),
                                                bid,
                                                bid_size,
                                                ask,
                                                ask_size,
                                                time,
                                                bid_orders: None,
                                                ask_orders: None,
                                                venue_seq: None,
                                                is_snapshot: Some(false),
                                            };
                                            // Publish to SHM snapshot (Quotes)
                                            let provider = tt_types::providers::ProviderKind::ProjectX(self.firm);
                                            let key = tt_types::keys::SymbolKey { instrument: instrument.clone(), provider };
                                            {
                                                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&bbo).unwrap_or_default();
                                                tt_shm::write_snapshot(Topic::Quotes, &key, &bytes);
                                            }
                                            // Publish to bus if attached
                                            if let Err(e) = self.bus.publish_quote(bbo).await {
                                                log::warn!("failed to publish quote: {}", e);
                                            }
                                        } else {
                                            info!(target: "projectx.ws", "GatewayQuote (invalid instrument): {}", px_quote.symbol);
                                        }
                                    } else {
                                        info!(target: "projectx.ws", "GatewayQuote (unparsed): {}", data_val);
                                    }
                                }
                            }
                            "GatewayTrade" => {
                                // Extract args and deserialize payload (can be single object or array of objects)
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let data_val = if args.len() >= 2 {
                                        &args[1]
                                    } else {
                                        args.get(0).unwrap_or(&Value::Null)
                                    };

                                    if let Ok(px_trade) =
                                        serde_json::from_value::<GatewayTrade>(data_val.to_owned())
                                    {
                                        if let Ok(instrument) =
                                            Instrument::from_str(&parse_px_instrument(px_trade.symbol_id.as_str()))
                                        {
                                            let symbol = extract_root(&instrument);
                                            let price = match Price::from_f64(px_trade.price) {
                                                Some(p) => p,
                                                None => {
                                                    log::warn!(
                                                        "invalid price for {}",
                                                        px_trade.price
                                                    );
                                                    return;
                                                }
                                            };
                                            let volume = match Volume::from_i64(px_trade.volume) {
                                                Some(s) => s,
                                                None => {
                                                    log::warn!(
                                                        "invalid size for {}",
                                                        px_trade.volume
                                                    );
                                                    return;
                                                }
                                            };
                                            let side = match px_trade.r#type {
                                                0 => Side::Buy,
                                                1 => Side::Sell,
                                                _ => Side::None,
                                            };
                                            let time = match DateTime::<Utc>::from_str(
                                                px_trade.timestamp.as_str(),
                                            ) {
                                                Ok(t) => t,
                                                Err(e) => {
                                                    log::warn!(
                                                        "invalid timestamp for {}: {}",
                                                        px_trade.timestamp,
                                                        e
                                                    );
                                                    Utc::now()
                                                }
                                            };
                                            let tick = Tick {
                                                symbol,
                                                instrument: instrument.clone(),
                                                price,
                                                volume,
                                                time,
                                                side,
                                                venue_seq: None,
                                            };
                                            // Write Tick snapshot to SHM
                                            let provider = tt_types::providers::ProviderKind::ProjectX(self.firm);
                                            let key = tt_types::keys::SymbolKey { instrument: instrument.clone(), provider };
                                            {
                                                let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&tick).unwrap_or_default();
                                                tt_shm::write_snapshot(Topic::Ticks, &key, &bytes);
                                            }
                                            if let Err(e) = self.bus.publish_tick(tick).await {
                                                log::warn!("failed to publish tick: {}", e);
                                            }
                                        }
                                    }
                                }
                            }
                            "GatewayDepth" => {
                                // Extract args and handle payload which may be an array of up to 10 levels
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let (instrument_opt, data_val) = if args.len() >= 2 {
                                        (args.get(0).and_then(|v| v.as_str()).map(|s| s.to_string()), &args[1])
                                    } else {
                                        (args.get(0).and_then(|v| v.as_str()).map(|s| s.to_string()), args.get(0).unwrap_or(&Value::Null))
                                    };

                                    // Resolve instrument from the first argument if present
                                    if let Some(instr_str) = instrument_opt {
                                        if let Ok(instrument) = Instrument::from_str(&parse_px_instrument(instr_str.as_str())) {
                                            let symbol = extract_root(&instrument);

                                            // Normalize payload to a vector of depth items
                                            let mut items: Vec<models::GatewayDepth> = Vec::new();
                                            if data_val.is_array() {
                                                if let Some(arr) = data_val.as_array() {
                                                    for v in arr {
                                                        if let Ok(it) = serde_json::from_value::<models::GatewayDepth>(v.clone()) {
                                                            items.push(it);
                                                        }
                                                    }
                                                }
                                            } else if data_val.is_object() {
                                                if let Ok(it) = serde_json::from_value::<models::GatewayDepth>(data_val.clone()) {
                                                    items.push(it);
                                                }
                                            }

                                            if !items.is_empty() {
                                                let mut bids: Vec<BookLevel> = Vec::new();
                                                let mut asks: Vec<BookLevel> = Vec::new();
                                                let mut latest_ts: Option<DateTime<Utc>> = None;

                                                for it in items.into_iter() {
                                                    println!("{:?}", it);
                                                    let price = match Price::from_f64(it.price) { Some(p) => p, None => continue };
                                                    let vol_i64 = if it.current_volume != 0 { it.current_volume } else { it.volume };
                                                    let volume = match Volume::from_i64(vol_i64) { Some(v) => v, None => continue };
                                                    let ts = DateTime::<Utc>::from_str(it.timestamp.as_str()).unwrap_or_else(|_| Utc::now());
                                                    if latest_ts.map(|t| ts > t).unwrap_or(true) { latest_ts = Some(ts); }


                                                    if let Some(index) = it.index {
                                                        match it.r#type {
                                                            1 => { // Ask/BestAsk/NewBestAsk
                                                                asks.insert(index as usize,BookLevel { price, volume, level: index as u32 });
                                                            }
                                                            2 => { // Bid/BestBid/NewBestBid
                                                                bids.insert(index as usize,BookLevel { price, volume, level: index as u32 });
                                                            }
                                                            _ => { /* ignore other DOM types in orderbook snapshot */ }
                                                        }
                                                    } else {
                                                        match it.r#type {
                                                            3 | 10 => { // Ask/BestAsk/NewBestAsk
                                                                asks.insert(0,BookLevel { price, volume, level: 0 });
                                                            }
                                                            4 | 9 => { // Bid/BestBid/NewBestBid
                                                                bids.insert(0,BookLevel { price, volume, level: 0 });
                                                            }
                                                            //todo, handle other types, ticks and quotes to be generated
                                                            _ => {

                                                            }
                                                        }
                                                    }
                                                }

                                                // Sort levels: bids desc (best first), asks asc (best first)
                                                bids.sort_by(|a, b| b.price.cmp(&a.price));
                                                asks.sort_by(|a, b| a.price.cmp(&b.price));

                                                let ob = OrderBook {
                                                    symbol,
                                                    instrument: instrument.clone(),
                                                    bids,
                                                    asks,
                                                    time: latest_ts.unwrap_or_else(|| Utc::now()),
                                                };

                                                // Build SymbolKey for this stream
                                                let provider = tt_types::providers::ProviderKind::ProjectX(self.firm);
                                                let key = tt_types::keys::SymbolKey { instrument: instrument.clone(), provider };
                                                // Write OrderBook snapshot to SHM
                                                {
                                                    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&ob).unwrap_or_default();
                                                    tt_shm::write_snapshot(Topic::Depth, &key, &bytes);
                                                }
                                                // Publish rkyv OrderBook snapshot via key-based fanout for precise routing
                                                if let Err(e) = self.bus.publish_orderbook_for_key(&key, ob).await {
                                                    error!(target: "projectx.ws", "failed to publish OrderBook: {:?}", e);
                                                }

                                            }
                                        } else {
                                            info!(target: "projectx.ws", "GatewayDepth: invalid instrument in args: {}", instr_str);
                                        }
                                    } else {
                                        info!(target: "projectx.ws", "GatewayDepth: missing instrument in arguments");
                                    }
                                }
                            }
                            // User hub events
                            "GatewayUserAccount" => {
                                let account =
                                    match serde_json::from_value::<GatewayUserAccount>(val) {
                                        Ok(a) => a,
                                        Err(e) => {
                                            error!("error parsing GatewayUserAccount: {}", e);
                                            return;
                                        }
                                    };
                                info!(target: "projectx.ws", "GatewayUserAccount: {:?}", account);
                                let name = AccountName::new(account.name);
                                let balance = match Decimal::from_f64(account.balance) {
                                    Some(b) => b,
                                    None => {
                                        error!("invalid balance for {}", account.balance);
                                        return;
                                    }
                                };
                                let snap_shot = AccountSnapShot {
                                    name,
                                    id: account.id,
                                    balance,
                                    can_trade: account.can_trade,
                                };
                                info!(target: "projectx.ws", "AccountSnapShot: {:?}", snap_shot);
                                // Publish minimal AccountDelta to bus
                                let delta = AccountDelta {
                                    equity: balance,
                                    day_realized_pnl: dec!(0),
                                    open_pnl: dec!(0),
                                    ts_ns: Utc::now().timestamp_nanos_opt().unwrap_or(0),
                                };
                                let batch = AccountDeltaBatch {
                                    topic: tt_types::keys::Topic::AccountEvt,
                                    seq: 0,
                                    accounts: vec![delta],
                                };
                                if let Err(e) = self.bus.publish_account_delta_batch(batch).await {
                                    error!(target: "projectx.ws", "failed to publish AccountDelta: {:?}", e);
                                }
                            }
                            "GatewayUserOrder" => {
                                let order =
                                    match serde_json::from_value::<GatewayUserOrder>(val.clone()) {
                                        Ok(o) => o,
                                        Err(e) => {
                                            error!("error parsing GatewayUserOrder: {}", e);
                                            return;
                                        }
                                    };
                                // Build identifiers
                                let instrument =
                                    match Instrument::from_str(order.symbol_id.as_str()) {
                                        Ok(i) => i,
                                        Err(e) => {
                                            error!("error parsing instrument: {:?}", e);
                                            return;
                                        }
                                    };
                                // Enum mappings
                                let side = if order.side == 0 {
                                    Side::Buy
                                } else {
                                    Side::Sell
                                };
                                let order_type = models::map_order_type(order.r#type);
                                let status = models::map_status(order.status);

                                // Quantities and prices
                                let qty = Volume::from_i64(order.size).unwrap_or_else(|| dec!(0));
                                let filled_qty =
                                    Volume::from_f64(order.fill_volume.unwrap_or(0) as f64);
                                let _limit_px = order.limit_price.map(|p| Price::from_f64(p));
                                let _stop_px = order.stop_price.map(|p| Price::from_f64(p));

                                let time_accepted =
                                    DateTime::<Utc>::from_str(&order.creation_timestamp)
                                        .unwrap_or_else(|_| Utc::now());
                                let time_last = DateTime::<Utc>::from_str(&order.update_timestamp)
                                    .unwrap_or_else(|_| Utc::now());

                                info!(target: "projectx.ws", "GatewayUserOrder standardized: id={} status={} side={:?} type={:?}", order.id, order.status, side, order_type);
                                // Publish minimal OrderUpdate to bus
                                let state_code: u8 = models::map_status(order.status) as u8;
                                let cum_qty: i64 = order.fill_volume.unwrap_or(0);
                                let leaves: i64 = (order.size - cum_qty).max(0);
                                let avg_px = order
                                    .filled_price
                                    .and_then(|p| Price::from_f64(p))
                                    .unwrap_or(dec!(0));
                                let ts_ns = DateTime::<Utc>::from_str(&order.update_timestamp)
                                    .unwrap_or_else(|_| Utc::now())
                                    .timestamp_nanos_opt()
                                    .unwrap_or(0);
                                let ou = OrderUpdate {
                                    provider_order_id: Some(ProviderOrderId(order.id.to_string())),
                                    client_order_id: None,
                                    state_code,
                                    leaves,
                                    cum_qty,
                                    avg_fill_px: avg_px,
                                    ts_ns,
                                };
                                let batch = OrdersBatch {
                                    topic: tt_types::keys::Topic::Orders,
                                    seq: 0,
                                    orders: vec![ou],
                                };
                                if let Err(e) = self.bus.publish_orders_batch(batch).await {
                                    error!(target: "projectx.ws", "failed to publish OrderUpdate: {:?}", e);
                                }
                            }
                            "GatewayUserPosition" => {
                                let position = match serde_json::from_value::<GatewayUserPosition>(
                                    val.clone(),
                                ) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        error!("error parsing GatewayUserPosition: {}", e);
                                        return;
                                    }
                                };

                                let instrument_id = match Instrument::from_str(
                                    &parse_px_instrument(position.contract_id.as_str()),
                                ) {
                                    Ok(i) => i,
                                    Err(e) => {
                                        error!(target: "projectx.ws", "invalid instrument for position: {} ({:?})", position.contract_id, e);
                                        return;
                                    }
                                };
                                let symbol = extract_root(&instrument_id);

                                self.positions
                                    .insert(instrument_id.clone(), position.clone());

                                // Publish minimal PositionDelta to bus (snapshot semantics)
                                let after = if position.r#type == models::PositionType::Short as i32
                                {
                                    -position.size
                                } else {
                                    position.size
                                };
                                let ts_ns = DateTime::<Utc>::from_str(&position.creation_timestamp)
                                    .unwrap_or_else(|_| Utc::now())
                                    .timestamp_nanos_opt()
                                    .unwrap_or(0);
                                let pd = PositionDelta {
                                    instrument: instrument_id.clone(),
                                    net_qty_before: 0,
                                    net_qty_after: after,
                                    realized_delta: dec!(0),
                                    open_pnl: dec!(0),
                                    ts_ns,
                                };
                                let batch = PositionsBatch {
                                    topic: tt_types::keys::Topic::Positions,
                                    seq: 0,
                                    positions: vec![pd],
                                };
                                if let Err(e) = self.bus.publish_positions_batch(batch).await {
                                    error!(target: "projectx.ws", "failed to publish PositionDelta: {:?}", e);
                                }

                                info!(target: "projectx.ws", "GatewayUserPosition cached: id={} account_id={} contract={}", position.id, position.account_id, position.contract_id);
                            }
                            "GatewayUserTrade" => {
                                let trade =
                                    match serde_json::from_value::<GatewayUserTrade>(val.clone()) {
                                        Ok(t) => t,
                                        Err(e) => {
                                            error!("error parsing GatewayUserTrade: {}", e);
                                            return;
                                        }
                                    };

                                let instrument_id = match Instrument::from_str(
                                    &parse_px_instrument(trade.contract_id.as_str()),
                                ) {
                                    Ok(i) => i,
                                    Err(e) => {
                                        error!(target: "projectx.ws", "invalid instrument for trade: {} ({:?})", trade.contract_id, e);
                                        return;
                                    }
                                };
                                let symbol = extract_root(&instrument_id);

                                let side = if trade.side == 0 {
                                    Side::Buy
                                } else {
                                    Side::Sell
                                };

                                let last_px = Price::from_f64(trade.price);
                                let last_qty = Volume::from_f64(trade.size as f64);
                                let commission = Decimal::from_f64(trade.fees);
                                let time_accepted =
                                    DateTime::<Utc>::from_str(&trade.creation_timestamp)
                                        .unwrap_or_else(|_| Utc::now());
                                info!(target: "projectx.ws", "GatewayUserTrade standardized: id={} order_id={} side={:?} px={} qty={} fees={}", trade.id, trade.order_id, side, trade.price, trade.size, trade.fees);
                            }
                            _ => {}
                        }
                    }
                }
                6 => {
                    // Ping
                }
                _ => {}
            }
        }
    }

    async fn invoke_market(&self, target: &str, args: Vec<Value>) -> anyhow::Result<()> {
        let id = self.next_request_id().to_string();
        let payload = serde_json::json!({
            "type": 1,
            "invocationId": id,
            "target": target,
            "arguments": args,
        });
        let frame = format!("{}\u{001e}", payload.to_string());
        self.send_market_text(frame).await
    }

    async fn invoke_user(&self, target: &str, args: Vec<Value>) -> anyhow::Result<()> {
        let id = self.next_request_id().to_string();
        let payload = serde_json::json!({
            "type": 1,
            "invocationId": id,
            "target": target,
            "arguments": args,
        });
        let frame = format!("{}\u{001e}", payload.to_string());
        self.send_user_text(frame).await
    }

    /// Subscribe to user events for an account
    ///
    /// Internally tracks the account for resubscription after reconnect.
    pub async fn subscribe_user_account(&self, account_id: i64) -> anyhow::Result<()> {
        let mut sub_guard = self.account_subs.write().await;
        if sub_guard.contains(&account_id) {
            return Ok(());
        }
        sub_guard.push(account_id);
        // In the JS example the client subscribes to Accounts (no arg) and then account-specific streams.
        if !self.user_accounts_subscribed.load(Ordering::SeqCst) {
            self.user_accounts_subscribed.store(true, Ordering::SeqCst);
            // Subscribe to broadcast accounts stream once
            let _ = self.invoke_user("SubscribeAccounts", vec![]).await;
        }
        // Track account across all user subscription lists (store as String)
        {
            let mut w = self.user_orders_subs.write().await;
            if !w.contains(&account_id) {
                w.push(account_id);
            }
        }
        {
            let mut w = self.user_positions_subs.write().await;
            let s = account_id.to_string();
            if !w.contains(&account_id) {
                w.push(account_id);
            }
        }
        {
            let mut w = self.user_trades_subs.write().await;
            let s = account_id.to_string();
            if !w.contains(&account_id) {
                w.push(account_id);
            }
        }
        // Subscribe to per-account streams
        let id_val = Value::from(account_id);
        let _ = self
            .invoke_user("SubscribeOrders", vec![id_val.clone()])
            .await;
        let _ = self
            .invoke_user("SubscribePositions", vec![id_val.clone()])
            .await;
        let _ = self.invoke_user("SubscribeTrades", vec![id_val]).await;
        Ok(())
    }

    /// Unsubscribe from user events for an account
    ///
    /// Removes the account from tracked subscriptions. If no accounts remain, the
    /// broadcast Accounts flag is left as-is to avoid unexpected side effects.
    pub async fn unsubscribe_user_account(&self, account_id: i64) -> anyhow::Result<()> {
        {
            let mut w = self.user_orders_subs.write().await;
            w.retain(|x| *x != account_id);
        }
        {
            let mut w = self.user_positions_subs.write().await;
            w.retain(|x| *x != account_id);
        }
        {
            let mut w = self.user_trades_subs.write().await;
            w.retain(|x| *x != account_id);
        }
        // Send unsubs for this account
        let id_val = Value::from(account_id);
        let _ = self
            .invoke_user("UnsubscribeOrders", vec![id_val.clone()])
            .await;
        let _ = self
            .invoke_user("UnsubscribePositions", vec![id_val.clone()])
            .await;
        let _ = self
            .invoke_user("UnsubscribeTrades", vec![id_val.clone()])
            .await;
        // If no more accounts tracked, optionally unsubscribe from accounts stream
        let no_accounts = {
            let o = self.user_orders_subs.read().await;
            let p = self.user_positions_subs.read().await;
            let t = self.user_trades_subs.read().await;
            o.is_empty() && p.is_empty() && t.is_empty()
        };
        if no_accounts {
            if self.user_accounts_subscribed.swap(false, Ordering::SeqCst) {
                let _ = self.invoke_user("UnsubscribeAccounts", vec![]).await;
            }
        }
        Ok(())
    }

    /// Subscribe to market data by contract ID (quotes)
    pub async fn subscribe_contract_quotes(&self, contract_id: &str) -> anyhow::Result<()> {
        {
            let mut w = self.market_contract_quotes_subs.write().await;
            let s = contract_id.to_string();
            if !w.contains(&s) {
                w.push(s);
            } else {
                return Ok(());
            }
        }
        // SignalR invocation
        self.invoke_market(
            "SubscribeContractQuotes",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by contract ID (quotes)
    pub async fn unsubscribe_contract_quotes(&self, contract_id: &str) -> anyhow::Result<()> {
        {
            let mut w = self.market_contract_quotes_subs.write().await;
            w.retain(|x| x != contract_id);
        }
        self.invoke_market(
            "UnsubscribeContractQuotes",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Subscribe to market data by contract ID (trades)
    pub async fn subscribe_contract_ticks(&self, contract_id: &str) -> anyhow::Result<()> {
        {
            let mut w = self.market_contract_tick_subs.write().await;
            let s = contract_id.to_string();
            if !w.contains(&s) {
                w.push(s);
            } else {
                return Ok(());
            }
        }
        self.invoke_market(
            "SubscribeContractTrades",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by contract ID (trades)
    pub async fn unsubscribe_contract_ticks(&self, contract_id: &str) -> anyhow::Result<()> {
        {
            let mut w = self.market_contract_tick_subs.write().await;
            w.retain(|x| x != contract_id);
        }
        self.invoke_market(
            "UnsubscribeContractTrades",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    pub async fn subscribe_account_positions(&self, account_id: i64) -> anyhow::Result<()> {
        {
            let mut w = self.user_positions_subs.write().await;
            if !w.contains(&account_id) {
                w.push(account_id);
            } else {
                return Ok(());
            }
        }
        self.invoke_market(
            "SubscribePositions",
            vec![Value::String(account_id.to_string())],
        )
        .await
    }

    pub async fn unsubscribe_account_positions(&self, account_id: i64) -> anyhow::Result<()> {
        {
            let mut w = self.user_positions_subs.write().await;
            w.retain(|x| *x != account_id);
        }
        self.invoke_market(
            "UnsubscribePositions",
            vec![Value::String(account_id.to_string())],
        )
        .await
    }

    /// Subscribe to market data by account ID (trades)
    pub async fn subscribe_account_orders(&self, account_id: i64) -> anyhow::Result<()> {
        {
            let mut w = self.user_orders_subs.write().await;
            if !w.contains(&account_id) {
                w.push(account_id);
            } else {
                return Ok(());
            }
        }
        self.invoke_market(
            "SubscribeOrders",
            vec![Value::String(account_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by account ID (trades)
    pub async fn unsubscribe_account_orders(&self, account_id: i64) -> anyhow::Result<()> {
        {
            let mut w = self.user_orders_subs.write().await;
            w.retain(|x| *x != account_id);
        }
        self.invoke_market(
            "UnsubscribeOrders",
            vec![Value::String(account_id.to_string())],
        )
        .await
    }

    /// Subscribe to market data by contract ID (market depth)
    pub async fn subscribe_contract_market_depth(&self, contract_id: &str) -> anyhow::Result<()> {
        {
            let mut w = self.market_contract_depth_subs.write().await;
            let s = contract_id.to_string();
            if !w.contains(&s) {
                w.push(s);
            }
        }
        self.invoke_market(
            "SubscribeContractMarketDepth",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by contract ID (market depth)
    pub async fn unsubscribe_contract_market_depth(&self, contract_id: &str) -> anyhow::Result<()> {
        {
            let mut w = self.market_contract_depth_subs.write().await;
            w.retain(|x| x != contract_id);
        }
        self.invoke_market(
            "UnsubscribeContractMarketDepth",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Subscribe to market data by contract ID (bars)
    pub async fn subscribe_contract_bars(&self, contract_id: &str) -> anyhow::Result<()> {
        {
            let mut w = self.market_contract_bars_subs.write().await;
            let s = contract_id.to_string();
            if !w.contains(&s) {
                w.push(s);
            }
        }
        // Minimal invocation variant (server may support additional args for unit/unitNumber)
        self.invoke_market(
            "SubscribeContractBars",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by contract ID (bars)
    pub async fn unsubscribe_contract_bars(&self, contract_id: &str) -> anyhow::Result<()> {
        {
            let mut w = self.market_contract_bars_subs.write().await;
            w.retain(|x| x != contract_id);
        }
        self.invoke_market(
            "UnsubscribeContractBars",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Helper to check if user hub is connected
    pub fn is_user_connected(&self) -> bool {
        self.user_connected.load(Ordering::SeqCst)
    }

    /// Helper to check if market hub is connected
    pub fn is_market_connected(&self) -> bool {
        self.market_connected.load(Ordering::SeqCst)
    }

    /// Generate a unique request ID for outbound messages
    pub fn next_request_id(&self) -> u64 {
        self.request_id_counter.fetch_add(1, Ordering::SeqCst)
    }
}

impl PxWebSocketClient {
    /// Get a snapshot of currently subscribed contract IDs for tick stream.
    pub async fn active_contract_ids_ticks(&self) -> Vec<String> {
        self.market_contract_tick_subs.read().await.clone()
    }
    /// Get a snapshot of currently subscribed contract IDs for quote stream.
    pub async fn active_contract_ids_quotes(&self) -> Vec<String> {
        self.market_contract_quotes_subs.read().await.clone()
    }
    /// Get a snapshot of currently subscribed contract IDs for market depth stream.
    pub async fn active_contract_ids_depth(&self) -> Vec<String> {
        self.market_contract_depth_subs.read().await.clone()
    }
}
