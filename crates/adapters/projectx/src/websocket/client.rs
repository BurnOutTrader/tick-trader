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
use tt_types::accounts::account::AccountName;
#[allow(unused)]
use tt_types::accounts::events::{
    AccountDelta, ClientOrderId, OrderUpdate, PositionDelta, ProviderOrderId, Side,
};
use tt_types::accounts::events::PositionSide;
use tt_types::Guid;
use tt_types::accounts::order::OrderState;
use tt_types::data::core::Tick;
use tt_types::data::models::{Price, TradeSide, Volume};
use tt_types::keys::Topic;
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::futures_helpers::{extract_month_year, extract_root, sanitize_code};
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{AccountDeltaBatch, Bytes, OrdersBatch, PositionsBatch, Trade};
// Map ProjectX depth items to MBP10 incremental updates and publish individually
use tt_types::data::mbp10::{
    Action as MbpAction, BookLevels, BookSide as MbpSide, Flags as MbpFlags, Mbp10,
};

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
    provider_kind: ProviderKind,
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

    // ---- Accounts ----
    accounts_by_id: Arc<DashMap<i64, String>>,

    trades: Arc<DashMap<Instrument, Vec<GatewayUserTrade>>>,
}

pub fn parse_px_instrument(input: &str) -> String {
    // Expect formats like "CON.F.US.MNQ.Z25" or root-only like "CON.F.US.MNQ".
    let mut parts: Vec<&str> = input.split('.').collect();
    if parts.len() >= 4 {
        // Last part is expiry (e.g., Z25), preceding is root (e.g., MNQ)
        let expiry = parts.pop().unwrap();
        let root = parts.pop().unwrap();
        // Ensure two-digit year in expiry; keep as provided if already two-digit
        // If expiry is like "Z5", convert to "Z05"? Requirement: format as ROOT.MonYY (two-digit). We'll map single digit to 0X? Better: assume PX always provides 2-digit (Z25). If 1-digit, left-pad with 0.
        let mut chars = expiry.chars();
        let month = chars.next().unwrap_or('Z');
        let yy: String = chars.collect();
        let yy2 = if yy.len() == 1 {
            format!("0{}", yy)
        } else {
            yy
        };
        return format!("{}.{}{}", root, month, yy2);
    } else if parts.len() >= 2 {
        // Fallback: root-only PX id -> produce continuous instrument ROOT (no expiry)
        let root = parts.pop().unwrap();
        return root.to_string();
    }
    input.to_string()
}

/// Format to ProjectX contract id:
/// - Continuous:  "CON.F.US.MNQ"
/// - Dated:       "CON.F.US.MNQ.Z25"
pub fn px_format_from_instrument(instrument: &Instrument) -> String {
    // Accept instruments like "MNQ.Z25" or legacy "MNQZ25" or root-only "MNQ".
    let code = sanitize_code(&instrument.to_string()).to_ascii_uppercase();

    // If it looks like a dated contract, format ROOT + . + <Mon><YY>
    if let Some((month_ch, yy)) = extract_month_year(instrument) {
        // ROOT: if the instrument contains a '.', take substring before it; else use extract_root
        let root = if let Some(dot) = code.find('.') {
            code[..dot].to_string()
        } else {
            sanitize_code(&extract_root(instrument)).to_ascii_uppercase()
        };
        // Two-digit year as provided by extract_month_year (1–2 digits). Left-pad if needed.
        let yy2: u8 = yy;
        return format!("CON.F.US.{}.{}{:02}", root, month_ch, yy2);
    }

    // Otherwise treat as a continuous root
    format!("CON.F.US.{}", code)
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
            provider_kind: ProviderKind::ProjectX(firm),
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
            // Accounts map: id -> name
            accounts_by_id: Arc::new(DashMap::new()),
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
                    Ok(Message::Ping(payload)) => {
                        // Respond with Pong to keep the connection healthy
                        let guard = this.user_ws.lock().await;
                        if let Some(client) = guard.as_ref() {
                            if let Err(e) = client.send_pong(payload).await {
                                tracing::warn!(target: "projectx.ws", "failed to send Pong on user ws: {e}");
                            }
                        }
                    }
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
                    Ok(Message::Ping(payload)) => {
                        // Respond with Pong to keep the connection healthy
                        let guard = this.market_ws.lock().await;
                        if let Some(client) = guard.as_ref() {
                            if let Err(e) = client.send_pong(payload).await {
                                tracing::warn!(target: "projectx.ws", "failed to send Pong on market ws: {e}");
                            }
                        }
                    }
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
                                // Handle payload that can be a single object or an array of objects.
                                // Also, resolve Instrument from the first argument (full contract id like "CON.F.US.MNQ.Z25").
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    // contract id is first arg when present
                                    let instrument_opt = args
                                        .get(0)
                                        .and_then(|v| v.as_str())
                                        .and_then(|s| Instrument::try_parse_dotted(s).ok());
                                    // data is second arg when present, otherwise first
                                    let maybe_data = if args.len() >= 2 {
                                        Some(&args[1])
                                    } else {
                                        args.get(0)
                                    };

                                    let mut quotes: Vec<GatewayQuote> = Vec::new();
                                    if let Some(data_val) = maybe_data {
                                        if let Some(arr) = data_val.as_array() {
                                            for item in arr {
                                                if let Ok(q) = serde_json::from_value::<GatewayQuote>(
                                                    item.clone(),
                                                ) {
                                                    quotes.push(q);
                                                }
                                            }
                                        } else if data_val.is_object() {
                                            if let Ok(q) = serde_json::from_value::<GatewayQuote>(
                                                data_val.clone(),
                                            ) {
                                                quotes.push(q);
                                            }
                                        }
                                    }
                                    if quotes.is_empty() {
                                        return;
                                    }

                                    if let Some(instrument) = instrument_opt {
                                        let symbol = extract_root(&instrument);
                                        let provider =
                                            tt_types::providers::ProviderKind::ProjectX(self.firm);
                                        let key = tt_types::keys::SymbolKey {
                                            instrument: instrument.clone(),
                                            provider,
                                        };

                                        let mut published = 0usize;
                                        for px_quote in quotes.into_iter() {
                                            let bid = Price::from_f64(px_quote.best_bid)
                                                .unwrap_or_default();
                                            let ask = Price::from_f64(px_quote.best_ask)
                                                .unwrap_or_default();
                                            let bid_size = Volume::from(0);
                                            let ask_size = Volume::from(0);
                                            let time = DateTime::<Utc>::from_str(
                                                px_quote.timestamp.as_str(),
                                            )
                                            .or_else(|_| {
                                                DateTime::<Utc>::from_str(
                                                    px_quote.last_updated.as_str(),
                                                )
                                            })
                                            .unwrap_or_else(|_| Utc::now());
                                            let bbo = tt_types::data::core::Bbo {
                                                symbol: symbol.clone(),
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
                                            let buf = bbo.to_aligned_bytes();
                                            tt_shm::write_snapshot(Topic::Quotes, &key, &buf);
                                            if let Err(e) = self.bus.publish_quote(bbo, self.provider_kind).await {
                                                log::warn!("failed to publish quote: {}", e);
                                            } else {
                                                published += 1;
                                            }
                                        }
                                        if published == 0 {
                                            info!(target: "projectx.ws", "GatewayQuote: parsed but no valid quotes for {}", instrument);
                                        }
                                    } else {
                                        info!(target: "projectx.ws", "GatewayQuote: could not derive instrument from args: {:?}", args);
                                    }
                                }
                            }
                            "GatewayTrade" => {
                                // Extract args and deserialize payload (can be array of objects now)
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    // First arg is the full contract id (e.g., "CON.F.US.MNQ.Z25"). Use it to derive Instrument.
                                    let instrument_opt = args
                                        .get(0)
                                        .and_then(|v| v.as_str())
                                        .and_then(|s| Instrument::try_parse_dotted(s).ok());

                                    // Second arg is either an array of trade objects or a single object
                                    let maybe_data = if args.len() >= 2 {
                                        Some(&args[1])
                                    } else {
                                        args.get(0)
                                    };

                                    let mut trades: Vec<GatewayTrade> = Vec::new();
                                    if let Some(data_val) = maybe_data {
                                        if let Some(arr) = data_val.as_array() {
                                            // Deserialize an array of GatewayTrade
                                            for item in arr {
                                                if let Ok(t) = serde_json::from_value::<GatewayTrade>(
                                                    item.clone(),
                                                ) {
                                                    trades.push(t);
                                                }
                                            }
                                        } else if data_val.is_object() {
                                            if let Ok(t) = serde_json::from_value::<GatewayTrade>(
                                                data_val.clone(),
                                            ) {
                                                trades.push(t);
                                            }
                                        }
                                    }

                                    if trades.is_empty() {
                                        // Nothing to do for this message
                                        return;
                                    }

                                    // Resolve instrument once
                                    if let Some(instrument) = instrument_opt {
                                        let symbol = extract_root(&instrument);
                                        let provider =
                                            tt_types::providers::ProviderKind::ProjectX(self.firm);
                                        let key = tt_types::keys::SymbolKey {
                                            instrument: instrument.clone(),
                                            provider,
                                        };

                                        let mut published = 0usize;
                                        for px_trade in trades.into_iter() {
                                            let price = match Price::from_f64(px_trade.price) {
                                                Some(p) => p,
                                                None => {
                                                    log::warn!(
                                                        "invalid price for {}",
                                                        px_trade.price
                                                    );
                                                    continue;
                                                }
                                            };
                                            let volume = match Volume::from_i64(px_trade.volume) {
                                                Some(s) => s,
                                                None => {
                                                    log::warn!(
                                                        "invalid size for {}",
                                                        px_trade.volume
                                                    );
                                                    continue;
                                                }
                                            };
                                            let side = match px_trade.r#type {
                                                0 => TradeSide::Buy,
                                                1 => TradeSide::Sell,
                                                _ => TradeSide::None,
                                            };
                                            let time = DateTime::<Utc>::from_str(
                                                px_trade.timestamp.as_str(),
                                            )
                                            .unwrap_or_else(|e| {
                                                log::warn!(
                                                    "invalid timestamp for {}: {}",
                                                    px_trade.timestamp,
                                                    e
                                                );
                                                Utc::now()
                                            });
                                            let tick = Tick {
                                                symbol: symbol.clone(),
                                                instrument: instrument.clone(),
                                                price,
                                                volume,
                                                time,
                                                side,
                                                venue_seq: None,
                                            };
                                            // Write Tick snapshot to SHM
                                            let buf = tick.to_aligned_bytes();
                                            tt_shm::write_snapshot(Topic::Ticks, &key, &buf);
                                            if let Err(e) = self.bus.publish_tick(tick, self.provider_kind).await {
                                                log::warn!("failed to publish tick: {}", e);
                                            } else {
                                                published += 1;
                                            }
                                        }
                                        if published == 0 {
                                            info!(target: "projectx.ws", "GatewayTrade: parsed but no valid ticks for {}", instrument);
                                        }
                                    } else {
                                        info!(target: "projectx.ws", "GatewayTrade: could not derive instrument from args: {:?}", args);
                                    }
                                }
                            }
                            "GatewayDepth" => {
                                // Extract args and handle payload which may be an array or a single object
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    // contract id is first arg; data is second when present
                                    let instr_opt = args
                                        .get(0)
                                        .and_then(|v| v.as_str())
                                        .and_then(|s| Instrument::try_parse_dotted(s).ok());
                                    let data_val = if args.len() >= 2 {
                                        Some(&args[1])
                                    } else {
                                        args.get(0)
                                    };

                                    if let Some(instrument) = instr_opt {
                                        let provider =
                                            tt_types::providers::ProviderKind::ProjectX(self.firm);
                                        let key = tt_types::keys::SymbolKey {
                                            instrument: instrument.clone(),
                                            provider,
                                        };

                                        // Normalize payload to a vector of depth items
                                        let mut items: Vec<models::GatewayDepth> = Vec::new();
                                        if let Some(data) = data_val {
                                            if data.is_array() {
                                                if let Some(arr) = data.as_array() {
                                                    for item in arr {
                                                        if let Ok(d) = serde_json::from_value::<
                                                            models::GatewayDepth,
                                                        >(
                                                            item.clone()
                                                        ) {
                                                            items.push(d);
                                                        }
                                                    }
                                                }
                                            } else if data.is_object() {
                                                if let Ok(d) =
                                                    serde_json::from_value::<models::GatewayDepth>(
                                                        data.clone(),
                                                    )
                                                {
                                                    items.push(d);
                                                }
                                            }
                                        }

                                        if items.is_empty() {
                                            return;
                                        }

                                        // If we received a batch array, opportunistically compose an aggregated top-10 book snapshot
                                        // from the batch to align with Databento optional book levels.
                                        let mut attach_snapshot_book: Option<BookLevels> = None;
                                        if let Some(data) = data_val {
                                            if let Some(arr) = data.as_array() {
                                                // Collect levels per side keyed by price
                                                use std::collections::BTreeMap;
                                                // For bids sort by price desc; for asks sort asc
                                                let mut bid_map: BTreeMap<Decimal, Decimal> =
                                                    BTreeMap::new();
                                                let mut ask_map: BTreeMap<Decimal, Decimal> =
                                                    BTreeMap::new();
                                                for itv in arr.iter() {
                                                    if let Ok(di) = serde_json::from_value::<
                                                        models::GatewayDepth,
                                                    >(
                                                        itv.clone()
                                                    ) {
                                                        // Determine side as in event mapping
                                                        match di.r#type {
                                                            1 | 3 | 10 => {
                                                                // Ask side: use total volume at price for book
                                                                let px_nanos = (di.price
                                                                    * 1_000_000_000.0)
                                                                    .round()
                                                                    as i64;
                                                                let px_dec =
                                                                    Decimal::from_i128_with_scale(
                                                                        px_nanos as i128,
                                                                        9,
                                                                    );
                                                                let sz_dec: Decimal = if di.volume
                                                                    < 0
                                                                {
                                                                    Decimal::ZERO
                                                                } else {
                                                                    Decimal::from(
                                                                        (di.volume as u64)
                                                                            .min(u32::MAX as u64),
                                                                    )
                                                                };
                                                                // For asks we want ascending sort; BTreeMap is ascending by key
                                                                ask_map.insert(px_dec, sz_dec);
                                                            }
                                                            2 | 4 | 9 => {
                                                                // Bid side
                                                                let px_nanos = (di.price
                                                                    * 1_000_000_000.0)
                                                                    .round()
                                                                    as i64;
                                                                let px_dec =
                                                                    Decimal::from_i128_with_scale(
                                                                        px_nanos as i128,
                                                                        9,
                                                                    );
                                                                let sz_dec: Decimal = if di.volume
                                                                    < 0
                                                                {
                                                                    Decimal::ZERO
                                                                } else {
                                                                    Decimal::from(
                                                                        (di.volume as u64)
                                                                            .min(u32::MAX as u64),
                                                                    )
                                                                };
                                                                bid_map.insert(px_dec, sz_dec);
                                                            }
                                                            _ => {}
                                                        }
                                                    }
                                                }
                                                if !bid_map.is_empty() || !ask_map.is_empty() {
                                                    // Extract top 10 bids (highest first) and asks (lowest first)
                                                    let mut bid_px: Vec<Decimal> = Vec::new();
                                                    let mut bid_sz: Vec<Decimal> = Vec::new();
                                                    for (px, sz) in bid_map.iter().rev().take(10) {
                                                        // highest first
                                                        bid_px.push(*px);
                                                        bid_sz.push(*sz);
                                                    }
                                                    let mut ask_px: Vec<Decimal> = Vec::new();
                                                    let mut ask_sz: Vec<Decimal> = Vec::new();
                                                    for (px, sz) in ask_map.iter().take(10) {
                                                        // lowest first
                                                        ask_px.push(*px);
                                                        ask_sz.push(*sz);
                                                    }
                                                    // Order counts not available from PX depth payload; set zeros to keep alignment
                                                    let bid_ct = vec![dec!(0); bid_px.len()];
                                                    let ask_ct = vec![dec!(0); ask_px.len()];
                                                    attach_snapshot_book = Some(BookLevels {
                                                        bid_px,
                                                        ask_px,
                                                        bid_sz,
                                                        ask_sz,
                                                        bid_ct,
                                                        ask_ct,
                                                    });
                                                }
                                            }
                                        }

                                        let mut published = 0usize;
                                        let mut first_in_batch = true;
                                        for it in items.into_iter() {
                                            // Map side using API DomType definitions
                                            // Ask(1), BestAsk(3), NewBestAsk(10) => Ask; Bid(2), BestBid(4), NewBestBid(9) => Bid; others => None
                                            let side = match it.r#type {
                                                1 | 3 | 10 => MbpSide::Ask,
                                                2 | 4 | 9 => MbpSide::Bid,
                                                _ => MbpSide::None,
                                            };
                                            // Map action to engine Action per API:
                                            // Unknown(0), Low(7), High(8) => None
                                            // Reset(6) => Clear
                                            // Trade(5) => Trade
                                            // Fill(11) => Fill
                                            // Otherwise (Ask/Bid/BestAsk/BestBid/NewBest*) => Modify (set total volume at price)
                                            let action = match it.r#type {
                                                0 | 7 | 8 => MbpAction::None,
                                                6 => MbpAction::Clear,
                                                5 => MbpAction::Trade,
                                                11 => MbpAction::Fill,
                                                _ => MbpAction::Modify,
                                            };

                                            // Build Mbp10 with proper timestamp semantics
                                            // ts_event: provider/event timestamp; ts_recv: local receive timestamp
                                            let ts_event =
                                                DateTime::<Utc>::from_str(it.timestamp.as_str())
                                                    .unwrap_or_else(|_| Utc::now());
                                            let ts_event_ns = (ts_event.timestamp() as u64)
                                                .saturating_mul(1_000_000_000)
                                                .saturating_add(
                                                    ts_event.timestamp_subsec_nanos() as u64
                                                );
                                            let ts_recv = Utc::now();
                                            let ts_recv_ns = (ts_recv.timestamp() as u64)
                                                .saturating_mul(1_000_000_000)
                                                .saturating_add(
                                                    ts_recv.timestamp_subsec_nanos() as u64
                                                );

                                            // Compute ts_in_delta = ts_recv - ts_event (ns), clamp to i32
                                            let mut bad_ts = false;
                                            let delta_i128 =
                                                (ts_recv_ns as i128) - (ts_event_ns as i128);
                                            let ts_in_delta_i32 = if delta_i128 < 0
                                                || delta_i128 > i32::MAX as i128
                                            {
                                                bad_ts = true;
                                                0
                                            } else {
                                                delta_i128 as i32
                                            };

                                            // Use currentVolume for Trade/Fill; otherwise, use total volume at price
                                            let use_current = matches!(it.r#type, 5 | 11);
                                            let raw_size = if use_current {
                                                it.current_volume
                                            } else {
                                                it.volume
                                            };
                                            let size_dec: Decimal = if raw_size < 0 {
                                                Decimal::ZERO
                                            } else {
                                                Decimal::from(
                                                    (raw_size as u64).min(u32::MAX as u64),
                                                )
                                            };
                                            let mut flags = MbpFlags::from(MbpFlags::F_MBP);
                                            if matches!(it.r#type, 3 | 4 | 9 | 10) {
                                                // best bid/ask related
                                                flags = MbpFlags(flags.0 | MbpFlags::F_TOB);
                                            }
                                            if bad_ts {
                                                flags = MbpFlags(flags.0 | MbpFlags::F_BAD_TS_RECV);
                                            }
                                            // Attach aggregated book snapshot only once per batch if available
                                            let mut book_opt = None;
                                            if first_in_batch {
                                                if let Some(bk) = attach_snapshot_book.clone() {
                                                    flags =
                                                        MbpFlags(flags.0 | MbpFlags::F_SNAPSHOT);
                                                    book_opt = Some(bk);
                                                }
                                            }

                                            // Build directly instead of make_mbp10 to avoid decimal conversion corner cases
                                            // Convert price to Decimal via integer nanos to avoid FP rounding issues
                                            let price_nanos =
                                                (it.price * 1_000_000_000.0).round() as i64;
                                            let event = Mbp10 {
                                                instrument: instrument.clone(),
                                                ts_recv,
                                                ts_event,
                                                rtype: 10,
                                                publisher_id: 0,
                                                instrument_id: 0,
                                                action,
                                                side,
                                                depth: it
                                                    .index
                                                    .unwrap_or(0)
                                                    .try_into()
                                                    .unwrap_or(0),
                                                price: Decimal::from_i128_with_scale(
                                                    price_nanos as i128,
                                                    9,
                                                ),
                                                size: size_dec,
                                                flags,
                                                ts_in_delta: ts_in_delta_i32,
                                                sequence: 0,
                                                book: book_opt,
                                            };
                                            // info!("{:?}", event);
                                            first_in_batch = false;

                                            // Write MBP10 snapshot/update to SHM before publishing
                                            {
                                                let buf = event.to_aligned_bytes();
                                                tt_shm::write_snapshot(Topic::MBP10, &key, &buf);
                                            }

                                            if let Err(e) =
                                                self.bus.publish_mbp10_for_key(&key, self.provider_kind, event).await
                                            {
                                                log::warn!(target: "projectx.ws", "failed to publish MBP10: {}", e);
                                            } else {
                                                published += 1;
                                            }
                                        }
                                        if published == 0 {
                                            info!(target: "projectx.ws", "GatewayDepth: parsed but no valid updates for instrument");
                                        }
                                    } else {
                                        info!(target: "projectx.ws", "GatewayDepth: could not derive instrument from args: {:?}", args);
                                    }
                                }
                            }
                            // User hub events
                            "Gate" => {
                                // SignalR user event payloads come in the 'arguments' array and may be wrapped as {action, data}.
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let data_val = args.get(0);
                                    let mut items: Vec<GatewayUserAccount> = Vec::new();
                                    if let Some(dv) = data_val {
                                        if let Some(arr) = dv.as_array() {
                                            for item in arr {
                                                let obj = if item.is_object() {
                                                    if let Some(d) = item.get("data") {
                                                        d.clone()
                                                    } else {
                                                        item.clone()
                                                    }
                                                } else {
                                                    item.clone()
                                                };
                                                if let Ok(acct) =
                                                    serde_json::from_value::<GatewayUserAccount>(
                                                        obj,
                                                    )
                                                {
                                                    items.push(acct);
                                                }
                                            }
                                        } else if dv.is_object() {
                                            let obj = if let Some(d) = dv.get("data") {
                                                d.clone()
                                            } else {
                                                dv.clone()
                                            };
                                            if let Ok(acct) =
                                                serde_json::from_value::<GatewayUserAccount>(obj)
                                            {
                                                items.push(acct);
                                            }
                                        }
                                    }
                                    if items.is_empty() {
                                        return;
                                    }

                                    for account in items.into_iter() {
                                        // Store account id -> name for later trade mapping
                                        self.accounts_by_id.insert(account.id, account.name.clone());
                                        let balance = match Decimal::from_f64(account.balance) {
                                            Some(b) => b,
                                            None => {
                                                error!("invalid balance for {}", account.balance);
                                                continue;
                                            }
                                        };
                                        let delta = AccountDelta {
                                            provider_kind: self.provider_kind,
                                            name: AccountName::new(account.name),
                                            equity: balance,
                                            day_realized_pnl: dec!(0),
                                            open_pnl: dec!(0),
                                            time: Utc::now(),
                                            can_trade: account.can_trade,
                                        };
                                        let batch = AccountDeltaBatch {
                                            topic: tt_types::keys::Topic::AccountEvt,
                                            seq: 0,
                                            accounts: vec![delta],
                                        };
                                        if let Err(e) =
                                            self.bus.publish_account_delta_batch(batch).await
                                        {
                                            error!(target: "projectx.ws", "failed to publish AccountDelta: {:?}", e);
                                        }
                                    }
                                }
                            }
                            "GatewayUserOrder" => {
                                // Extract payloads from 'arguments' and unwrap wrappers: {action, data}
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let maybe_data = if args.len() >= 2 {
                                        Some(&args[1])
                                    } else {
                                        args.get(0)
                                    };
                                    let mut orders: Vec<GatewayUserOrder> = Vec::new();
                                    if let Some(dv) = maybe_data {
                                        if let Some(arr) = dv.as_array() {
                                            for item in arr {
                                                let obj = if item.is_object() {
                                                    item.get("data")
                                                        .cloned()
                                                        .unwrap_or(item.clone())
                                                } else {
                                                    item.clone()
                                                };
                                                if let Ok(o) =
                                                    serde_json::from_value::<GatewayUserOrder>(obj)
                                                {
                                                    orders.push(o);
                                                }
                                            }
                                        } else if dv.is_object() {
                                            let obj = dv.get("data").cloned().unwrap_or(dv.clone());
                                            if let Ok(o) =
                                                serde_json::from_value::<GatewayUserOrder>(obj)
                                            {
                                                orders.push(o);
                                            }
                                        }
                                    }
                                    if orders.is_empty() {
                                        return;
                                    }
                                    for order in orders.into_iter() {
                                        let instrument = match Instrument::try_parse_dotted(
                                            order.contract_id.as_str(),
                                        ) {
                                            Ok(i) => i,
                                            Err(e) => {
                                                error!("error parsing instrument: {:?}", e);
                                                continue;
                                            }
                                        };
                                        let state_code: OrderState= models::map_status(order.status);
                                        let cum_qty: i64 = order.fill_volume.unwrap_or(0);
                                        let leaves: i64 = (order.size - cum_qty).max(0);
                                        let avg_px = order
                                            .filled_price
                                            .and_then(|p| Price::from_f64(p))
                                            .unwrap_or(dec!(0));
                                        let ts_dt =
                                            DateTime::<Utc>::from_str(&order.update_timestamp)
                                                .unwrap_or_else(|_| Utc::now());
                                        let ou = OrderUpdate {
                                            provider_kind: self.provider_kind,
                                            instrument,
                                            provider_order_id: Some(ProviderOrderId(
                                                order.id.to_string(),
                                            )),
                                            client_order_id: None,
                                            state: state_code,
                                            leaves,
                                            cum_qty,
                                            avg_fill_px: avg_px,
                                            time: ts_dt,
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
                                }
                            }
                            "GatewayUserPosition" => {
                                // Extract payloads from 'arguments' and unwrap wrappers: {action, data}
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let maybe_data = if args.len() >= 2 {
                                        Some(&args[1])
                                    } else {
                                        args.get(0)
                                    };
                                    let mut positions: Vec<GatewayUserPosition> = Vec::new();
                                    if let Some(dv) = maybe_data {
                                        if let Some(arr) = dv.as_array() {
                                            for item in arr {
                                                let obj = if item.is_object() {
                                                    item.get("data")
                                                        .cloned()
                                                        .unwrap_or(item.clone())
                                                } else {
                                                    item.clone()
                                                };
                                                if let Ok(p) =
                                                    serde_json::from_value::<GatewayUserPosition>(
                                                        obj,
                                                    )
                                                {
                                                    positions.push(p);
                                                }
                                            }
                                        } else if dv.is_object() {
                                            let obj = dv.get("data").cloned().unwrap_or(dv.clone());
                                            if let Ok(p) =
                                                serde_json::from_value::<GatewayUserPosition>(obj)
                                            {
                                                positions.push(p);
                                            }
                                        }
                                    }
                                    if positions.is_empty() {
                                        return;
                                    }
                                    for position in positions.into_iter() {
                                        let instrument_id = match Instrument::from_str(
                                            &parse_px_instrument(position.contract_id.as_str()),
                                        ) {
                                            Ok(i) => i,
                                            Err(e) => {
                                                error!(target: "projectx.ws", "invalid instrument for position: {} ({:?})", position.contract_id, e);
                                                continue;
                                            }
                                        };
                                        self.positions
                                            .insert(instrument_id.clone(), position.clone());
                                        let (net, side) = if position.r#type
                                            == models::PositionType::Short as i32
                                        {
                                            (-Decimal::from(position.size), PositionSide::Short)
                                        } else {
                                            (Decimal::from(position.size), PositionSide::Long)
                                        };
                                        let ts_ns =
                                            DateTime::<Utc>::from_str(&position.creation_timestamp)
                                                .unwrap_or_else(|_| Utc::now());

                                        let average_price = match Decimal::from_f64(position.average_price) {
                                            None => return,
                                            Some(p) => p
                                        };
                                        let pd = PositionDelta {
                                            provider_kind: self.provider_kind,
                                            instrument: instrument_id,
                                            net_qty: net,
                                            average_price,
                                            open_pnl: dec!(0),
                                            time: ts_ns,
                                            side,
                                        };
                                        let batch = PositionsBatch {
                                            topic: tt_types::keys::Topic::Positions,
                                            seq: 0,
                                            positions: vec![pd],
                                        };
                                        if let Err(e) =
                                            self.bus.publish_positions_batch(batch).await
                                        {
                                            error!(target: "projectx.ws", "failed to publish PositionDelta: {:?}", e);
                                        }
                                    }
                                }
                            }
                            "GatewayUserTrade" => {
                                // Extract payloads from 'arguments' and unwrap wrappers: {action, data}
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let maybe_data = if args.len() >= 2 { Some(&args[1]) } else { args.get(0) };
                                    let mut items: Vec<GatewayUserTrade> = Vec::new();
                                    if let Some(dv) = maybe_data {
                                        if let Some(arr) = dv.as_array() {
                                            for item in arr {
                                                let obj = if item.is_object() {
                                                    item.get("data").cloned().unwrap_or(item.clone())
                                                } else { item.clone() };
                                                if let Ok(t) = serde_json::from_value::<GatewayUserTrade>(obj) {
                                                    items.push(t);
                                                }
                                            }
                                        } else if dv.is_object() {
                                            let obj = dv.get("data").cloned().unwrap_or(dv.clone());
                                            if let Ok(t) = serde_json::from_value::<GatewayUserTrade>(obj) {
                                                items.push(t);
                                            }
                                        }
                                    }
                                    if items.is_empty() { return; }

                                    // Map ProjectX trades to wire::Trade
                                    let mut out: Vec<Trade> = Vec::with_capacity(items.len());
                                    for t in items.into_iter() {
                                        let instrument = match Instrument::try_parse_dotted(t.contract_id.as_str()) {
                                            Ok(i) => i,
                                            Err(e) => {
                                                error!(target: "projectx.ws", "invalid instrument for trade: {} ({:?})", t.contract_id, e);
                                                continue;
                                            }
                                        };
                                        let creation_time = DateTime::<Utc>::from_str(&t.creation_timestamp).unwrap_or_else(|_| Utc::now());
                                        let price = Decimal::from_f64(t.price).unwrap_or(dec!(0));
                                        let pnl = Decimal::from_f64(t.profit_and_loss).unwrap_or(dec!(0));
                                        let fees = Decimal::from_f64(t.fees).unwrap_or(dec!(0));
                                        let size = Decimal::from_i64(t.size).unwrap_or(dec!(0));
                                        let side = match t.side { 0 => Side::Buy, 1 => Side::Sell, _ => Side::Buy };
                                        let account_name = if let Some(name) = self.accounts_by_id.get(&t.account_id).map(|e| e.value().clone()) {
                                            AccountName::new(name)
                                        } else {
                                            AccountName::new(t.account_id.to_string())
                                        };
                                        let trade = Trade {
                                            id: ClientOrderId::new(),
                                            provider: self.provider_kind,
                                            account_name,
                                            instrument,
                                            creation_time,
                                            price,
                                            profit_and_loss: pnl,
                                            fees,
                                            side,
                                            size,
                                            voided: t.voided,
                                            order_id: Guid::new_v4(),
                                        };
                                        out.push(trade);
                                    }
                                    if !out.is_empty() {
                                        if let Err(e) = self.bus.publish_closed_trades(out).await {
                                            error!(target: "projectx.ws", "failed to publish ClosedTrades: {:?}", e);
                                        }
                                    }
                                }
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
        self.invoke_user("SubscribePositions", vec![Value::from(account_id)])
            .await
    }

    pub async fn unsubscribe_account_positions(&self, account_id: i64) -> anyhow::Result<()> {
        {
            let mut w = self.user_positions_subs.write().await;
            w.retain(|x| *x != account_id);
        }
        self.invoke_user("UnsubscribePositions", vec![Value::from(account_id)])
            .await
    }

    /// Subscribe to user orders by account ID
    pub async fn subscribe_account_orders(&self, account_id: i64) -> anyhow::Result<()> {
        {
            let mut w = self.user_orders_subs.write().await;
            if !w.contains(&account_id) {
                w.push(account_id);
            } else {
                return Ok(());
            }
        }
        self.invoke_user("SubscribeOrders", vec![Value::from(account_id)])
            .await
    }

    /// Unsubscribe from user orders by account ID
    pub async fn unsubscribe_account_orders(&self, account_id: i64) -> anyhow::Result<()> {
        {
            let mut w = self.user_orders_subs.write().await;
            w.retain(|x| *x != account_id);
        }
        self.invoke_user("UnsubscribeOrders", vec![Value::from(account_id)])
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
