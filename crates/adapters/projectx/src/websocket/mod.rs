// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Realtime client scaffolding for the ProjectX SignalR hubs
//!
//! This module defines data models for the ProjectX websocket events and a minimal
//! stub client API that documents the intended usage and subscription surface.
//! A full SignalR/WebSocket implementation will be added in a follow-up change.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use chrono::Utc;
use dashmap::DashMap;
use futures_util::{Stream, StreamExt};
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info};

use crate::common::{
    consts::PROJECT_X_VENUE,
    futures_helpers::{extract_root, parse_symbol_from_contract_id},
};
// ---------------- Enums ----------------

/// DOM type for depth/DOM events
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[repr(i32)]
pub enum DomType {
    Unknown = 0,
    Ask = 1,
    Bid = 2,
    BestAsk = 3,
    BestBid = 4,
    Trade = 5,
    Reset = 6,
    Low = 7,
    High = 8,
    NewBestBid = 9,
    NewBestAsk = 10,
    Fill = 11,
}

/// Order side for orders and trades
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[repr(i32)]
pub enum OrderSide {
    Bid = 0,
    Ask = 1,
}

/// Order type for orders
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[repr(i32)]
pub enum OrderType {
    Unknown = 0,
    Limit = 1,
    Market = 2,
    StopLimit = 3,
    Stop = 4,
    TrailingStop = 5,
    JoinBid = 6,
    JoinAsk = 7,
}

/// Order status for orders
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[repr(i32)]
pub enum OrderStatus {
    None = 0,
    Open = 1,
    Filled = 2,
    Cancelled = 3,
    Expired = 4,
    Rejected = 5,
    Pending = 6,
}

/// Trade log type for market trades
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[repr(i32)]
pub enum TradeLogType {
    Buy = 0,
    Sell = 1,
}

/// Position type for positions
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[repr(i32)]
pub enum PositionType {
    Undefined = 0,
    Long = 1,
    Short = 2,
}

// ---------------- User hub payloads ----------------

/// User account update payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayUserAccount {
    pub id: i64,
    pub name: String,
    pub balance: f64,
    pub can_trade: bool,
    pub is_visible: bool,
    pub simulated: bool,
}

/// User position update payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayUserPosition {
    pub id: i64,
    pub account_id: i64,
    pub contract_id: String,
    pub creation_timestamp: String,
    pub r#type: i32,
    pub size: i64,
    pub average_price: f64,
}

/// User order update payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayUserOrder {
    pub id: i64,
    pub account_id: i64,
    pub contract_id: String,
    pub symbol_id: String,
    pub creation_timestamp: String,
    pub update_timestamp: String,
    pub status: i32,
    pub r#type: i32,
    pub side: i32,
    pub size: i64,
    #[serde(default)]
    pub limit_price: Option<f64>,
    #[serde(default)]
    pub stop_price: Option<f64>,
    #[serde(default)]
    pub fill_volume: Option<i64>,
    #[serde(default)]
    pub filled_price: Option<f64>,
    #[serde(default)]
    pub custom_tag: Option<String>,
}

/// User trade update payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayUserTrade {
    pub id: i64,
    pub account_id: i64,
    pub contract_id: String,
    pub creation_timestamp: String,
    pub price: f64,
    pub profit_and_loss: f64,
    pub fees: f64,
    pub side: i32,
    pub size: i64,
    pub voided: bool,
    pub order_id: i64,
}

// ---------------- Market hub payloads ----------------

/// Market quote payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayQuote {
    pub symbol: String,
    pub symbol_name: String,
    pub last_price: f64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub change: f64,
    pub change_percent: f64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub volume: i64,
    pub last_updated: String,
    pub timestamp: String,
}

/// Market depth/DOM payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayDepth {
    pub timestamp: String,
    pub r#type: i32,
    pub price: f64,
    pub volume: i64,
    pub current_volume: i64,
    #[serde(default)]
    pub index: Option<i32>,
}

/// Market trade payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayTrade {
    pub symbol_id: String,
    pub price: f64,
    pub timestamp: String,
    pub r#type: i32,
    pub volume: i64,
}

// ---------------- Message bus ----------------

/// Standardized messages emitted by the ProjectX websocket client data stream
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum NautilusWsMessage {
    Data(Vec<Data>),
    Deltas(OrderBookDeltas),
    Instrument(Box<InstrumentAny>),
    AccountUpdate(AccountState),
    OrderRejected(OrderRejected),
    OrderCancelRejected(OrderCancelRejected),
    OrderModifyRejected(OrderModifyRejected),
    ExecutionReports(Vec<ExecutionReport>),
    Error(PxWebSocketError),
    Raw(Value),
    Reconnected,
}

/// A minimal ProjectX websocket error payload for surfacing client/server errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PxWebSocketError {
    pub code: Option<String>,
    pub message: String,
}

/// Execution report wrapper using Nautilus standard report types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "report")]
pub enum ExecutionReport {
    Order(OrderStatusReport),
    Fill(FillReport),
}

// ---------------- Client scaffold ----------------

#[allow(unused)]
/// Realtime client scaffold for ProjectX hubs
///
/// This client documents the API surface and subscriptions for both user and
/// market hubs. It will acquire a JWT token from the HTTP client and use it as
/// a bearer for SignalR access tokens.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.core.nautilus_pyo3.adapters")
)]
pub struct PxWebSocketClient {
    firm: String,
    /// Base URL for the websocket service, e.g. `https://rtc.tradeify.projectx.com`
    pub base_url: String,
    /// Optional bearer token used for hub access
    pub bearer_token: Option<String>,

    // ---- Connection state ----
    /// Indicates whether the user hub is connected
    user_connected: Arc<AtomicBool>,
    /// Indicates whether the market hub is connected
    market_connected: Arc<AtomicBool>,
    /// Stop signal shared across background tasks
    signal: Arc<AtomicBool>,
    /// Monotonic counter for tracking outbound requests/messages
    request_id_counter: Arc<AtomicU64>,

    /// Internal receiver used to feed data to Nautilus via `stream()`
    rx: Option<Arc<UnboundedReceiver<NautilusWsMessage>>>,
    /// Internal sender used by callbacks to emit messages into the stream
    tx: Option<Arc<UnboundedSender<NautilusWsMessage>>>,

    // ---- User hub subscriptions ----
    /// Whether the broadcast "accounts" stream is subscribed
    user_accounts_subscribed: Arc<AtomicBool>,
    /// Tracked account IDs for orders subscription
    user_orders_subs: Arc<DashMap<i64, ()>>,
    /// Tracked account IDs for positions subscription
    user_positions_subs: Arc<DashMap<i64, ()>>,
    /// Tracked account IDs for trades subscription
    user_trades_subs: Arc<DashMap<i64, ()>>,

    // ---- Market hub subscriptions ----
    /// Tracked symbols for quotes subscription
    market_quotes_subs: Arc<DashMap<String, ()>>,
    /// Tracked symbols for depth subscription
    market_depth_subs: Arc<DashMap<String, ()>>,
    /// Tracked symbols for trades subscription
    market_trades_subs: Arc<DashMap<String, ()>>,

    /// Tracked contract IDs for quotes subscription
    market_contract_quotes_subs: Arc<DashMap<String, ()>>,
    /// Tracked contract IDs for depth subscription
    market_contract_depth_subs: Arc<DashMap<String, ()>>,
    /// Tracked contract IDs for trades subscription
    market_contract_trades_subs: Arc<DashMap<String, ()>>,

    market_contract_bars_subs: Arc<DashMap<String, ()>>,

    // ---- Live connections ----
    user_ws: Arc<tokio::sync::Mutex<Option<WebSocketClient>>>,
    user_reader_task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,
    market_ws: Arc<tokio::sync::Mutex<Option<WebSocketClient>>>,
    market_reader_task: Arc<tokio::sync::Mutex<Option<JoinHandle<()>>>>,

    // ---- Order book state ----
    /// Persisted 10-level books per instrument (updated in-place on depth events)
    market_books: Arc<DashMap<nautilus_model::identifiers::InstrumentId, Box<OrderBookDepth10>>>,
    /// Mapping from contract_id -> instrument_id to facilitate cleanup on unsubscribe
    contract_to_instrument: Arc<DashMap<String, nautilus_model::identifiers::InstrumentId>>,

    precisions: Arc<DashMap<String, u8>>,

    // ---- Positions ----
    positions: Arc<DashMap<InstrumentId, GatewayUserPosition>>,

    // ---- Orders ----
    pending_orders: Arc<DashMap<InstrumentId, GatewayUserOrder>>,

    trades: Arc<DashMap<InstrumentId, Vec<GatewayUserTrade>>>,
}

impl PxWebSocketClient {
    /// Create a new websocket client with optional bearer token
    pub fn new(base_url: impl Into<String>, bearer_token: Option<String>, firm: String) -> Self {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<NautilusWsMessage>();
        Self {
            firm,
            base_url: base_url.into(),
            bearer_token,
            user_connected: Arc::new(AtomicBool::new(false)),
            market_connected: Arc::new(AtomicBool::new(false)),
            signal: Arc::new(AtomicBool::new(false)),
            request_id_counter: Arc::new(AtomicU64::new(1)),
            rx: Some(Arc::new(rx)),
            tx: Some(Arc::new(tx)),
            user_accounts_subscribed: Arc::new(AtomicBool::new(false)),
            user_orders_subs: Arc::new(DashMap::new()),
            user_positions_subs: Arc::new(DashMap::new()),
            user_trades_subs: Arc::new(DashMap::new()),
            market_quotes_subs: Arc::new(DashMap::new()),
            market_depth_subs: Arc::new(DashMap::new()),
            market_trades_subs: Arc::new(DashMap::new()),
            market_contract_quotes_subs: Arc::new(DashMap::new()),
            market_contract_depth_subs: Arc::new(DashMap::new()),
            market_contract_trades_subs: Arc::new(DashMap::new()),
            market_contract_bars_subs: Arc::new(DashMap::new()),
            user_ws: Arc::new(tokio::sync::Mutex::new(None)),
            user_reader_task: Arc::new(tokio::sync::Mutex::new(None)),
            market_ws: Arc::new(tokio::sync::Mutex::new(None)),
            market_reader_task: Arc::new(tokio::sync::Mutex::new(None)),
            market_books: Arc::new(DashMap::new()),
            contract_to_instrument: Arc::new(DashMap::new()),
            precisions: Arc::new(DashMap::new()),
            positions: Arc::new(Default::default()),
            pending_orders: Arc::new(Default::default()),
            trades: Arc::new(DashMap::new()),
        }
    }

    /// Returns the full URL for the user hub (ws/wss scheme)
    pub fn user_hub_url(&self) -> String {
        let mut base = self.base_url.trim_end_matches('/').to_string();
        // Ensure websocket scheme for tungstenite
        if base.starts_with("https://") {
            base = base.replacen("https://", "wss://", 1);
        } else if base.starts_with("http://") {
            base = base.replacen("http://", "ws://", 1);
        }
        if let Some(token) = &self.bearer_token {
            format!("{}/hubs/user?access_token={}", base, token)
        } else {
            format!("{}/hubs/user", base)
        }
    }

    /// Returns the full URL for the market hub (ws/wss scheme)
    pub fn market_hub_url(&self) -> String {
        let mut base = self.base_url.trim_end_matches('/').to_string();
        // Ensure websocket scheme for tungstenite
        if base.starts_with("https://") {
            base = base.replacen("https://", "wss://", 1);
        } else if base.starts_with("http://") {
            base = base.replacen("http://", "ws://", 1);
        }
        if let Some(token) = &self.bearer_token {
            format!("{}/hubs/market?access_token={}", base, token)
        } else {
            format!("{}/hubs/market", base)
        }
    }

    pub fn kill(&self) {}

    /// Connect to the user hub
    pub async fn connect_user(&self) -> anyhow::Result<()> {
        let url = self.user_hub_url();
        let config = WebSocketConfig {
            url,
            headers: vec![(
                "User-Agent".to_string(),
                "nautilus-projectx/1.0".to_string(),
            )],
            message_handler: None,
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
                                this.handle_signalr_message(val);
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
                                    this.handle_signalr_message(val);
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
                        this.emit_error(PxWebSocketError {
                            code: Some("reader".into()),
                            message: format!("user reader error: {err}"),
                        });
                        break;
                    }
                }
            }
        });
        {
            let mut g = self.user_reader_task.lock().await;
            *g = Some(task);
        }

        self.user_connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Connect to the market hub
    pub async fn connect_market(&self) -> anyhow::Result<()> {
        let url = self.market_hub_url();
        let config = WebSocketConfig {
            url,
            headers: vec![(
                "User-Agent".to_string(),
                "nautilus-projectx/1.0".to_string(),
            )],
            message_handler: None,
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
                                this.handle_signalr_message(val);
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
                                    this.handle_signalr_message(val);
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
                        this.emit_error(PxWebSocketError {
                            code: Some("reader".into()),
                            message: format!("market reader error: {err}"),
                        });
                        break;
                    }
                }
            }
        });
        {
            let mut g = self.market_reader_task.lock().await;
            *g = Some(task);
        }

        self.market_connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Send a message into the internal stream if the sender is available
    fn send(&self, msg: NautilusWsMessage) {
        if let Some(tx) = &self.tx {
            let _ = tx.send(msg);
        }
    }

    /// Emit a reconnection signal to downstream consumers
    pub fn emit_reconnected(&self) {
        self.send(NautilusWsMessage::Reconnected);
    }

    /// Emit standardized market data payloads
    pub fn emit_data(&self, data: Vec<Data>) {
        self.send(NautilusWsMessage::Data(data));
    }

    /// Emit standardized order book deltas
    pub fn emit_deltas(&self, deltas: OrderBookDeltas) {
        self.send(NautilusWsMessage::Deltas(deltas));
    }

    /// Emit a discovered/updated instrument
    pub fn emit_instrument(&self, instrument: InstrumentAny) {
        self.send(NautilusWsMessage::Instrument(Box::new(instrument)));
    }

    /// Emit a standardized account update
    pub fn emit_account_update(&self, state: AccountState) {
        self.send(NautilusWsMessage::AccountUpdate(state));
    }

    /// Emit standardized rejection events
    pub fn emit_order_rejected(&self, evt: OrderRejected) {
        self.send(NautilusWsMessage::OrderRejected(evt));
    }
    pub fn emit_order_cancel_rejected(&self, evt: OrderCancelRejected) {
        self.send(NautilusWsMessage::OrderCancelRejected(evt));
    }
    pub fn emit_order_modify_rejected(&self, evt: OrderModifyRejected) {
        self.send(NautilusWsMessage::OrderModifyRejected(evt));
    }

    /// Emit execution reports (status/fill)
    pub fn emit_execution_reports(&self, reports: Vec<ExecutionReport>) {
        self.send(NautilusWsMessage::ExecutionReports(reports));
    }

    /// Emit an error or raw payload
    pub fn emit_error(&self, error: PxWebSocketError) {
        self.send(NautilusWsMessage::Error(error));
    }
    pub fn emit_raw(&self, value: Value) {
        self.send(NautilusWsMessage::Raw(value));
    }

    /// Clear all persisted order books and contract mapping (e.g., on disconnect)
    pub fn clear_books(&self) {
        self.market_books.clear();
        self.contract_to_instrument.clear();
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

    fn handle_signalr_message(&self, val: Value) {
        // Basic handling: surface raw messages and emit reconnection event on ping/completion as needed
        // We expect market hub to send Invocation messages with targets like GatewayQuote, GatewayTrade, GatewayDepth
        if let Some(t) = val.get("type").and_then(|v| v.as_i64()) {
            match t {
                1 => {
                    // Invocation todo finish this instead of raw
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
                                        serde_json::from_value::<GatewayQuote>(data_val.clone())
                                    {
                                        use nautilus_core::{
                                            UnixNanos, datetime::iso8601_to_unix_nanos,
                                        };
                                        use nautilus_model::{
                                            identifiers::{InstrumentId, Symbol},
                                            types::{Price, Quantity},
                                        };

                                        use crate::common::consts::PROJECT_X_VENUE;
                                        // Build instrument ID from symbol + venue
                                        if let Ok(symbol) =
                                            Symbol::new_checked(px_quote.symbol.clone())
                                        {
                                            let instrument_id =
                                                InstrumentId::new(symbol, *PROJECT_X_VENUE);
                                            let prec = *self
                                                .precisions
                                                .entry(extract_root(&px_quote.symbol))
                                                .or_insert_with(|| {
                                                    crate::common::root_specs::get_precision(
                                                        &extract_root(&px_quote.symbol),
                                                    )
                                                    .unwrap_or(6)
                                                });
                                            // Prices: use bestBid/bestAsk; ProjectX quote payload does not include bid/ask sizes -> default to 0
                                            let bid_price = Price::new(px_quote.best_bid, prec);
                                            let ask_price = Price::new(px_quote.best_ask, prec);
                                            let bid_size = Quantity::new(0.0, 0);
                                            let ask_size = Quantity::new(0.0, 0);
                                            // Timestamps: prefer timestamp, fallback to lastUpdated; default to 0 on parse error
                                            let ts_evt =
                                                iso8601_to_unix_nanos(px_quote.timestamp.clone())
                                                    .or_else(|_| {
                                                        iso8601_to_unix_nanos(
                                                            px_quote.last_updated.clone(),
                                                        )
                                                    })
                                                    .unwrap_or_else(|_| UnixNanos::from(0));
                                            let ts_init = UnixNanos::from(
                                                std::time::SystemTime::now()
                                                    .duration_since(std::time::UNIX_EPOCH)
                                                    .map(|d| d.as_nanos() as u64)
                                                    .unwrap_or(0),
                                            );
                                            let quote = QuoteTick {
                                                instrument_id,
                                                bid_price,
                                                ask_price,
                                                bid_size,
                                                ask_size,
                                                ts_event: ts_evt,
                                                ts_init,
                                            };
                                            info!("{}", quote);
                                            self.emit_data(vec![Data::from(quote)]);
                                        } else {
                                            // If symbol invalid for InstrumentId, emit raw for debugging
                                            info!(target: "projectx.ws", "GatewayQuote (invalid symbol): {}", data_val);
                                            self.emit_raw(val);
                                        }
                                    } else {
                                        // If deserialization fails, emit raw for debugging
                                        info!(target: "projectx.ws", "GatewayQuote (unparsed): {}", data_val);
                                        self.emit_raw(val);
                                    }
                                } else {
                                    self.emit_raw(val);
                                }
                            }
                            "GatewayTrade" => {
                                // Extract args and deserialize payload
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let data_val = if args.len() >= 2 {
                                        &args[1]
                                    } else {
                                        args.get(0).unwrap_or(&Value::Null)
                                    };
                                    match serde_json::from_value::<GatewayTrade>(data_val.clone()) {
                                        Ok(px_trade) => {
                                            use nautilus_core::{
                                                UnixNanos, datetime::iso8601_to_unix_nanos,
                                            };
                                            use nautilus_model::{
                                                identifiers::{InstrumentId, Symbol},
                                                types::{Price, Quantity},
                                            };

                                            use crate::common::consts::PROJECT_X_VENUE;

                                            // Build instrument id from symbolId
                                            if let Ok(symbol) =
                                                Symbol::new_checked(px_trade.symbol_id.clone())
                                            {
                                                let instrument_id =
                                                    InstrumentId::new(symbol, *PROJECT_X_VENUE);
                                                // Map types
                                                let prec = *self
                                                    .precisions
                                                    .entry(extract_root(&px_trade.symbol_id))
                                                    .or_insert_with(|| {
                                                        crate::common::root_specs::get_precision(
                                                            &extract_root(&px_trade.symbol_id),
                                                        )
                                                        .unwrap_or(6)
                                                    });
                                                let price = Price::new(px_trade.price, prec);
                                                let size =
                                                    Quantity::non_zero(px_trade.volume as f64, 0);
                                                let aggressor = match px_trade.r#type {
                                                    0 => AggressorSide::Buyer,
                                                    1 => AggressorSide::Seller,
                                                    _ => AggressorSide::NoAggressor,
                                                };
                                                // Derive a trade ID (ProjectX trade stream lacks one)
                                                let tid_str = format!(
                                                    "{}-{}-{}",
                                                    px_trade.symbol_id,
                                                    px_trade.timestamp,
                                                    px_trade.price
                                                );
                                                let trade_id = TradeId::new(tid_str);

                                                let ts_evt: UnixNanos = iso8601_to_unix_nanos(
                                                    px_trade.timestamp.clone(),
                                                )
                                                .unwrap_or(UnixNanos::from(0));
                                                let ts_init = UnixNanos::from(
                                                    std::time::SystemTime::now()
                                                        .duration_since(std::time::UNIX_EPOCH)
                                                        .map(|d| d.as_nanos() as u64)
                                                        .unwrap_or(0),
                                                );

                                                let tick = TradeTick {
                                                    instrument_id,
                                                    price,
                                                    size,
                                                    aggressor_side: aggressor,
                                                    trade_id,
                                                    ts_event: ts_evt,
                                                    ts_init,
                                                };
                                                info!("{}", tick);
                                                self.emit_data(vec![Data::Trade(tick)]);
                                            } else {
                                                info!(target: "projectx.ws", "GatewayTrade (invalid symbol): {}", data_val);
                                                self.emit_raw(val);
                                            }
                                        }
                                        Err(_) => {
                                            info!(target: "projectx.ws", "GatewayTrade (unparsed): {}", data_val);
                                            self.emit_raw(val);
                                        }
                                    }
                                } else {
                                    self.emit_raw(val);
                                }
                            }
                            "GatewayDepth" => {
                                // Extract args and handle payload which may be an array of up to 10 levels
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    let (contract_id_opt, data_val) = if args.len() >= 2 {
                                        (args.get(0).and_then(|v| v.as_str()), &args[1])
                                    } else {
                                        (None, args.get(0).unwrap_or(&Value::Null))
                                    };

                                    use nautilus_core::{
                                        UnixNanos, datetime::iso8601_to_unix_nanos,
                                    };
                                    use nautilus_model::{
                                        identifiers::{InstrumentId, Symbol},
                                        types::{Price, Quantity},
                                    };

                                    use crate::common::consts::PROJECT_X_VENUE;

                                    // Derive symbol (ROOT.CODE) from ProjectX contract ID using common helper
                                    let symbol_id = if let Some(cid) = contract_id_opt {
                                        crate::common::futures_helpers::parse_symbol_from_contract_id(cid).unwrap_or_default()
                                    } else {
                                        String::new()
                                    };

                                    if let Ok(symbol) = Symbol::new_checked(symbol_id.clone()) {
                                        let instrument_id =
                                            InstrumentId::new(symbol, *PROJECT_X_VENUE);
                                        // Remember mapping for cleanup on unsubscribe
                                        if let Some(cid) = contract_id_opt {
                                            self.contract_to_instrument
                                                .insert(cid.to_string(), instrument_id);
                                        }
                                        // Resolve price precision for this symbol
                                        let prec = *self
                                            .precisions
                                            .entry(extract_root(&symbol_id))
                                            .or_insert_with(|| {
                                                crate::common::root_specs::get_precision(
                                                    &extract_root(&symbol_id),
                                                )
                                                .unwrap_or(6)
                                            });

                                        // Get or create persistent book
                                        let mut entry = self
                                            .market_books
                                            .entry(instrument_id)
                                            .or_insert_with(|| {
                                                let empty = BookOrder::default();
                                                let bids = [empty; 10];
                                                let asks = [empty; 10];
                                                let bid_counts = [0u32; 10];
                                                let ask_counts = [0u32; 10];
                                                Box::new(OrderBookDepth10::new(
                                                    instrument_id,
                                                    bids,
                                                    asks,
                                                    bid_counts,
                                                    ask_counts,
                                                    0,
                                                    0,
                                                    UnixNanos::from(0),
                                                    UnixNanos::from(0),
                                                ))
                                            });
                                        let mut_book = entry.value_mut();

                                        let mut ts_evt: UnixNanos = UnixNanos::from(0);

                                        if let Some(levels) = data_val.as_array() {
                                            // Reset counts and overwrite levels per side in the order received
                                            let mut b_idx: usize = 0;
                                            let mut a_idx: usize = 0;
                                            mut_book.bid_counts = [0u32; 10];
                                            mut_book.ask_counts = [0u32; 10];
                                            mut_book.bids = [BookOrder::default(); 10];
                                            mut_book.asks = [BookOrder::default(); 10];

                                            // Build deltas: CLEAR + ADDs for snapshot arrays
                                            let mut deltas_vec: Vec<OrderBookDelta> = Vec::new();
                                            let ts_init_now = UnixNanos::from(
                                                std::time::SystemTime::now()
                                                    .duration_since(std::time::UNIX_EPOCH)
                                                    .map(|d| d.as_nanos() as u64)
                                                    .unwrap_or(0),
                                            );

                                            for item in levels.iter() {
                                                if b_idx >= 10 && a_idx >= 10 {
                                                    break;
                                                }
                                                if let Ok(depth) =
                                                    serde_json::from_value::<GatewayDepth>(
                                                        item.clone(),
                                                    )
                                                {
                                                    if let Ok(t) = iso8601_to_unix_nanos(
                                                        depth.timestamp.clone(),
                                                    ) {
                                                        ts_evt = t;
                                                    }
                                                    let price = Price::new(depth.price, prec);
                                                    let size =
                                                        Quantity::new(depth.volume as f64, 0);
                                                    // Skip zero-size levels to avoid invalid deltas and keep book clean
                                                    if size.raw == 0 {
                                                        continue;
                                                    }
                                                    match depth.r#type {
                                                        x if x == DomType::Bid as i32 => {
                                                            if b_idx < 10 {
                                                                let bo = BookOrder::new(
                                                                    NautilusOrderSide::Buy,
                                                                    price,
                                                                    size,
                                                                    0,
                                                                );
                                                                info!("{}", bo);
                                                                mut_book.bids[b_idx] = bo;
                                                                mut_book.bid_counts[b_idx] = 1;
                                                                // Add delta for this level
                                                                deltas_vec.push(
                                                                    OrderBookDelta::new(
                                                                        instrument_id,
                                                                        BookAction::Add,
                                                                        bo,
                                                                        0,
                                                                        0,
                                                                        ts_evt,
                                                                        ts_init_now,
                                                                    ),
                                                                );
                                                                b_idx += 1;
                                                            }
                                                        }
                                                        x if x == DomType::Ask as i32 => {
                                                            if a_idx < 10 {
                                                                let bo = BookOrder::new(
                                                                    NautilusOrderSide::Sell,
                                                                    price,
                                                                    size,
                                                                    0,
                                                                );
                                                                info!("{}", bo);
                                                                mut_book.asks[a_idx] = bo;
                                                                mut_book.ask_counts[a_idx] = 1;
                                                                // Add delta for this level
                                                                deltas_vec.push(
                                                                    OrderBookDelta::new(
                                                                        instrument_id,
                                                                        BookAction::Add,
                                                                        bo,
                                                                        0,
                                                                        0,
                                                                        ts_evt,
                                                                        ts_init_now,
                                                                    ),
                                                                );
                                                                a_idx += 1;
                                                            }
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                            mut_book.ts_event = ts_evt;
                                            mut_book.ts_init = ts_init_now;

                                            // Prepend a CLEAR delta and emit the batch if we have at least 1 level
                                            if !deltas_vec.is_empty() {
                                                let mut all =
                                                    Vec::with_capacity(deltas_vec.len() + 1);
                                                all.push(OrderBookDelta::clear(
                                                    instrument_id,
                                                    0,
                                                    ts_evt,
                                                    ts_init_now,
                                                ));
                                                all.extend(deltas_vec);
                                                let deltas =
                                                    OrderBookDeltas::new(instrument_id, all);
                                                self.emit_deltas(deltas);
                                            }
                                        } else {
                                            // Single level update: update book based on DomType
                                            if let Ok(px_depth) =
                                                serde_json::from_value::<GatewayDepth>(
                                                    data_val.clone(),
                                                )
                                            {
                                                // Handle reset: clear the stored book
                                                if px_depth.r#type == DomType::Reset as i32 {
                                                    mut_book.bids = [BookOrder::default(); 10];
                                                    mut_book.asks = [BookOrder::default(); 10];
                                                    mut_book.bid_counts = [0u32; 10];
                                                    mut_book.ask_counts = [0u32; 10];
                                                    mut_book.ts_event = iso8601_to_unix_nanos(
                                                        px_depth.timestamp.clone(),
                                                    )
                                                    .unwrap_or(UnixNanos::from(0));
                                                    mut_book.ts_init = UnixNanos::from(
                                                        std::time::SystemTime::now()
                                                            .duration_since(std::time::UNIX_EPOCH)
                                                            .map(|d| d.as_nanos() as u64)
                                                            .unwrap_or(0),
                                                    );
                                                    info!(target: "projectx.ws", "GatewayDepth RESET: contract={:?}", contract_id_opt);
                                                } else {
                                                    let price = Price::new(px_depth.price, prec);
                                                    let size =
                                                        Quantity::new(px_depth.volume as f64, 0);
                                                    // Track whether we should emit an additional Quote or Trade alongside the book snapshot
                                                    let mut emit_quote = false;
                                                    let mut emit_trade = false;
                                                    let mut idx_i32 = match px_depth.index {
                                                        None => {
                                                            warn!("No depth for level update");
                                                            return;
                                                        }
                                                        Some(d) => d,
                                                    };
                                                    if idx_i32 < 0 {
                                                        idx_i32 = 0;
                                                    }
                                                    let mut idx: usize = idx_i32 as usize;
                                                    if idx > 9 {
                                                        idx = 9;
                                                    }
                                                    match px_depth.r#type {
                                                        x if x == DomType::Bid as i32
                                                            || x == DomType::BestBid as i32 =>
                                                        {
                                                            if size.raw > 0 {
                                                                let bo = BookOrder::new(
                                                                    NautilusOrderSide::Buy,
                                                                    price,
                                                                    size,
                                                                    0,
                                                                );
                                                                info!("{}", bo);
                                                                mut_book.bids[idx] = bo;
                                                                mut_book.bid_counts[idx] = 1;
                                                            } else {
                                                                // Remove the level entirely
                                                                mut_book.bids[idx] =
                                                                    BookOrder::default();
                                                                mut_book.bid_counts[idx] = 0;
                                                            }
                                                            emit_quote = idx == 0
                                                                || x == DomType::BestBid as i32; // best bid update implies quote change
                                                        }
                                                        x if x == DomType::Ask as i32
                                                            || x == DomType::BestAsk as i32 =>
                                                        {
                                                            if size.raw > 0 {
                                                                let bo = BookOrder::new(
                                                                    NautilusOrderSide::Sell,
                                                                    price,
                                                                    size,
                                                                    0,
                                                                );
                                                                info!("{}", bo);
                                                                mut_book.asks[idx] = bo;
                                                                mut_book.ask_counts[idx] = 1;
                                                            } else {
                                                                mut_book.asks[idx] =
                                                                    BookOrder::default();
                                                                mut_book.ask_counts[idx] = 0;
                                                            }
                                                            emit_quote = idx == 0
                                                                || x == DomType::BestAsk as i32; // best ask update implies quote change
                                                        }
                                                        x if x == DomType::NewBestBid as i32 => {
                                                            if size.raw > 0 {
                                                                let bo = BookOrder::new(
                                                                    NautilusOrderSide::Buy,
                                                                    price,
                                                                    size,
                                                                    0,
                                                                );
                                                                info!("{}", bo);
                                                                mut_book.bids[idx] = bo;
                                                                mut_book.bid_counts[idx] = 1;
                                                            } else {
                                                                mut_book.bids[idx] =
                                                                    BookOrder::default();
                                                                mut_book.bid_counts[idx] = 0;
                                                            }
                                                            emit_quote = true;
                                                        }
                                                        x if x == DomType::NewBestAsk as i32 => {
                                                            if size.raw > 0 {
                                                                let bo = BookOrder::new(
                                                                    NautilusOrderSide::Sell,
                                                                    price,
                                                                    size,
                                                                    0,
                                                                );
                                                                info!("{}", bo);
                                                                mut_book.asks[idx] = bo;
                                                                mut_book.ask_counts[idx] = 1;
                                                            } else {
                                                                mut_book.asks[idx] =
                                                                    BookOrder::default();
                                                                mut_book.ask_counts[idx] = 0;
                                                            }
                                                            emit_quote = true;
                                                        }
                                                        x if x == DomType::Trade as i32
                                                            || x == DomType::Fill as i32 =>
                                                        {
                                                            // Trade-related DOM event: do not modify book sides here (server may send separate book updates)
                                                            emit_trade = true;
                                                        }
                                                        _ => {
                                                            // Other types (Low/High/Reset handled above) ignored for book construction
                                                        }
                                                    }
                                                    let ts_evt_local = iso8601_to_unix_nanos(
                                                        px_depth.timestamp.clone(),
                                                    )
                                                    .unwrap_or(UnixNanos::from(0));
                                                    mut_book.ts_event = ts_evt_local;
                                                    mut_book.ts_init = UnixNanos::from(
                                                        std::time::SystemTime::now()
                                                            .duration_since(std::time::UNIX_EPOCH)
                                                            .map(|d| d.as_nanos() as u64)
                                                            .unwrap_or(0),
                                                    );

                                                    // Emit a single delta for this level update (skip Trade/Fill)
                                                    let side_for_delta = match px_depth.r#type {
                                                        x if x == DomType::Bid as i32
                                                            || x == DomType::BestBid as i32
                                                            || x == DomType::NewBestBid as i32 =>
                                                        {
                                                            Some(NautilusOrderSide::Buy)
                                                        }
                                                        x if x == DomType::Ask as i32
                                                            || x == DomType::BestAsk as i32
                                                            || x == DomType::NewBestAsk as i32 =>
                                                        {
                                                            Some(NautilusOrderSide::Sell)
                                                        }
                                                        _ => None,
                                                    };
                                                    if let Some(delta_side) = side_for_delta {
                                                        // For Delete actions, OrderBookDelta requires a positive size. Use the existing
                                                        // book size at the level if available; otherwise skip emitting the delta.
                                                        let (action, size_for_delta) =
                                                            if size.raw == 0 {
                                                                let existing = match delta_side {
                                                                    NautilusOrderSide::Buy => {
                                                                        mut_book.bids[idx].size
                                                                    }
                                                                    NautilusOrderSide::Sell => {
                                                                        mut_book.asks[idx].size
                                                                    }
                                                                    _ => Quantity::new(0.0, 0),
                                                                };
                                                                if existing.raw > 0 {
                                                                    (BookAction::Delete, existing)
                                                                } else {
                                                                    (BookAction::Delete, size)
                                                                }
                                                            } else {
                                                                (BookAction::Update, size)
                                                            };
                                                        // Only emit if we have a positive size (model enforces > 0)
                                                        if size_for_delta.raw > 0 {
                                                            let bo = BookOrder::new(
                                                                delta_side,
                                                                price,
                                                                size_for_delta,
                                                                0,
                                                            );
                                                            let delta = OrderBookDelta::new(
                                                                instrument_id,
                                                                action,
                                                                bo,
                                                                0,
                                                                0,
                                                                ts_evt_local,
                                                                mut_book.ts_init,
                                                            );
                                                            let deltas = OrderBookDeltas::new(
                                                                instrument_id,
                                                                vec![delta],
                                                            );
                                                            self.emit_deltas(deltas);
                                                        }
                                                    }

                                                    let _display_vol = if px_depth.r#type
                                                        == DomType::Trade as i32
                                                        || px_depth.r#type == DomType::Fill as i32
                                                    {
                                                        px_depth.current_volume
                                                    } else {
                                                        px_depth.volume
                                                    };

                                                    // Emit an accompanying Quote when best bid/ask changed //todo, not sure if we should do this, +decreases need for # feeds, -could be handled wrong .
                                                    if emit_quote {
                                                        // Only emit a Quote when both best bid and best ask are present
                                                        if mut_book.bid_counts[0] > 0
                                                            && mut_book.ask_counts[0] > 0
                                                        {
                                                            let bid_price = mut_book.bids[0].price;
                                                            let ask_price = mut_book.asks[0].price;
                                                            let bid_size = mut_book.bids[0].size;
                                                            let ask_size = mut_book.asks[0].size;
                                                            let quote = QuoteTick {
                                                                instrument_id,
                                                                bid_price,
                                                                ask_price,
                                                                bid_size,
                                                                ask_size,
                                                                ts_event: ts_evt_local,
                                                                ts_init: mut_book.ts_init,
                                                            };
                                                            info!("Quote {}", quote);
                                                            self.emit_data(vec![Data::Quote(
                                                                quote,
                                                            )]);
                                                        } else {
                                                            info!(target: "projectx.ws", "Skipping Quote emit: missing side (bid_count={} ask_count={})", mut_book.bid_counts[0], mut_book.ask_counts[0]);
                                                        }
                                                    }

                                                    // Emit a Trade when trade-related DOM event received //todo, not sure if we should do this, +decreases need for # feeds, -could be handled wrong .
                                                    if emit_trade {
                                                        use nautilus_model::identifiers::TradeId;
                                                        let trade_price =
                                                            Price::new(px_depth.price, prec);
                                                        // Prefer currentVolume for per-trade size if available, else fall back to volume
                                                        let trade_size = if px_depth.current_volume
                                                            > 0
                                                        {
                                                            Quantity::new(
                                                                px_depth.current_volume as f64,
                                                                0,
                                                            )
                                                        } else {
                                                            Quantity::new(px_depth.volume as f64, 0)
                                                        };
                                                        let trade_id = TradeId::new("".to_string());
                                                        // Determine aggressor based on BBO vs trade price
                                                        let aggressor_side = {
                                                            let has_bid =
                                                                mut_book.bid_counts[0] > 0;
                                                            let has_ask =
                                                                mut_book.ask_counts[0] > 0;
                                                            if has_bid || has_ask {
                                                                let tr = trade_price.raw;
                                                                // Prefer ask comparison first (lifting the offer)
                                                                if has_ask
                                                                    && tr
                                                                        >= mut_book.asks[0]
                                                                            .price
                                                                            .raw
                                                                {
                                                                    AggressorSide::Buyer
                                                                } else if has_bid
                                                                    && tr
                                                                        <= mut_book.bids[0]
                                                                            .price
                                                                            .raw
                                                                {
                                                                    AggressorSide::Seller
                                                                } else {
                                                                    AggressorSide::NoAggressor
                                                                }
                                                            } else {
                                                                AggressorSide::NoAggressor
                                                            }
                                                        };
                                                        let tick = TradeTick {
                                                            instrument_id,
                                                            price: trade_price,
                                                            size: trade_size,
                                                            aggressor_side,
                                                            trade_id,
                                                            ts_event: ts_evt_local,
                                                            ts_init: mut_book.ts_init,
                                                        };
                                                        info!("Tick {}", tick);
                                                        self.emit_data(vec![Data::Trade(tick)]);
                                                    }
                                                }
                                            } else {
                                                info!(target: "projectx.ws", "GatewayDepth (unparsed): {}", data_val);
                                                self.emit_raw(val.clone());
                                                return;
                                            }
                                        }
                                        // Emit a clone of the updated book
                                        self.emit_data(vec![Data::Depth10(Box::new(
                                            (**mut_book).clone(),
                                        ))]);
                                    } else {
                                        info!(target: "projectx.ws", "GatewayDepth (unknown symbol from contract): {:?} payload={}", contract_id_opt, data_val);
                                        self.emit_raw(val);
                                    }
                                } else {
                                    self.emit_raw(val);
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
                                let id = match AccountId::new_checked(format!(
                                    "{}-{}",
                                    &self.firm, account.id
                                )) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        error!("error parsing AccountId: {}", e);
                                        return;
                                    }
                                };
                                let ts = UnixNanos::new(
                                    Utc::now().timestamp_nanos_opt().unwrap() as u64
                                );
                                //todo, get balances from positions??
                                let state = AccountState::new(
                                    id,
                                    AccountType::Margin,
                                    vec![],
                                    vec![],
                                    false,
                                    Default::default(),
                                    ts,
                                    ts,
                                    Some(Currency::USD()),
                                );
                                self.emit_account_update(state);
                            }
                            "GatewayUserOrder" => {
                                use nautilus_core::{UnixNanos, datetime::iso8601_to_unix_nanos};
                                use nautilus_model::{
                                    enums::{
                                        OrderSide as NautOrderSide, OrderStatus as NautOrderStatus,
                                        OrderType as NautOrderType, TimeInForce,
                                    },
                                    identifiers::VenueOrderId,
                                    types::{Price, Quantity},
                                };
                                let order =
                                    match serde_json::from_value::<GatewayUserOrder>(val.clone()) {
                                        Ok(o) => o,
                                        Err(e) => {
                                            error!("error parsing GatewayUserOrder: {}", e);
                                            return;
                                        }
                                    };
                                // Build identifiers
                                let account_id = match AccountId::new_checked(format!(
                                    "{}-{}",
                                    &self.firm, order.account_id
                                )) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        error!("error building AccountId: {}", e);
                                        return;
                                    }
                                };
                                let symbol_str =
                                    match parse_symbol_from_contract_id(&order.contract_id) {
                                        Some(s) => s,
                                        None => {
                                            error!(
                                                "error parsing symbol from contract: {}",
                                                order.contract_id
                                            );
                                            return;
                                        }
                                    };
                                let instrument_id = InstrumentId::new(
                                    Symbol::new(symbol_str.clone()),
                                    *PROJECT_X_VENUE,
                                );
                                let venue_order_id = VenueOrderId::new(order.id.to_string());

                                // Enum mappings
                                let side = if order.side == OrderSide::Bid as i32 {
                                    NautOrderSide::Buy
                                } else {
                                    NautOrderSide::Sell
                                };
                                let otype = match order.r#type {
                                    x if x == OrderType::Limit as i32 => NautOrderType::Limit,
                                    x if x == OrderType::Market as i32 => NautOrderType::Market,
                                    x if x == OrderType::Stop as i32 => NautOrderType::StopMarket,
                                    x if x == OrderType::StopLimit as i32 => {
                                        NautOrderType::StopLimit
                                    }
                                    _ => NautOrderType::Limit,
                                };
                                let status = match order.status {
                                    x if x == OrderStatus::Open as i32 => NautOrderStatus::Accepted,
                                    x if x == OrderStatus::Filled as i32 => NautOrderStatus::Filled,
                                    x if x == OrderStatus::Cancelled as i32 => {
                                        NautOrderStatus::Canceled
                                    }
                                    x if x == OrderStatus::Expired as i32 => {
                                        NautOrderStatus::Expired
                                    }
                                    x if x == OrderStatus::Rejected as i32 => {
                                        NautOrderStatus::Rejected
                                    }
                                    x if x == OrderStatus::Pending as i32 => {
                                        NautOrderStatus::Submitted
                                    }
                                    _ => NautOrderStatus::Initialized,
                                };
                                let tif = TimeInForce::Gtc; // ProjectX does not provide TIF in payload

                                // Quantities and prices
                                let qty = Quantity::new(order.size as f64, 0);
                                let filled_qty =
                                    Quantity::new(order.fill_volume.unwrap_or(0) as f64, 0);
                                let prec = *self
                                    .precisions
                                    .entry(extract_root(&symbol_str))
                                    .or_insert_with(|| {
                                        crate::common::root_specs::get_precision(&extract_root(
                                            &symbol_str,
                                        ))
                                        .unwrap_or(6)
                                    });
                                let _limit_px = order.limit_price.map(|p| Price::new(p, prec));
                                let _stop_px = order.stop_price.map(|p| Price::new(p, prec));

                                // Timestamps
                                let ts_accepted =
                                    iso8601_to_unix_nanos(order.creation_timestamp.clone())
                                        .unwrap_or(UnixNanos::from(0));
                                let ts_last = iso8601_to_unix_nanos(order.update_timestamp.clone())
                                    .unwrap_or(ts_accepted);
                                let ts_init = UnixNanos::from(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .map(|d| d.as_nanos() as u64)
                                        .unwrap_or(0),
                                );

                                let mut report = OrderStatusReport::new(
                                    account_id,
                                    instrument_id,
                                    None,
                                    venue_order_id,
                                    side,
                                    otype,
                                    tif,
                                    status,
                                    qty,
                                    filled_qty,
                                    ts_accepted,
                                    ts_last,
                                    ts_init,
                                    None,
                                );
                                // Optionally attach prices
                                report.price = _limit_px;
                                report.trigger_price = _stop_px;

                                info!(target: "projectx.ws", "GatewayUserOrder standardized: id={} status={} side={:?} type={:?}", order.id, order.status, side, otype);
                                self.emit_execution_reports(vec![ExecutionReport::Order(report)]);
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
                                let symbol =
                                    match parse_symbol_from_contract_id(&position.contract_id) {
                                        Some(s) => s,
                                        None => {
                                            error!(
                                                "error parsing symbol from contract: {}",
                                                position.contract_id
                                            );
                                            return;
                                        }
                                    };
                                let symbol = Symbol::new(symbol);
                                let instrument_id =
                                    InstrumentId::new(symbol, PROJECT_X_VENUE.clone());
                                self.positions
                                    .insert(instrument_id.clone(), position.clone());

                                info!(target: "projectx.ws", "GatewayUserPosition cached: id={} account_id={} contract={}", position.id, position.account_id, position.contract_id);
                                // TODO: Map to PositionStatusReport and emit via execution bus if/when standardized for ProjectX
                            }
                            "GatewayUserTrade" => {
                                use nautilus_core::{UnixNanos, datetime::iso8601_to_unix_nanos};
                                use nautilus_model::{
                                    enums::{LiquiditySide, OrderSide as NautOrderSide},
                                    identifiers::{TradeId as VenueTradeId, VenueOrderId},
                                    types::{Money, Price, Quantity},
                                };
                                let trade =
                                    match serde_json::from_value::<GatewayUserTrade>(val.clone()) {
                                        Ok(t) => t,
                                        Err(e) => {
                                            error!("error parsing GatewayUserTrade: {}", e);
                                            return;
                                        }
                                    };
                                let account_id = match AccountId::new_checked(format!(
                                    "{}-{}",
                                    &self.firm, trade.account_id
                                )) {
                                    Ok(id) => id,
                                    Err(e) => {
                                        error!("error building AccountId: {}", e);
                                        return;
                                    }
                                };
                                let symbol_str =
                                    match parse_symbol_from_contract_id(&trade.contract_id) {
                                        Some(s) => s,
                                        None => {
                                            error!(
                                                "error parsing symbol from contract: {}",
                                                trade.contract_id
                                            );
                                            return;
                                        }
                                    };
                                let instrument_id = InstrumentId::new(
                                    Symbol::new(symbol_str.clone()),
                                    *PROJECT_X_VENUE,
                                );
                                let venue_order_id = VenueOrderId::new(trade.order_id.to_string());
                                let trade_id = VenueTradeId::new(trade.id.to_string());
                                let side = if trade.side == OrderSide::Bid as i32 {
                                    NautOrderSide::Buy
                                } else {
                                    NautOrderSide::Sell
                                };
                                let prec = *self
                                    .precisions
                                    .entry(extract_root(&symbol_str))
                                    .or_insert_with(|| {
                                        crate::common::root_specs::get_precision(&extract_root(
                                            &symbol_str,
                                        ))
                                        .unwrap_or(6)
                                    });
                                let last_px = Price::new(trade.price, prec);
                                let last_qty = Quantity::new(trade.size as f64, 0);
                                let commission = Money::new(trade.fees, Currency::USD());
                                let ts_event =
                                    iso8601_to_unix_nanos(trade.creation_timestamp.clone())
                                        .unwrap_or(UnixNanos::from(0));
                                let ts_init = UnixNanos::from(
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .map(|d| d.as_nanos() as u64)
                                        .unwrap_or(0),
                                );
                                let fill = FillReport::new(
                                    account_id,
                                    instrument_id,
                                    venue_order_id,
                                    trade_id,
                                    side,
                                    last_qty,
                                    last_px,
                                    commission,
                                    LiquiditySide::NoLiquiditySide,
                                    None,
                                    None,
                                    ts_event,
                                    ts_init,
                                    None,
                                );
                                info!(target: "projectx.ws", "GatewayUserTrade standardized: id={} order_id={} side={:?} px={} qty={} fees={}", trade.id, trade.order_id, side, trade.price, trade.size, trade.fees);
                                self.emit_execution_reports(vec![ExecutionReport::Fill(fill)]);
                            }
                            _ => self.emit_raw(val),
                        }
                    } else {
                        self.emit_raw(val);
                    }
                }
                6 => {
                    // Ping
                }
                _ => {
                    self.emit_raw(val);
                }
            }
        } else {
            self.emit_raw(val);
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
        // In the JS example the client subscribes to Accounts (no arg) and then account-specific streams.
        if !self.user_accounts_subscribed.load(Ordering::SeqCst) {
            self.user_accounts_subscribed.store(true, Ordering::SeqCst);
            // Subscribe to broadcast accounts stream once
            let _ = self.invoke_user("SubscribeAccounts", vec![]).await;
        }
        self.user_orders_subs.insert(account_id, ());
        self.user_positions_subs.insert(account_id, ());
        self.user_trades_subs.insert(account_id, ());
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
        self.user_orders_subs.remove(&account_id);
        self.user_positions_subs.remove(&account_id);
        self.user_trades_subs.remove(&account_id);
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
        if self.user_orders_subs.is_empty()
            && self.user_positions_subs.is_empty()
            && self.user_trades_subs.is_empty()
        {
            if self.user_accounts_subscribed.swap(false, Ordering::SeqCst) {
                let _ = self.invoke_user("UnsubscribeAccounts", vec![]).await;
            }
        }
        Ok(())
    }

    /// Subscribe to market quotes for a symbol
    pub async fn subscribe_quotes(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_quotes_subs.insert(contract_id.to_string(), ());
        Ok(())
    }

    /// Unsubscribe from market quotes for a symbol
    pub async fn unsubscribe_quotes(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_quotes_subs.remove(contract_id);
        Ok(())
    }

    /// Subscribe to market trades for a symbol
    pub async fn subscribe_trades(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_trades_subs.insert(contract_id.to_string(), ());
        Ok(())
    }

    /// Unsubscribe from market trades for a symbol
    pub async fn unsubscribe_trades(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_trades_subs.remove(contract_id);
        Ok(())
    }

    /// Subscribe to market depth (DOM) for a symbol
    pub async fn subscribe_depth(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_depth_subs.insert(contract_id.to_string(), ());
        Ok(())
    }

    /// Unsubscribe from market depth (DOM) for a symbol
    pub async fn unsubscribe_depth(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_depth_subs.remove(contract_id);
        Ok(())
    }

    /// Subscribe to market data by contract ID (quotes)
    pub async fn subscribe_contract_quotes(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_contract_quotes_subs
            .insert(contract_id.to_string(), ());
        // SignalR invocation
        self.invoke_market(
            "SubscribeContractQuotes",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by contract ID (quotes)
    pub async fn unsubscribe_contract_quotes(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_contract_quotes_subs.remove(contract_id);
        self.invoke_market(
            "UnsubscribeContractQuotes",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Subscribe to market data by contract ID (trades)
    pub async fn subscribe_contract_trades(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_contract_trades_subs
            .insert(contract_id.to_string(), ());
        self.invoke_market(
            "SubscribeContractTrades",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by contract ID (trades)
    pub async fn unsubscribe_contract_trades(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_contract_trades_subs.remove(contract_id);
        self.invoke_market(
            "UnsubscribeContractTrades",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Subscribe to market data by contract ID (market depth)
    pub async fn subscribe_contract_market_depth(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_contract_depth_subs
            .insert(contract_id.to_string(), ());
        self.invoke_market(
            "SubscribeContractMarketDepth",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by contract ID (market depth)
    pub async fn unsubscribe_contract_market_depth(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_contract_depth_subs.remove(contract_id);
        // Cleanup persisted book state for this contract
        if let Some(entry) = self.contract_to_instrument.remove(contract_id) {
            let (_cid, iid) = entry;
            self.market_books.remove(&iid);
        } else {
            // Try heuristic symbol derivation if mapping not found
            if let Some(sym_str) =
                crate::common::futures_helpers::parse_symbol_from_contract_id(contract_id)
            {
                if let Ok(sym) = nautilus_model::identifiers::Symbol::new_checked(sym_str) {
                    let iid = nautilus_model::identifiers::InstrumentId::new(
                        sym,
                        *crate::common::consts::PROJECT_X_VENUE,
                    );
                    self.market_books.remove(&iid);
                }
            }
        }
        self.invoke_market(
            "UnsubscribeContractMarketDepth",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Subscribe to market data by contract ID (bars)
    pub async fn subscribe_contract_bars(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_contract_bars_subs
            .insert(contract_id.to_string(), ());
        // Minimal invocation variant (server may support additional args for unit/unitNumber)
        self.invoke_market(
            "SubscribeContractBars",
            vec![Value::String(contract_id.to_string())],
        )
        .await
    }

    /// Unsubscribe from market data by contract ID (bars)
    pub async fn unsubscribe_contract_bars(&self, contract_id: &str) -> anyhow::Result<()> {
        self.market_contract_bars_subs.remove(contract_id);
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

    /// Provides the internal data stream as a channel-based stream
    ///
    /// # Panics
    ///
    /// This function panics if:
    /// - The websocket is not connected.
    /// - `stream_data` has already been called somewhere else (stream receiver is then taken).
    pub fn stream(&mut self) -> impl Stream<Item = NautilusWsMessage> + 'static {
        let rx = self
            .rx
            .take()
            .expect("Data stream receiver already taken or not connected");
        let mut rx = Arc::try_unwrap(rx).expect("Cannot take ownership - other references exist");
        async_stream::stream! {
            while let Some(data) = rx.recv().await {
                yield data;
            }
        }
    }
}
