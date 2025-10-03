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

use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use chrono::{DateTime, ParseResult, Utc};
use dashmap::DashMap;
use futures_util::{Stream, StreamExt, SinkExt};
use log::warn;
use serde::{Deserialize, Serialize};
use serde_json::{Error, Value};
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info};
use tt_types::securities::symbols::{get_symbol_info, Instrument, SymbolInfo};

// Minimal WebSocket client used by PxWebSocketClient
use futures_util::stream::BoxStream;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal_macros::dec;
use tokio_tungstenite::{connect_async, WebSocketStream};
use tokio_tungstenite::tungstenite;
use tokio::net::TcpStream;
use tokio_tungstenite::MaybeTlsStream;
use tt_types::accounts::account::{AccountId, AccountName, AccountSnapShot};
use tt_types::base_data::{Price, Side, Tick, Volume};
use tt_types::orders::{OrderEventType, OrderType};
use tt_types::securities::futures_helpers::extract_root;

#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub message_handler: Option<()>,
    pub heartbeat: Option<u64>,
    pub heartbeat_msg: Option<String>,
    pub ping_handler: Option<()>,
    pub reconnect_timeout_ms: Option<u64>,
    pub reconnect_delay_initial_ms: Option<u64>,
    pub reconnect_delay_max_ms: Option<u64>,
    pub reconnect_backoff_factor: Option<f64>,
    pub reconnect_jitter_ms: Option<u64>,
}

#[derive(Debug)]
pub struct WebSocketClient {
    write: Arc<tokio::sync::Mutex<futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
}

impl WebSocketClient {
    pub async fn connect_stream(
        config: WebSocketConfig,
        _protocols: Vec<String>,
        _proxy: Option<()>,
        _tls: Option<()>,
    ) -> anyhow::Result<(
        BoxStream<'static, Result<Message, tungstenite::Error>>,
        WebSocketClient,
    )> {
        // Note: we currently ignore custom headers in config to avoid extra dependencies.
        // If necessary, this can be extended to build a Request with headers.
        let (ws_stream, _response) = connect_async(&config.url).await?;
        let (write, read) = ws_stream.split();
        let client = WebSocketClient { write: Arc::new(tokio::sync::Mutex::new(write)) };
        let reader: BoxStream<'static, Result<Message, tungstenite::Error>> = Box::pin(read);
        Ok((reader, client))
    }

    pub async fn send_text(&self, data: String, _request_id: Option<u64>) -> anyhow::Result<()> {
        let mut write = self.write.lock().await;
        write.send(Message::Text(data.into())).await.map_err(|e| anyhow::anyhow!(e))
    }
}
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


/// A minimal ProjectX websocket error payload for surfacing client/server errors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PxWebSocketError {
    pub code: Option<String>,
    pub message: String,
}

fn map_order_type(order_type: i32) -> OrderType {
    match order_type {
        1 | 6 | 7 => OrderType::Limit,
        2 => OrderType::Market,
        4 => OrderType::StopMarket,
        5 => OrderType::TrailingStopMarket,
        _ => OrderType::Unknown,
    }
}

pub fn map_status(status: i32) -> OrderEventType {
    match status {
        0 => OrderEventType::Initialized,
        1 => OrderEventType::Accepted,
        2 => OrderEventType::Filled,
        3 => OrderEventType::Canceled,
        4 => OrderEventType::Expired,
        5 => OrderEventType::Rejected,
        6 => OrderEventType::PendingUpdate,
        _ => OrderEventType::Initialized,
    }
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
    

    precisions: Arc<DashMap<String, u8>>,

    // ---- Positions ----
    positions: Arc<DashMap<Instrument, GatewayUserPosition>>,

    // ---- Orders ----
    pending_orders: Arc<DashMap<Instrument, GatewayUserOrder>>,

    trades: Arc<DashMap<Instrument, Vec<GatewayUserTrade>>>,
}

impl PxWebSocketClient {
    /// Create a new websocket client with optional bearer token
    pub fn new(base_url: impl Into<String>, bearer_token: Option<String>, firm: String) -> Self {
        Self {
            firm,
            base_url: base_url.into(),
            bearer_token,
            bus: tt_bus::MessageBus::new(),
            user_connected: Arc::new(AtomicBool::new(false)),
            market_connected: Arc::new(AtomicBool::new(false)),
            signal: Arc::new(AtomicBool::new(false)),
            request_id_counter: Arc::new(AtomicU64::new(1)),
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

    fn handle_signalr_message(&self, val: Value) {
        use tt_types::base_data::Bbo;
        use tt_types::keys::Topic;
        use tt_types::wire::{QuoteBatch, TickBatch};

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
                                    if let Ok(px_quote) = serde_json::from_value::<GatewayQuote>(data_val.clone()) {
                                        if let Ok(instrument) = Instrument::from_str(px_quote.symbol.as_str()) {
                                            let symbol = extract_root(&instrument);
                                            let bid = Price::from_f64(px_quote.best_bid).unwrap_or_default();
                                            let ask = Price::from_f64(px_quote.best_ask).unwrap_or_default();
                                            let bid_size = Volume::from(0);
                                            let ask_size = Volume::from(0);
                                            // prefer timestamp then last_updated
                                            let time = DateTime::<Utc>::from_str(px_quote.timestamp.as_str())
                                                .or_else(|_| DateTime::<Utc>::from_str(px_quote.last_updated.as_str()))
                                                .unwrap_or_else(|_| Utc::now());
                                            let bbo = tt_types::base_data::Bbo {
                                                symbol,
                                                instrument,
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
                                            info!(target: "projectx.ws", "BBO: {:?}", bbo);
                                        } else {
                                            info!(target: "projectx.ws", "GatewayQuote (invalid instrument): {}", px_quote.symbol);
                                        }
                                    } else {
                                        info!(target: "projectx.ws", "GatewayQuote (unparsed): {}", data_val);
                                    }
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
                                            // Build instrument id from symbolId
                                            if let Ok(instrument) = Instrument::from_str(px_trade.symbol_id.as_str())
                                            {
                                                let symbol = extract_root(&instrument);

                                                let price = match Price::from_f64(px_trade.price) {
                                                    Some(p) => p,
                                                    None => {
                                                        log::warn!("invalid price for {}", px_trade.price);
                                                        return;
                                                    }
                                                };
                                                let size = match Volume::from_i64(px_trade.volume) {
                                                    Some(s) => s,
                                                    None => {
                                                        log::warn!("invalid size for {}", px_trade.volume);
                                                        return;
                                                    }
                                                };
                                                let aggressor = match px_trade.r#type {
                                                    0 => Side::Buy,
                                                    1 => Side::Sell,
                                                    _ => Side::None,
                                                };

                                               let time = match DateTime::<Utc>::from_str(px_trade.timestamp.as_str()) {
                                                   Ok(t) => t,
                                                   Err(e) => {
                                                       log::warn!("invalid timestamp for {}: {}", px_trade.timestamp, e);
                                                       return;
                                                   }
                                               };

                                                let tick = Tick {
                                                    symbol,
                                                    instrument,
                                                    price,
                                                    volume: Default::default(),
                                                    time,
                                                    side: Side::Buy,
                                                    venue_seq: None,
                                                };
                                                info!("{:?}", tick);
                                            } 
                                        }
                                        Err(_) => {
                                            info!(target: "projectx.ws", "GatewayTrade (unparsed): {}", data_val);
                                        }
                                    }
                                }
                            }
                            "GatewayDepth" => {
                               // Extract args and handle payload which may be an array of up to 10 levels
                                let args_opt = val.get("arguments").and_then(|a| a.as_array());
                                if let Some(args) = args_opt {
                                    info!("GatewayDepth: {:?}", args);
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
                                let id = AccountId::new(account.id);
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
                                    id,
                                    balance,
                                    can_trade: account.can_trade,
                                };
                                info!(target: "projectx.ws", "AccountSnapShot: {:?}", snap_shot);
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
                                let account_id = AccountId::new(order.account_id);
                                let instrument = match Instrument::from_str(order.symbol_id.as_str()) {
                                    Ok(i) => i,
                                    Err(e) => {
                                        error!("error parsing instrument: {}", e);
                                        return;
                                    }
                                };
                                // Enum mappings
                                let side = if order.side == 0 {
                                    Side::Buy
                                } else {
                                    Side::Sell
                                };
                                let order_type = map_order_type(order.r#type);
                                let status = map_status(order.status);

                                // Quantities and prices
                                let qty = Volume::from_i64(order.size).unwrap_or_else(|| {
                                    dec!(0)
                                });
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
