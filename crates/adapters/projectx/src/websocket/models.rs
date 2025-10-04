use serde::{Deserialize, Serialize};
use tt_types::order_models::enums::{OrderEventType, OrderType};

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

/// Market quote payload
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GatewayQuote {
    pub symbol: String,
    #[serde(default)]
    pub symbol_name: Option<String>,
    pub last_price: f64,
    pub best_bid: f64,
    pub best_ask: f64,
    pub change: f64,
    pub change_percent: f64,
    #[serde(default)]
    pub open: Option<f64>,
    #[serde(default)]
    pub high: Option<f64>,
    #[serde(default)]
    pub low: Option<f64>,
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
struct PxWebSocketError {
    pub code: Option<String>,
    pub message: String,
}

pub fn map_order_type(order_type: i32) -> OrderType {
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