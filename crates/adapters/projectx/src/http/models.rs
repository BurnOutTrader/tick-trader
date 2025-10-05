use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tokio::{sync::watch, task::JoinHandle};
use tt_types::base_data::{Candle, Exchange, Resolution};
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::market_hours::hours_for_exchange;
use tt_types::securities::symbols::Instrument;

#[allow(unused)]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginKeyReq {
    user_name: String,
    api_key: String,
}

#[allow(unused)]
#[derive(Debug, Deserialize)]
struct LoginKeyResp {
    token: String,
    success: bool,
    #[allow(dead_code)]
    error_code: Option<i32>,
    #[allow(dead_code)]
    error_message: Option<String>,
}

#[allow(unused)]
/// Response body from POST /api/Auth/validate
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidateResp {
    success: bool,
    error_code: Option<i32>,
    error_message: Option<String>,
    pub(crate) new_token: Option<String>,
}

// ---------------- Account Search ----------------

/// Request body for POST /api/Account/search
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountSearchReq {
    pub only_active_accounts: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    pub id: i64,
    pub name: String,
    pub balance: f64,
    pub can_trade: bool,
    pub is_visible: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountSearchResponse {
    pub accounts: Vec<AccountInfo>,
    pub success: bool,
    pub error_code: Option<i32>,
    pub error_message: Option<String>,
}

// ---------------- Retrieve Bars ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveBarsReq {
    pub contract_id: String,
    pub live: bool,
    pub start_time: String, // RFC3339 timestamp
    pub end_time: String,   // RFC3339 timestamp
    pub unit: i32,
    pub unit_number: i32,
    pub limit: i32,
    pub include_partial_bar: bool,
}

// When bars is null, treat as empty array
fn deserialize_null_to_empty_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    let opt = Option::<Vec<T>>::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

#[derive(Debug, Deserialize, Clone)]
pub struct PxApiBar {
    pub t: String,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: i64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveBarsResponse {
    #[serde(default, deserialize_with = "deserialize_null_to_empty_vec")]
    pub bars: Vec<PxApiBar>,
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

impl RetrieveBarsResponse {
    /// Convert the API bars into engine Bars.
    ///
    /// Parameters:
    /// - instrument_id: the engine InstrumentId for the requested contract
    /// - unit/unit_number: timeframe info as supplied in the request
    /// - price_precision: desired price precision for the instrument
    /// - size_precision: desired size precision for the instrument
    pub fn to_engine_candles(
        &self,
        instrument: Instrument,
        resolution: Resolution,
        exchange: Exchange,
    ) -> anyhow::Result<Vec<Candle>> {
        self.bars
            .iter()
            .map(|bar| Self::bar_to_candle(bar, instrument.clone(), resolution, exchange))
            .collect()
    }

    pub fn bar_to_candle(
        bar: &PxApiBar,
        instrument: Instrument,
        resolution: Resolution,
        exchange: Exchange,
    ) -> anyhow::Result<Candle> {
        let time_start = DateTime::<Utc>::from_str(&bar.t)?;
        let time_end = match resolution {
            Resolution::Daily => {
                let _market_hours = hours_for_exchange(exchange);
                todo!()
            }
            Resolution::Weekly => {
                let _market_hours = hours_for_exchange(exchange);
                todo!()
            }
            _ => time_start + resolution.as_duration(),
        };
        let root = extract_root(&instrument);
        let candle = Candle {
            symbol: root,
            instrument,
            time_start,
            time_end,
            open: Decimal::from_f64(bar.o).unwrap_or_default(),
            high: Decimal::from_f64(bar.h).unwrap_or_default(),
            low: Decimal::from_f64(bar.l).unwrap_or_default(),
            close: Decimal::from_f64(bar.c).unwrap_or_default(),
            volume: Decimal::from_i64(bar.v).unwrap_or_default(),
            ask_volume: Default::default(),
            bid_volume: Default::default(),
            resolution,
        };

        Ok(candle)
    }
}

// ---------------- Contract Available ----------------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AvailableContractsReq {
    pub live: bool,
}

// ---------------- Contract Search ----------------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractSearchReq {
    pub live: bool,
    pub search_text: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContractInfo {
    pub id: String,
    pub name: String,
    pub description: String,
    pub tick_size: f64,
    pub tick_value: f64,
    pub active_contract: bool,
    pub symbol_id: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContractSearchResponse {
    pub contracts: Vec<ContractInfo>,
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

// ---------------- Contract Search By Id ----------------

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContractSearchByIdReq {
    pub contract_id: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ContractSearchByIdResponse {
    pub contract: Option<ContractInfo>,
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

// ---------------- Order Search ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderSearchReq {
    pub account_id: i64,
    pub start_timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: Option<String>,
}

// ---------------- Order Search Open ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderSearchOpenReq {
    pub account_id: i64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderInfo {
    pub id: i64,
    pub account_id: i64,
    pub contract_id: String,
    pub symbol_id: String,
    pub creation_timestamp: String,
    pub update_timestamp: String,
    pub status: i32,
    #[serde(rename = "type")]
    pub type_: i32,
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

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct OrderSearchResponse {
    pub orders: Vec<OrderInfo>,
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

// ---------------- Order Place ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BracketCfg {
    pub ticks: i32,
    #[serde(rename = "type")]
    pub type_: i32,
}

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderReq {
    pub account_id: i64,
    pub contract_id: String,
    #[serde(rename = "type")]
    pub type_: i32,
    pub side: i32,
    pub size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_loss_bracket: Option<BracketCfg>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub take_profit_bracket: Option<BracketCfg>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PlaceOrderResponse {
    pub order_id: i64,
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

// ---------------- Order Cancel ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderReq {
    pub account_id: i64,
    pub order_id: i64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CancelOrderResponse {
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

// ---------------- Order Modify ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ModifyOrderReq {
    pub account_id: i64,
    pub order_id: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop_price: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trail_price: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ModifyOrderResponse {
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

pub struct PxAuthGuard {
    stop_tx: watch::Sender<bool>,
    #[allow(unused)]
    handle: JoinHandle<()>,
}

impl Drop for PxAuthGuard {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        // Allow task to finish gracefully; you can also `abort()` if desired
    }
}

// ---------------- Position Close Contract ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CloseContractReq {
    pub account_id: i64,
    pub contract_id: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct CloseContractResponse {
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

// ---------------- Position Partial Close Contract ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PartialCloseContractReq {
    pub account_id: i64,
    pub contract_id: String,
    pub size: i64,
}

// ---------------- Position Search Open ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PositionSearchOpenReq {
    pub account_id: i64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PositionInfo {
    pub id: i64,
    pub account_id: i64,
    pub contract_id: String,
    pub creation_timestamp: String,
    #[serde(rename = "type")]
    pub type_: i32,
    pub size: i64,
    pub average_price: f64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PositionSearchResponse {
    pub positions: Vec<PositionInfo>,
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}

// ---------------- Trade Search ----------------

#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeSearchReq {
    pub account_id: i64,
    pub start_timestamp: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_timestamp: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeInfo {
    pub id: i64,
    pub account_id: i64,
    pub contract_id: String,
    pub creation_timestamp: String,
    pub price: f64,
    #[serde(default)]
    pub profit_and_loss: Option<f64>,
    pub fees: f64,
    pub side: i32,
    pub size: i64,
    pub voided: bool,
    pub order_id: i64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct TradeSearchResponse {
    pub trades: Vec<TradeInfo>,
    pub success: bool,
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub error_message: Option<String>,
}
