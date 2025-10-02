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

use chrono::DateTime;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarSpecification, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::InstrumentId,
    types::{Price, Quantity},
};
use serde::{Deserialize, Serialize};
use tokio::{sync::watch, task::JoinHandle};

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

fn map_unit_to_aggregation(unit: i32) -> BarAggregation {
    match unit {
        // These codes are based on observed API behavior; default to Minute if unknown
        0 => BarAggregation::Second,
        1 => BarAggregation::Second,
        2 => BarAggregation::Minute, // observed in practice
        3 => BarAggregation::Hour,
        4 => BarAggregation::Day,
        5 => BarAggregation::Week,
        6 => BarAggregation::Month,
        _ => BarAggregation::Minute,
    }
}

impl PxApiBar {
    fn to_engine_bar(
        &self,
        bar_type: BarType,
        price_precision: u8,
        size_precision: u8,
    ) -> anyhow::Result<Bar> {
        // Parse RFC3339 timestamp to UnixNanos
        let dt = DateTime::parse_from_rfc3339(&self.t)
            .map_err(|e| anyhow::anyhow!("invalid bar timestamp '{}': {}", &self.t, e))?;
        let ts_nanos = dt
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow::anyhow!("timestamp overflow for '{}'", &self.t))?;
        let ts_nanos_u64: u64 = ts_nanos
            .try_into()
            .map_err(|_| anyhow::anyhow!("negative timestamp for '{}'", &self.t))?;
        let ts = UnixNanos::from(ts_nanos_u64);

        let open = Price::new(self.o, price_precision);
        let high = Price::new(self.h, price_precision);
        let low = Price::new(self.l, price_precision);
        let close = Price::new(self.c, price_precision);
        let volume = Quantity::new(self.v as f64, size_precision);

        Ok(Bar::new(bar_type, open, high, low, close, volume, ts, ts))
    }
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
    pub fn to_engine_bars(
        &self,
        instrument_id: InstrumentId,
        unit: i32,
        unit_number: i32,
        price_precision: u8,
        size_precision: u8,
    ) -> anyhow::Result<Vec<Bar>> {
        let aggregation = map_unit_to_aggregation(unit);
        let spec = BarSpecification::new(unit_number as usize, aggregation, PriceType::Last);
        let bar_type = BarType::new(instrument_id, spec, AggregationSource::External);
        self.bars
            .iter()
            .map(|b| b.to_engine_bar(bar_type, price_precision, size_precision))
            .collect()
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
pub enum OrderStatus {
    None = 0,
    Open = 1,
    Filled = 2,
    Cancelled = 3,
    Expired = 4,
    Rejected = 5,
    Pending = 6,
}
impl OrderStatus {
    pub fn from_i32(num: i32) -> Self {
        match num {
            0 => OrderStatus::None,
            1 => OrderStatus::Open,
            2 => OrderStatus::Filled,
            3 => OrderStatus::Cancelled,
            4 => OrderStatus::Expired,
            5 => OrderStatus::Rejected,
            6 => OrderStatus::Pending,
            _ => panic!("unknown order status {}", num),
        }
    }
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
