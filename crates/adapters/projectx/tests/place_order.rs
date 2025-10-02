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

use nautilus_projectx::http::models::{BracketCfg, PlaceOrderReq};
use rstest::rstest;

#[rstest]
fn place_order_full_serializes_camel_case() {
    let req = PlaceOrderReq {
        account_id: 465,
        contract_id: "CON.F.US.DA6.M25".to_string(),
        type_: 2,
        side: 1,
        size: 1,
        limit_price: None,
        stop_price: None,
        trail_price: None,
        custom_tag: None,
        stop_loss_bracket: Some(BracketCfg {
            ticks: 10,
            type_: 1,
        }),
        take_profit_bracket: Some(BracketCfg {
            ticks: 20,
            type_: 1,
        }),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"contractId\""));
    assert!(json.contains("\"type\""));
    assert!(json.contains("\"side\""));
    assert!(json.contains("\"size\""));
    assert!(json.contains("\"stopLossBracket\""));
    assert!(json.contains("\"takeProfitBracket\""));
    assert!(json.contains("\"ticks\""));
}

#[test]
fn place_order_min_omits_optionals() {
    let req = PlaceOrderReq {
        account_id: 1,
        contract_id: "CID".to_string(),
        type_: 2,
        side: 0,
        size: 3,
        limit_price: None,
        stop_price: None,
        trail_price: None,
        custom_tag: None,
        stop_loss_bracket: None,
        take_profit_bracket: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"contractId\""));
    assert!(json.contains("\"type\""));
    assert!(!json.contains("limitPrice"));
    assert!(!json.contains("stopPrice"));
    assert!(!json.contains("trailPrice"));
    assert!(!json.contains("customTag"));
    assert!(!json.contains("stopLossBracket"));
    assert!(!json.contains("takeProfitBracket"));
}
