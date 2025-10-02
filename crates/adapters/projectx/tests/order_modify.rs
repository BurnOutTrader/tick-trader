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

use nautilus_projectx::http::models::ModifyOrderReq;
use rstest::rstest;

#[rstest]
fn modify_order_req_serializes_camel_case() {
    let req = ModifyOrderReq {
        account_id: 465,
        order_id: 26974,
        size: Some(1),
        limit_price: None,
        stop_price: Some(1604.0),
        trail_price: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"orderId\""));
    assert!(json.contains("\"size\""));
    assert!(json.contains("\"stopPrice\""));
}

#[rstest]
fn modify_order_req_omits_optionals() {
    let req = ModifyOrderReq {
        account_id: 1,
        order_id: 2,
        size: None,
        limit_price: None,
        stop_price: None,
        trail_price: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"orderId\""));
    assert!(!json.contains("size"));
    assert!(!json.contains("limitPrice"));
    assert!(!json.contains("stopPrice"));
    assert!(!json.contains("trailPrice"));
}
