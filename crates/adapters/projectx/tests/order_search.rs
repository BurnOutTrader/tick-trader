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

use nautilus_projectx::http::models::OrderSearchReq;
use rstest::rstest;

#[rstest]
fn order_search_req_serializes_camel_case_with_end() {
    let req = OrderSearchReq {
        account_id: 704,
        start_timestamp: "2025-07-18T21:00:01.268009+00:00".to_string(),
        end_timestamp: Some("2025-07-18T21:00:01.278009+00:00".to_string()),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"startTimestamp\""));
    assert!(json.contains("\"endTimestamp\""));
}

#[rstest]
fn order_search_req_omits_end_when_none() {
    let req = OrderSearchReq {
        account_id: 704,
        start_timestamp: "2025-07-18T21:00:01.268009+00:00".to_string(),
        end_timestamp: None,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"startTimestamp\""));
    assert!(!json.contains("endTimestamp"));
}
