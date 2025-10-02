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
use nautilus_projectx::http::models::RetrieveBarsReq;
use rstest::rstest;

#[rstest]
fn retrieve_bars_req_serializes_camel_case() {
    let req = RetrieveBarsReq {
        contract_id: "CON.F.US.RTY.Z24".to_string(),
        live: false,
        start_time: "2024-12-01T00:00:00Z".to_string(),
        end_time: "2024-12-31T21:00:00Z".to_string(),
        unit: 3,
        unit_number: 1,
        limit: 7,
        include_partial_bar: false,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("contractId"));
    assert!(json.contains("startTime"));
    assert!(json.contains("endTime"));
    assert!(json.contains("unitNumber"));
    assert!(json.contains("includePartialBar"));
}
