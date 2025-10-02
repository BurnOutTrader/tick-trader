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
use nautilus_projectx::http::models::PartialCloseContractReq;
use rstest::rstest;

#[rstest]
fn partial_close_contract_req_serializes_camel_case_with_size() {
    let req = PartialCloseContractReq {
        account_id: 536,
        contract_id: "CON.F.US.GMET.J25".to_string(),
        size: 1,
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("\"accountId\""));
    assert!(json.contains("\"contractId\""));
    assert!(json.contains("\"size\""));
    assert!(json.contains("536"));
    assert!(json.contains("CON.F.US.GMET.J25"));
}
