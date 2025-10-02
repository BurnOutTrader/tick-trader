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

use nautilus_projectx::http::models::ContractSearchByIdReq;
use rstest::rstest;

#[rstest]
fn contract_search_by_id_serializes_camel_case() {
    let req = ContractSearchByIdReq {
        contract_id: "CON.F.US.ENQ.H25".to_string(),
    };
    let json = serde_json::to_string(&req).expect("serialize");
    assert!(json.contains("contractId"));
    assert!(json.contains("CON.F.US.ENQ.H25"));
}
