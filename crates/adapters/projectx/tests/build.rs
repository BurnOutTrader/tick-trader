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

use nautilus_projectx::http::{client::PxHttpInnerClient, credentials::PxCredential};
use rstest::rstest;

#[rstest]
fn builds_client_with_dummy_config() {
    // This test should not perform any network I/O. It only ensures that the
    // ProjectX HTTP client can be constructed without panicking given a
    // minimally valid config structure.
    let cfg = PxCredential {
        firm: "example_firm".to_string(),
        user_name: "example_user".to_string(),
        api_key: "example_key".into(),
    };

    let _client = PxHttpInnerClient::new_default(cfg).unwrap();
    // If construction succeeds without panic, the test passes.
}
