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

use nautilus_projectx::http::credentials::PxCredential;
use rstest::rstest;

const FIRM: &str = "example_firm";
const USER_NAME: &str = "example_user";
const API_KEY: &str = "example_api_key_123";

#[rstest]
fn test_new_constructs_fields() {
    let cred = PxCredential::new(FIRM.to_string(), USER_NAME.to_string(), API_KEY.to_string());
    assert_eq!(cred.firm, FIRM);
    assert_eq!(cred.user_name, USER_NAME);
    // api_key is a Ustr; compare by as_str()
    assert_eq!(cred.api_key.as_str(), API_KEY);
}

#[rstest]
fn test_debug_redacts_secret() {
    let cred = PxCredential::new(FIRM.to_string(), USER_NAME.to_string(), API_KEY.to_string());
    let dbg_out = format!("{:?}", cred);

    // Ensure redaction markers are present
    assert!(dbg_out.contains("user_name: \"<redacted>\""));
    assert!(dbg_out.contains("api_key: \"<redacted>\""));

    // Ensure raw values are not leaked
    assert!(!dbg_out.contains(USER_NAME));
    assert!(!dbg_out.contains(API_KEY));

    // Ensure raw bytes of secret are not leaked either
    let api_key_bytes_dbg = format!("{:?}", API_KEY.as_bytes());
    assert!(
        !dbg_out.contains(&api_key_bytes_dbg),
        "Debug output must not contain raw secret bytes"
    );
}
