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

use dotenvy::dotenv;
use nautilus_projectx::http::{client::PxHttpInnerClient, credentials::PxCredential};
use rstest::rstest;

#[rstest]
#[ignore]
#[tokio::test]
async fn test_authenticate_returns_token() {
    dotenv().ok();

    let cfg = PxCredential::from_env().expect("Missing PX env vars");
    let http = PxHttpInnerClient::new(cfg, None, None, None, None).unwrap();

    // now returns Result<(), PxError>
    http.authenticate().await.expect("auth failed");

    // read token for assertions
    let token = http.token_string().await.expect("no token after auth");

    // Basic sanity checks
    assert!(!token.is_empty(), "Token was empty");
    assert!(token.len() > 10, "Token too short");
    assert!(
        token.starts_with("eyJ"),
        "Token doesn’t look like a JWT: {}",
        token
    );
}

#[rstest]
#[ignore]
#[tokio::test]
async fn auth_key_smoke_test() {
    dotenv().ok();

    let cfg = PxCredential::from_env().expect("PX env var missing");
    let http = PxHttpInnerClient::new(cfg, None, None, None, None).unwrap();

    http.authenticate().await.expect("Failed to auth");

    let token = http.token_string().await.expect("no token after auth");
    assert!(!token.is_empty());
    assert!(token.len() > 10);
    assert!(token.starts_with("eyJ"));
}

#[rstest]
#[ignore]
#[tokio::test]
async fn validate_returns_new_token() {
    dotenv().ok();

    let cfg = PxCredential::from_env().expect("env");
    let http = PxHttpInnerClient::new(cfg, None, None, None, None).unwrap();

    // initial auth
    http.authenticate().await.expect("auth");

    #[allow(unused)]
    let tok1 = http.token_string().await.expect("missing tok1");

    // rotate/validate (now Result<()>)
    http.validate().await.expect("validate");

    let tok2 = http.token_string().await.expect("missing tok2");

    assert!(!tok2.is_empty(), "empty token after validate");
    // Depending on tenant, token may or may not change; if it should rotate:
    // assert_ne!(tok1, tok2, "token did not rotate");
}
