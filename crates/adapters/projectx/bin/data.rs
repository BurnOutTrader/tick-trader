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

//! Sandbox test bed: authenticate, connect realtime, subscribe MNQ trades, quotes or depth, print incoming.

use std::env;

use dotenvy::dotenv;
use tracing::{error, info, level_filters::LevelFilter, warn};
use projectx::http::credentials::PxCredential;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .init();

    dotenv().ok();

    // Authenticate via HTTP to obtain a JWT for the SignalR market hub
    let cfg = PxCredential::from_env().expect("Missing PX env vars");
    let http = PxHttpClient::new(cfg, None, None, None, None)?;
    http.start().await?;
    info!("Authentication: Success");

    // Resolve MNQ contract ID: allow override via env, else search
    let contract_id = match env::var("PX_MNQ_CONTRACT_ID") {
        Ok(v) if !v.trim().is_empty() => v,
        _ => {
            // Try a simple contract search for MNQ
            match http.inner.search_contracts(false, "MNQ").await {
                Ok(ContractSearchResponse { contracts, .. }) if !contracts.is_empty() => {
                    let first = &contracts[0];
                    info!(
                        "Using contract: {} ({}), symbol_id={}, active={} ",
                        first.id, first.name, first.symbol_id, first.active_contract
                    );
                    first.id.clone()
                }
                Ok(_) => {
                    error!("No MNQ contracts found in search. Set PX_MNQ_CONTRACT_ID env var.");
                    return Ok(());
                }
                Err(e) => {
                    error!("Contract search failed: {e:?}");
                    return Ok(());
                }
            }
        }
    };

    // Build realtime client using the authenticated token
    let token = http
        .inner
        .token_string()
        .await
        .expect("missing bearer token after authentication");
    let base = http.inner.rtc_base();
    let rt = PxWebSocketClient::new(base, Some(token), http.firm.clone());

    // Connect market hub and subscribe to trades for the contract
    rt.connect_market().await?;
    info!("Connected to market hub");

    rt.subscribe_contract_market_depth(&contract_id).await?;
    info!("Subscribed depth for contract_id={}", contract_id);

    info!("Waiting for depth... press Ctrl-C to exit");

    // Keep the process alive; the PxWebSocketClient logs GatewayTrade events
    // as they arrive via tracing (see px websocket handler).
    // Await Ctrl-C for graceful exit.
    tokio::signal::ctrl_c().await.ok();

    warn!("Shutting down");
    Ok(())
}
