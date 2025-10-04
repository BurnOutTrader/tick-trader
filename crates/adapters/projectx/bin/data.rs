//! Sandbox test bed: authenticate, connect realtime, subscribe MNQ trades, quotes or depth, print incoming.
use std::sync::Arc;
use dotenvy::dotenv;
use rustls::crypto::{CryptoProvider, ring};
use tracing::{error, info, level_filters::LevelFilter, warn};
use projectx::http::client::PxHttpClient;
use projectx::http::credentials::PxCredential;
use projectx::http::models::ContractSearchResponse;
use projectx::websocket::client::PxWebSocketClient;
use tt_bus::MessageBus;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::DEBUG)
        .init();

    dotenv().ok();
    let _ = CryptoProvider::install_default(ring::default_provider());

    // Authenticate via HTTP to obtain a JWT for the SignalR market hub
    let cfg = PxCredential::from_env().expect("Missing PX env vars");
    let http = PxHttpClient::new(cfg, None, None, None, None)?;
    http.start().await?;
    info!("Authentication: Success");

    // Resolve MNQ contract ID: allow override via env, else search
    let contract_id =
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
        };


// Build realtime client using the authenticated token
    let token = http
        .inner
        .token_string()
        .await;
    let base = http.inner.rtc_base();
    let bus = Arc::new(MessageBus::new());
    let rt = PxWebSocketClient::new(base, token,http.firm.clone(), bus.clone());

    // Connect market hub and subscribe to trades for the contract
    rt.connect_market().await?;
    info!("Connected to market hub");

    match rt.subscribe_contract_market_depth(&contract_id).await {
        Ok(_) => info!("Subscribed trades for contract_id={}", contract_id),
        Err(_) => warn!("Failed to subscribe to trades for contract_id={}", contract_id),
    }

    info!("Waiting for depth... press Ctrl-C to exit");

    // Keep the process alive; the PxWebSocketClient logs GatewayTrade events
    // as they arrive via tracing (see px websocket handler).
    // Await Ctrl-C for graceful exit.
    tokio::signal::ctrl_c().await.ok();

    warn!("Shutting down");
    Ok(())
}
