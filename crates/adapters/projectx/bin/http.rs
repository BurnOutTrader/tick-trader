use std::sync::Arc;
use dotenvy::dotenv;
use rustls::crypto::{ring, CryptoProvider};
use tracing::level_filters::LevelFilter;
use projectx::http::client::PxHttpClient;
use projectx::http::credentials::PxCredential;
use tokio::sync::watch;
use tt_bus::MessageBus;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    dotenv().ok();

    let _ = CryptoProvider::install_default(ring::default_provider());
    let bus = Arc::new(MessageBus::new());
    let cfg = PxCredential::from_env().expect("Missing PX env vars");
    #[allow(unused)]
    let client = PxHttpClient::new(cfg, None, None, None, None,bus.clone())?;
    
    let _ = client.start().await?;
    tracing::info!("Authentication: Success");

    let resp = client.account_ids().await?;

    if resp.is_empty() {
        println!("No contracts found");
    }

    for acc in resp {
        println!("{:?}", acc);
    }

    /*let req = RetrieveBarsReq {
        contract_id: first.id.clone(),
        live: false,
        start_time: (Utc::now() - Duration::days(5)).to_rfc3339(),
        end_time: Utc::now().to_rfc3339(),
        unit: 2,
        unit_number: 1,
        limit: 100,
        include_partial_bar: false,
    };
    let bars = client.retrieve_bars(&req).await?;

    if !bars.success {
        info!(
            "RetrieveBars failed: error_code={:?} message={:?}",
            bars.error_code, bars.error_message
        );
    } else {
        info!("Retrieved bars: {:?}", bars);
    }*/

    Ok(())
}
