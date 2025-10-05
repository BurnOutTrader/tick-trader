use dotenvy::dotenv;
use projectx::http::client::PxHttpClient;
use projectx::http::credentials::PxCredential;
use rustls::crypto::{CryptoProvider, ring};
use tracing::level_filters::LevelFilter;
use provider::traits::ProviderSessionSpec;
use tt_types::providers::{ProjectXTenant, ProviderKind};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    let _ = CryptoProvider::install_default(ring::default_provider());
    dotenv().ok();
    let session_creds = ProviderSessionSpec::from_env();
    let firm = ProjectXTenant::Topstep;
    let provider = ProviderKind::ProjectX(firm);
    let cfg = PxCredential::new(firm, session_creds.user_names.get(&provider).unwrap().clone(), session_creds.api_keys.get(&provider).unwrap().clone() );
    #[allow(unused)]
    let client = PxHttpClient::new(cfg, None, None, None, None)?;

    let _ = client.start().await?;
    tracing::info!("Authentication: Success");

    let resp = client.account_snapshots().await?;

    if resp.is_empty() {
        println!("No contracts found");
    }

    for acc in resp.iter() {
        println!("{:?}", acc.value());
    }

    let resp = client.manual_update_instruments(false).await?;
    let resp = resp.read().await;
    for (_, inst) in resp.iter() {
        println!("{:?}", inst);
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
