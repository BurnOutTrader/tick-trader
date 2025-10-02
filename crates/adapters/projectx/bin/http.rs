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
use nautilus_projectx::http::{
    client::{PxHttpClient, PxHttpInnerClient},
    credentials::PxCredential,
    models::ContractSearchResponse,
};
use tracing::level_filters::LevelFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    dotenv().ok();

    let cfg = PxCredential::from_env().expect("Missing PX env vars");
    #[allow(unused)]
    let client = PxHttpClient::new(cfg, None, None, None, None)?;

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
