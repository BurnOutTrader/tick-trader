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

//! Live market data client implementation for the ProjectX adapter.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::anyhow;
use crate::{
    common::consts::PROJECT_X_VENUE, http::client::PxHttpClient, websocket::PxWebSocketClient,
};

/// Minimal ProjectX data client scaffold.
///
/// This mirrors the OKX adapter structure but keeps a lean implementation for now.
/// It implements the DataClient trait so the adapter can be wired into systems that
/// expect a data client, while we incrementally add full functionality.
pub struct PxDataClient {
    client_id: ClientId,
    venue: Venue,
    http_client: Arc<PxHttpClient>,
    websocket: Option<PxWebSocketClient>,
    is_connected: AtomicBool,
}

impl PxDataClient {
    /// Create a new ProjectX data client using the provided HTTP client and client id.
    pub fn new(client_id: ClientId, http_client: PxHttpClient) -> Self {
        Self {
            client_id,
            venue: PROJECT_X_VENUE.clone(),
            http_client: Arc::new(http_client),
            websocket: None,
            is_connected: AtomicBool::new(false),
        }
    }

    /// Access the underlying HTTP client.
    pub fn http(&self) -> &PxHttpClient {
        &self.http_client
    }

    fn spawn_ws<F>(&self, fut: F, context: &'static str)
    where
        F: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        tokio::spawn(async move {
            if let Err(err) = fut.await {
                tracing::error!("{context}: {err:?}");
            }
        });
    }
}
