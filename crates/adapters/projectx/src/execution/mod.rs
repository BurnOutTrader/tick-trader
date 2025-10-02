//! Live execution client implementation for the ProjectX adapter.

use std::{cell::Ref, future::Future, sync::Mutex};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::{StreamExt, pin_mut};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::{
    common::consts::PROJECT_X_VENUE,
    http::{client::PxHttpClient, credentials::PxCredential},
    websocket::{PxWebSocketClient},
};

#[derive(Debug)]
pub struct PxExecutionClient {
    http_client: PxHttpClient,
    ws_client: Option<PxWebSocketClient>,
    started: bool,
    connected: bool,
    ws_stream_handle: Option<JoinHandle<()>>,
    pending_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl PxExecutionClient {
    /// Creates a new [`PxExecutionClient`].
    ///
    /// The ProjectX adapter uses environment variables for REST auth (see PxCredential::from_env).
    /// WebSocket (user hub) will be initialized on connect() after REST authentication.
    pub fn new() -> Result<Self> {
        let creds = PxCredential::from_env().context("Missing ProjectX env credentials")?;
        let http_client = PxHttpClient::new(creds, None, None, None, None)
            .context("failed to construct ProjectX HTTP client")?;

        Ok(Self {
            http_client,
            ws_client: None,
            started: false,
            connected: false,
            ws_stream_handle: None,
            pending_tasks: Mutex::new(Vec::new()),
        })
    }

    fn spawn_task<F>(&self, label: &'static str, fut: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let handle = tokio::spawn(async move {
            if let Err(err) = fut.await {
                tracing::error!(target = "projectx.exec", "{label}: {err:?}");
            }
        });
        self.pending_tasks
            .lock()
            .expect("pending task lock poisoned")
            .push(handle);
    }

    fn abort_pending_tasks(&self) {
        let mut guard = self
            .pending_tasks
            .lock()
            .expect("pending task lock poisoned");
        for handle in guard.drain(..) {
            handle.abort();
        }
    }
}
