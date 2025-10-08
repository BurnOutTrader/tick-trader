use std::{sync::Arc, time::Duration};
use tt_types::providers::ProjectXTenant;

use crate::http::inner_client::PxHttpInnerClient;
#[allow(unused_imports)]
use crate::http::{
    credentials::PxCredential,
    error::PxError,
    models::{
        CloseContractReq, CloseContractResponse, ContractSearchByIdReq, ContractSearchByIdResponse,
        ContractSearchReq, ContractSearchResponse, ModifyOrderReq, ModifyOrderResponse,
        OrderSearchOpenReq, OrderSearchReq, OrderSearchResponse, PartialCloseContractReq,
        PlaceOrderReq, PlaceOrderResponse, PositionSearchOpenReq, PositionSearchResponse,
        RetrieveBarsReq, RetrieveBarsResponse, TradeSearchReq, TradeSearchResponse, ValidateResp,
    },
};

#[derive(Clone)]
pub struct PxHttpClient {
    pub firm: ProjectXTenant,
    pub inner: Arc<PxHttpInnerClient>,
}

impl PxHttpClient {
    /// Creates a new [`PxHttpClient`] using the passed in PxCredential
    ///
    /// # Errors
    ///
    /// Returns an error if the retry manager cannot be created.
    pub fn new(
        px_credential: PxCredential,
        timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            firm: px_credential.firm.clone(),
            inner: Arc::new(PxHttpInnerClient::new(
                px_credential,
                timeout_secs,
                max_retries,
                retry_delay_ms,
                retry_delay_max_ms,
            )?),
        })
    }

    /// Authenticate once, then start background token validation on an interval,
    /// Returns Ok when the task is spawned.
    pub async fn start(&self) -> Result<(), PxError> {
        // 1) authenticate once
        self.inner.authenticate().await?;

        // 2) spawn background validator (client-managed)
        self.spawn_auto_validate(Duration::from_secs(12 * 3600))
            .await; // ~12h
        Ok(())
    }

    /// Spawns Autovalidate to update token every 12 hours.
    /// Stores the joinhandle in the inner client so that it is maintained on clone and drop of this object via the Arc<InnerClient>
    async fn spawn_auto_validate(&self, period: Duration) {
        let mut rx = self.inner.stop_tx.subscribe();
        let client = self.inner.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            // track last sent token to avoid redundant notifications
            let mut last_token: Option<String> = None;
            // attempt to read and send current token at startup
            if let Some(cur) = client.token.read().await.clone() {
                last_token = Some(cur.clone());
            }
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if client.validate().await.is_ok() {
                            if let Some(cur) = client.token.read().await.clone() {
                                if last_token.as_ref().map(|s| s.as_str()) != Some(cur.as_str()) {
                                    last_token = Some(cur.clone());
                                }
                            }
                        }
                    }
                    _ = rx.changed() => {
                        if *rx.borrow() { break; }
                    }
                }
            }
        });
        *self.inner.bg_task.write().await = Some(handle);
    }

    pub fn kill(&self) {
        let _ = self.inner.stop_tx.send(true);
    }
}
