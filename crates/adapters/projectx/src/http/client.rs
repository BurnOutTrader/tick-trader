use std::{
    sync::Arc,
    time::Duration,
};
use std::str::FromStr;
use ahash::AHashMap;
use dashmap::DashMap;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tokio::sync::RwLock;
use tt_types::accounts::account::AccountName;
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::{get_symbol_info, Instrument, SecurityType};

use crate::http::{
    credentials::PxCredential,
    error::PxError,
    models::{
        CancelOrderResponse, CloseContractReq, CloseContractResponse, ContractSearchByIdReq,
        ContractSearchByIdResponse, ContractSearchReq, ContractSearchResponse, ModifyOrderReq,
        ModifyOrderResponse, OrderSearchOpenReq, OrderSearchReq, OrderSearchResponse,
        PartialCloseContractReq, PlaceOrderReq, PlaceOrderResponse, PositionSearchOpenReq,
        PositionSearchResponse, RetrieveBarsReq, RetrieveBarsResponse, TradeSearchReq,
        TradeSearchResponse, ValidateResp,
    },
};
use crate::http::inner_client::PxHttpInnerClient;

#[derive(Clone)]
pub struct PxHttpClient {
    pub firm: String,
    pub inner: Arc<PxHttpInnerClient>,
    internal_accounts_ids: Arc<DashMap<AccountName, i64>>,
    instruments: Arc<RwLock<AHashMap<Instrument, FuturesContract>>>,
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
                retry_delay_max_ms
            )?),
            internal_accounts_ids: Arc::new(DashMap::new()),
            instruments: Arc::new(RwLock::new(AHashMap::new())),
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

    pub async fn update_instruments(&self) -> anyhow::Result<Arc<RwLock<AHashMap<Instrument, FuturesContract>>>> {
        let resp: ContractSearchResponse = self
            .inner
            .list_all_contracts(false)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut lock = self.instruments.write().await;
        for inst in &resp.contracts {
            let instrument = match Instrument::from_str(inst.name.as_str()) {
                Ok(instrument) => instrument,
                Err(e) => {
                    log::error!("Failed to parse instrument: {}: {:?}", inst.name, e);
                    continue
                },
            };
            let symbol = extract_root(&instrument);
            let symbol_info = match get_symbol_info(&symbol) {
                Some(info) => info,
                None => {
                    log::error!("Failed to parse symbol: {}", symbol);
                    continue
                },
            };
            let mut s = match FuturesContract::from_root_with_default_models(&instrument, symbol_info.exchange, SecurityType::Future, inst.id.clone()) {
                None => continue,
                Some(s) => s
            };
            s.value_per_tick = Decimal::from_f64(inst.tick_value).unwrap_or_else(|| symbol_info.value_per_tick);
            s.tick_size = Decimal::from_f64(inst.tick_size).unwrap_or_else(|| symbol_info.tick_size);

            lock.insert(instrument, s);
        }

        Ok(self.instruments.clone())
    }

    /// Initialise or refresh the account id using the api
    pub async fn account_ids(&self) -> anyhow::Result<Vec<i64>> {
        let resp = self.inner.search_accounts(true).await?;

        let mut ids = Vec::new();
        for acc in &resp.accounts {
            let name = match AccountName::from_str(&acc.name) {
                Ok(name) => name,
                Err(e) => {
                    log::error!("Failed to parse account id: {}", e);
                    continue;
                }
            };
            self.internal_accounts_ids.insert(name, acc.id);
            ids.push(acc.id);
        }
        Ok(ids)
    }

    /// Find a correlated nautilus account id from an account name
    pub async fn account_id(&self, account_name: AccountName) -> anyhow::Result<i64> {
        match self.internal_accounts_ids.get(&account_name) {
            None => {
                // if we don't have the account id cached, we need to refresh the list
                self.account_ids().await?;
                match self.internal_accounts_ids.get(&account_name) {
                    None => Err(anyhow::anyhow!("Failed to find account id for {}, please check the account name and try again", account_name)),
                    Some(id) => Ok(id.clone())
                }
            },
            Some(id) => Ok(id.clone())
        }
    }

    pub fn kill(&self) {

    }
}
