use ahash::AHashMap;
use dashmap::DashMap;
use log::info;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use std::str::FromStr;
use std::{sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tt_types::accounts::account::{AccountName, AccountSnapShot};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::{Instrument, SecurityType, get_symbol_info};

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
    internal_accounts: Arc<DashMap<AccountName, AccountSnapShot>>,
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
                retry_delay_max_ms,
            )?),
            internal_accounts: Arc::new(DashMap::new()),
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
        let _ = self.account_snapshots().await?;
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

    pub async fn auto_update(&self) -> anyhow::Result<()> {
        crate::http::client::PxHttpClient::auto_update_instruments(
            self.inner.clone(),
            self.instruments.clone(),
            ProviderKind::ProjectX(self.firm),
        )
        .await?;
        crate::http::client::PxHttpClient::auto_update_account_ids(
            self.inner.clone(),
            self.internal_accounts.clone(),
        )
        .await?;
        Ok(())
    }

    pub async fn auto_update_instruments(
        inner: Arc<PxHttpInnerClient>,
        instruments: Arc<RwLock<AHashMap<Instrument, FuturesContract>>>,
        id: ProviderKind,
    ) -> anyhow::Result<()> {
        info!("ProjectX Auto Updating instruments");
        let resp: ContractSearchResponse = inner
            .list_all_contracts(true)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut lock = instruments.write().await;
        let mut new = 0;
        for inst in &resp.contracts {
            let instrument = match Instrument::from_str(inst.name.as_str()) {
                Ok(instrument) => instrument,
                Err(e) => {
                    log::error!("Failed to parse instrument: {}: {:?}", inst.name, e);
                    continue;
                }
            };
            if lock.contains_key(&instrument) {
                continue;
            }
            new += 1;
            let symbol = extract_root(&instrument);
            let symbol_info = match get_symbol_info(&symbol) {
                Some(info) => info,
                None => {
                    log::error!("Failed to parse symbol: {}", symbol);
                    continue;
                }
            };
            let mut s = match FuturesContract::from_root_with_default_models(
                &instrument,
                symbol_info.exchange,
                SecurityType::Future,
                inst.id.clone(),
                id,
            ) {
                None => continue,
                Some(s) => s,
            };
            s.value_per_tick =
                Decimal::from_f64(inst.tick_value).unwrap_or_else(|| symbol_info.value_per_tick);
            s.tick_size =
                Decimal::from_f64(inst.tick_size).unwrap_or_else(|| symbol_info.tick_size);

            lock.insert(instrument, s);
        }
        info!(
            "ProjectX Auto Updated instruments Successfully, {} new instruments",
            new
        );
        Ok(())
    }

    pub async fn manual_update_instruments(
        &self,
        live_only: bool,
    ) -> anyhow::Result<Arc<RwLock<AHashMap<Instrument, FuturesContract>>>> {
        info!("ProjectX Manually Updating instruments");
        let resp: ContractSearchResponse = self
            .inner
            .list_all_contracts(live_only)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut lock = self.instruments.write().await;
        let mut new = 0;
        for inst in &resp.contracts {
            let instrument = match Instrument::from_str(inst.name.as_str()) {
                Ok(instrument) => instrument,
                Err(e) => {
                    log::error!("Failed to parse instrument: {}: {:?}", inst.name, e);
                    continue;
                }
            };
            if lock.contains_key(&instrument) {
                continue;
            }
            new += 1;
            let symbol = extract_root(&instrument);
            let symbol_info = match get_symbol_info(&symbol) {
                Some(info) => info,
                None => {
                    log::error!("Failed to parse symbol: {}", symbol);
                    continue;
                }
            };
            let mut s = match FuturesContract::from_root_with_default_models(
                &instrument,
                symbol_info.exchange,
                SecurityType::Future,
                inst.id.clone(),
                ProviderKind::ProjectX(self.firm.clone()),
            ) {
                None => continue,
                Some(s) => s,
            };
            s.value_per_tick =
                Decimal::from_f64(inst.tick_value).unwrap_or_else(|| symbol_info.value_per_tick);
            s.tick_size =
                Decimal::from_f64(inst.tick_size).unwrap_or_else(|| symbol_info.tick_size);

            lock.insert(instrument, s);
        }
        info!(
            "ProjectX Manual Updated instruments Successfully, {} new instruments",
            new
        );
        Ok(self.instruments.clone())
    }

    pub async fn auto_update_account_ids(
        inner: Arc<PxHttpInnerClient>,
        accounts: Arc<DashMap<AccountName, AccountSnapShot>>,
    ) -> anyhow::Result<()> {
        info!("ProjectX Auto Updating account ids");
        let resp = inner.search_accounts(true).await?;

        for acc in &resp.accounts {
            let name = match AccountName::from_str(&acc.name) {
                Ok(name) => name,
                Err(e) => {
                    log::error!("Failed to parse account id: {}", e);
                    continue;
                }
            };
            let snap_shot = AccountSnapShot {
                name: name.clone(),
                id: acc.id,
                balance: Decimal::from_f64(acc.balance).unwrap_or_default(),
                can_trade: acc.can_trade,
            };
            accounts.insert(name, snap_shot);
        }
        Ok(())
    }

    /// Initialise or refresh the account id using the api
    pub async fn account_snapshots(
        &self,
    ) -> anyhow::Result<Arc<DashMap<AccountName, AccountSnapShot>>> {
        let resp = self.inner.search_accounts(true).await?;

        for acc in &resp.accounts {
            let name = match AccountName::from_str(&acc.name) {
                Ok(name) => name,
                Err(e) => {
                    log::error!("Failed to parse account id: {}", e);
                    continue;
                }
            };
            let snap_shot = AccountSnapShot {
                name: name.clone(),
                id: acc.id,
                balance: Decimal::from_f64(acc.balance).unwrap_or_default(),
                can_trade: acc.can_trade,
            };
            self.internal_accounts.insert(name, snap_shot);
        }
        Ok(self.internal_accounts.clone())
    }

    /// Find a correlated nautilus account id from an account name
    pub async fn account_id(&self, account_name: AccountName) -> anyhow::Result<i64> {
        match self.internal_accounts.get(&account_name) {
            None => {
                // if we don't have the account id cached, we need to refresh the list
                let map = self.account_snapshots().await?;
                match map.get(&account_name) {
                    None => Err(anyhow::anyhow!(
                        "Failed to find account id for {}, please check the account name and try again",
                        account_name
                    )),
                    Some(acc) => Ok(acc.id.clone()),
                }
            }
            Some(acc) => Ok(acc.id.clone()),
        }
    }

    pub fn kill(&self) {
        let _ = self.inner.stop_tx.send(true);
    }

    /// Returns a clone of the current instruments map (Instrument -> FuturesContract)
    pub async fn instruments_snapshot(&self) -> AHashMap<Instrument, FuturesContract> {
        self.instruments.read().await.clone()
    }
}
