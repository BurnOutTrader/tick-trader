use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};
use std::str::FromStr;
use anyhow::anyhow;
use chrono::Duration as ChronoDuration;
use dashmap::DashMap;
use reqwest::Method;
use serde::{Serialize, de::DeserializeOwned};
use tokio::{
    sync::{RwLock, watch},
    task::JoinHandle,
};
use ustr::Ustr;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use tokio::sync::watch::Sender;
use tt_bus::MessageBus;
use tt_types::accounts::account::AccountName;
use tt_types::api_helpers::rate_limiter::RateLimiter;
use tt_types::api_helpers::retry_manager::{RetryManager, RetryConfig};
use tt_types::securities::futures_helpers::extract_root;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::{get_symbol_info, Instrument, SecurityType};
use crate::common::consts::{PX_GLOBAL_RATE_KEY, PX_REST_QUOTA, PX_BARS_QUOTA};

// Lightweight reqwest-based HTTP client with per-key rate limiting
#[derive(Clone, Debug)]
struct SimpleResponse {
    status: reqwest::StatusCode,
    body: Vec<u8>,
}

struct SimpleHttp {
    client: reqwest::Client,
    default_headers: HeaderMap,
    rate_limiter: RateLimiter<Ustr>,
}

impl SimpleHttp {
    fn new(
        default_headers: HashMap<String, String>,
        rate_limiter: RateLimiter<Ustr>,
        timeout_secs: Option<u64>,
    ) -> Self {
        let mut headers = HeaderMap::new();
        for (k, v) in default_headers {
            if let (Ok(name), Ok(val)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(&v)) {
                headers.insert(name, val);
            }
        }
        let mut builder = reqwest::Client::builder().default_headers(headers.clone());
        if let Some(secs) = timeout_secs {
            builder = builder.timeout(Duration::from_secs(secs));
        }
        let client = builder.build().expect("failed to build reqwest client");
        Self { client, default_headers: headers, rate_limiter }
    }

    async fn take_slots(&self, keys: &[Ustr]) {
        if keys.is_empty() { return; }
        loop {
            let mut waits = Vec::new();
            for k in keys {
                match self.rate_limiter.check_key(k) {
                    Ok(()) => {},
                    Err(d) => waits.push(d),
                }
            }
            if waits.is_empty() {
                break;
            }
            if let Some(maxw) = waits.into_iter().max() {
                tokio::time::sleep(maxw).await;
            }
        }
    }

    async fn request_with_ustr_keys(
        &self,
        method: Method,
        url: String,
        headers: Option<HashMap<String, String>>,
        body_bytes: Option<Vec<u8>>,
        _query: Option<HashMap<String, String>>,
        rate_keys: Option<Vec<Ustr>>,
    ) -> Result<SimpleResponse, reqwest::Error> {
        if let Some(keys) = &rate_keys {
            self.take_slots(keys).await;
        }
        let mut rb = self.client.request(method, &url);
        if let Some(hs) = headers {
            let mut hmap = HeaderMap::new();
            for (k, v) in hs {
                if let (Ok(name), Ok(val)) = (HeaderName::from_bytes(k.as_bytes()), HeaderValue::from_str(&v)) {
                    hmap.insert(name, val);
                }
            }
            rb = rb.headers(hmap);
        }
        if let Some(bytes) = body_bytes {
            rb = rb.body(bytes);
        }
        let resp = rb.send().await?;
        let status = resp.status();
        let body = resp.bytes().await?.to_vec();
        Ok(SimpleResponse { status, body })
    }
}

use crate::{
    http::{
        credentials::PxCredential,
        endpoints::PxEndpoints,
        error::PxError,
        models::{
            AccountSearchReq, AccountSearchResponse, AvailableContractsReq, CancelOrderReq,
            CancelOrderResponse, CloseContractReq, CloseContractResponse, ContractSearchByIdReq,
            ContractSearchByIdResponse, ContractSearchReq, ContractSearchResponse, ModifyOrderReq,
            ModifyOrderResponse, OrderSearchOpenReq, OrderSearchReq, OrderSearchResponse,
            PartialCloseContractReq, PlaceOrderReq, PlaceOrderResponse, PositionSearchOpenReq,
            PositionSearchResponse, RetrieveBarsReq, RetrieveBarsResponse, TradeSearchReq,
            TradeSearchResponse, ValidateResp,
        },
    },
};

/// HTTP client for the ProjectX Gateway API
pub struct PxHttpInnerClient {
    cfg: PxCredential,
    http: SimpleHttp,
    retry_manager: RetryManager<PxError>,
    token: Arc<RwLock<Option<String>>>,
    auth_headers: Arc<RwLock<HashMap<String, String>>>,
    stop_tx: Arc<watch::Sender<bool>>,
    bg_task: Arc<RwLock<Option<JoinHandle<()>>>>,
    end_points: PxEndpoints,
}

impl PxHttpInnerClient {
    fn default_headers() -> HashMap<String, String> {
        HashMap::from([("User-Agent".to_string(), "Tick Trader".to_string())])
    }

    fn make_auth_headers(token: &str) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), format!("Bearer {}", token));
        headers.insert("accept".to_string(), "text/plain".to_string());
        headers.insert("content-type".to_string(), "application/json".to_string());
        headers
    }

    async fn set_auth_headers(&self, token: &str) {
        let mut guard = self.auth_headers.write().await;
        *guard = Self::make_auth_headers(token);
    }

    fn rate_limit_keys(endpoint: &str) -> Vec<Ustr> {
        let normalized = endpoint.split('?').next().unwrap_or(endpoint);
        // Use base keys instead of full path: always apply global, and add a specific
        // base key for the bars endpoint.
        let mut keys = vec![Ustr::from(PX_GLOBAL_RATE_KEY)];
        if normalized == "/api/History/retrieveBars" {
            keys.push(Ustr::from("px:bars"));
        }
        keys
    }

    fn build_rate_limiter() -> RateLimiter<Ustr> {
        // From policy: All other endpoints 200/60s; retrieveBars 50/30s
        let base = Some((PX_REST_QUOTA as u32, ChronoDuration::seconds(60)));
        let keyed = vec![(Ustr::from("px:bars"), (PX_BARS_QUOTA as u32, ChronoDuration::seconds(30)))];
        RateLimiter::new_with_limits(base, keyed)
    }

    /// Create a new client with the given credentials
    pub fn new(
        cfg: PxCredential,
        timeout_secs: Option<u64>,
        max_retries: Option<u32>,
        retry_delay_ms: Option<u64>,
        retry_delay_max_ms: Option<u64>,
    ) -> anyhow::Result<Self> {
        let (tx, _rx) = watch::channel(false);

        let http = SimpleHttp::new(
            Self::default_headers(),
            Self::build_rate_limiter(),
            timeout_secs,
        );
        let retry_config = RetryConfig {
            max_retries: max_retries.unwrap_or(3),
            initial_delay_ms: retry_delay_ms.unwrap_or(1_000),
            max_delay_ms: retry_delay_max_ms.unwrap_or(10_000),
            backoff_factor: 2.0,
            jitter_ms: 1_000,
            operation_timeout_ms: Some(60_000),
            immediate_first: false,
            max_elapsed_ms: Some(180_000),
        };
        Ok(Self {
            retry_manager: RetryManager::new(retry_config)?,
            end_points: crate::http::endpoints::PxEndpoints::from_firm(cfg.firm.as_str()),
            cfg,
            http,
            token: Arc::new(RwLock::new(None)),
            auth_headers: Arc::new(RwLock::new(HashMap::new())),
            stop_tx: Arc::new(tx),
            bg_task: Arc::new(RwLock::new(None)),
        })
    }


    /// Internal helper to send a JSON HTTP request and deserialize the JSON response
    async fn request_json<TResp, TBody>(
        &self,
        method: Method,
        path: &str,
        body: Option<&TBody>,
    ) -> Result<TResp, PxError>
    where
        TResp: DeserializeOwned,
        TBody: Serialize,
    {
        let base = self.end_points.api_base.trim_end_matches('/');
        let url = format!("{base}{path}");

        let headers = self.auth_headers.read().await.clone();
        let rate_keys = Self::rate_limit_keys(path);
        let body_bytes = if let Some(b) = body {
            Some(serde_json::to_vec(b).map_err(|e| PxError::Other(anyhow!(e)))?)
        } else {
            None
        };

        let op_name = format!("{} {}", method.as_str(), path);
        let operation = {
            let method = method.clone();
            let url = url.clone();
            let headers = headers.clone();
            let body_bytes = body_bytes.clone();
            let rate_keys = rate_keys.clone();
            move || {
                let method = method.clone();
                let url = url.clone();
                let headers = headers.clone();
                let body_bytes = body_bytes.clone();
                let rate_keys = rate_keys.clone();
                async move {
                    let resp = self
                        .http
                        .request_with_ustr_keys(
                            method,
                            url,
                            Some(headers),
                            body_bytes,
                            None,
                            Some(rate_keys),
                        )
                        .await
                        .map_err(PxError::Http)?;

                    if !resp.status.is_success() {
                        let body_txt = String::from_utf8_lossy(&resp.body).to_string();
                        return Err(PxError::UnexpectedStatus {
                            status: resp.status.as_u16(),
                            body: body_txt,
                        });
                    }

                    let out: TResp = serde_json::from_slice(&resp.body)
                        .map_err(|e| PxError::Other(anyhow!(e)))?;
                    Ok(out)
                }
            }
        };

        let should_retry = |err: &PxError| match err {
            PxError::Http(e) => e.is_connect() || e.is_timeout() || e.is_request(),
            PxError::UnexpectedStatus { status, .. } => *status == 429 || *status >= 500,
            PxError::Auth(_) => false,
            PxError::Other(_) => false,
        };
        let create_error = |msg: String| PxError::Other(anyhow!(msg));

        self.retry_manager
            .execute_with_retry(&op_name, operation, should_retry, create_error)
            .await
    }

    /// Validate the current bearer token, rotating it if the server returns a new token
    pub async fn validate(&self) -> Result<(), PxError> {
        const PATH: &str = "/api/Auth/validate";
        let vr: ValidateResp = self
            .request_json::<ValidateResp, serde_json::Value>(Method::POST, PATH, None)
            .await?;

        if let Some(new) = vr.new_token {
            *self.token.write().await = Some(new.clone());
            // Refresh default auth headers with rotated token
            self.set_auth_headers(&new).await;
        }
        Ok(())
    }

    /// Authenticate with the ProjectX API using the configured user name and API key
    pub async fn authenticate(&self) -> Result<(), PxError> {
        const PATH: &str = "/api/Auth/loginKey";
        let url = format!("{}{PATH}", self.end_points.api_base.trim_end_matches('/'));
        let body = serde_json::json!({
            "userName": self.cfg.user_name,
            "apiKey": self.cfg.api_key.to_string()
        });

        let body_bytes = serde_json::to_vec(&body).map_err(|e| PxError::Other(anyhow!(e)))?;
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        let rate_keys = Self::rate_limit_keys(PATH);

        let op_name = format!("POST {}", PATH);
        let operation = {
            let url = url.clone();
            let headers = headers.clone();
            let body_bytes = body_bytes.clone();
            let rate_keys = rate_keys.clone();
            move || {
                let url = url.clone();
                let headers = headers.clone();
                let body_bytes = body_bytes.clone();
                let rate_keys = rate_keys.clone();
                async move {
                    let resp = self
                        .http
                        .request_with_ustr_keys(
                            Method::POST,
                            url,
                            Some(headers),
                            Some(body_bytes),
                            None,
                            Some(rate_keys),
                        )
                        .await
                        .map_err(PxError::Http)?;
                    if !resp.status.is_success() {
                        let body_txt = String::from_utf8_lossy(&resp.body).to_string();
                        return Err(PxError::UnexpectedStatus {
                            status: resp.status.as_u16(),
                            body: body_txt,
                        });
                    }
                    let v: serde_json::Value = serde_json::from_slice(&resp.body)
                        .map_err(|e| PxError::Other(anyhow!(e)))?;
                    let tok = v
                        .get("token")
                        .and_then(|x| x.as_str())
                        .unwrap_or_default()
                        .to_string();
                    if tok.is_empty() {
                        return Err(PxError::Auth("missing token".into()));
                    }
                    Ok(tok)
                }
            }
        };

        let should_retry = |err: &PxError| match err {
            PxError::Http(e) => e.is_connect() || e.is_timeout() || e.is_request(),
            PxError::UnexpectedStatus { status, .. } => *status == 429 || *status >= 500,
            PxError::Auth(_) => false,
            PxError::Other(_) => false,
        };
        let create_error = |msg: String| PxError::Other(anyhow!(msg));

        let tok = self
            .retry_manager
            .execute_with_retry(&op_name, operation, should_retry, create_error)
            .await?;

        *self.token.write().await = Some(tok.clone());
        // Set default auth headers now that we have a token
        self.set_auth_headers(&tok).await;
        Ok(())
    }

    /// Get a clone of the current bearer token if available
    pub async fn token_string(&self) -> Arc<RwLock<Option<String>>> {
        self.token.clone()
    }

    /// Return a human-readable overview of the ProjectX rate limiting policy
    pub fn rate_limit_overview() -> &'static str {
        "Overview\nThe Gateway API employs a rate limiting system for all authenticated requests. Its goal is to promote fair usage, prevent abuse, and ensure the stability and reliability of the service, while clearly defining the level of performance clients can expect.\n\nRate Limit Table\nEndpoint(s)\tLimit\nPOST /api/History/retrieveBars\t50 requests / 30 seconds\nAll other Endpoints\t200 requests / 60 seconds\nWhat Happens If You Exceed Rate Limits?\nIf you exceed the allowed rate limits, the API will respond with an HTTP 429 Too Many Requests error. When this occurs, you should reduce your request frequency and try again after a short delay."
    }

    /// Perform an authenticated GET request and deserialize the JSON response
    pub async fn get_json<T: for<'de> serde::Deserialize<'de>>(
        &self,
        path: &str,
    ) -> Result<T, PxError> {
        let base = self.end_points.api_base.trim_end_matches('/');
        let url = format!("{base}{path}");

        let mut headers = self.auth_headers.read().await.clone();
        headers.insert("accept".to_string(), "application/json".to_string());
        let rate_keys = Self::rate_limit_keys(path);

        let op_name = format!("GET {}", path);
        let operation = {
            let url = url.clone();
            let headers = headers.clone();
            let rate_keys = rate_keys.clone();
            move || {
                let url = url.clone();
                let headers = headers.clone();
                let rate_keys = rate_keys.clone();
                async move {
                    let resp = self
                        .http
                        .request_with_ustr_keys(
                            Method::GET,
                            url,
                            Some(headers),
                            None,
                            None,
                            Some(rate_keys),
                        )
                        .await
                        .map_err(PxError::Http)?;
                    if !resp.status.is_success() {
                        let body_txt = String::from_utf8_lossy(&resp.body).to_string();
                        return Err(PxError::UnexpectedStatus {
                            status: resp.status.as_u16(),
                            body: body_txt,
                        });
                    }
                    let v: T = serde_json::from_slice(&resp.body)
                        .map_err(|e| PxError::Other(anyhow!(e)))?;
                    Ok(v)
                }
            }
        };

        let should_retry = |err: &PxError| match err {
            PxError::Http(e) => e.is_connect() || e.is_timeout() || e.is_request(),
            PxError::UnexpectedStatus { status, .. } => *status == 429 || *status >= 500,
            PxError::Auth(_) => false,
            PxError::Other(_) => false,
        };
        let create_error = |msg: String| PxError::Other(anyhow!(msg));

        self.retry_manager
            .execute_with_retry(&op_name, operation, should_retry, create_error)
            .await
    }

    /// Expose the RTC base URL (without the `/hubs/...` suffix) for websocket clients.
    pub fn rtc_base(&self) -> String {
        // Prefer market hub, fall back to user hub
        let hub = self.end_points.market_hub.as_str();
        if let Some(base) = hub.strip_suffix("/hubs/market") {
            base.to_string()
        } else if let Some(base) = hub.strip_suffix("/hubs/user") {
            base.to_string()
        } else {
            // If already a base URL, return as-is
            hub.to_string()
        }
    }

    /// Search for accounts
    /// POST /api/Account/search
    /// Parameter: onlyActiveAccounts (boolean, required)
    pub async fn search_accounts(
        &self,
        only_active_accounts: bool,
    ) -> Result<AccountSearchResponse, PxError> {
        const PATH: &str = "/api/Account/search";

        let body = AccountSearchReq {
            only_active_accounts,
        };

        self.request_json::<AccountSearchResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Retrieve historical bars
    /// POST /api/History/retrieveBars
    /// Note: maximum of 20,000 bars per request enforced by the server
    pub async fn retrieve_bars(
        &self,
        req: &RetrieveBarsReq,
    ) -> Result<RetrieveBarsResponse, PxError> {
        const PATH: &str = "/api/History/retrieveBars";

        self.request_json::<RetrieveBarsResponse, _>(Method::POST, PATH, Some(req))
            .await
    }

    /// List available contracts
    /// POST /api/Contract/available
    pub async fn available_contracts(&self, live: bool) -> Result<ContractSearchResponse, PxError> {
        const PATH: &str = "/api/Contract/available";
        let body = AvailableContractsReq { live };
        self.request_json::<ContractSearchResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Search for contracts, returns up to 20 results
    /// POST /api/Contract/search
    pub async fn search_contracts(
        &self,
        live: bool,
        search_text: impl Into<String>,
    ) -> Result<ContractSearchResponse, PxError> {
        const PATH: &str = "/api/Contract/search";
        let body = ContractSearchReq {
            live,
            search_text: search_text.into(),
        };
        self.request_json::<ContractSearchResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Search for a specific contract by its ID
    /// POST /api/Contract/searchById
    pub async fn search_contract_by_id(
        &self,
        contract_id: impl Into<String>,
    ) -> Result<ContractSearchByIdResponse, PxError> {
        const PATH: &str = "/api/Contract/searchById";
        let body = ContractSearchByIdReq {
            contract_id: contract_id.into(),
        };
        self.request_json::<ContractSearchByIdResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Search for orders
    /// POST /api/Order/search
    pub async fn search_orders(
        &self,
        req: &OrderSearchReq,
    ) -> Result<OrderSearchResponse, PxError> {
        const PATH: &str = "/api/Order/search";
        self.request_json::<OrderSearchResponse, _>(Method::POST, PATH, Some(req))
            .await
    }

    /// Search for open orders
    /// POST /api/Order/searchOpen
    pub async fn search_open_orders(
        &self,
        account_id: i64,
    ) -> Result<OrderSearchResponse, PxError> {
        const PATH: &str = "/api/Order/searchOpen";
        let body = OrderSearchOpenReq { account_id };
        self.request_json::<OrderSearchResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Place an order
    /// POST /api/Order/place
    pub async fn place_order(&self, req: &PlaceOrderReq) -> Result<PlaceOrderResponse, PxError> {
        const PATH: &str = "/api/Order/place";
        self.request_json::<PlaceOrderResponse, _>(Method::POST, PATH, Some(req))
            .await
    }

    /// Cancel an order
    /// POST /api/Order/cancel
    pub async fn cancel_order(
        &self,
        account_id: i64,
        order_id: i64,
    ) -> Result<CancelOrderResponse, PxError> {
        const PATH: &str = "/api/Order/cancel";
        let body = CancelOrderReq {
            account_id,
            order_id,
        };
        self.request_json::<CancelOrderResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Modify an open order
    /// POST /api/Order/modify
    pub async fn modify_order(&self, req: &ModifyOrderReq) -> Result<ModifyOrderResponse, PxError> {
        const PATH: &str = "/api/Order/modify";
        self.request_json::<ModifyOrderResponse, _>(Method::POST, PATH, Some(req))
            .await
    }

    /// Search for open positions
    /// POST /api/Position/searchOpen
    pub async fn search_open_positions(
        &self,
        account_id: i64,
    ) -> Result<PositionSearchResponse, PxError> {
        const PATH: &str = "/api/Position/searchOpen";
        let body = PositionSearchOpenReq { account_id };
        self.request_json::<PositionSearchResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Close a position for a contract
    /// POST /api/Position/closeContract
    pub async fn close_contract(
        &self,
        account_id: i64,
        contract_id: impl Into<String>,
    ) -> Result<CloseContractResponse, PxError> {
        const PATH: &str = "/api/Position/closeContract";
        let body = CloseContractReq {
            account_id,
            contract_id: contract_id.into(),
        };
        self.request_json::<CloseContractResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Partially close a position for a contract
    /// POST /api/Position/partialCloseContract
    pub async fn partial_close_contract(
        &self,
        account_id: i64,
        contract_id: impl Into<String>,
        size: i64,
    ) -> Result<CloseContractResponse, PxError> {
        const PATH: &str = "/api/Position/partialCloseContract";
        let body = PartialCloseContractReq {
            account_id,
            contract_id: contract_id.into(),
            size,
        };
        self.request_json::<CloseContractResponse, _>(Method::POST, PATH, Some(&body))
            .await
    }

    /// Search for trades.
    /// POST /api/Trade/search
    pub async fn search_trades(
        &self,
        req: &TradeSearchReq,
    ) -> Result<TradeSearchResponse, PxError> {
        const PATH: &str = "/api/Trade/search";
        self.request_json::<TradeSearchResponse, _>(Method::POST, PATH, Some(req))
            .await
    }
}


#[derive(Clone)]
pub struct PxHttpClient {
    pub firm: String,
    pub inner: Arc<PxHttpInnerClient>,
    internal_accounts_ids: Arc<DashMap<AccountName, i64>>,
    cache_initialized: bool,
    bus: Arc<MessageBus>,
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
        bus: Arc<MessageBus>,
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
            cache_initialized: false,
            bus
        })
    }

    /// Authenticate once, then start background token validation on an interval,
    /// Returns Ok when the task is spawned.
    pub async fn start(&self, token_update_sender: Sender<String>,) -> Result<(), PxError> {
        // 1) authenticate once
        self.inner.authenticate().await?;

        // 1.5) send initial token to downstream listeners (e.g., websocket client)
        if let Some(tok) = self.inner.token.read().await.clone() {
            // Send the freshly authenticated token so listeners can initialize
            let _ = token_update_sender.send_replace(tok);
        }

        // 2) spawn background validator (client-managed)
        self.spawn_auto_validate(Duration::from_secs(12 * 3600), token_update_sender)
            .await; // ~12h
        Ok(())
    }

    /// Spawns Autovalidate to update token every 12 hours.
    /// Stores the joinhandle in the inner client so that it is maintained on clone and drop of this object via the Arc<InnerClient>
    async fn spawn_auto_validate(&self, period: Duration, token_update_sender: Sender<String>,) {
        let mut rx = self.inner.stop_tx.subscribe();
        let client = self.inner.clone();
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            // track last sent token to avoid redundant notifications
            let mut last_token: Option<String> = None;
            // attempt to read and send current token at startup
            if let Some(cur) = client.token.read().await.clone() {
                last_token = Some(cur.clone());
                let _ = token_update_sender.send_replace(cur);
            }
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if client.validate().await.is_ok() {
                            if let Some(cur) = client.token.read().await.clone() {
                                if last_token.as_ref().map(|s| s.as_str()) != Some(cur.as_str()) {
                                    last_token = Some(cur.clone());
                                    let _ = token_update_sender.send_replace(cur);
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

    pub async fn request_instruments(&self) -> anyhow::Result<Vec<FuturesContract>> {
        let resp: ContractSearchResponse = self
            .inner
            .available_contracts(false)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        let mut instruments: Vec<FuturesContract> = Vec::new();
        for inst in &resp.contracts {
            let instrument = match Instrument::from_str(inst.id.as_str()) {
                Ok(instrument) => instrument,
                Err(e) => {
                    log::error!("Failed to parse instrument: {}: {:?}", inst.id, e);
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
            let s = match FuturesContract::from_root_with_default_models(&instrument, symbol_info.exchange, SecurityType::Future) {
                None => continue,
                Some(s) => s
            };
            instruments.push(s);
        }

        Ok(instruments)
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
        let id = self.internal_accounts_ids
            .get(&account_name)
            .ok_or_else(|| anyhow::anyhow!(format!("Account {:?} ID not found", account_name)))?;
        Ok(id.clone())
    }

    pub fn kill(&self) {

    }
}
