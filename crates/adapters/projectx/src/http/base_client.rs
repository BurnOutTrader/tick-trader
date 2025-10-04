use std::collections::HashMap;
use std::time::Duration;
use tt_types::api_helpers::rate_limiter::RateLimiter;
use ustr::Ustr;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::Method;

// Lightweight reqwest-based HTTP client with per-key rate limiting
#[derive(Clone, Debug)]
pub(crate) struct SimpleResponse {
    pub(crate) status: reqwest::StatusCode,
    pub(crate) body: Vec<u8>,
}

pub struct SimpleHttp {
    client: reqwest::Client,
    default_headers: HeaderMap,
    rate_limiter: RateLimiter<Ustr>,
}

impl SimpleHttp {
    pub(crate) fn new(
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

    pub(crate) async fn request_with_ustr_keys(
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