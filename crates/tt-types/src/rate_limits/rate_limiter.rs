use std::{collections::HashMap, hash::Hash, sync::Mutex, time::{Duration, Instant}};
use futures::StreamExt;
use tokio::time::sleep;
use crate::rate_limits::quota::Quota;

/// A simple per-key token-bucket rate limiter using the system clock.
///
/// Allows setting a default quota and per-key quotas. Uses std::time::Instant
/// for monotonic time and tokio::time::sleep for async waiting.
pub struct RateLimiter<K>
where
    K: Eq + Hash + Clone,
{
    default_quota: Option<Quota>,
    keyed_quotas: Mutex<HashMap<K, Quota>>,    // per-key quotas override default
    states: Mutex<HashMap<K, BucketState>>,     // per-key token buckets
}

#[derive(Clone, Copy, Debug)]
struct BucketState {
    tokens: f64,
    last: Instant,
    // cached params for speed
    max_burst: f64,
    replenish_secs: f64, // seconds per token
}

impl<K> RateLimiter<K>
where
    K: Eq + Hash + Clone,
{
    /// Creates a new rate limiter with an optional default quota and keyed quotas.
    #[must_use]
    pub fn new_with_quota(base_quota: Option<Quota>, keyed_quotas: Vec<(K, Quota)>) -> Self {
        let keyed_quotas_map = keyed_quotas.into_iter().collect::<HashMap<_, _>>();
        Self {
            default_quota: base_quota,
            keyed_quotas: Mutex::new(keyed_quotas_map),
            states: Mutex::new(HashMap::new()),
        }
    }

    /// Adds or updates a quota for a specific key.
    pub fn add_quota_for_key(&self, key: K, value: Quota) {
        let mut q = self.keyed_quotas.lock().unwrap();
        q.insert(key, value);
    }

    /// Checks if the given key is allowed under the rate limit.
    /// Returns Ok(()) if allowed now, or Err(Duration) indicating how long to wait.
    pub fn check_key(&self, key: &K) -> Result<(), Duration> {
        let quota = {
            let q = self.keyed_quotas.lock().unwrap();
            q.get(key).copied().or(self.default_quota)
        };

        let Some(q) = quota else {
            // No quota configured: allow
            return Ok(());
        };

        let mut states = self.states.lock().unwrap();
        let state = states.entry(key.clone()).or_insert_with(|| BucketState {
            tokens: q.burst_size().get() as f64, // start full
            last: Instant::now(),
            max_burst: q.burst_size().get() as f64,
            replenish_secs: q.replenish_interval().as_secs_f64(),
        });

        // Refill based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(state.last).as_secs_f64();
        if state.replenish_secs > 0.0 {
            let rate = 1.0 / state.replenish_secs; // tokens per second
            state.tokens = (state.tokens + elapsed * rate).min(state.max_burst);
        }
        state.last = now;

        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            Ok(())
        } else {
            // compute time until one token available
            if state.replenish_secs == 0.0 {
                // Should not happen, but avoid div-by-zero
                return Err(Duration::from_secs(1));
            }
            let deficit = 1.0 - state.tokens;
            let rate = 1.0 / state.replenish_secs; // tokens per second
            let secs = deficit / rate; // seconds until one token
            let wait = Duration::from_secs_f64(secs.max(0.0));
            Err(wait)
        }
    }

    /// Waits until the specified key is ready (not rate-limited).
    pub async fn until_key_ready(&self, key: &K) {
        loop {
            match self.check_key(key) {
                Ok(()) => break,
                Err(wait) => sleep(wait).await,
            }
        }
    }

    /// Waits until all specified keys are ready (not rate-limited).
    /// If no keys are provided, this function returns immediately.
    pub async fn await_keys_ready(&self, keys: Option<Vec<K>>) {
        let keys = keys.unwrap_or_default();
        let tasks = keys.iter().map(|key| self.until_key_ready(key));
        futures::stream::iter(tasks)
            .for_each_concurrent(None, |fut| async move { fut.await })
            .await;
    }
}