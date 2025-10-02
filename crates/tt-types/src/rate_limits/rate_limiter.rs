use std::hash::Hash;
use std::thread::sleep;
use chrono::Duration;
use crate::rate_limits::quota::Quota;

/// A rate limiter that enforces different quotas per key using the GCRA algorithm.
///
/// This implementation allows setting different rate limits for different keys,
/// with an optional default quota for keys that don't have specific quotas.
pub struct RateLimiter<K, C>
where
    C: Clock,
{
    default_gcra: Option<Gcra>,
    state: DashMapStateStore<K>,
    gcra: DashMap<K, Gcra>,
    clock: C,
    start: C::Instant,
}

impl<K, C> Debug for RateLimiter<K, C>
where
    K: Debug,
    C: Clock,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(RateLimiter)).finish()
    }
}

impl<K> RateLimiter<K, MonotonicClock>
where
    K: Eq + Hash,
{
    /// Creates a new rate limiter with a base quota and keyed quotas.
    ///
    /// The base quota applies to all keys that don't have specific quotas.
    /// Keyed quotas override the base quota for specific keys.
    #[must_use]
    pub fn new_with_quota(base_quota: Option<Quota>, keyed_quotas: Vec<(K, Quota)>) -> Self {
        let clock = MonotonicClock {};
        let start = MonotonicClock::now(&clock);
        let gcra = DashMap::from_iter(keyed_quotas.into_iter().map(|(k, q)| (k, Gcra::new(q))));
        Self {
            default_gcra: base_quota.map(Gcra::new),
            state: DashMapStateStore::new(),
            gcra,
            clock,
            start,
        }
    }
}

impl<K> RateLimiter<K, FakeRelativeClock>
where
    K: Hash + Eq + Clone,
{
    /// Advances the fake clock by the specified duration.
    ///
    /// This is only available for testing with `FakeRelativeClock`.
    pub fn advance_clock(&self, by: Duration) {
        self.clock.advance(by);
    }
}

impl<K, C> RateLimiter<K, C>
where
    K: Hash + Eq + Clone,
    C: Clock,
{
    /// Adds or updates a quota for a specific key.
    pub fn add_quota_for_key(&self, key: K, value: Quota) {
        self.gcra.insert(key, Gcra::new(value));
    }

    /// Checks if the given key is allowed under the rate limit.
    ///
    /// # Errors
    ///
    /// Returns `Err(NotUntil)` if the key is rate-limited, indicating when it will be allowed.
    pub fn check_key(&self, key: &K) -> Result<(), NotUntil<C::Instant>> {
        match self.gcra.get(key) {
            Some(quota) => quota.test_and_update(self.start, key, &self.state, self.clock.now()),
            None => self.default_gcra.as_ref().map_or(Ok(()), |gcra| {
                gcra.test_and_update(self.start, key, &self.state, self.clock.now())
            }),
        }
    }

    /// Waits until the specified key is ready (not rate-limited).
    pub async fn until_key_ready(&self, key: &K) {
        loop {
            match self.check_key(key) {
                Ok(()) => {
                    break;
                }
                Err(neg) => {
                    sleep(neg.wait_time_from(self.clock.now())).await;
                }
            }
        }
    }

    /// Waits until all specified keys are ready (not rate-limited).
    ///
    /// If no keys are provided, this function returns immediately.
    pub async fn await_keys_ready(&self, keys: Option<Vec<K>>) {
        let keys = keys.unwrap_or_default();
        let tasks = keys.iter().map(|key| self.until_key_ready(key));

        futures::stream::iter(tasks)
            .for_each_concurrent(None, |key_future| async move {
                key_future.await;
            })
            .await;
    }
}