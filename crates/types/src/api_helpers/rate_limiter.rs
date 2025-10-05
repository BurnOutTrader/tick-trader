use chrono::Duration as ChronoDuration;
use futures::StreamExt;
use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::Mutex,
    time::Duration as StdDuration,
};
use tokio::time::{Instant as TokioInstant, sleep};

/// A per-key sliding-window rate limiter using the system (tokio) clock.
///
/// For each key, at most `max` events are permitted in any rolling `per` duration window.
/// If the current key has already reached the limit (equal) or would exceed it, calls will
/// return a wait Duration; `until_key_ready` awaits that time.
pub struct RateLimiter<K>
where
    K: Eq + Hash + Clone,
{
    default_limit: Option<WindowLimit>,
    keyed_limits: Mutex<HashMap<K, WindowLimit>>, // per-key limits override default
    states: Mutex<HashMap<K, VecDeque<TokioInstant>>>, // per-key recent event timestamps
}

#[derive(Clone, Copy, Debug)]
struct WindowLimit {
    max: u32,
    per: StdDuration,
}

impl WindowLimit {
    fn new(max: u32, per: ChronoDuration) -> Self {
        assert!(max > 0, "max must be > 0");
        let per_std = per.to_std().expect("per must be positive");
        assert!(per_std > StdDuration::from_nanos(0), "per must be > 0");
        Self { max, per: per_std }
    }
}

impl<K> RateLimiter<K>
where
    K: Eq + Hash + Clone,
{
    /// Creates a new rate limiter with an optional default limit and keyed limits.
    /// The limit is expressed as (max, per) where per is a chrono::Duration (seconds-preferrable).
    #[must_use]
    pub fn new_with_limits(
        base_limit: Option<(u32, ChronoDuration)>,
        keyed_limits: Vec<(K, (u32, ChronoDuration))>,
    ) -> Self {
        let keyed_limits_map = keyed_limits
            .into_iter()
            .map(|(k, (m, d))| (k, WindowLimit::new(m, d)))
            .collect::<HashMap<_, _>>();
        Self {
            default_limit: base_limit.map(|(m, d)| WindowLimit::new(m, d)),
            keyed_limits: Mutex::new(keyed_limits_map),
            states: Mutex::new(HashMap::new()),
        }
    }

    fn limit_for_key(&self, key: &K) -> Option<WindowLimit> {
        let q = self.keyed_limits.lock().unwrap();
        q.get(key).copied().or(self.default_limit)
    }

    fn time_until_ready_locked(
        deque: &mut VecDeque<TokioInstant>,
        limit: WindowLimit,
    ) -> Option<StdDuration> {
        let now = TokioInstant::now();
        let window_start = now - limit.per;
        while let Some(&front) = deque.front() {
            if front <= window_start {
                deque.pop_front();
            } else {
                break;
            }
        }
        if (deque.len() as u32) < limit.max {
            None
        } else {
            let oldest = *deque.front().expect("non-empty due to len >= max");
            let elapsed = now.duration_since(oldest);
            Some(limit.per.saturating_sub(elapsed))
        }
    }

    /// Checks if the given key is allowed under the rate limit.
    /// Returns Ok(()) if allowed now, or Err(Duration) indicating how long to wait
    /// until the next event for this key is permitted.
    pub fn check_key(&self, key: &K) -> Result<(), StdDuration> {
        let Some(limit) = self.limit_for_key(key) else {
            return Ok(());
        };

        let mut states = self.states.lock().unwrap();
        let deque = states.entry(key.clone()).or_insert_with(VecDeque::new);

        match Self::time_until_ready_locked(deque, limit) {
            None => {
                // Consume one slot by recording now
                deque.push_back(TokioInstant::now());
                Ok(())
            }
            Some(wait) => Err(wait),
        }
    }

    /// Waits until the specified key is ready (not rate-limited). Does not consume a slot.
    pub async fn until_key_ready(&self, key: &K) {
        loop {
            let wait = {
                let Some(limit) = self.limit_for_key(key) else {
                    return; // no limit configured
                };
                let mut states = self.states.lock().unwrap();
                let deque = states.entry(key.clone()).or_insert_with(VecDeque::new);
                Self::time_until_ready_locked(deque, limit)
            };
            match wait {
                None => break,
                Some(d) => sleep(d).await,
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn limit(max: u32, secs: i64) -> (u32, ChronoDuration) {
        (max, ChronoDuration::seconds(secs))
    }

    #[tokio::test(start_paused = true)]
    async fn allows_up_to_max_then_blocks() {
        let rl = RateLimiter::new_with_limits(Some(limit(3, 1)), vec![]);
        let key = "k".to_string();
        // First 3 allowed
        assert!(rl.check_key(&key).is_ok());
        assert!(rl.check_key(&key).is_ok());
        assert!(rl.check_key(&key).is_ok());
        // At limit: next blocks
        let err = rl.check_key(&key).unwrap_err();
        assert!(err >= StdDuration::from_millis(900) && err <= StdDuration::from_secs(1));
        // Advance less than 1s: still blocked
        tokio::time::advance(StdDuration::from_millis(500)).await;
        assert!(rl.check_key(&key).is_err());
        // Advance remainder: allow
        tokio::time::advance(StdDuration::from_millis(500)).await;
        assert!(rl.check_key(&key).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn replenishes_strictly_on_window_expiry() {
        let rl = RateLimiter::new_with_limits(Some(limit(2, 1)), vec![]);
        let key = "w".to_string();
        // Use 2 immediately
        assert!(rl.check_key(&key).is_ok());
        assert!(rl.check_key(&key).is_ok());
        // Next should block ~1s
        let wait = rl.check_key(&key).unwrap_err();
        assert!(wait >= StdDuration::from_millis(900));
        // Advance 999ms: still blocked
        tokio::time::advance(StdDuration::from_millis(999)).await;
        assert!(rl.check_key(&key).is_err());
        // Advance 1ms: now allowed
        tokio::time::advance(StdDuration::from_millis(1)).await;
        assert!(rl.check_key(&key).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn per_key_limits_independent() {
        let rl = RateLimiter::new_with_limits(
            None,
            vec![
                ("a".to_string(), limit(1, 1)),
                ("b".to_string(), limit(2, 1)),
            ],
        );
        // a allows 1
        assert!(rl.check_key(&"a".to_string()).is_ok());
        assert!(rl.check_key(&"a".to_string()).is_err());
        // b allows 2
        assert!(rl.check_key(&"b".to_string()).is_ok());
        assert!(rl.check_key(&"b".to_string()).is_ok());
        assert!(rl.check_key(&"b".to_string()).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn no_limit_means_unlimited() {
        let rl = RateLimiter::<String>::new_with_limits(None, vec![]);
        let key = "free".to_string();
        for _ in 0..100 {
            assert!(rl.check_key(&key).is_ok());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn until_key_ready_waits_for_expiry() {
        let rl = RateLimiter::new_with_limits(Some(limit(1, 1)), vec![]);
        let key = "wait".to_string();
        assert!(rl.check_key(&key).is_ok());
        // Next should wait ~1s
        let fut = rl.until_key_ready(&key);
        tokio::pin!(fut);
        // Not ready immediately
        assert!(fut.as_mut().now_or_never().is_none());
        tokio::time::advance(StdDuration::from_secs(1)).await;
        fut.await;
        // Now allowed again
        assert!(rl.check_key(&key).is_ok());
    }

    #[tokio::test(start_paused = true)]
    async fn await_keys_ready_for_multiple_windows() {
        let rl = RateLimiter::new_with_limits(
            None,
            vec![
                ("a".to_string(), limit(1, 1)),
                ("b".to_string(), limit(1, 2)),
            ],
        );
        let _ = rl.check_key(&"a".to_string());
        let _ = rl.check_key(&"b".to_string());
        let fut = rl.await_keys_ready(Some(vec!["a".to_string(), "b".to_string()]));
        tokio::pin!(fut);
        // Not ready immediately
        assert!(fut.as_mut().now_or_never().is_none());
        // Advance 1s: a ready, b not yet
        tokio::time::advance(StdDuration::from_secs(1)).await;
        assert!(fut.as_mut().now_or_never().is_none());
        // Advance another 1s: b ready
        tokio::time::advance(StdDuration::from_secs(1)).await;
        fut.await;
        assert!(rl.check_key(&"a".to_string()).is_ok());
        assert!(rl.check_key(&"b".to_string()).is_ok());
    }
}
