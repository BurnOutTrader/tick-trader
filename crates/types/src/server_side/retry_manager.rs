use std::marker::PhantomData;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct RetryConfig {
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_factor: f64,
    pub jitter_ms: u64,
    pub immediate_first: bool,
    pub max_elapsed_ms: Option<u64>,
    pub operation_timeout_ms: Option<u64>,
    pub max_retries: u32,
}

impl RetryConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.backoff_factor >= 1.0, "backoff_factor must be >= 1.0");
        anyhow::ensure!(
            self.max_delay_ms >= self.initial_delay_ms,
            "max_delay_ms must be >= initial_delay_ms"
        );
        Ok(())
    }
}

// Minimal exponential backoff with optional jitter and an immediate-first option.
#[derive(Clone, Debug)]
struct ExponentialBackoff {
    current: Duration,
    max: Duration,
    factor: f64,
    jitter_ms: u64,
    immediate_next: bool,
    prng_state: u64,
}

impl ExponentialBackoff {
    fn new(
        initial: Duration,
        max: Duration,
        factor: f64,
        jitter_ms: u64,
        immediate_first: bool,
    ) -> Result<Self, &'static str> {
        if factor < 1.0 {
            return Err("backoff_factor must be >= 1.0");
        }
        if max < initial {
            return Err("max_delay_ms must be >= initial_delay_ms");
        }
        Ok(Self {
            current: initial,
            max,
            factor,
            jitter_ms,
            immediate_next: immediate_first,
            prng_state: 0x9E3779B97F4A7C15, // fixed seed for deterministic jitter
        })
    }

    fn next_duration(&mut self) -> Duration {
        if self.immediate_next {
            self.immediate_next = false;
            return Duration::from_millis(0);
        }

        let base = self.current;

        // Prepare next base for subsequent call.
        let next_ms = (self.current.as_millis() as f64 * self.factor).round() as u128;
        let clamped_next_ms = self.max.as_millis().min(next_ms) as u64;
        self.current = Duration::from_millis(clamped_next_ms);

        // Deterministic pseudo‑random jitter in [0, jitter_ms].
        let jitter = if self.jitter_ms == 0 {
            0
        } else {
            // xorshift64*
            let mut x = self.prng_state;
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            self.prng_state = x;
            let r = x.wrapping_mul(0x2545F4914F6CDD1D);
            r % (self.jitter_ms + 1)
        };

        base.saturating_add(Duration::from_millis(jitter))
            .min(self.max)
    }
}

pub struct RetryManager<E>
where
    E: std::error::Error,
{
    pub config: RetryConfig,
    pub _phantom: PhantomData<E>,
}

// Replace the existing `const fn new(...)` with this validated constructor.
impl<E> RetryManager<E>
where
    E: std::error::Error,
{
    pub fn new(config: RetryConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            _phantom: PhantomData,
        })
    }

    /// Execute an async operation with retries using exponential backoff.
    /// - name: used for logs/metrics (currently unused here)
    /// - operation: factory returning a Future each attempt
    /// - should_retry: decides if a given error is retriable
    /// - create_error: builds a final error message when exhausted
    pub async fn execute_with_retry<T, Fut, FOp, FShould, FErr>(
        &self,
        _name: &str,
        mut operation: FOp,
        mut should_retry: FShould,
        mut create_error: FErr,
    ) -> Result<T, E>
    where
        FOp: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        FShould: FnMut(&E) -> bool,
        FErr: FnMut(String) -> E,
    {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(self.config.initial_delay_ms),
            Duration::from_millis(self.config.max_delay_ms),
            self.config.backoff_factor,
            self.config.jitter_ms,
            self.config.immediate_first,
        )
        .map_err(|msg| create_error(msg.to_string()))?;

        let start = tokio::time::Instant::now();
        let mut attempts: u32 = 0;

        loop {
            attempts = attempts.saturating_add(1);
            // Optional per-operation timeout
            let fut = operation();
            let result = if let Some(op_to_ms) = self.config.operation_timeout_ms {
                match tokio::time::timeout(Duration::from_millis(op_to_ms), fut).await {
                    Ok(res) => res,
                    Err(_elapsed) => {
                        // Treat timeout as a retriable error via create_error wrapper
                        let e = create_error("operation timeout".to_string());
                        Err(e)
                    }
                }
            } else {
                fut.await
            };

            match result {
                Ok(v) => return Ok(v),
                Err(e) => {
                    let retriable = should_retry(&e);
                    let elapsed_ok = self
                        .config
                        .max_elapsed_ms
                        .map(|max| start.elapsed() <= Duration::from_millis(max))
                        .unwrap_or(true);
                    let retries_left = attempts <= self.config.max_retries;
                    if retriable && elapsed_ok && retries_left {
                        let sleep_dur = backoff.next_duration();
                        tokio::time::sleep(sleep_dur).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[derive(Debug, thiserror::Error)]
    #[error("dummy error")]
    struct DummyError;

    #[test]
    fn test_retry_manager_new_valid() {
        let config = RetryConfig {
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_factor: 2.0,
            jitter_ms: 10,
            immediate_first: true,
            max_elapsed_ms: Some(5000),
            operation_timeout_ms: Some(1000),
            max_retries: 3,
        };
        let mgr = RetryManager::<DummyError>::new(config.clone());
        assert!(mgr.is_ok());
        assert_eq!(mgr.unwrap().config, config);
    }

    #[test]
    fn test_retry_manager_new_invalid_factor() {
        let config = RetryConfig {
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_factor: 0.5,
            jitter_ms: 10,
            immediate_first: true,
            max_elapsed_ms: Some(5000),
            operation_timeout_ms: Some(1000),
            max_retries: 3,
        };
        let mgr = RetryManager::<DummyError>::new(config);
        assert!(mgr.is_err());
    }

    #[test]
    fn test_retry_manager_new_invalid_delay() {
        let config = RetryConfig {
            initial_delay_ms: 2000,
            max_delay_ms: 1000,
            backoff_factor: 2.0,
            jitter_ms: 10,
            immediate_first: true,
            max_elapsed_ms: Some(5000),
            operation_timeout_ms: Some(1000),
            max_retries: 3,
        };
        let mgr = RetryManager::<DummyError>::new(config);
        assert!(mgr.is_err());
    }

    #[test]
    fn test_exponential_backoff_immediate_first() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            2.0,
            0,
            true,
        )
        .unwrap();
        // The irst call should be zero if immediate_first
        assert_eq!(backoff.next_duration(), Duration::from_millis(0));
        // The next call should be initial
        assert_eq!(backoff.next_duration(), Duration::from_millis(100));
        // The next call should be doubled
        assert_eq!(backoff.next_duration(), Duration::from_millis(200));
        // The next call should be doubled again
        assert_eq!(backoff.next_duration(), Duration::from_millis(400));
        // The next call should be doubled but clamped to max
        assert_eq!(backoff.next_duration(), Duration::from_millis(800));
        // The next call should be clamped to max
        assert_eq!(backoff.next_duration(), Duration::from_millis(1000));
        assert_eq!(backoff.next_duration(), Duration::from_millis(1000));
    }

    #[test]
    fn test_exponential_backoff_jitter() {
        let mut backoff = ExponentialBackoff::new(
            Duration::from_millis(100),
            Duration::from_millis(1000),
            2.0,
            10,
            false,
        )
        .unwrap();
        let d1 = backoff.next_duration();
        let d2 = backoff.next_duration();
        let d3 = backoff.next_duration();
        // Should be >= base and <= base + jitter, and <= max
        assert!(d1 >= Duration::from_millis(100) && d1 <= Duration::from_millis(110));
        assert!(d2 >= Duration::from_millis(200) && d2 <= Duration::from_millis(210));
        assert!(d3 >= Duration::from_millis(400) && d3 <= Duration::from_millis(410));
    }
}
