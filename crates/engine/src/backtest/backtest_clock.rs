use chrono::DateTime;
use std::sync::atomic::{AtomicU64, Ordering};

// BacktestClock: single source of deterministic simulated time
#[derive(Debug)]
pub struct BacktestClock {
    // nanoseconds since UNIX_EPOCH
    now_ns: AtomicU64,
}
#[allow(dead_code)]
impl BacktestClock {
    pub fn new(start_ns: u64) -> Self {
        Self {
            now_ns: AtomicU64::new(start_ns),
        }
    }
    #[inline]
    pub fn now_ns(&self) -> u64 {
        self.now_ns.load(Ordering::SeqCst)
    }
    #[inline]
    pub fn bump_ns(&self, delta: u64) {
        self.now_ns.fetch_add(delta, Ordering::SeqCst);
    }

    #[allow(dead_code)]
    #[inline]
    pub fn advance_to_at_least(&self, target_ns: u64) {
        let mut cur = self.now_ns.load(Ordering::SeqCst);
        while target_ns > cur {
            match self
                .now_ns
                .compare_exchange(cur, target_ns, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
    #[allow(dead_code)]
    #[inline]
    pub fn now_dt(&self) -> chrono::DateTime<chrono::Utc> {
        let ns = self.now_ns();
        let secs = (ns / 1_000_000_000) as i64;
        let nsub = (ns % 1_000_000_000) as u32;
        DateTime::from_timestamp(secs, nsub).unwrap_or_default()
    }
}
