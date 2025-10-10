use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct RouterMetrics {
    fanout_sent: AtomicU64,
    fanout_dropped: AtomicU64,
}

impl RouterMetrics {
    pub fn inc_sent(&self, n: u64) {
        self.fanout_sent.fetch_add(n, Ordering::Relaxed);
    }
    pub fn inc_dropped(&self, n: u64) {
        self.fanout_dropped.fetch_add(n, Ordering::Relaxed);
    }
    pub fn sent(&self) -> u64 {
        self.fanout_sent.load(Ordering::Relaxed)
    }
    pub fn dropped(&self) -> u64 {
        self.fanout_dropped.load(Ordering::Relaxed)
    }
}

pub static METRICS: Lazy<RouterMetrics> = Lazy::new(|| RouterMetrics {
    fanout_sent: AtomicU64::new(0),
    fanout_dropped: AtomicU64::new(0),
});

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counters_increment() {
        // reset by creating a fresh local (not possible for static), so test relative increments
        let before_s = METRICS.sent();
        let before_d = METRICS.dropped();
        METRICS.inc_sent(5);
        METRICS.inc_dropped(2);
        assert_eq!(METRICS.sent(), before_s + 5);
        assert_eq!(METRICS.dropped(), before_d + 2);
    }
}
