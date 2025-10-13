use crate::backtest::realism_models::traits::LatencyModel;
use std::time::Duration;

/// ProjectX CME-like latency profile (tight but non-zero).
#[derive(Debug, Clone, Copy)]
pub struct PxLatency {
    pub submit_ack_ms: u64,
    pub ack_fill_ms: u64,
    pub cancel_rtt_ms: u64,
    pub replace_rtt_ms: u64,
}

impl PxLatency {
    pub fn new(
        submit_ack_ms: u64,
        ack_fill_ms: u64,
        cancel_rtt_ms: u64,
        replace_rtt_ms: u64,
    ) -> Self {
        Self {
            submit_ack_ms,
            ack_fill_ms,
            cancel_rtt_ms,
            replace_rtt_ms,
        }
    }
}

impl Default for PxLatency {
    fn default() -> Self {
        Self::new(3, 6, 12, 12)
    }
}

impl LatencyModel for PxLatency {
    fn submit_to_ack(&mut self) -> Duration {
        Duration::from_millis(self.submit_ack_ms)
    }
    fn ack_to_first_fill(&mut self) -> Duration {
        Duration::from_millis(self.ack_fill_ms)
    }
    fn cancel_rtt(&mut self) -> Duration {
        Duration::from_millis(self.cancel_rtt_ms)
    }
    fn replace_rtt(&mut self) -> Duration {
        Duration::from_millis(self.replace_rtt_ms)
    }
    fn positions_refresh_interval(&mut self) -> Duration {
        // Tie the snapshot cadence to order acknowledgment granularity by default.
        Duration::from_millis(self.submit_ack_ms.clamp(1, 250))
    }
}
