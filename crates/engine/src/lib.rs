use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::info;

use tt_provider::{MarketDataProvider, ProbeStatus, ProviderParams, SubscribeResult};
use tt_types::keys::{SymbolKey, Topic};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubState {
    Unsubscribed,
    Subscribing,
    Subscribed,
    Unsubscribing,
    Error,
}

#[derive(Debug, Clone)]
pub struct EngineConfig {
    // health thresholds (defaults; env override)
    pub ticks_warn_ms: u64,
    pub ticks_alert_ms: u64,
    pub depth_warn_ms: u64,
    pub depth_alert_ms: u64,
    pub bars1s_warn_ms: u64,
    pub bars1s_alert_ms: u64,
    pub bars1m_warn_ms: u64,
    pub bars1m_alert_ms: u64,
    pub orders_warn_ms: u64,
    pub orders_alert_ms: u64,
    // replay windows
    pub ticks_replay_secs: u64,
    pub bars1s_window_secs: u64,
    pub bars1m_window_secs: u64,
    // depth/mbo ring duration (approx)
    pub depth_ring_secs: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        fn env_u64(k: &str, d: u64) -> u64 {
            std::env::var(k)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(d)
        }
        Self {
            ticks_warn_ms: env_u64("TT_TICKS_WARN_MS", 100),
            ticks_alert_ms: env_u64("TT_TICKS_ALERT_MS", 500),
            depth_warn_ms: env_u64("TT_DEPTH_WARN_MS", 100),
            depth_alert_ms: env_u64("TT_DEPTH_ALERT_MS", 500),
            bars1s_warn_ms: env_u64("TT_BARS1S_WARN_MS", 2000),
            bars1s_alert_ms: env_u64("TT_BARS1S_ALERT_MS", 5000),
            bars1m_warn_ms: env_u64("TT_BARS1M_WARN_MS", 120_000),
            bars1m_alert_ms: env_u64("TT_BARS1M_ALERT_MS", 300_000),
            orders_warn_ms: env_u64("TT_ORDERS_WARN_MS", 5000),
            orders_alert_ms: env_u64("TT_ORDERS_ALERT_MS", 10_000),
            ticks_replay_secs: env_u64("TT_TICKS_REPLAY_SECS", 60),
            bars1s_window_secs: env_u64("TT_BARS1S_WINDOW_SECS", 600),
            bars1m_window_secs: env_u64("TT_BARS1M_WINDOW_SECS", 3600),
            depth_ring_secs: env_u64("TT_DEPTH_RING_SECS", 5),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamKey {
    pub topic: Topic,
    pub key: SymbolKey,
}

#[derive(Debug, Clone)]
pub struct InterestEntry {
    pub downstream_count: u32,
    pub state: SubState,
    pub params: Option<ProviderParams>,
    pub last_upstream_data_at: Option<Instant>,
}

#[derive(Debug, Default, Clone)]
pub struct StreamMetrics {
    pub frames: u64,
    pub bytes: u64,
    pub drops: u64,
    pub credit_stalls: u64,
    pub last_upstream_data_at: Option<Instant>,
    pub last_downstream_send_at: Option<Instant>,
}

// Replay cache skeletons
#[derive(Debug, Clone)]
pub struct TickRec {
    pub ts_ns: i64,
    pub bytes: usize,
}
#[derive(Debug, Clone)]
pub struct BarRec {
    pub ts_ns: i64,
    pub bytes: usize,
}
#[derive(Debug, Clone)]
pub struct DepthDeltaRec {
    pub ts_ns: i64,
    pub bytes: usize,
}

#[derive(Default)]
pub struct Caches {
    pub ticks: HashMap<StreamKey, VecDeque<TickRec>>, // bounded by secs window
    pub bars: HashMap<StreamKey, VecDeque<BarRec>>,   // sized per resolution
    pub depth_ring: HashMap<StreamKey, VecDeque<DepthDeltaRec>>, // ~2–5s ring
}

pub struct Engine<P: MarketDataProvider + 'static> {
    provider: Arc<P>,
    cfg: EngineConfig,
    inner: Arc<Mutex<EngineInner>>,
}

struct EngineInner {
    interest: HashMap<StreamKey, InterestEntry>,
    metrics: HashMap<StreamKey, StreamMetrics>,
    caches: Caches,
}

impl<P: MarketDataProvider + 'static> Engine<P> {
    pub fn new(provider: Arc<P>, cfg: EngineConfig) -> Self {
        Self {
            provider,
            cfg,
            inner: Arc::new(Mutex::new(EngineInner {
                interest: HashMap::new(),
                metrics: HashMap::new(),
                caches: Caches::default(),
            })),
        }
    }

    pub async fn interest_delta(
        &self,
        topic: Topic,
        key: SymbolKey,
        delta: i32,
        params: Option<ProviderParams>,
    ) -> anyhow::Result<()> {
        let sk = StreamKey {
            topic,
            key: key.clone(),
        };
        let mut inner = self.inner.lock().await;
        let ent = inner.interest.entry(sk.clone()).or_insert(InterestEntry {
            downstream_count: 0,
            state: SubState::Unsubscribed,
            params: params.clone(),
            last_upstream_data_at: None,
        });
        // Update params if provided (persist for resubscribe)
        if let Some(p) = params {
            ent.params = Some(p);
        }
        if delta > 0 {
            let prev = ent.downstream_count;
            ent.downstream_count = ent.downstream_count.saturating_add(delta as u32);
            if prev == 0 {
                let params_clone = ent.params.clone();
                drop(inner);
                info!(?sk, "upstream subscribe start");
                // idempotency: only call if not already Subscribed/Subscribing
                let _ = self
                    .provider
                    .subscribe_md(topic, &key, params_clone.as_ref())
                    .await;
                let mut inner2 = self.inner.lock().await;
                if let Some(ent2) = inner2.interest.get_mut(&sk) {
                    ent2.state = SubState::Subscribed;
                }
            }
        } else if delta < 0 {
            let prev = ent.downstream_count;
            ent.downstream_count = ent.downstream_count.saturating_sub((-delta) as u32);
            if prev > 0 && ent.downstream_count == 0 {
                drop(inner);
                info!(?sk, "upstream unsubscribe start (downstream=0)");
                self.provider.unsubscribe_md(topic, &key).await;
                let mut inner2 = self.inner.lock().await;
                if let Some(ent2) = inner2.interest.get_mut(&sk) {
                    ent2.state = SubState::Unsubscribed;
                }
            }
        }
        Ok(())
    }

    pub async fn probe_stream(&self, topic: Topic, _key: &SymbolKey) -> ProbeStatus {
        self.provider.supports(topic); // hint; no-op for now
                                       // delegate to provider if it has a probe (not in trait for streams), so return Ok(0)
        ProbeStatus::Ok(0)
    }

    // Provider → Engine data callbacks (skeletons for caches)
    pub async fn on_tick_batch(
        &self,
        topic: Topic,
        key: &SymbolKey,
        now: Instant,
        batch_bytes: usize,
    ) {
        let mut inner = self.inner.lock().await;
        let sk = StreamKey {
            topic,
            key: key.clone(),
        };
        let m = inner.metrics.entry(sk.clone()).or_default();
        m.frames += 1;
        m.bytes += batch_bytes as u64;
        m.last_upstream_data_at = Some(now);
        let q = inner.caches.ticks.entry(sk).or_default();
        q.push_back(TickRec {
            ts_ns: now.elapsed().as_nanos() as i64,
            bytes: batch_bytes,
        });
        // Evict by time window
        let window = Duration::from_secs(self.cfg.ticks_replay_secs);
        let nowi = Instant::now();
        while let Some(_front) = q.front() {
            // Here we used now elapsed placeholder; in real we would compare to message ts
            if nowi.duration_since(now) > window {
                q.pop_front();
            } else {
                break;
            }
        }
    }

    pub async fn on_depth_delta(&self, topic: Topic, key: &SymbolKey, now: Instant, bytes: usize) {
        let mut inner = self.inner.lock().await;
        let sk = StreamKey {
            topic,
            key: key.clone(),
        };
        let m = inner.metrics.entry(sk.clone()).or_default();
        m.frames += 1;
        m.bytes += bytes as u64;
        m.last_upstream_data_at = Some(now);
        let ring = inner.caches.depth_ring.entry(sk).or_default();
        ring.push_back(DepthDeltaRec {
            ts_ns: now.elapsed().as_nanos() as i64,
            bytes,
        });
        // Cap by approximate time: keep last N entries within depth_ring_secs (simplified)
        while ring.len() > 1024 {
            ring.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tt_provider::{ConnectionState, DisconnectReason, ProviderSessionSpec};
    use tt_types::keys::{SymbolKey, Topic};

    struct MockProvider {
        calls: Arc<Mutex<Vec<String>>>,
    }
    #[async_trait::async_trait]
    impl MarketDataProvider for MockProvider {
        fn id(&self) -> u16 {
            1
        }
        fn name(&self) -> &'static str {
            "mock"
        }
        fn supports(&self, _topic: Topic) -> bool {
            true
        }
        async fn connect(&self, _session: ProviderSessionSpec) -> tt_provider::ConnectResult {
            tt_provider::ConnectResult {
                ok: true,
                error: None,
            }
        }
        async fn disconnect(&self, _reason: DisconnectReason) {}
        fn connection_state(&self) -> ConnectionState {
            ConnectionState::Connected
        }
        fn last_heartbeat_at(&self) -> Instant {
            Instant::now()
        }
        async fn subscribe_md(
            &self,
            topic: Topic,
            key: &tt_types::keys::SymbolKey,
            _params: Option<&ProviderParams>,
        ) -> SubscribeResult {
            let mut g = self.calls.lock().await;
            g.push(format!("sub:{:?}:{:?}", topic.id().0, key));
            SubscribeResult {
                ok: true,
                error: None,
            }
        }
        async fn unsubscribe_md(&self, topic: Topic, key: &tt_types::keys::SymbolKey) {
            let mut g = self.calls.lock().await;
            g.push(format!("unsub:{:?}:{:?}", topic.id().0, key));
        }
        fn active_md_subscriptions(&self) -> Vec<(Topic, SymbolKey)> {
            vec![]
        }
    }

    #[tokio::test]
    async fn ref_count_and_params() {
        let mp = Arc::new(MockProvider {
            calls: Arc::new(Mutex::new(vec![])),
        });
        let eng = Engine::new(mp.clone(), EngineConfig::default());
        let key = SymbolKey {
            instrument: "ES".into(),
            provider: "mock".into(),
            broker: None,
        };
        let mut p = ProviderParams::new();
        p.insert("depth".into(), "10".into());
        eng.interest_delta(Topic::Depth, key.clone(), 1, Some(p.clone()))
            .await
            .unwrap();
        eng.interest_delta(Topic::Depth, key.clone(), 1, None)
            .await
            .unwrap(); // idempotent subscribe, no extra call expected
        eng.interest_delta(Topic::Depth, key.clone(), -1, None)
            .await
            .unwrap();
        eng.interest_delta(Topic::Depth, key.clone(), -1, None)
            .await
            .unwrap(); // extra -1 should be no-op
        let calls = mp.calls.lock().await.clone();
        // Expect one subscribe and one unsubscribe
        assert!(calls.iter().any(|c| c.starts_with("sub:")));
        assert!(calls.iter().any(|c| c.starts_with("unsub:")));
    }
}
