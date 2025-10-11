use crate::models::{
    BarRec, DepthDeltaRec, EngineConfig, InterestEntry, StreamKey, StreamMetrics, SubState, TickRec,
};
use ahash::AHashMap;
use anyhow::anyhow;
use sqlx::{Pool, Postgres};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::info;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::server_side::traits::{MarketDataProvider, ProbeStatus, ProviderParams};
use tt_types::wire::{AccountDeltaBatch, OrdersBatch, PositionsBatch};

#[allow(dead_code)]
#[derive(Default)]
pub struct Caches {
    pub ticks: AHashMap<StreamKey, VecDeque<TickRec>>, // bounded by secs window
    pub bars: AHashMap<StreamKey, VecDeque<BarRec>>,   // sized per resolution
    pub depth_ring: AHashMap<StreamKey, VecDeque<DepthDeltaRec>>, // ~2–5s ring
}

#[allow(dead_code)]
pub struct Engine<P: MarketDataProvider + 'static> {
    provider: Arc<P>,
    cfg: EngineConfig,
    inner: Arc<Mutex<EngineInner>>,
}
#[allow(dead_code)]
struct EngineInner {
    interest: AHashMap<StreamKey, InterestEntry>,
    metrics: AHashMap<StreamKey, StreamMetrics>,
    #[allow(dead_code)]
    db: Pool<Postgres>,
    caches: Caches,
}

impl<P: MarketDataProvider + 'static> Engine<P> {
    #[allow(dead_code)]
    pub fn new(provider: Arc<P>, cfg: EngineConfig) -> anyhow::Result<Self> {
        // Initialize Postgres connection pool lazily (non-async) using sqlx
        // Prefer env var DATABASE_URL; fallback to cfg.db_path if it looks like a URL.
        let db_url = std::env::var("DATABASE_URL").unwrap_or_else(|_| cfg.db_path.clone());
        if db_url.is_empty() {
            return Err(anyhow!(
                "DATABASE_URL is not set and cfg.db_path is empty; cannot initialize Postgres pool"
            ));
        }
        // Use a small default pool; strategies typically run few concurrent queries.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(8)
            .connect_lazy(&db_url)?;
        Ok(Self {
            provider,
            cfg,
            inner: Arc::new(Mutex::new(EngineInner {
                interest: AHashMap::new(),
                metrics: AHashMap::new(),
                caches: Caches::default(),
                db: pool,
            })),
        })
    }
    #[allow(dead_code)]
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
                drop(inner);
                info!(?sk, "upstream subscribe start");
                // idempotency: only call if not already Subscribed/Subscribing
                let _ = self.provider.subscribe_md(topic, &key).await;
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
                self.provider.unsubscribe_md(topic, &key).await?;
                let mut inner2 = self.inner.lock().await;
                if let Some(ent2) = inner2.interest.get_mut(&sk) {
                    ent2.state = SubState::Unsubscribed;
                }
            }
        }
        Ok(())
    }
    #[allow(dead_code)]
    pub async fn probe_stream(&self, topic: Topic, _key: &SymbolKey) -> ProbeStatus {
        self.provider.supports(topic); // hint; no-op for now
        // delegate to provider if it has a probe (not in trait for streams), so return Ok(0)
        ProbeStatus::Ok(0)
    }
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

pub(crate) struct EngineAccountsState {
    pub(crate) last_orders: Arc<RwLock<Option<OrdersBatch>>>,
    pub(crate) last_positions: Arc<RwLock<Option<PositionsBatch>>>,
    pub(crate) last_accounts: Arc<RwLock<Option<AccountDeltaBatch>>>,
}
