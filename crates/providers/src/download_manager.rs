use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use dotenv::dotenv;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Duration as TokioDuration, timeout};
use tracing::info;
use tt_database::ingest::{ingest_bbo, ingest_candles, ingest_ticks};
use tt_database::init::Connection;
use tt_database::queries::latest_data_time;
use tt_types::data::models::Resolution;
use tt_types::engine_id::EngineUuid;
use tt_types::history::{HistoricalRangeRequest, HistoryEvent};
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::hours::market_hours::{
    MarketHours, SessionKind, hours_for_exchange, next_session_open_after,
};
use tt_types::securities::symbols::Instrument;
use tt_types::server_side::traits::HistoricalDataProvider;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct DownloadKey {
    provider_kind: ProviderKind,
    instrument: Instrument,
    topic: Topic,
}
impl DownloadKey {
    pub fn new(provider_kind: ProviderKind, instrument: Instrument, topic: Topic) -> DownloadKey {
        Self {
            provider_kind,
            instrument,
            topic,
        }
    }
}

pub struct Entry {
    result: Mutex<Option<anyhow::Result<()>>>,
    pub notify: Notify,
    id: EngineUuid,
    join: Mutex<Option<JoinHandle<()>>>,
}

struct DownloadManagerInner {
    inflight: Arc<RwLock<HashMap<DownloadKey, std::sync::Arc<Entry>>>>,
    db: tt_database::init::Connection,
}

#[derive(Clone)]
pub struct DownloadManager {
    inner: std::sync::Arc<DownloadManagerInner>,
}

#[derive(Clone)]
pub struct DownloadTaskHandle {
    key: DownloadKey,
    pub entry: Arc<Entry>,
}

impl DownloadTaskHandle {
    pub fn id(&self) -> EngineUuid {
        self.entry.id
    }
    pub fn key(&self) -> &DownloadKey {
        &self.key
    }
    pub fn is_complete(&self) -> bool {
        self.entry
            .result
            .try_lock()
            .map(|g| g.is_some())
            .unwrap_or(false)
    }
    pub async fn wait(self) -> anyhow::Result<()> {
        {
            let g = self.entry.result.lock().await;
            if let Some(res) = g.as_ref() {
                return match res {
                    Ok(()) => Ok(()),
                    Err(e) => Err(anyhow::anyhow!("{}", e)),
                };
            }
        }
        self.entry.notify.notified().await;
        let g = self.entry.result.lock().await;
        match g.as_ref() {
            Some(Ok(())) => Ok(()),
            Some(Err(e)) => Err(anyhow::anyhow!("{}", e)),
            None => Ok(()),
        }
    }
    pub fn cancel(&self) {
        if let Ok(mut g) = self.entry.join.try_lock()
            && let Some(j) = g.take()
        {
            j.abort();
        }
    }
}

impl Default for DownloadManager {
    fn default() -> Self {
        Self::new()
    }
}

impl DownloadManager {
    pub fn new() -> Self {
        // Load .env first
        dotenv().ok();
        // Try to read DATABASE_URL; else synthesize from DB_PATH fallback and pg defaults
        let fallback_url = if std::env::var("DATABASE_URL").is_err() {
            // DB_PATH format like "127.0.0.1:5432:5432" (host:host_port:container_port)
            let db_path =
                std::env::var("DB_PATH").unwrap_or_else(|_| "127.0.0.1:5432:5432".to_string());
            let parts: Vec<&str> = db_path.split(':').collect();
            let (host, port) = match parts.as_slice() {
                [h, p_host, _p_container] => (*h, *p_host),
                [h, p] => (*h, *p),
                [h] => (*h, "5432"),
                _ => ("127.0.0.1", "5432"),
            };
            let user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string());
            let pass = std::env::var("POSTGRES_PASSWORD")
                .unwrap_or_else(|_| "change-this-super-secret".to_string());
            let db = std::env::var("TT_DB").unwrap_or_else(|_| "tick_trader".to_string());
            Some(format!(
                "postgres://{}:{}@{}:{}/{}",
                user, pass, host, port, db
            ))
        } else {
            None
        };
        let db = match tt_database::init::pool_from_env() {
            Ok(p) => p,
            Err(_) => {
                let url = fallback_url.unwrap_or_else(|| {
                    panic!("DATABASE_URL not set and no fallback could be constructed")
                });
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(
                        std::env::var("TT_DB_MAX_CONNS")
                            .ok()
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(8),
                    )
                    .connect_lazy(&url)
                    .expect("failed to create Postgres pool from fallback URL")
            }
        };
        Self {
            inner: std::sync::Arc::new(DownloadManagerInner {
                inflight: Arc::new(RwLock::new(HashMap::new())),
                db,
            }),
        }
    }

    /// If an identical update task is already inflight, return a handle to it; otherwise None.
    /// Callers can then decide to start a new task via `start_update`.
    pub async fn request_update(
        &self,
        _client: std::sync::Arc<dyn HistoricalDataProvider>,
        req: HistoricalRangeRequest,
    ) -> anyhow::Result<Option<DownloadTaskHandle>> {
        let key = DownloadKey::new(req.provider_kind, req.instrument.clone(), req.topic);
        let guard = self.inner.inflight.read().await;
        if let Some(entry) = guard.get(&key) {
            let handle = DownloadTaskHandle {
                key,
                entry: entry.clone(),
            };
            Ok(Some(handle))
        } else {
            Ok(None)
        }
    }

    /// Create or reuse an inflight update task for the given request and return its handle.
    /// If a task already exists, no new one is spawned.
    pub async fn start_update(
        &self,
        client: std::sync::Arc<dyn HistoricalDataProvider>,
        req: HistoricalRangeRequest,
    ) -> anyhow::Result<DownloadTaskHandle> {
        let key = DownloadKey::new(req.provider_kind, req.instrument.clone(), req.topic);
        // fast path: existing
        {
            let guard = self.inner.inflight.read().await;
            if let Some(e) = guard.get(&key) {
                return Ok(DownloadTaskHandle {
                    key,
                    entry: e.clone(),
                });
            }
        }
        // need to create; do it with insertion lock to avoid races
        let entry_arc = {
            let mut guard = self.inner.inflight.write().await;
            if let Some(e) = guard.get(&key) {
                e.clone()
            } else {
                let e = Arc::new(Entry {
                    result: Mutex::new(None),
                    notify: Notify::new(),
                    id: EngineUuid::new(),
                    join: Mutex::new(None),
                });
                guard.insert(key.clone(), e.clone());
                e
            }
        };

        // If join is empty, we are the creator; spawn and set the join handle.
        let mut should_spawn = false;
        if let Ok(mut jg) = entry_arc.join.try_lock()
            && jg.is_none()
        {
            should_spawn = true;
            // placeholder so other threads don't also spawn
            *jg = Some(tokio::spawn(async {}));
        }
        if should_spawn {
            let dm = self.clone();
            let entry2 = entry_arc.clone();
            let key2 = key.clone();
            let client2 = client.clone();
            let req2 = req.clone();
            let db_conn = dm.inner.db.clone();
            let handle = tokio::spawn(async move {
                let res = run_download(client2, req2, db_conn).await;
                // store result and notify
                {
                    let mut g = entry2.result.lock().await;
                    *g = Some(res);
                }
                entry2.notify.notify_waiters();
                // remove from inflight
                let mut map = dm.inner.inflight.write().await;
                map.remove(&key2);
            });
            // replace the placeholder
            if let Ok(mut jg) = entry_arc.join.try_lock() {
                *jg = Some(handle);
            }
        }
        Ok(DownloadTaskHandle {
            key,
            entry: entry_arc,
        })
    }
}

// your helper
async fn run_download(
    client: Arc<dyn HistoricalDataProvider>,
    req: HistoricalRangeRequest,
    connection: Connection,
) -> anyhow::Result<()> {
    // Ensure the schema exists before we start persisting fetched data. This is idempotent.
    tt_database::schema::ensure_schema(&connection).await?;
    const CAL_TZ: Tz = chrono_tz::UTC;
    let resolution = match req.topic {
        Topic::Ticks => None,
        Topic::Quotes => None,
        Topic::MBP10 => None,
        Topic::Candles1s => Some(Resolution::Seconds(1)),
        Topic::Candles1m => Some(Resolution::Minutes(1)),
        Topic::Candles1h => Some(Resolution::Hours(1)),
        Topic::Candles1d => Some(Resolution::Daily),
        _ => return Err(anyhow::anyhow!("invalid topic")),
    };

    let provider_code = client.name();
    let hours = hours_for_exchange(req.exchange);

    let earliest = client
        .earliest_available(req.instrument.clone(), req.topic)
        .await?;
    let now = Utc::now();
    let wants_intraday = req.topic != Topic::Candles1d;

    // Cursor := max(persisted_ts)+1ns, else provider earliest
    let mut cursor: DateTime<Utc> =
        match latest_data_time(&connection, req.provider_kind, &req.instrument, req.topic).await {
            Ok(Some(ts)) => ts + Duration::nanoseconds(1),
            Ok(None) => match earliest {
                None => return Err(anyhow::anyhow!("no data available")),
                Some(t) => t,
            },
            Err(e) => return Err(e),
        };
    let symbol = req.instrument.to_string();
    if cursor >= now {
        tracing::info!(%symbol, ?provider_code, %req.exchange, "up to date");
        return Ok(());
    }

    // If we're on a fully-closed calendar day for intraday-ish kinds, jump to next open.
    if req.topic != Topic::Candles1d
        && hours.is_closed_all_day_at(cursor, CAL_TZ, SessionKind::Both)
    {
        let open = next_session_open_after(&hours, cursor);
        if open > cursor {
            cursor = open;
        }
    }

    while cursor < now {
        // Pick a batch window that fits the kind/resolution.
        let span = choose_span(resolution);
        let end = (cursor + span).min(now);

        // For intraday-ish kinds, skip windows that are 100% closed.
        if req.topic != Topic::Candles1d && window_is_all_closed(&hours, CAL_TZ, cursor, end) {
            cursor = next_session_open_after(&hours, cursor);
            continue;
        }

        let req = HistoricalRangeRequest {
            provider_kind: req.provider_kind,
            topic: req.topic,
            instrument: req.instrument.clone(),
            exchange: req.exchange,
            start: cursor,
            end,
        };

        tracing::info!(symbol=%symbol, start=%req.start, end=%req.end, topic=?req.topic, res=?resolution, "fetching");
        if let Err(e) = client.ensure_connected().await {
            tracing::error!(error=%e, "ensure_connected failed; aborting task");
            return Err(e);
        }
        let fetch_res = timeout(
            TokioDuration::from_secs(1000),
            client.clone().fetch(req.clone()),
        )
        .await;
        let events = match fetch_res {
            Ok(Ok(ev)) => {
                tracing::info!("received {} events", ev.len());
                ev
            }
            Ok(Err(e)) => {
                tracing::error!(error=%e, start=%cursor, end=%end, "history fetch error; aborting task");
                return Err(e);
            }
            Err(_) => {
                tracing::warn!(start=%cursor, end=%end, "history fetch timed out; aborting task");
                return Err(anyhow::anyhow!("history fetch timed out"));
            }
        };

        // Accumulators and watermark of the max timestamp we actually received.
        let mut max_ts: Option<DateTime<Utc>> = None;
        let mut ticks: Vec<tt_types::data::core::Tick> = Vec::new();
        let mut candles: Vec<tt_types::data::core::Candle> = Vec::new();
        let mut quotes: Vec<tt_types::data::core::Bbo> = Vec::new();

        for ev in events {
            match ev {
                HistoryEvent::Candle(c) => {
                    if matches!(
                        req.topic,
                        Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d
                    ) {
                        // Advance by candle END to guarantee no gaps/overlaps between bars
                        let ts = c.time_end;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        if !c.is_closed(Utc::now()) {
                            continue;
                        }
                        candles.push(c);
                    }
                }
                HistoryEvent::Tick(t) => {
                    if matches!(req.topic, Topic::Ticks) {
                        let ts = t.time;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        ticks.push(t);
                    }
                }
                HistoryEvent::Bbo(q) => {
                    if matches!(req.topic, Topic::Quotes) {
                        let ts = q.time;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        quotes.push(q);
                    }
                }
                HistoryEvent::EndOfStream => break,
                HistoryEvent::Error(e) => {
                    tracing::error!(error=%e, %cursor, "history fetch error");
                    continue;
                }
            }
        }

        // Persist
        match req.topic {
            Topic::Ticks => {
                if !ticks.is_empty() {
                    let _rows =
                        ingest_ticks(&connection, req.provider_kind, &req.instrument, ticks)
                            .await?;
                    tracing::info!(rows = _rows, start=%cursor, end=%end, "persisted ticks");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no ticks returned");
                }
            }
            Topic::Quotes => {
                if !quotes.is_empty() {
                    let rows =
                        ingest_bbo(&connection, req.provider_kind, &req.instrument, quotes).await?;
                    tracing::info!(rows = rows, start=%cursor, end=%end, "persisted bbo");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no bbo returned");
                }
            }
            Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d => {
                if !candles.is_empty() {
                    let rows =
                        ingest_candles(&connection, req.provider_kind, &req.instrument, candles)
                            .await?;
                    tracing::info!(rows = rows, start=%cursor, end=%end, "persisted candles");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no candles returned");
                }
            }
            Topic::MBP10 => {}
            _ => {}
        }

        // Advance cursor by what we *actually* got, otherwise by window end.
        if let Some(ts) = max_ts {
            cursor = ts + Duration::nanoseconds(1);
        } else if wants_intraday {
            let next_open = next_session_open_after(&hours, cursor);
            cursor = next_open.max(end + Duration::nanoseconds(1));
        } else {
            cursor = end + Duration::seconds(1);
        }
    }
    info!("Historical database update completed");
    Ok(())
}

// ---- helpers ----

fn choose_span(res: Option<Resolution>) -> chrono::Duration {
    // Policy:
    // - Intraday (<= 1h): request 1 calendar day at a time
    // - Higher than 1h (daily/weekly or multi-hour): request 1 week at a time
    match res {
        Some(Resolution::Seconds(_)) => chrono::Duration::days(1),
        Some(Resolution::Minutes(_)) => chrono::Duration::days(1),
        Some(Resolution::Hours(h)) => {
            if h <= 1 {
                chrono::Duration::days(1)
            } else {
                chrono::Duration::days(7)
            }
        }
        Some(Resolution::Daily) | Some(Resolution::Weekly) => chrono::Duration::days(7),
        None => chrono::Duration::days(1), // ticks/quotes/depth: be conservative
    }
}

/// True if every calendar day intersecting [start,end) is fully closed.
fn window_is_all_closed(
    hours: &MarketHours,
    cal_tz: chrono_tz::Tz,
    start: chrono::DateTime<Utc>,
    end: chrono::DateTime<Utc>,
) -> bool {
    if end <= start {
        return true;
    }
    let mut d = start.with_timezone(&cal_tz).date_naive();
    let last = (end - chrono::Duration::nanoseconds(1))
        .with_timezone(&cal_tz)
        .date_naive();
    while d <= last {
        if !hours.is_closed_all_day_in_calendar(d, cal_tz, SessionKind::Both) {
            return false;
        }
        d = d.succ_opt().unwrap();
    }
    true
}
