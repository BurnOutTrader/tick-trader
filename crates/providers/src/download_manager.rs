use anyhow::Context;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Duration as TokioDuration, timeout};
use tracing::info;
use tt_database::ingest::{ingest_bbo, ingest_candles, ingest_mbp1, ingest_mbp10, ingest_ticks};
use tt_database::init::{Connection, init_db};
use tt_database::queries::latest_data_time;
use tt_types::data::mbp10::Mbp10;
use tt_types::data::mbp1::Mbp1;
use tt_types::data::models::Resolution;
use tt_types::engine_id::EngineUuid;
use tt_types::history::{HistoricalRangeRequest, HistoryEvent};
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
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

impl DownloadManager {
    // Async ctor for the current-thread case (and generally the idiomatic choice)
    pub async fn new_async() -> anyhow::Result<DownloadManager> {
        let db = init_db().context("init_db failed")?;
        tt_database::schema::ensure_schema(&db)
            .await
            .context("ensure_schema failed")?;
        Ok(Self {
            inner: std::sync::Arc::new(DownloadManagerInner {
                inflight: Arc::new(RwLock::new(HashMap::new())),
                db,
            }),
        })
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
            let db_conn = dm.inner.db.clone();

            let entry2 = entry_arc.clone();
            let key2 = key.clone();
            let client2 = client.clone();
            let req2 = req.clone();

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

    let now = Utc::now();

    // Simplified cursoring: start from latest persisted time if available; otherwise from provider's earliest.
    let last_opt =
        latest_data_time(&connection, req.provider_kind, &req.instrument, req.topic).await?;
    info!("updating from latest data time: {:?}", last_opt);
    let mut cursor: DateTime<Utc> = if let Some(ts) = last_opt {
        ts
    } else {
        client
            .earliest_available(req.instrument.clone(), req.topic)
            .await?
            .ok_or_else(|| anyhow::anyhow!("no data available"))?
    };
    let symbol = req.instrument.to_string();
    if cursor >= now {
        tracing::info!(%symbol, ?provider_code, "up to date");
        return Ok(());
    }

    while cursor < now {
        let now = Utc::now();
        // Pick a batch window that fits the kind/resolution.
        let span = choose_span(resolution);
        let end = (cursor + span).min(now);

        let req = HistoricalRangeRequest {
            provider_kind: req.provider_kind,
            topic: req.topic,
            instrument: req.instrument.clone(),
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
        let mut mdp10:  Vec<Mbp10> = Vec::new();
        let mut mdp1:  Vec<Mbp1> = Vec::new();
        let mut saw_candle_events: bool = false;
        // Diagnostics counters for candles
        let total_events: usize = events.len();
        let mut filtered_wrong_res: usize = 0;
        let mut filtered_not_closed: usize = 0;
        let mut filtered_le_cursor: usize = 0;

        for ev in events {
            match ev {
                HistoryEvent::Candle(c) => {
                    saw_candle_events = true;
                    // Ensure resolution matches the topic (prevents 1s resume issues)
                    if let Some(expected) = resolution
                        && c.resolution != expected
                    {
                        filtered_wrong_res += 1;
                        continue;
                    }
                    // Skip duplicates at or before the cursor to avoid infinite loops
                    if c.time_end <= cursor {
                        filtered_le_cursor += 1;
                        continue;
                    }
                    if !c.is_closed(Utc::now()) {
                        filtered_not_closed += 1;
                        continue;
                    }
                    let ts = c.time_end;
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                    candles.push(c);
                }
                HistoryEvent::Tick(t) => {
                    let ts = t.time;
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                    ticks.push(t);
                }
                HistoryEvent::Bbo(q) => {
                    let ts = q.time;
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                    quotes.push(q);
                }
                HistoryEvent::EndOfStream => break,
                HistoryEvent::Error(e) => {
                    tracing::error!(error=%e, %cursor, "history fetch error");
                    continue;
                }
                HistoryEvent::Mbp10(m) => {
                    let ts = m.ts_event;
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                    mdp10.push(m);
                }
                HistoryEvent::Mbp1(m) => {
                    let ts = m.ts_event;
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                    mdp1.push(m);
                }
            }
        }

        // Diagnostics summary for candles and prep rows_affected capture
        if matches!(
            req.topic,
            Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d
        ) {
            let kept_candles = candles.len();
            let (first_end, last_end) = if kept_candles > 0 {
                let min = candles.iter().map(|c| c.time_end).min().unwrap();
                let max = candles.iter().map(|c| c.time_end).max().unwrap();
                (Some(min), Some(max))
            } else {
                (None, None)
            };
            let table = match req.topic {
                Topic::Candles1s => "bars_1s",
                Topic::Candles1m => "bars_1m",
                Topic::Candles1h => "bars_1h",
                Topic::Candles1d => "bars_1d",
                _ => "",
            };
            tracing::info!(total_events, kept_candles, filtered_wrong_res, filtered_not_closed, filtered_le_cursor, %table, first_end=?first_end, last_end=?last_end, start=%cursor, end=%end, "fetch filter summary");
        }

        let mut last_rows_affected: Option<u64> = None;

        // Persist
        match req.topic {
            Topic::Ticks => {
                if !ticks.is_empty() {
                    let rows = ingest_ticks(&connection, req.provider_kind, &req.instrument, ticks)
                        .await?;
                    tracing::debug!(topic=?req.topic, rows_affected=rows, start=%cursor, end=%end, "persisted ticks");
                    last_rows_affected = Some(rows);
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no ticks returned");
                    last_rows_affected = Some(0);
                }
            }
            Topic::Quotes => {
                if !quotes.is_empty() {
                    let rows =
                        ingest_bbo(&connection, req.provider_kind, &req.instrument, quotes).await?;
                    tracing::debug!(topic=?req.topic, rows_affected=rows, start=%cursor, end=%end, "persisted bbo");
                    last_rows_affected = Some(rows);
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no bbo returned");
                    last_rows_affected = Some(0);
                }
            }
            Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d => {
                if !candles.is_empty() {
                    let rows =
                        ingest_candles(&connection, req.provider_kind, &req.instrument, candles)
                            .await?;
                    tracing::debug!(topic=?req.topic, rows_affected=rows, start=%cursor, end=%end, "persisted candles");
                    last_rows_affected = Some(rows);
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no candles returned");
                    last_rows_affected = Some(0);
                }
            }
            Topic::MBP10 => {
                if !mdp10.is_empty() {
                    let rows =
                        ingest_mbp10(&connection, req.provider_kind, &req.instrument, mdp10)
                            .await?;
                    tracing::debug!(topic=?req.topic, rows_affected=rows, start=%cursor, end=%end, "persisted mdp10");
                    last_rows_affected = Some(rows);
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no mdp10 returned");
                    last_rows_affected = Some(0);
                }
            }
            Topic::MBP1 => {
                if !mdp1.is_empty() {
                    let rows =
                        ingest_mbp1(&connection, req.provider_kind, &req.instrument, mdp1)
                            .await?;
                    tracing::debug!(topic=?req.topic, rows_affected=rows, start=%cursor, end=%end, "persisted mdp1");
                    last_rows_affected = Some(rows);
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no mdp1 returned");
                    last_rows_affected = Some(0);
                }
            }
            _ => {}
        }

        // Advance cursor by what we actually saw; if none (or no progress), move to window end.
        let old_cursor = cursor;
        if let Some(ts) = max_ts {
            if ts <= cursor {
                cursor = end;
            } else {
                cursor = ts;
            }
        } else {
            if matches!(
                req.topic,
                Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d
            ) && saw_candle_events
            {
                tracing::warn!(start=%cursor, end=%end, "no progress from candle batch (likely all <= cursor); advancing by full window");
            }
            cursor = end;
        }
        tracing::debug!(topic=?req.topic, rows_affected=?last_rows_affected, cursor_old=%old_cursor, cursor_new=%cursor, max_ts=?max_ts, "cursor advance");
    }
    info!("Historical database update completed to {}", now);
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
        Some(Resolution::Hours(_)) => chrono::Duration::days(100),
        Some(Resolution::Daily) | Some(Resolution::Weekly) => chrono::Duration::days(365),
        None => chrono::Duration::days(1), // ticks/quotes/depth: be conservative
    }
}
