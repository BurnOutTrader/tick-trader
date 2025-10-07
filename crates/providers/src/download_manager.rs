use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tt_database::paths::provider_kind_to_db_string;
use tt_types::keys::Topic;
use tt_database::ingest::{ingest_bbo, ingest_candles, ingest_ticks};
use tt_database::models::{BboRow, CandleRow, TickRow};
use tt_database::queries::latest_data_time;
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use rust_decimal::prelude::ToPrimitive;
use std::path::Path;
use std::sync::Arc;
use dotenv::dotenv;
use tt_database::init::init_db;
use tt_types::history::{HistoricalRequest, HistoryEvent};
use tt_types::providers::ProviderKind;
use tt_types::securities::hours::market_hours::{
    hours_for_exchange, next_session_open_after, MarketHours, SessionKind,
};
use tt_types::securities::symbols::{exchange_market_type, Instrument};
use tt_types::server_side::traits::HistoricalDataProvider;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;
use tokio::time::{timeout, Duration as TokioDuration};
use tt_types::data::models::{Resolution, Side};

pub struct Entry {
    result: Mutex<Option<anyhow::Result<()>>>,
    pub notify: Notify,
    id: Uuid,
    join: Mutex<Option<JoinHandle<()>>>
}

struct DownloadManagerInner {
    inflight: Arc<Mutex<HashMap<(ProviderKind, Instrument, Topic), std::sync::Arc<Entry>>>>,
}

#[derive(Clone)]
pub struct DownloadManager {
    inner: std::sync::Arc<DownloadManagerInner>,
}

#[derive(Clone)]
pub struct DownloadTaskHandle {
    key: (ProviderKind, Instrument, Topic),
    pub entry: Arc<Entry>,
}

impl DownloadTaskHandle {
    pub fn id(&self) -> Uuid { self.entry.id }
    pub fn key(&self) -> &(ProviderKind, Instrument, Topic) { &self.key }
    pub fn is_complete(&self) -> bool {
        self.entry.result.try_lock().map(|g| g.is_some()).unwrap_or(false)
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
            None => Ok(())
        }
    }
    pub fn cancel(&self) {
        if let Ok(mut g) = self.entry.join.try_lock() {
            if let Some(j) = g.take() {
                j.abort();
            }
        }
    }
}

impl DownloadManager {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(DownloadManagerInner {
                inflight: Arc::new(Mutex::new(HashMap::new())),
            }),
        }
    }

    /// If an identical update task is already inflight, return a handle to it; otherwise None.
    /// Callers can then decide to start a new task via `start_update`.
    pub async fn request_update(
        &self,
        _client: std::sync::Arc<dyn HistoricalDataProvider>,
        req: HistoricalRequest,
    ) -> anyhow::Result<Option<DownloadTaskHandle>> {
        let key = (req.provider_kind, req.instrument.clone(), req.topic.clone());
        let guard = self.inner.inflight.lock().await;
        if let Some(entry) = guard.get(&key) {
            let handle = DownloadTaskHandle { key, entry: entry.clone() };
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
        req: HistoricalRequest,
    ) -> anyhow::Result<DownloadTaskHandle> {
        let key = (req.provider_kind, req.instrument.clone(), req.topic.clone());
        // fast path: existing
        {
            let guard = self.inner.inflight.lock().await;
            if let Some(e) = guard.get(&key) {
                return Ok(DownloadTaskHandle { key, entry: e.clone() });
            }
        }
        // need to create; do it with insertion lock to avoid races
        let entry_arc = {
            let mut guard = self.inner.inflight.lock().await;
            if let Some(e) = guard.get(&key) {
                e.clone()
            } else {
                let e = Arc::new(Entry {
                    result: Mutex::new(None),
                    notify: Notify::new(),
                    id: Uuid::new_v4(),
                    join: Mutex::new(None),
                });
                guard.insert(key.clone(), e.clone());
                e
            }
        };

        // If join is empty, we are the creator; spawn and set the join handle.
        let mut should_spawn = false;
        if let Ok(mut jg) = entry_arc.join.try_lock() {
            if jg.is_none() {
                should_spawn = true;
                // placeholder so other threads don't also spawn
                *jg = Some(tokio::spawn(async {}));
            }
        }
        if should_spawn {
            let dm = self.clone();
            let entry2 = entry_arc.clone();
            let key2 = key.clone();
            let client2 = client.clone();
            let req2 = req.clone();
            let handle = tokio::spawn(async move {
                let res = run_download(client2, req2).await;
                // store result and notify
                {
                    let mut g = entry2.result.lock().await;
                    *g = Some(res);
                }
                entry2.notify.notify_waiters();
                // remove from inflight
                let mut map = dm.inner.inflight.lock().await;
                map.remove(&key2);
            });
            // replace the placeholder
            if let Ok(mut jg) = entry_arc.join.try_lock() {
                *jg = Some(handle);
            }
        }
        Ok(DownloadTaskHandle { key, entry: entry_arc })
    }
}


// your helper
async fn run_download(
    client: Arc<dyn HistoricalDataProvider>,
    req: HistoricalRequest
) -> anyhow::Result<()> {
    dotenv().ok();
    let db_path = std::env::var("DB_PATH").unwrap_or_else(|_| "./storage".to_string());
    let db_path = Path::new(&db_path);
    const CAL_TZ: Tz = chrono_tz::UTC;
    let connection = init_db(&db_path)?;
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
    let market_type = exchange_market_type(req.exchange);
    let hours = hours_for_exchange(req.exchange);

    let earliest = client.earliest_available(req.instrument.clone(), req.topic).await?;
    let now = Utc::now();
    let wants_intraday = req.topic != Topic::Candles1d;

    // Cursor := max(persisted_ts)+1ns, else provider earliest
    let mut cursor: DateTime<Utc> = match latest_data_time(&connection, req.provider_kind, &req.instrument, req.topic) {
        Ok(Some(ts)) => ts + Duration::nanoseconds(1),
        Ok(None) => match earliest {
            None => return Err(anyhow::anyhow!("no data available")),
            Some(t) => t
        },
        Err(e) => return Err(e),
    };
    let symbol = req.instrument.to_string();
    if cursor >= now {
        tracing::info!(%symbol, ?provider_code, %req.exchange, "up to date");
        return Ok(());
    }

    // If we're on a fully-closed calendar day for intraday-ish kinds, jump to next open.
    if req.topic != Topic::Candles1d && hours.is_closed_all_day_at(cursor, CAL_TZ, SessionKind::Both) {
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

        let req = HistoricalRequest {
            provider_kind:req.provider_kind,
            topic:req.topic.clone(),
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
        let fetch_res = timeout(TokioDuration::from_secs(1000), client.clone().fetch(req.clone())).await;
        let events = match fetch_res {
            Ok(Ok(ev)) => {
                tracing::info!("received {} events", ev.len());
                ev
            },
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
        let mut ticks: Vec<TickRow> = Vec::new();
        let mut candles: Vec<CandleRow> = Vec::new();
        let mut quotes: Vec<BboRow> = Vec::new();

        for ev in events { 
            match ev {
                HistoryEvent::Candle(c) => {
                    if matches!(req.topic, Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d) {
                        // Advance by candle END to guarantee no gaps/overlaps between bars
                        let ts = c.time_end;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        if !c.is_closed(Utc::now()) {
                            continue;
                        }
                        candles.push(CandleRow::from_candle(provider_code, c));
                    }
                }
                HistoryEvent::Tick(t) => {
                    if matches!(req.topic, Topic::Ticks) {
                        let ts = t.time;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        let side_u8 = match t.side {
                            Side::Buy => 1,
                            Side::Sell => 2,
                            _ => 0,
                        };
                        ticks.push(TickRow {
                            provider: provider_kind_to_db_string(provider_code),
                            symbol_id: t.symbol,
                            price: t.price.to_f64().unwrap_or(0.0),
                            size: t.volume.to_f64().unwrap_or(0.0),
                            side: side_u8,
                            key_ts_utc_ns: dt_to_ns_i64(t.time), // robust ns key
                            key_tie: t.venue_seq.unwrap_or(0),
                            venue_seq: t.venue_seq,
                            exec_id: None,
                        });
                    }
                }
                HistoryEvent::Bbo(q) => {
                    if matches!(req.topic, Topic::Quotes) {
                        let ts = q.time;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        quotes.push(BboRow {
                            provider: provider_kind_to_db_string(provider_code),
                            symbol_id: q.symbol,
                            key_ts_utc_ns: dt_to_ns_i64(q.time),
                            bid: q.bid.to_f64().unwrap_or(0.0),
                            bid_size: q.bid_size.to_f64().unwrap_or(0.0),
                            ask: q.ask.to_f64().unwrap_or(0.0),
                            ask_size: q.ask_size.to_f64().unwrap_or(0.0),
                            bid_orders: q.bid_orders,
                            ask_orders: q.ask_orders,
                            venue_seq: q.venue_seq,
                            is_snapshot: q.is_snapshot,
                        });
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
                    let out_paths = ingest_ticks(
                        &connection,
                        &req.provider_kind,
                        &req.instrument,
                        market_type,
                        req.topic,
                        &ticks,
                        &db_path,
                        9,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted ticks");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no ticks returned");
                }
            }
            Topic::Quotes => {
                if !quotes.is_empty() {
                    let out_paths = ingest_bbo(
                        &connection,
                        &req.provider_kind,
                        &req.instrument,
                        market_type,
                        req.topic,
                        &quotes,
                        &db_path,
                        9,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted bbo");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no bbo returned");
                }
            }
            Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d => {
                if !candles.is_empty() {
                    let out_paths = ingest_candles(
                        &connection,
                        &req.provider_kind,
                        &req.instrument,
                        market_type,
                        req.topic,
                        &candles,
                        &db_path,
                        9,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted candles");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no candles returned");
                }
            }
            Topic::MBP10 => {

            }
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
            if h <= 1 { chrono::Duration::days(1) } else { chrono::Duration::days(7) }
        }
        Some(Resolution::Daily) | Some(Resolution::Weekly) => chrono::Duration::days(7),
        None => chrono::Duration::days(1), // ticks/quotes/depth: be conservative
    }
}

#[inline]
fn dt_to_ns_i64(dt: chrono::DateTime<Utc>) -> i64 {
    // Try the safe method that returns Option<i64>
    if let Some(n) = dt.timestamp_nanos_opt() {
        n
    } else {
        // Fallback: dt is out of representable range, clamp
        let secs = dt.timestamp();
        let sub = dt.timestamp_subsec_nanos() as i64;
        // Compose, but guard overflow manually
        // Note timestamp() * 1_000_000_000 may overflow i64, so clamp
        match secs.checked_mul(1_000_000_000) {
            Some(sec_nano_base) => {
                // may still overflow on addition
                match sec_nano_base.checked_add(sub) {
                    Some(v) => v,
                    None => {
                        if sec_nano_base.is_negative() {
                            i64::MIN
                        } else {
                            i64::MAX
                        }
                    }
                }
            }
            None => {
                // overflow in multiplication
                if secs.is_negative() {
                    i64::MIN
                } else {
                    i64::MAX
                }
            }
        }
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
