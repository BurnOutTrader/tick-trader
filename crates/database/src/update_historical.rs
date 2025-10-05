use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::sync::Notify;

struct Entry {
    result: Mutex<Option<Result<(), String>>>,
    notify: Notify,
}

struct DownloadManagerInner {
    inflight: Mutex<HashMap<Feed, std::sync::Arc<Entry>>>,
}

#[derive(Clone)]
pub struct DownloadManager {
    inner: std::sync::Arc<DownloadManagerInner>,
}

impl DownloadManager {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(DownloadManagerInner {
                inflight: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Create or join a download task for the same (provider,symbol,exchange,resolution,kind).
    /// If an identical task is already running, this will await that same task.
    pub async fn request(
        &self,
        client: std::sync::Arc<dyn HistoricalDataProvider>,
        conn: std::sync::Arc<duckdb::Connection>,
        data_root: PathBuf,
        feed: Feed,
    ) -> anyhow::Result<()> {
        // Fast path: join an in-flight task if one exists.
        if let Some(entry) = { self.inner.inflight.lock().await.get(&key).cloned() } {
            loop {
                if let Some(done) = entry.result.lock().await.clone() {
                    return done.map_err(|e| anyhow::anyhow!(e));
                }
                entry.notify.notified().await;
            }
        }

        // Insert a new entry marking the task as running.
        let entry = std::sync::Arc::new(Entry {
            result: Mutex::new(None),
            notify: Notify::new(),
        });
        {
            let mut map = self.inner.inflight.lock().await;
            // Handle a race: if someone inserted meanwhile, join theirs.
            if let std::collections::hash_map::Entry::Vacant(v) = map.entry(feed.clone()) {
                v.insert(entry.clone());
            } else {
                drop(map);
                if let Some(existing) = { self.inner.inflight.lock().await.get(&feed).cloned() } {
                    loop {
                        if let Some(done) = existing.result.lock().await.clone() {
                            return done.map_err(|e| anyhow::anyhow!(e));
                        }
                        existing.notify.notified().await;
                    }
                } else {
                    // Extremely unlikely; fall through to starting our own task.
                    error!("DownloadManager race condition");
                }
            }
        }

        // Run the download task inline (stays on this thread; no Send required).
        let res_str: Result<(), String> = run_download(client, conn, data_root, feed)
            .await
            .map_err(|e| e.to_string());

        // Publish result to waiters, notify, and clean up the map entry.
        {
            let mut slot = entry.result.lock().await;
            *slot = Some(res_str.clone());
            entry.notify.notify_waiters();
        }
        {
            let mut map = self.inner.inflight.lock().await;
            map.remove(&feed);
        }

        // Return the original result in anyhow form.
        res_str.map_err(|e| anyhow::anyhow!(e))
    }
}
use crate::ingest::{ingest_bbo, ingest_books, ingest_candles, ingest_ticks};
use crate::models::{BboRow, CandleRow, DataKind, TickRow};
use crate::queries::latest_data_time;
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use rust_decimal::prelude::ToPrimitive;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::error;
use provider::traits::HistoricalDataProvider;
use tt_types::base_data::Feed;
use tt_types::keys::SymbolKey;
use tt_types::providers::ProviderKind;
use tt_types::securities::market_hours::hours_for_exchange;
use tt_types::securities::symbols::{exchange_market_type, Instrument};

// your helper
async fn run_download(
    client: Arc<dyn HistoricalDataProvider>,
    conn: Arc<duckdb::Connection>,
    data_root: PathBuf,
    feed: Feed,
) -> anyhow::Result<()> {
    const CAL_TZ: Tz = chrono_tz::UTC;

    let provider_code = client.name();
    let market_type = exchange_market_type(feed.exchange());
    let hours = hours_for_exchange(feed.exchange());

    let earliest = client.earliest_available(feed.clone());
    let now = Utc::now();

    // Cursor := max(persisted_ts)+1ns, else provider earliest
    let mut cursor: DateTime<Utc> = match latest_data_time(&conn, feed.provider(), feed.clone()) {
        Ok(Some(ts)) => ts + Duration::nanoseconds(1),
        Ok(None) => earliest,
        Err(e) => return Err(e),
    };
    let symbol = feed.instrument();
    let exchange = feed.exchange();
    if cursor >= now {
        tracing::info!(%symbol, %provider_code, %exchange, "up to date");
        return Ok(());
    }

    let kind = match feed {
        Feed::Bbo { .. } => DataKind::Bbo,
        Feed::Ticks { .. } => DataKind::Tick,
        Feed::Candles { .. } => DataKind::Candle,
        Feed::OrderBookL2 { .. } => DataKind::BookL2,
        Feed::OrderBookL3 { .. } => todo!(),
    };

    let wants_intraday = wants_intraday(kind, feed);
    // If we're on a fully-closed calendar day for intraday-ish kinds, jump to next open.
    if wants_intraday && hours.is_closed_all_day_at(cursor, CAL_TZ, SessionKind::Both) {
        let open = next_session_open_after(&hours, cursor);
        if open > cursor {
            cursor = open;
        }
    }

    while cursor < now {
        // Pick a batch window that fits the kind/resolution.
        let span = choose_span(kind, feed.resolution());
        let end = (cursor + span).min(now);

        // For intraday-ish kinds, skip windows that are 100% closed.
        if wants_intraday && window_is_all_closed(&hours, CAL_TZ, cursor, end) {
            cursor = next_session_open_after(&hours, cursor);
            continue;
        }

        let req = HistoricalRequest {
            vendor: provider_code.to_string(),
            feed: feed.clone(),
            start: cursor,
            end, // end-exclusive expected; +1ns cursor guards even if provider is inclusive
        };

        tracing::info!(%symbol, start=%req.start, end=%req.end, kind=?kind, res=?feed.resolution(), "fetching");
        let (_handle, mut rx) = client.clone().fetch(req).await?;

        // Accumulators and watermark of the max timestamp we actually received.
        let mut max_ts: Option<DateTime<Utc>> = None;
        let mut ticks: Vec<TickRow> = Vec::new();
        let mut candles: Vec<CandleRow> = Vec::new();
        let mut quotes: Vec<BboRow> = Vec::new();
        let mut books: Vec<crate::market_data::base_data::OrderBook> = Vec::new();

        while let Some(ev) = rx.recv().await {
            match ev {
                HistoryEvent::Candle(c) => {
                    if matches!(kind, DataKind::Candle) {
                        // Advance by candle END to guarantee no gaps/overlaps between bars
                        let ts = c.time_end;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        candles.push(CandleRow::from_candle(provider_code.to_string(), c));
                    }
                }
                HistoryEvent::Tick(t) => {
                    if matches!(kind, DataKind::Tick) {
                        let ts = t.time;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        let side_u8 = match t.side {
                            crate::market_data::base_data::Side::Buy => 1,
                            crate::market_data::base_data::Side::Sell => 2,
                            _ => 0,
                        };
                        ticks.push(TickRow {
                            provider: provider_code.to_string(),
                            symbol_id: t.symbol,
                            exchange: format!("{:?}", t.exchange),
                            price: t.price.to_f64().unwrap_or(0.0),
                            size: t.volume.to_f64().unwrap_or(0.0),
                            side: side_u8,
                            key_ts_utc_ns: dt_to_ns_i64(t.time), // robust ns key
                            key_tie: t.venue_seq.unwrap_or(0),
                            venue_seq: t.venue_seq,
                            exec_id: t.exec_id,
                        });
                    }
                }
                HistoryEvent::Bbo(q) => {
                    if matches!(kind, DataKind::Bbo) {
                        let ts = q.time;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        quotes.push(BboRow {
                            provider: provider_code.to_string(),
                            symbol_id: q.symbol,
                            exchange: format!("{:?}", q.exchange),
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
                HistoryEvent::OrderBookL2(ob) => {
                    if matches!(kind, DataKind::BookL2) {
                        let ts = ob.time;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        books.push(ob);
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
        match kind {
            DataKind::Tick => {
                if !ticks.is_empty() {
                    let out_paths = ingest_ticks(
                        &conn,
                        &data_root,
                        provider_code,
                        market_type,
                        &symbol,
                        &ticks,
                        9,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted ticks");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no ticks returned");
                }
            }
            DataKind::Bbo => {
                if !quotes.is_empty() {
                    let res = feed
                        .resolution()
                        .expect("BBO requires a resolution (e.g., sec1)");
                    let out_paths = ingest_bbo(
                        &conn,
                        provider_code,
                        market_type,
                        &symbol,
                        res,
                        &quotes,
                        &data_root,
                        9,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted bbo");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no bbo returned");
                }
            }
            DataKind::Candle => {
                if !candles.is_empty() {
                    let out_paths = ingest_candles(
                        &conn,
                        provider_code,
                        &symbol,
                        market_type,
                        feed.resolution().expect("Candle requires resolution"),
                        &candles,
                        &data_root,
                        9,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted candles");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no candles returned");
                }
            }
            DataKind::BookL2 => {
                if !books.is_empty() {
                    let res = feed
                        .resolution()
                        .expect("OrderBook requires a resolution (depth/window key)");
                    let out_paths = ingest_books(
                        &conn,
                        provider_code,
                        market_type,
                        &symbol,
                        res,
                        &books,
                        &data_root,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted order books");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no order books returned");
                }
            }
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
    Ok(())
}

// ---- helpers ----

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

fn choose_span(kind: DataKind, res: Option<Resolution>) -> chrono::Duration {
    match kind {
        DataKind::Candle => match res.expect("Candle requires resolution") {
            Resolution::Weekly => chrono::Duration::days(365), // ~1y
            Resolution::Daily => chrono::Duration::days(365),  // ~1y
            Resolution::Hours(h) if h >= 4 => chrono::Duration::days(60),
            Resolution::Hours(_) => chrono::Duration::days(30),
            Resolution::Minutes(m) if m >= 15 => chrono::Duration::days(14),
            Resolution::Minutes(_) => chrono::Duration::days(7),
            Resolution::Seconds(_) => chrono::Duration::days(1),
            _ => chrono::Duration::days(7),
        },
        DataKind::Bbo => match res.expect("BBO requires resolution") {
            Resolution::Minutes(m) if m >= 1 => chrono::Duration::days(14),
            Resolution::Seconds(_) => chrono::Duration::days(2),
            _ => chrono::Duration::days(2),
        },
        DataKind::BookL2 => match res.expect("OrderBook requires resolution") {
            Resolution::Minutes(m) if m >= 1 => chrono::Duration::days(2),
            _ => chrono::Duration::days(1),
        },
        DataKind::Tick => chrono::Duration::days(1),
    }
}

fn is_daily_or_weekly(res: Option<Resolution>) -> bool {
    matches!(res, Some(Resolution::Daily) | Some(Resolution::Weekly))
}

fn wants_intraday(kind: DataKind, res: Option<Resolution>) -> bool {
    match kind {
        DataKind::Tick | DataKind::Bbo | DataKind::BookL2 => true,
        DataKind::Candle => !is_daily_or_weekly(res),
    }
}

/// True if every calendar day intersecting [start,end) is fully closed.
fn window_is_all_closed(
    hours: &crate::securities::market_hours::MarketHours,
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
