use std::collections::HashMap;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use crate::paths::provider_kind_to_db_string;
use tt_types::keys::Topic;
use tt_types::securities::symbols::Exchange;

struct Entry {
    result: Mutex<Option<anyhow::Result<()>>>,
    notify: Notify,
}

struct DownloadManagerInner {
    inflight: Mutex<HashMap<(ProviderKind, Instrument, Topic), std::sync::Arc<Entry>>>,
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

    pub async fn request(
        &self,
        client: std::sync::Arc<dyn HistoricalDataProvider>,
        conn: std::sync::Arc<duckdb::Connection>,
        data_root: PathBuf,
        provider_kind: ProviderKind,
        instrument: Instrument,
        exchange: Exchange,

        topic: Topic,
    ) -> anyhow::Result<()> {
        // Simplified: run directly without in-flight dedup to avoid Clone/Join complexity
        run_download(client, conn, data_root, provider_kind, instrument, topic, exchange).await
    }
}
use crate::ingest::{ingest_bbo, ingest_candles, ingest_ticks, ingest_books};
use crate::models::{BboRow, CandleRow, TickRow};
use crate::queries::latest_data_time;
use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;
use rust_decimal::prelude::ToPrimitive;
use std::path::PathBuf;
use std::sync::Arc;
use tt_types::base_data::{OrderBook, Resolution, Side};
use tt_types::history::{HistoricalRequest, HistoryEvent};
use tt_types::providers::ProviderKind;
use tt_types::securities::market_hours::{
    MarketHours, SessionKind, hours_for_exchange, next_session_open_after,
};
use tt_types::securities::symbols::{Instrument, exchange_market_type};
use tt_types::server_side::traits::HistoricalDataProvider;

// your helper
async fn run_download(
    client: Arc<dyn HistoricalDataProvider>,
    conn: Arc<duckdb::Connection>,
    data_root: PathBuf,
    provider_kind: ProviderKind,
    instrument: Instrument,
    topic: Topic,
    exchange: Exchange,
) -> anyhow::Result<()> {
    const CAL_TZ: Tz = chrono_tz::UTC;

    let resolution = match topic {
        Topic::Ticks => None,
        Topic::Quotes => None,
        Topic::Depth => None,
        Topic::Candles1s => Some(Resolution::Seconds(1)),
        Topic::Candles1m => Some(Resolution::Minutes(1)),
        Topic::Candles1h => Some(Resolution::Hours(1)),
        Topic::Candles1d => Some(Resolution::Daily),
        _ => return Err(anyhow::anyhow!("invalid topic")),
    };

    let provider_code = client.name();
    let market_type = exchange_market_type(exchange);
    let hours = hours_for_exchange(exchange);

    let earliest = client.earliest_available(instrument.clone(), topic);
    let now = Utc::now();
    let wants_intraday = topic != Topic::Candles1d;

    // Cursor := max(persisted_ts)+1ns, else provider earliest
    let mut cursor: DateTime<Utc> = match latest_data_time(&conn, provider_kind, &instrument, topic) {
        Ok(Some(ts)) => ts + Duration::nanoseconds(1),
        Ok(None) => earliest,
        Err(e) => return Err(e),
    };
    let symbol = instrument.to_string();
    if cursor >= now {
        tracing::info!(%symbol, ?provider_code, %exchange, "up to date");
        return Ok(());
    }

    // If we're on a fully-closed calendar day for intraday-ish kinds, jump to next open.
    if topic != Topic::Candles1d && hours.is_closed_all_day_at(cursor, CAL_TZ, SessionKind::Both) {
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
        if topic != Topic::Candles1d && window_is_all_closed(&hours, CAL_TZ, cursor, end) {
            cursor = next_session_open_after(&hours, cursor);
            continue;
        }

        let req = HistoricalRequest {
            topic:topic.clone(),
            instrument: instrument.clone(),
            start: cursor,
            end,
        };

        tracing::info!(%symbol, start=%req.start, end=%req.end, topic=?topic, res=?resolution, "fetching");
        let events = client.clone().fetch(req).await?;

        // Accumulators and watermark of the max timestamp we actually received.
        let mut max_ts: Option<DateTime<Utc>> = None;
        let mut ticks: Vec<TickRow> = Vec::new();
        let mut candles: Vec<CandleRow> = Vec::new();
        let mut quotes: Vec<BboRow> = Vec::new();
        let mut books: Vec<OrderBook> = Vec::new();

        for ev in events { 
            match ev {
                HistoryEvent::Candle(c) => {
                    if matches!(topic, Topic::Candles1s | Topic::Candles1m | Topic::Candles1h | Topic::Candles1d) {
                        // Advance by candle END to guarantee no gaps/overlaps between bars
                        let ts = c.time_end;
                        max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                        candles.push(CandleRow::from_candle(provider_code, c));
                    }
                }
                HistoryEvent::Tick(t) => {
                    if matches!(topic, Topic::Ticks) {
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
                    if matches!(topic, Topic::Quotes) {
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
                HistoryEvent::OrderBook(ob) => {
                    if matches!(topic, Topic::Depth) {
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
        match topic {
            Topic::Ticks => {
                if !ticks.is_empty() {
                    let out_paths = ingest_ticks(
                        &conn,
                        &provider_kind,
                        &instrument,
                        market_type,
                        topic,
                        &ticks,
                        &data_root,
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
                        &conn,
                        &provider_kind,
                        &instrument,
                        market_type,
                        topic,
                        &quotes,
                        &data_root,
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
                        &conn,
                        &provider_kind,
                        &instrument,
                        market_type,
                        topic,
                        &candles,
                        &data_root,
                        9,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted candles");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no candles returned");
                }
            }
            Topic::Depth => {
                if !books.is_empty() {
                    let out_paths = ingest_books(
                        &conn,
                        &provider_kind,
                        &instrument,
                        market_type,
                        topic,
                        &books,
                        &data_root,
                    )?;
                    tracing::info!(files = out_paths.len(), start=%cursor, end=%end, "persisted order books");
                } else {
                    tracing::debug!(start=%cursor, end=%end, "no order books returned");
                }
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
    Ok(())
}

// ---- helpers ----

fn choose_span(res: Option<Resolution>) -> chrono::Duration {
    if let Some(res) = res {
        return match res {
            Resolution::Daily | Resolution::Weekly => chrono::Duration::days(5000),
            Resolution::Hours(h) =>  chrono::Duration::days(h as i64 * 100),
            _ => chrono::Duration::days(1),
        }
    }
    chrono::Duration::days(30)
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
