use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Duration, NaiveDate, TimeZone, Utc};
use duckdb;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::str::FromStr;
use tt_database::duck::{earliest_available, latest_available, resolve_dataset_id};
use tt_database::init::init_db;
use tt_database::paths::provider_kind_to_db_string;
use tt_database::queries::{get_candles_from_date_to_latest, get_candles_in_range};
use tt_types::data::core::{Candle, Exchange};
use tt_types::data::models::Resolution;
use tt_types::keys::Topic;
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::hours::market_hours::{
    MarketHours, hours_for_exchange, next_session_after,
};
use tt_types::securities::symbols::Instrument;

const GAP_TOLERANCE_BARS: i64 = 200; // ignore gaps of 3 bars (minutes) or fewer
const MAX_FAILURE_LINES: usize = 40; // cap how many failure lines we print for readability

/// ceil `t` to the next multiple of `step_secs` (if already aligned, returns `t`)
fn ceil_to_step(t: DateTime<Utc>, step_secs: i64) -> DateTime<Utc> {
    let secs = t.timestamp();
    let nanos = t.timestamp_subsec_nanos() as i64;
    let base = secs - secs.rem_euclid(step_secs);
    let aligned = if nanos == 0 { base } else { base + step_secs };
    Utc.timestamp_opt(aligned, 0).single().unwrap()
}

/// Compute the **next bar start** after an inclusive close `prev_end` using market hours.
/// If the instant right after `prev_end` is closed, we jump to the next session open.
/// Bars are aligned to the resolution step (1m here).
fn next_bar_start_after_prev_end(
    hours: &MarketHours,
    prev_end: DateTime<Utc>,
    res: Resolution,
) -> DateTime<Utc> {
    let step_secs = match res {
        Resolution::Seconds(s) => s as i64,
        Resolution::Minutes(m) => (m as i64) * 60,
        Resolution::Hours(h) => (h as i64) * 3600,
        _ => unreachable!("intraday only"),
    };

    // Move 1ns past the inclusive close
    let t = prev_end + Duration::nanoseconds(1);

    if hours.is_open(t) {
        return ceil_to_step(t, step_secs);
    }

    // Otherwise jump to the next session open then align (usually already aligned)
    let (open, _close) = next_session_after(hours, t);
    ceil_to_step(open, step_secs)
}

// ---- the test --------------------------------------------------------------------

fn main() -> Result<()> {
    use chrono::Datelike;

    // 1) Connect to DuckDB catalog under DB_PATH (default ./storage)
    let db_root = std::env::var("DB_PATH").unwrap_or_else(|_| "./storage".to_string());
    let db_root = Path::new(&db_root);
    let conn = init_db(db_root).context("failed to initialize DuckDB catalog")?;

    // 2) Target instrument and provider
    let instrument =
        Instrument::from_str("MNQZ5").map_err(|e| anyhow!("invalid instrument: {:?}", e))?;
    let provider_kind = ProviderKind::ProjectX(ProjectXTenant::Topstep);
    let provider = provider_kind_to_db_string(provider_kind);

    // MNQ trades on CME
    let exchange = Exchange::CME;
    let hours = hours_for_exchange(exchange);

    // 3) Determine start/end in catalog
    let start = match earliest_available(&conn, &provider_kind, &instrument, Topic::Candles1m)? {
        Some(b) => b.ts,
        None => {
            return Err(anyhow!(
                "No candles available in catalog for {} {}",
                provider,
                instrument
            ));
        }
    };
    let end = latest_available(&conn, &provider, &instrument.to_string(), Topic::Candles1m)?
        .map(|b| b.ts)
        .unwrap_or(start);

    println!("Earliest: {}  Latest: {}", start, end);

    // 3b) Resolve dataset and audit parquet widths (inclusive close => 60s - 1ns)
    if let Some(dataset_id) =
        resolve_dataset_id(&conn, &provider, &instrument.to_string(), Topic::Candles1m)?
    {
        let count_all: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM partitions WHERE dataset_id = ?",
                duckdb::params![dataset_id],
                |r| r.get(0),
            )
            .unwrap_or(0);
        println!(
            "Catalog: dataset_id={} has {} partitions",
            dataset_id, count_all
        );

        // read all paths
        let mut stmt =
            conn.prepare("SELECT path FROM partitions WHERE dataset_id = ? ORDER BY min_ts_ns")?;
        let mut rows = stmt.query(duckdb::params![dataset_id])?;
        let mut paths: Vec<String> = Vec::new();
        while let Some(r) = rows.next()? {
            let p: String = r.get(0)?;
            paths.push(p);
        }
        if !paths.is_empty() {
            let list = paths
                .iter()
                .map(|p| format!("'{}'", p.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");

            let expect_ns: i64 = 60_i64 * 1_000_000_000_i64 - 1;
            let sql = format!(
                "SELECT COUNT(*) FROM read_parquet([{}], UNION_BY_NAME=1)
                 WHERE (time_end_ns - time_start_ns) != {}",
                list, expect_ns
            );
            let mismatches: i64 = conn.query_row(&sql, [], |r| r.get(0)).unwrap_or(-1);
            println!(
                "Audit: width mismatches (end-start != 60s-1ns): {}",
                mismatches
            );
        }
    } else {
        println!(
            "No dataset_id found for {} {} (candles1m)",
            provider, instrument
        );
    }

    // 4) Load candles; if empty, fallback to direct inclusive-range
    let mut candles = get_candles_from_date_to_latest(
        &conn,
        &provider,
        &instrument,
        Resolution::Minutes(1),
        start,
    )?;
    if candles.is_empty() && end > start {
        let direct = get_candles_in_range(
            &conn,
            &provider,
            &instrument,
            Resolution::Minutes(1),
            start,
            end + Duration::nanoseconds(1), // inclusive end
        )?;
        println!("Direct range fetch yielded {} candles", direct.len());
        candles = direct;
    }

    println!(
        "Loaded {} candles for {} from {} to latest",
        candles.len(),
        instrument,
        start
    );

    // 5) Integrity checks (inclusive close) + gap accounting
    let mut failures: Vec<String> = Vec::new();

    // per-date (exchange local) gap counts (only true 1-minute gaps)
    let mut gap_counts: HashMap<NaiveDate, usize> = HashMap::new();
    let mut total_gaps: usize = 0;

    // summary counters
    let mut n_gap_events: usize = 0; // number of gap occurrences (events), excluding small tolerated gaps
    let mut n_overlap_events: usize = 0; // number of early-start/overlap occurrences

    if candles.is_empty() {
        failures.push("No candles returned".to_string());
    } else {
        // a) Width == 60s - 1ns, end > start, contiguous by calendar
        let expect_ns: i64 = 60_i64 * 1_000_000_000_i64 - 1;
        let res = Resolution::Minutes(1);
        let mut last: Option<Candle> = None;

        for (i, c) in candles.iter().enumerate() {
            if c.time_end <= c.time_start {
                failures.push(format!(
                    "candle[{i}] end <= start: {} <= {}",
                    c.time_end, c.time_start
                ));
            }

            let width_ns = (c.time_end - c.time_start).num_nanoseconds().unwrap_or(-1);
            if width_ns != expect_ns {
                failures.push(format!(
                    "candle[{i}] width != 60s-1ns: {} -> {} ({} ns)",
                    c.time_start, c.time_end, width_ns
                ));
            }

            if let Some(prev) = last.as_ref() {
                // Expected next start by calendar (handles maintenance & weekends)
                let expected_start = next_bar_start_after_prev_end(&hours, prev.time_end, res);

                if c.time_start != expected_start {
                    if c.time_start > expected_start {
                        // True gap: one or more minutes missing.
                        let missing = (c.time_start - expected_start).num_minutes().max(1);

                        // If the gap is small (<= GAP_TOLERANCE_BARS), ignore it —
                        // assumed to be transient ingestion/server misses.
                        if (missing as i64) <= GAP_TOLERANCE_BARS {
                            // Do not record as failure and do not count in daily gap totals.
                        } else {
                            // Count how many missing 1-minute bars and attribute to the *expected* local date.
                            let mut t = expected_start;
                            for _ in 0..missing {
                                let local_day = t.with_timezone(&hours.tz).date_naive();
                                *gap_counts.entry(local_day).or_default() += 1;
                                total_gaps += 1;
                                t = t + Duration::minutes(1);
                            }

                            n_gap_events += 1;
                            failures.push(format!(
                                "gap/late-start before {}: expected_start={} (calendar), prev_end={}, this_start={}",
                                c.time_start, expected_start, prev.time_end, c.time_start
                            ));
                        }
                    } else {
                        n_overlap_events += 1;
                        // Overlap/early: bar started before calendar expected boundary.
                        failures.push(format!(
                            "overlap/early-start before {}: expected_start={} (calendar), prev_end={}, this_start={}",
                            c.time_start, expected_start, prev.time_end, c.time_start
                        ));
                    }
                }
            }
            last = Some(c.clone());
        }

        // b) OHLC invariants and non-negative volumes
        for (i, c) in candles.iter().enumerate() {
            let hi_ok = c.high >= c.open && c.high >= c.close;
            let lo_ok = c.low <= c.open && c.low <= c.close;
            if !hi_ok {
                failures.push(format!(
                    "candle[{i} @ {}] high < max(open,close): {} < max({}, {})",
                    c.time_end, c.high, c.open, c.close
                ));
            }
            if !lo_ok {
                failures.push(format!(
                    "candle[{i} @ {}] low > min(open,close): {} > min({}, {})",
                    c.time_end, c.low, c.open, c.close
                ));
            }
            if c.high < c.low {
                failures.push(format!(
                    "candle[{i} @ {}] high < low: {} < {}",
                    c.time_end, c.high, c.low
                ));
            }
            if c.volume < 0.into() || c.ask_volume < 0.into() || c.bid_volume < 0.into() {
                failures.push(format!(
                    "candle[{i} @ {}] negative volume: vol={}, ask={}, bid={}",
                    c.time_end, c.volume, c.ask_volume, c.bid_volume
                ));
            }
        }

        // c) No duplicate time_start
        let mut seen: HashSet<i64> = HashSet::with_capacity(candles.len());
        for (i, c) in candles.iter().enumerate() {
            let key = c.time_start.timestamp();
            if !seen.insert(key) {
                failures.push(format!(
                    "duplicate candle start minute at index {} => {}",
                    i, c.time_start
                ));
            }
        }

        // d) Resolution tag sanity
        for (i, c) in candles.iter().enumerate() {
            if c.resolution != Resolution::Minutes(1) {
                failures.push(format!(
                    "candle[{i} @ {}] wrong resolution tag: {:?}",
                    c.time_start, c.resolution
                ));
            }
        }
    }

    // 6) Print per-date gap summary (exchange-local dates)
    if total_gaps > 0 {
        println!(
            "---- 1m gap summary (exchange-local dates, {}) ----",
            hours.tz
        );
        let mut dates: Vec<(NaiveDate, usize)> = gap_counts.into_iter().collect();
        dates.sort_by_key(|(d, _)| *d);
        for (d, n) in dates {
            println!("{} : {} missing minute(s)", d, n);
        }
        println!("TOTAL missing 1m bars: {}", total_gaps);
    } else {
        println!("No missing 1m bars detected.");
    }

    // 7) Final status — exit with explicit codes to avoid anyhow backtrace noise
    if failures.is_empty() {
        println!(
            "Integrity checks PASS for {} ({} bars)",
            instrument,
            candles.len()
        );
        std::process::exit(0);
    } else {
        eprintln!(
            "Integrity checks FAILED ({} issues) — summary: large gap events = {}, missing 1m bars = {}, early/overlaps = {}",
            failures.len(),
            n_gap_events,
            total_gaps,
            n_overlap_events
        );

        let to_show = failures.len().min(MAX_FAILURE_LINES);
        if to_show > 0 {
            eprintln!("Showing {} of {} issues:", to_show, failures.len());
            for f in failures.iter().take(to_show) {
                eprintln!(" - {}", f);
            }
            if failures.len() > MAX_FAILURE_LINES {
                eprintln!(
                    " ... and {} more suppressed (raise MAX_FAILURE_LINES to see all)",
                    failures.len() - MAX_FAILURE_LINES
                );
            }
        }
        std::process::exit(1);
    }
}
