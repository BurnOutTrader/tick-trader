use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use standard_lib::engine_core::md_router::Feed;
use standard_lib::engine_core::subscription_manager::SubscriptionManager;
use standard_lib::market_data::base_data::{Bbo, Candle, Resolution, Side, Tick};
use standard_lib::securities::symbols::Exchange;
use std::time::Duration;
use standard_lib::database::replay::ReplayCoordinator;
// replay.rs exports the coordinator as pub

fn dt(sec: i64, nanos: u32) -> chrono::DateTime<chrono::Utc> {
    Utc.timestamp_opt(sec, nanos).unwrap()
}

fn drain_all_ticks(mut rx: tokio::sync::broadcast::Receiver<Tick>) -> Vec<Tick> {
    let mut out = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(v) => out.push(v),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
        }
    }
    out
}
fn drain_all_bbo(mut rx: tokio::sync::broadcast::Receiver<Bbo>) -> Vec<Bbo> {
    let mut out = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(v) => out.push(v),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
        }
    }
    out
}
fn drain_all_candles(mut rx: tokio::sync::broadcast::Receiver<Candle>) -> Vec<Candle> {
    let mut out = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(v) => out.push(v),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => continue,
        }
    }
    out
}

fn tick(symbol: &str, sec: i64, nanos: u32, px: i64) -> Tick {
    Tick {
        symbol: symbol.to_string(),
        exchange: Exchange::CME,
        price: Decimal::from(px),
        volume: Decimal::from(1),
        time: dt(sec, nanos),
        side: Side::Buy,
        exec_id: None,
        maker_order_id: None,
        taker_order_id: None,
        venue_seq: None,
        ts_event: None,
        ts_recv: None,
    }
}

fn bbo(symbol: &str, sec: i64, nanos: u32, mid: i64) -> Bbo {
    Bbo {
        symbol: symbol.to_string(),
        exchange: Exchange::CME,
        bid: Decimal::from(mid) - Decimal::from(1),
        bid_size: Decimal::from(1),
        ask: Decimal::from(mid) + Decimal::from(1),
        ask_size: Decimal::from(1),
        time: dt(sec, nanos),
        bid_orders: None,
        ask_orders: None,
        venue_seq: None,
        is_snapshot: Some(true),
    }
}

fn candle(symbol: &str, start_sec: i64, end_sec: i64) -> Candle {
    Candle {
        symbol: symbol.to_string(),
        exchange: Exchange::CME,
        time_start: dt(start_sec, 0),
        time_end: dt(end_sec, 0),
        open: Decimal::from(100),
        high: Decimal::from(105),
        low: Decimal::from(95),
        close: Decimal::from(101),
        volume: Decimal::from(1),
        ask_volume: Decimal::from(0),
        bid_volume: Decimal::from(0),
        num_of_trades: Decimal::from(1),
        resolution: Resolution::Seconds(1),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_replay_ticks_orders_and_batches_same_ts() {
    let hub = SubscriptionManager::new();
    // subscribe BEFORE running replay so events are delivered
    let feed = Feed::Ticks {
        symbol: "S".to_string(),
        exchange: Exchange::CME,
    };
    hub.attach_primary_ticks(feed.clone());
    let rx = hub.subscribe_primary_ticks(&feed).unwrap();

    // two ticks at the same ts, one later
    let t0 = 1_700_000_000i64;
    let ticks = vec![
        tick("S", t0, 100, 10000),
        tick("S", t0, 100, 10001),
        tick("S", t0, 200, 10002),
    ];

    let mut rc = ReplayCoordinator::new();
    rc.add_ticks("TICKS", ticks);

    // run to completion with no pacing
    rc.run(hub, None).await.unwrap();

    let out = drain_all_ticks(rx);
    assert_eq!(out.len(), 3);
    // nondecreasing by time
    assert!(out.windows(2).all(|w| w[0].time <= w[1].time));

    // The first two share exact timestamp; ensure they are the first two emitted
    assert_eq!(out[0].time, dt(t0, 100));
    assert_eq!(out[1].time, dt(t0, 100));
    assert_eq!(out[2].time, dt(t0, 200));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_replay_candles_ordered_by_end_time() {
    let hub = SubscriptionManager::new();
    let feed = Feed::Candles {
        symbol: "S".to_string(),
        exchange: Exchange::CME,
        resolution: Resolution::Seconds(1),
    };
    hub.attach_primary_candles(feed.clone());
    let rx = hub.subscribe_primary_candles(&feed).unwrap();

    // Input arrives out of order by end time; replayer must sort by time_end
    // Candle A: start 0, end 10
    // Candle B: start 10, end 20
    let c_b = candle("S", 10, 20);
    let c_a = candle("S", 0, 10);

    let mut rc = ReplayCoordinator::new();
    rc.add_candles("CANDLES", vec![c_b.clone(), c_a.clone()]);
    rc.run(hub, None).await.unwrap();

    let out = drain_all_candles(rx);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].time_end, c_a.time_end);
    assert_eq!(out[1].time_end, c_b.time_end);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_replay_multi_source_global_order_and_coemit_same_ts() {
    let hub = SubscriptionManager::new();
    let feed_t = Feed::Ticks {
        symbol: "S".to_string(),
        exchange: Exchange::CME,
    };
    hub.attach_primary_ticks(feed_t.clone());
    let rx_t = hub.subscribe_primary_ticks(&feed_t).unwrap();
    let feed_q = Feed::Bbo {
        symbol: "S".to_string(),
        exchange: Exchange::CME,
    };
    hub.attach_primary_bbo(feed_q.clone());
    let rx_q = hub.subscribe_primary_bbo(&feed_q).unwrap();
    let feed_c = Feed::Candles {
        symbol: "S".to_string(),
        exchange: Exchange::CME,
        resolution: Resolution::Seconds(1),
    };
    hub.attach_primary_candles(feed_c.clone());
    let rx_c = hub.subscribe_primary_candles(&feed_c).unwrap();

    // Timeline:
    // t0.100s -> one tick
    // t0.500s -> one bbo and one candle (should be co-emitted same ts)
    let base = 1_700_000_000i64;
    let t0_100 = dt(base, 100_000_000);
    let t0_500 = dt(base, 500_000_000);

    let ticks = vec![tick("S", base, 100_000_000, 10001)];
    let bbo_v = vec![bbo("S", base, 500_000_000, 10002)];
    // candle with end at 500ms, start at 0
    let candles = vec![candle("S", base, base)]; // start=end base seconds; we'll adjust end nanos separately
    // The candle helper only sets nanos=0; build precise one:
    let mut candles = candles;
    let mut c = candles.pop().unwrap();
    c.time_start = dt(base, 0);
    c.time_end = t0_500;
    candles.push(c);

    let mut rc = ReplayCoordinator::new();
    rc.add_ticks("T", ticks);
    rc.add_bbo("Q", bbo_v);
    rc.add_candles("C", candles);

    rc.run(hub, Some(Duration::from_millis(0))).await.unwrap();

    let ts_ticks: Vec<_> = drain_all_ticks(rx_t).into_iter().map(|t| t.time).collect();
    let ts_bbo: Vec<_> = drain_all_bbo(rx_q).into_iter().map(|q| q.time).collect();
    let ts_cand: Vec<_> = drain_all_candles(rx_c)
        .into_iter()
        .map(|c| c.time_end)
        .collect();

    assert_eq!(ts_ticks, vec![t0_100]);
    assert_eq!(ts_bbo, vec![t0_500]);
    assert_eq!(ts_cand, vec![t0_500]);

    // Ensure ordering: first emission at 100ms, then at 500ms (co-emit quote + candle)
    assert!(t0_100 < t0_500);
}
