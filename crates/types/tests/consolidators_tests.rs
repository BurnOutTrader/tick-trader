use chrono::{Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use tokio::sync::broadcast;
use tt_types::base_data::{Bbo, Candle, Exchange, Feed, Resolution, Side, Tick};
use tt_types::consolidators::{
    BboToCandlesConsolidator, CandlesToCandlesConsolidator, Consolidator,
    TicksToCandlesConsolidator, TicksToTickBarsConsolidator,
};
use tt_types::securities::symbols::Instrument;

fn d(v: i64) -> Decimal {
    Decimal::from_i128_with_scale(v as i128, 0)
}

fn tick(sym: &str, t_ns: i64, px: i64, sz: i64, side: Side) -> Tick {
    let ts = chrono::DateTime::<Utc>::from_timestamp(
        t_ns.div_euclid(1_000_000_000),
        (t_ns.rem_euclid(1_000_000_000)) as u32,
    )
    .unwrap();
    let instrument = Instrument::try_from(sym).unwrap();
    Tick {
        symbol: sym.to_string(),
        instrument,
        price: d(px),
        volume: d(sz),
        time: ts,
        side,
        venue_seq: None,
    }
}

fn bbo(sym: &str, t_ns: i64, bid: i64, ask: i64) -> Bbo {
    let ts = chrono::DateTime::<Utc>::from_timestamp(
        t_ns.div_euclid(1_000_000_000),
        (t_ns.rem_euclid(1_000_000_000)) as u32,
    )
    .unwrap();
    Bbo {
        symbol: sym.to_string(),
        instrument: Instrument::try_from(sym).unwrap(),
        bid: d(bid),
        bid_size: d(1),
        ask: d(ask),
        ask_size: d(1),
        time: ts,
        is_snapshot: Some(true),
        venue_seq: None,
        bid_orders: None,
        ask_orders: None,
    }
}

fn candle(
    sym: &str,
    start: chrono::DateTime<Utc>,
    secs: i64,
    o: i64,
    h: i64,
    l: i64,
    c: i64,
    vol: i64,
) -> Candle {
    Candle {
        symbol: sym.to_string(),
        instrument: Instrument::try_from(sym).unwrap(),
        time_start: start,
        time_end: start + Duration::seconds(secs) - Duration::nanoseconds(1),
        open: d(o),
        high: d(h),
        low: d(l),
        close: d(c),
        volume: d(vol),
        ask_volume: d(0),
        bid_volume: d(0),
        resolution: Resolution::Minutes(1),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn ticks_to_m1_single_bar() {
    let symbol = "MNQZ5";
    let (tx_in, rx_in) = broadcast::channel::<Tick>(64);
    let cons = TicksToCandlesConsolidator::new(
        Resolution::Minutes(1),
        rx_in,
        symbol.to_string(),
        None,
        Instrument::try_from(symbol).unwrap(),
        None,
    );
    let mut rx_out = cons.subscribe(Feed::Candles {
        symbol: symbol.to_string(),
        exchange: Exchange::CME,
        resolution: Resolution::Minutes(1),
    });

    let base = Utc
        .with_ymd_and_hms(2025, 9, 21, 10, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    // 3 ticks within the first minute bucket
    tx_in
        .send(tick(symbol, base + 100_000_000, 10000, 1, Side::Buy))
        .unwrap();
    tx_in
        .send(tick(symbol, base + 200_000_000, 10005, 2, Side::Sell))
        .unwrap();
    tx_in
        .send(tick(symbol, base + 500_000_000, 10003, 3, Side::Buy))
        .unwrap();

    // A tick in the next minute should flush the first bar
    tx_in
        .send(tick(
            symbol,
            base + 60_000_000_000 + 1_000_000,
            10002,
            1,
            Side::Sell,
        ))
        .unwrap();

    let bar = tokio::time::timeout(std::time::Duration::from_secs(2), rx_out.recv())
        .await
        .expect("timeout")
        .unwrap();

    assert_eq!(bar.symbol, symbol);
    assert_eq!(bar.resolution, Resolution::Minutes(1));
    assert_eq!(bar.open, d(10000));
    assert_eq!(bar.high, d(10005));
    assert_eq!(bar.low, d(10000));
    assert_eq!(bar.close, d(10003));
    // vols: total=1+2+3=6, ask_vol accumulates buys, bid_vol accumulates sells (per consolidator logic)
    assert_eq!(bar.volume, d(6));
    assert_eq!(bar.ask_volume, d(1 + 3));
    assert_eq!(bar.bid_volume, d(2));

    // window check: end is inclusive end-1ns
    let expected_start = chrono::DateTime::<Utc>::from_timestamp(base / 1_000_000_000, 0).unwrap();
    assert_eq!(bar.time_start, expected_start);
    let expected_end = expected_start + Duration::minutes(1) - Duration::nanoseconds(1);
    assert_eq!(bar.time_end, expected_end);
}

#[tokio::test(flavor = "current_thread")]
async fn ticks_to_tickbars_two_bars() {
    let symbol = "MNQZ5";
    let (tx_in, rx_in) = broadcast::channel::<Tick>(64);
    let cons = TicksToTickBarsConsolidator::new(
        3,
        rx_in,
        symbol.to_string(),
        Exchange::CME,
        Instrument::try_from(symbol).unwrap(),
    );
    let mut rx_out = cons.subscribe(Feed::TickBars {
        symbol: symbol.to_string(),
        exchange: Exchange::CME,
        ticks: 3,
    });

    let base = Utc
        .with_ymd_and_hms(2025, 9, 21, 10, 5, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    // 6 ticks -> expect two bars of 3 each
    for i in 0..6 {
        tx_in
            .send(tick(
                symbol,
                base + (i as i64) * 1_000_000,
                10000 + i as i64,
                1,
                if i % 2 == 0 { Side::Buy } else { Side::Sell },
            ))
            .unwrap();
    }

    let bar1 = tokio::time::timeout(std::time::Duration::from_secs(2), rx_out.recv())
        .await
        .unwrap()
        .unwrap();
    let bar2 = tokio::time::timeout(std::time::Duration::from_secs(2), rx_out.recv())
        .await
        .unwrap()
        .unwrap();

    // First bar OHLC 10000..10002
    assert_eq!(bar1.open, d(10000));
    assert_eq!(bar1.high, d(10002));
    assert_eq!(bar1.low, d(10000));
    assert_eq!(bar1.close, d(10002));
    // Second bar 10003..10005
    assert_eq!(bar2.open, d(10003));
    assert_eq!(bar2.high, d(10005));
    assert_eq!(bar2.low, d(10003));
    assert_eq!(bar2.close, d(10005));
}

#[tokio::test(flavor = "current_thread")]
async fn candles_to_m5_from_m1() {
    let symbol = "MNQZ5";
    let (tx_in, rx_in) = broadcast::channel::<Candle>(64);
    let cons = CandlesToCandlesConsolidator::new(
        Resolution::Minutes(5),
        rx_in,
        symbol.to_string(),
        None,
        Instrument::try_from(symbol).unwrap(),
    );
    let mut rx_out = cons.subscribe(Feed::Candles {
        symbol: symbol.to_string(),
        exchange: Exchange::CME,
        resolution: Resolution::Minutes(5),
    });

    // 5 x 1m candles starting at 10:00:00
    let start = Utc.with_ymd_and_hms(2025, 9, 21, 10, 0, 0).unwrap();
    for i in 0..5 {
        let c_in = candle(
            symbol,
            start + Duration::minutes(i),
            60,
            10000 + i,
            10010 + i,
            9990 + i,
            10005 + i,
            10,
        );
        tx_in.send(c_in).unwrap();
    }
    // Next candle should flush
    let c_next = candle(
        symbol,
        start + Duration::minutes(5),
        60,
        10020,
        10025,
        10015,
        10022,
        7,
    );
    tx_in.send(c_next).unwrap();

    let out = tokio::time::timeout(std::time::Duration::from_secs(2), rx_out.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.resolution, Resolution::Minutes(5));
    assert_eq!(out.open, d(10000));
    assert_eq!(out.high, d(10014)); // max of highs 10010..10014
    assert_eq!(out.low, d(9990));
    assert_eq!(out.close, d(10009));
    assert_eq!(out.volume, d(50));

    assert_eq!(out.time_start, start);
    let expected_end = start + Duration::minutes(5) - Duration::nanoseconds(1);
    assert_eq!(out.time_end, expected_end);
}

#[tokio::test(flavor = "current_thread")]
async fn bbo_to_m1_two_bars_midprice() {
    let symbol = "MNQZ5";
    let (tx_in, rx_in) = broadcast::channel::<Bbo>(64);
    let cons = BboToCandlesConsolidator::new(
        Resolution::Minutes(1),
        rx_in,
        symbol.to_string(),
        None,
        Instrument::try_from(symbol).unwrap(),
        None,
    );
    let mut rx_out = cons.subscribe(Feed::Candles {
        symbol: symbol.to_string(),
        exchange: Exchange::CME,
        resolution: Resolution::Minutes(1),
    });

    let base = Utc
        .with_ymd_and_hms(2025, 9, 21, 11, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();

    // First minute
    tx_in
        .send(bbo(symbol, base + 100_000_000, 10000, 10002))
        .unwrap(); // mid 10001
    tx_in
        .send(bbo(symbol, base + 200_000_000, 10005, 10007))
        .unwrap(); // mid 10006
    // Second minute -> triggers flush
    tx_in
        .send(bbo(symbol, base + 60_000_000_000 + 1_000_000, 9999, 10001))
        .unwrap(); // mid 10000

    let out1 = tokio::time::timeout(std::time::Duration::from_secs(2), rx_out.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out1.resolution, Resolution::Minutes(1));
    assert_eq!(out1.open, d(10001));
    assert_eq!(out1.high, d(10006));
    assert_eq!(out1.low, d(10001));
    assert_eq!(out1.close, d(10006));
    assert_eq!(out1.volume, d(0)); // BBO consolidator uses ZERO volume

    // Ensure we can get second bar too if needed
    let _maybe_second = tokio::time::timeout(std::time::Duration::from_millis(200), rx_out.recv())
        .await
        .ok();
}

#[tokio::test(flavor = "current_thread")]
async fn candles_to_m5_no_feedback_duplicate() {
    let symbol = "MNQZ5";

    // Input channel for 1m candles to the consolidator
    let (tx_in, rx_in) = broadcast::channel::<Candle>(64);
    let cons = CandlesToCandlesConsolidator::new(
        Resolution::Minutes(5),
        rx_in,
        symbol.to_string(),
        None,
        Instrument::try_from(symbol).unwrap(),
    );
    // We will receive consolidated 5m bars from the consolidator
    let mut rx_out = cons.subscribe(Feed::Candles {
        symbol: symbol.to_string(),
        exchange: Exchange::CME,
        resolution: Resolution::Minutes(5),
    });

    // 5 x 1m candles starting at 10:00:00
    let start = Utc.with_ymd_and_hms(2025, 9, 21, 10, 0, 0).unwrap();
    for i in 0..5 {
        let c_in = candle(
            symbol,
            start + Duration::minutes(i),
            60,
            10000 + i,
            10010 + i,
            9990 + i,
            10005 + i,
            10,
        );
        tx_in.send(c_in).unwrap();
    }
    // Next minute starts the new 5m window and should flush the previous window (emit exactly one 5m)
    let c_next = candle(
        symbol,
        start + Duration::minutes(5),
        60,
        10020,
        10025,
        10015,
        10022,
        7,
    );
    tx_in.send(c_next).unwrap();

    // Expect exactly one 5m output
    let out = tokio::time::timeout(std::time::Duration::from_secs(2), rx_out.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(out.resolution, Resolution::Minutes(5));
    assert_eq!(out.time_start, start);

    // Ensure no immediate second 5m bar is emitted due to feedback; short timeout should not yield another
    let maybe_extra =
        tokio::time::timeout(std::time::Duration::from_millis(200), rx_out.recv()).await;
    assert!(
        maybe_extra.is_err(),
        "unexpected extra 5m bar emitted due to feedback loop"
    );
}
