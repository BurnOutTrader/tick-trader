use chrono::{Duration, TimeZone, Utc};
use rust_decimal::Decimal;
use std::str::FromStr;
use tt_types::consolidators::{
    BboToCandlesConsolidator, CandlesToCandlesConsolidator, TicksToCandlesConsolidator,
    TicksToTickBarsConsolidator,
};
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::models::{Resolution, TradeSide};
use tt_types::securities::symbols::Instrument;

fn d(v: i64) -> Decimal {
    Decimal::from_i128_with_scale(v as i128, 0)
}

fn tick(sym: &str, t_ns: i64, px: i64, sz: i64, side: TradeSide) -> Tick {
    let ts = chrono::DateTime::<Utc>::from_timestamp(
        t_ns.div_euclid(1_000_000_000),
        t_ns.rem_euclid(1_000_000_000) as u32,
    )
    .unwrap();
    let instrument = Instrument::from_str(sym).unwrap();
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
        t_ns.rem_euclid(1_000_000_000) as u32,
    )
    .unwrap();
    Bbo {
        symbol: sym.to_string(),
        instrument: Instrument::from_str(sym).unwrap(),
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

#[allow(clippy::too_many_arguments)]
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
        instrument: Instrument::from_str(sym).unwrap(),
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

#[allow(clippy::too_many_arguments)]
fn candle_with_res(
    sym: &str,
    start: chrono::DateTime<Utc>,
    secs: i64,
    o: i64,
    h: i64,
    l: i64,
    c: i64,
    vol: i64,
    res: Resolution,
) -> Candle {
    Candle {
        symbol: sym.to_string(),
        instrument: Instrument::from_str(sym).unwrap(),
        time_start: start,
        time_end: start + Duration::seconds(secs) - Duration::nanoseconds(1),
        open: d(o),
        high: d(h),
        low: d(l),
        close: d(c),
        volume: d(vol),
        ask_volume: d(0),
        bid_volume: d(0),
        resolution: res,
    }
}

#[test]
fn ticks_to_m1_single_bar() {
    let symbol = "MNQ.Z25";
    let mut cons = TicksToCandlesConsolidator::new(
        Resolution::Minutes(1),
        symbol.to_string(),
        None,
        Instrument::from_str(symbol).unwrap(),
    );

    let base = Utc
        .with_ymd_and_hms(2025, 9, 21, 10, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    // 3 ticks within the first minute bucket
    assert!(
        cons.update_tick(&tick(symbol, base + 100_000_000, 10000, 1, TradeSide::Buy))
            .is_none()
    );
    assert!(
        cons.update_tick(&tick(symbol, base + 200_000_000, 10005, 2, TradeSide::Sell))
            .is_none()
    );
    assert!(
        cons.update_tick(&tick(symbol, base + 500_000_000, 10003, 3, TradeSide::Buy))
            .is_none()
    );

    // A tick in the next minute should flush the first bar
    let bar = cons
        .update_tick(&tick(
            symbol,
            base + 60_000_000_000 + 1_000_000,
            10002,
            1,
            TradeSide::Sell,
        ))
        .expect("expected 1m bar on minute crossover");

    assert_eq!(bar.symbol, symbol);
    assert_eq!(bar.resolution, Resolution::Minutes(1));
    assert_eq!(bar.open, d(10000));
    assert_eq!(bar.high, d(10005));
    assert_eq!(bar.low, d(10000));
    assert_eq!(bar.close, d(10003));
    // vols: total=1+2+3=6, ask_vol accumulates buys, bid_vol accumulates sells
    assert_eq!(bar.volume, d(6));
    assert_eq!(bar.ask_volume, d(1 + 3));
    assert_eq!(bar.bid_volume, d(2));

    // window check: the end is inclusive end-1ns
    let expected_start = chrono::DateTime::<Utc>::from_timestamp(base / 1_000_000_000, 0).unwrap();
    assert_eq!(bar.time_start, expected_start);
    let expected_end = expected_start + Duration::minutes(1) - Duration::nanoseconds(1);
    assert_eq!(bar.time_end, expected_end);
}

#[test]
fn ticks_to_tickbars_two_bars() {
    let symbol = "MNQ.Z25";
    let mut cons = TicksToTickBarsConsolidator::new(
        3,
        symbol.to_string(),
        Instrument::from_str(symbol).unwrap(),
    );

    let base = Utc
        .with_ymd_and_hms(2025, 9, 21, 10, 5, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();
    // 6 ticks -> expect two bars of 3 each
    let mut outs = vec![];
    for i in 0..6 {
        if let Some(tb) = cons.update_tick(&tick(
            symbol,
            base + (i as i64) * 1_000_000,
            10000 + i as i64,
            1,
            if i % 2 == 0 {
                TradeSide::Buy
            } else {
                TradeSide::Sell
            },
        )) {
            outs.push(tb);
        }
    }

    assert_eq!(outs.len(), 2, "expected two tick bars");
    let bar1 = &outs[0];
    let bar2 = &outs[1];

    // First bar OHLC 10000..10002
    assert_eq!(bar1.open, d(10000));
    assert_eq!(bar1.high, d(10002));
    assert_eq!(bar1.low, d(10000));
    assert_eq!(bar1.close, d(10002));
    // Second bar 10003. 10005
    assert_eq!(bar2.open, d(10003));
    assert_eq!(bar2.high, d(10005));
    assert_eq!(bar2.low, d(10003));
    assert_eq!(bar2.close, d(10005));
}

#[test]
fn candles_to_m5_from_m1() {
    let symbol = "MNQ.Z25";
    let mut cons = CandlesToCandlesConsolidator::new(
        Resolution::Minutes(5),
        symbol.to_string(),
        None,
        Instrument::from_str(symbol).unwrap(),
    );

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
        assert!(cons.update_candle(&c_in).is_none());
    }
    // The next candle should flush
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
    let out = cons.update_candle(&c_next).expect("expected 5m bar");
    assert_eq!(out.resolution, Resolution::Minutes(5));
    assert_eq!(out.open, d(10000));
    assert_eq!(out.high, d(10014)); // max of highs 10010. 10014
    assert_eq!(out.low, d(9990));
    assert_eq!(out.close, d(10009));
    assert_eq!(out.volume, d(50));

    assert_eq!(out.time_start, start);
    let expected_end = start + Duration::minutes(5) - Duration::nanoseconds(1);
    assert_eq!(out.time_end, expected_end);
}

#[test]
fn bbo_to_m1_two_bars_midprice() {
    let symbol = "MNQ.Z25";
    let mut cons = BboToCandlesConsolidator::new(
        Resolution::Minutes(1),
        symbol.to_string(),
        None,
        Instrument::from_str(symbol).unwrap(),
    );

    let base = Utc
        .with_ymd_and_hms(2025, 9, 21, 11, 0, 0)
        .unwrap()
        .timestamp_nanos_opt()
        .unwrap();

    // First minute
    assert!(
        cons.update_bbo(&bbo(symbol, base + 100_000_000, 10000, 10002))
            .is_none()
    ); // mid 10001
    assert!(
        cons.update_bbo(&bbo(symbol, base + 200_000_000, 10005, 10007))
            .is_none()
    ); // mid 10006
    // Second minute -> triggers flush
    let out1 = cons
        .update_bbo(&bbo(symbol, base + 60_000_000_000 + 1_000_000, 9999, 10001))
        .expect("expected 1m bar from BBO midprice");

    assert_eq!(out1.resolution, Resolution::Minutes(1));
    assert_eq!(out1.open, d(10001));
    assert_eq!(out1.high, d(10006));
    assert_eq!(out1.low, d(10001));
    assert_eq!(out1.close, d(10006));
    assert_eq!(out1.volume, d(0)); // BBO consolidator uses ZERO volume
}

#[test]
fn candles_to_m5_no_feedback_duplicate() {
    let symbol = "MNQ.Z25";

    let mut cons = CandlesToCandlesConsolidator::new(
        Resolution::Minutes(5),
        symbol.to_string(),
        None,
        Instrument::from_str(symbol).unwrap(),
    );

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
        assert!(cons.update_candle(&c_in).is_none());
    }
    // Next minute starts the new 5 m window and should flush the previous window (emit exactly one 5 m)
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

    let out = cons
        .update_candle(&c_next)
        .expect("expected a single 5m bar");
    assert_eq!(out.resolution, Resolution::Minutes(5));
    assert_eq!(out.time_start, start);

    // Ensure no immediate second 5 m bar is emitted due to feedback
    assert!(cons.update_candle(&c_next).is_none());
}

#[test]
fn candles_to_m5_reject_equal_and_coarser() {
    let symbol = "MNQ.Z25";
    let mut cons = CandlesToCandlesConsolidator::new(
        Resolution::Minutes(5),
        symbol.to_string(),
        None,
        Instrument::from_str(symbol).unwrap(),
    );

    let start = Utc.with_ymd_and_hms(2025, 9, 21, 10, 0, 0).unwrap();

    // Equal resolution 5m candle should be ignored
    let c_5m = candle_with_res(
        symbol,
        start,
        300,
        100,
        105,
        95,
        102,
        10,
        Resolution::Minutes(5),
    );
    assert!(cons.update_candle(&c_5m).is_none());

    // Coarser resolution 15m candle should be ignored
    let c_15m = candle_with_res(
        symbol,
        start,
        900,
        200,
        210,
        190,
        205,
        20,
        Resolution::Minutes(15),
    );
    assert!(cons.update_candle(&c_15m).is_none());

    // Finer 1m candles should still accumulate and produce a 5m bar after 5 inputs
    for i in 0..5 {
        let c_1m = candle_with_res(
            symbol,
            start + Duration::minutes(i),
            60,
            300 + i,
            305 + i,
            295 + i,
            302 + i,
            5,
            Resolution::Minutes(1),
        );
        assert!(cons.update_candle(&c_1m).is_none());
    }
    let next_1m = candle_with_res(
        symbol,
        start + Duration::minutes(5),
        60,
        400,
        405,
        395,
        402,
        5,
        Resolution::Minutes(1),
    );
    let out = cons
        .update_candle(&next_1m)
        .expect("expected 5m bar after 5 x 1m");
    assert_eq!(out.resolution, Resolution::Minutes(5));
    assert_eq!(out.time_start, start);
}
