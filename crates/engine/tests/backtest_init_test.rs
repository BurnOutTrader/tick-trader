use tt_engine::backtest::init::BacktestInit;

#[test]
fn backtest_init_cme_defaults_builds() {
    let cfg = BacktestInit::px_defaults()
        .with_step(chrono::Duration::milliseconds(250))
        .build();
    // Basic assertions on config shape
    assert_eq!(cfg.step, chrono::Duration::milliseconds(250));
    // Verify feeder factories are present and callable
    let latency = (cfg.feeder.make_latency)();
    let fill = (cfg.feeder.make_fill)();
    let slip = (cfg.feeder.make_slippage)();
    let fee = (cfg.feeder.make_fee)();
    // Avoid unused variable warnings
    let _ = (latency, fill, slip, fee);
}
