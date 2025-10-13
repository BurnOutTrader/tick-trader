use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::Context;
use chrono::{Duration as ChronoDuration, NaiveDate};
use tokio::sync::oneshot;
use tokio::time::{Duration, timeout};
use tracing::info;
use tracing::level_filters::LevelFilter;

use tt_engine::backtest::orchestrator::{BacktestConfig, start_backtest};
use tt_engine::handle::EngineHandle;
use tt_engine::models::DataTopic;
use tt_engine::traits::Strategy;

use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::AccountDelta;
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{self, Trade};

struct TestStrategy {
    engine: Option<EngineHandle>,
    sk: SymbolKey,
    bars_seen: Arc<AtomicUsize>,
    signal_tx: Option<oneshot::Sender<()>>, // signal when we see first few bars
}

impl Strategy for TestStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        info!("test strategy start");
        h.subscribe_now(DataTopic::Candles1m, self.sk.clone());
        self.engine = Some(h);
    }

    fn on_stop(&mut self) {
        info!("test strategy stop");
    }

    fn on_tick(&mut self, _t: &tt_types::data::core::Tick, _provider_kind: ProviderKind) {}
    fn on_quote(&mut self, _q: &tt_types::data::core::Bbo, _provider_kind: ProviderKind) {}

    fn on_bar(&mut self, _b: &tt_types::data::core::Candle, _provider_kind: ProviderKind) {
        let c = self.bars_seen.fetch_add(1, Ordering::Relaxed) + 1;
        if c >= 5
            && let Some(tx) = self.signal_tx.take()
        {
            let _ = tx.send(());
        }
    }

    fn on_mbp10(&mut self, _d: &Mbp10, _provider_kind: ProviderKind) {}
    fn on_orders_batch(&mut self, _b: &wire::OrdersBatch) {}
    fn on_positions_batch(&mut self, _b: &wire::PositionsBatch) {}
    fn on_account_delta(&mut self, _accounts: &[AccountDelta]) {}
    fn on_trades_closed(&mut self, _trades: Vec<Trade>) {}

    fn on_subscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic, _success: bool) {}
    fn on_unsubscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic) {}

    fn accounts(&self) -> Vec<AccountKey> {
        // Keep parity with live; unused for this test
        let account = AccountKey::new(
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
            AccountName::from_str("PRAC-V2-6").unwrap(),
        );
        vec![account]
    }
}

#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backtest_feeder_emits_candles_to_strategy() -> anyhow::Result<()> {
    // Load .env and init tracing for debug logs (optional in tests)
    let _ = dotenvy::dotenv();
    let _ = tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .try_init();

    // Ensure we can talk to Postgres and the schema exists
    let db = tt_database::init::pool_from_env()?;
    tt_database::schema::ensure_schema(&db)
        .await
        .context("ensure_schema")?;

    // Build the symbol key for MNQ.Z25 on ProjectX Topstep
    let sk = SymbolKey::new(
        Instrument::from_str("MNQ.Z25").unwrap(),
        ProviderKind::ProjectX(ProjectXTenant::Topstep),
    );

    // Check DB extent for Candles1m to choose a bounded test range with data
    let (earliest_opt, latest_opt) =
        tt_database::queries::get_extent(&db, sk.provider, &sk.instrument, Topic::Candles1m)
            .await
            .context("get_extent")?;

    let (earliest, latest) = match (earliest_opt, latest_opt) {
        (Some(e), Some(l)) if e < l => (e, l),
        _ => {
            // No data available for this symbol/topic/provider; skip test
            eprintln!(
                "Skipping test: no Candles1m extent for {:?} {}",
                sk.provider, sk.instrument
            );
            return Ok(());
        }
    };

    // Use at most a 1-day span to keep test light; clamp to latest
    let start_dt = earliest;
    let end_dt = (earliest + ChronoDuration::days(1)).min(latest);

    // Map to naive dates for BacktestConfig::from_to
    let start_date: NaiveDate = start_dt.date_naive();
    let end_date: NaiveDate = end_dt.date_naive();

    // Prepare signaling and counters
    let (tx, rx) = oneshot::channel();
    let bars_seen = Arc::new(AtomicUsize::new(0));

    let strategy = TestStrategy {
        engine: None,
        sk: sk.clone(),
        bars_seen: bars_seen.clone(),
        signal_tx: Some(tx),
    };

    // Start backtest with the chosen date range
    let cfg = BacktestConfig::from_to(chrono::Duration::minutes(1), start_date, end_date);
    let (_engine_handle, feeder_handle) = start_backtest(db, cfg, strategy).await?;

    // Wait until the strategy sees a few bars or timeout
    let wait_res = timeout(Duration::from_secs(10), rx).await;
    // Stop feeder regardless of outcome
    feeder_handle.stop().await;

    match wait_res {
        Ok(Ok(())) => {
            let n = bars_seen.load(Ordering::Relaxed);
            assert!(n >= 5, "expected at least 5 bars, got {}", n);
        }
        Ok(Err(_canceled)) => panic!("signal channel canceled before receiving bars"),
        Err(_elapsed) => panic!("timed out waiting for bars from feeder"),
    }

    Ok(())
}
