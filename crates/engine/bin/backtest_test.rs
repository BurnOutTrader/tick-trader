mod backtest_orders;

use chrono::Utc;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;

use tt_engine::backtest::orchestrator::{BacktestConfig, start_backtest};
use tt_engine::handle::EngineHandle;
use tt_engine::models::DataTopic;
use tt_engine::traits::Strategy;

use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::AccountDelta;
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{self, Trade};

#[derive(Default)]
struct BacktestDataStrategy {
    engine: Option<EngineHandle>,
}

impl Strategy for BacktestDataStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        info!("backtest strategy start");
        // Subscribe using the engine handle to the desired instrument/topic
        h.subscribe_now(
            DataTopic::Candles1m,
            SymbolKey::new(
                Instrument::from_str("MNQ.Z25").unwrap(),
                ProviderKind::ProjectX(ProjectXTenant::Topstep),
            ),
        );
        self.engine = Some(h);
    }

    fn on_stop(&mut self) {
        info!("backtest strategy stop");
    }

    fn on_tick(&mut self, t: &tt_types::data::core::Tick, _provider_kind: ProviderKind) {
        println!("{:?}", t)
    }

    fn on_quote(&mut self, q: &tt_types::data::core::Bbo, _provider_kind: ProviderKind) {
        println!("{:?}", q);
    }

    fn on_bar(&mut self, b: &tt_types::data::core::Candle, _provider_kind: ProviderKind) {
        println!("{:?}", b)
    }

    fn on_mbp10(&mut self, d: &Mbp10, _provider_kind: ProviderKind) {
        println!(
            "MBP10 evt: action={:?} side={:?} px={} sz={} flags={:?} ts_event={} ts_recv={}",
            d.action, d.side, d.price, d.size, d.flags, d.ts_event, d.ts_recv
        );
    }

    fn on_orders_batch(&mut self, b: &wire::OrdersBatch) {
        println!("{:?}", b);
    }

    fn on_positions_batch(&mut self, b: &wire::PositionsBatch) {
        println!("{:?}", b);
    }

    fn on_account_delta(&mut self, accounts: &[AccountDelta]) {
        for account_delta in accounts {
            println!("{:?}", account_delta);
        }
    }

    fn on_trades_closed(&mut self, _trades: Vec<Trade>) {
        // implement when needed
    }

    fn on_subscribe(&mut self, instrument: Instrument, data_topic: DataTopic, success: bool) {
        println!(
            "Subscribed to {} on topic {:?}: Success: {}",
            instrument, data_topic, success
        );
    }

    fn on_unsubscribe(&mut self, _instrument: Instrument, data_topic: DataTopic) {
        println!("{:?}", data_topic);
    }

    // Accounts are not strictly necessary for data-only backtest, but keep parity with live test
    fn accounts(&self) -> Vec<AccountKey> {
        let target_account_name = "PRAC-V2-6";
        let account = AccountKey::new(
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
            AccountName::from_str(target_account_name).unwrap(),
        );
        vec![account]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load env for DATABASE_URL etc.
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    // Create DB pool from env (Postgres)
    let db = tt_database::init::pool_from_env()?;
    // Ensure database schema is initialized like the server does
    tt_database::schema::ensure_schema(&db).await?;

    let end_date = Utc::now().date_naive();
    let start_date = end_date - chrono::Duration::days(10);

    // Configure and start backtest
    let cfg = BacktestConfig::from_to(chrono::Duration::minutes(1), start_date, end_date);
    let strategy = BacktestDataStrategy::default();
    let (_engine_handle, _feeder_handle) = start_backtest(db, cfg, strategy).await?;

    // Let it run for a short while to stream historical data
    sleep(Duration::from_secs(200)).await;

    // On exit, tasks will be aborted by runtime; explicit shutdown not strictly needed here
    Ok(())
}
