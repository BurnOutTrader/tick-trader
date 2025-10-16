use colored::Colorize;
use rust_decimal::dec;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tt_database::schema::ensure_schema;
use tt_engine::backtest::orchestrator::{BacktestConfig, start_backtest};
use tt_engine::models::DataTopic;
use tt_engine::statics::consolidators::add_consolidator;
use tt_engine::statics::subscriptions::subscribe;
use tt_engine::traits::Strategy;
use tt_types::accounts::account::AccountName;
use tt_types::consolidators::CandlesToCandlesConsolidator;
use tt_types::data::core::Utc;
use tt_types::data::mbp10::Mbp10;
use tt_types::data::models::Resolution;
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::ProviderKind::ProjectX;
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire;

struct HistoricalDataTestStrategy {
    account_key: AccountKey,
    symbol_key: SymbolKey,
}

impl HistoricalDataTestStrategy {
    pub fn new(account_key: AccountKey, symbol_key: SymbolKey) -> HistoricalDataTestStrategy {
        Self {
            account_key,
            symbol_key,
        }
    }
}

impl Strategy for HistoricalDataTestStrategy {
    fn on_start(&mut self) {
        info!("strategy start");

        // Non-blocking subscribe via handle command queue, you can do this at run time from anywhere to subscribe or unsubscribe a custom universe
        subscribe(DataTopic::Candles1s, self.symbol_key.clone());
        let consolidator = CandlesToCandlesConsolidator::new(
            Resolution::Minutes(15),
            None,
            self.symbol_key.instrument.clone(),
        );
        add_consolidator(
            DataTopic::Candles1s,
            self.symbol_key.clone(),
            Box::new(consolidator),
        );
    }

    fn on_stop(&mut self) {
        info!("strategy stop");
    }

    fn on_tick(&mut self, t: &tt_types::data::core::Tick, _provider_kind: ProviderKind) {
        println!("{:?}", t)
    }

    fn on_quote(&mut self, q: &tt_types::data::core::Bbo, _provider_kind: ProviderKind) {
        println!("{:?}", q);
    }

    fn on_bar(&mut self, c: &tt_types::data::core::Candle, _provider_kind: ProviderKind) {
        let candle_msg = format!(
            "C: {}, {}, H:{}, L:{}, O:{}, C:{}, @{}",
            c.instrument,
            c.resolution.as_key().unwrap(),
            c.high,
            c.low,
            c.open,
            c.close,
            c.time_end
        );
        if c.close > c.open {
            println!("{}", candle_msg.as_str().bright_green());
        } else if c.close < c.open {
            println!("{}", candle_msg.as_str().bright_red());
        } else {
            println!("{}", candle_msg);
        }
    }

    fn on_mbp10(&mut self, d: &Mbp10, _provider_kind: ProviderKind) {
        println!(
            "MBP10 evt: action={:?} side={:?} px={} sz={} flags={:?} ts_event={} ts_recv={}",
            d.action, d.side, d.price, d.size, d.flags, d.ts_event, d.ts_recv
        );
    }

    fn on_orders_batch(&mut self, b: &wire::OrdersBatch) {
        for order in b.orders.iter() {
            println!("{:?}", order)
        }
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

    fn accounts(&self) -> Vec<AccountKey> {
        vec![self.account_key.clone()]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    // Create DB pool from env (Postgres) and ensure schema
    let db = tt_database::init::init_db()?;
    ensure_schema(&db).await?;

    // Backtest for a recent 30-day period
    let end_date = Utc::now().date_naive();
    let start_date = end_date - chrono::Duration::days(30);

    let account_name = AccountName::new("TST-1234".to_string());
    let inst = Instrument::from_str("MNQ.Z25")?;
    let key = SymbolKey::new(
        inst.clone(),
        ProviderKind::ProjectX(ProjectXTenant::Topstep),
    );
    let account_key = AccountKey::new(ProjectX(ProjectXTenant::Topstep), account_name);
    // Configure and start backtest
    let cfg = BacktestConfig::from_to(chrono::Duration::milliseconds(250), start_date, end_date);
    let strategy = HistoricalDataTestStrategy::new(account_key, key);
    start_backtest(db, cfg, strategy, dec!(150_000)).await?;

    sleep(Duration::from_secs(60)).await;

    Ok(())
}
