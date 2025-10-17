use colored::Colorize;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tt_engine::models::DataTopic;
use tt_engine::runtime::EngineRuntime;
use tt_engine::statics::bus::connect_live_bus;
use tt_engine::statics::consolidators::add_hybrid_tick_or_candle;
use tt_engine::statics::subscriptions::subscribe;
use tt_engine::traits::Strategy;
use tt_types::accounts::account::AccountName;
use tt_types::data::mbp10::Mbp10;
use tt_types::data::models::Resolution;
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire;

struct DataTestStrategy {
    account_key: AccountKey,
    symbol_key: SymbolKey,
    data_topic: DataTopic,
    is_warmed_up: bool,
}

impl DataTestStrategy {
    pub fn new(
        account_key: AccountKey,
        symbol_key: SymbolKey,
        data_topic: DataTopic,
    ) -> DataTestStrategy {
        Self {
            account_key,
            symbol_key,
            data_topic,
            is_warmed_up: false,
        }
    }
}

impl Strategy for DataTestStrategy {
    fn on_start(&mut self) {
        info!("strategy start: warming up");
        // Non-blocking subscribe via handle command queue, you can do this at run time from anywhere to subscribe or unsubscribe a custom universe
        subscribe(self.data_topic, self.symbol_key.clone());
        subscribe(DataTopic::Candles1h, self.symbol_key.clone());
        add_hybrid_tick_or_candle(
            self.data_topic,
            self.symbol_key.clone(),
            Resolution::Seconds(1),
            None,
        );
    }

    fn on_warmup_complete(&mut self) {
        self.is_warmed_up = true;
        println!("warmup complete");
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

    connect_live_bus().await?;

    let sk = SymbolKey::new(
        Instrument::from_str("MNQ.Z25")?,
        ProviderKind::ProjectX(ProjectXTenant::Topstep),
    );
    let account = AccountKey::new(
        ProviderKind::ProjectX(ProjectXTenant::Topstep),
        AccountName::from_str("PRAC-V2-64413-98419885").unwrap(),
    );
    let data_topic = DataTopic::Candles1h;


    let mut engine = EngineRuntime::new(Some(100_000));
    let strategy = DataTestStrategy::new(account, sk, data_topic);
    engine.start(strategy, false).await?;

    sleep(Duration::from_secs(60)).await;

    Ok(())
}
