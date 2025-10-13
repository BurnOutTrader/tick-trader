use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tt_bus::ClientMessageBus;
use tt_engine::handle::EngineHandle;
use tt_engine::models::DataTopic;
use tt_engine::runtime::EngineRuntime;
use tt_engine::traits::Strategy;
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::AccountDelta;
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire;
use tt_types::wire::Trade;

#[derive(Default)]
struct DataTestStrategy {
    engine: Option<EngineHandle>,
}

impl Strategy for DataTestStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        info!("strategy start");
        // Non-blocking subscribe via handle command queue
        h.subscribe_now(
            DataTopic::MBP10,
            SymbolKey::new(
                Instrument::from_str("MNQ.Z25").unwrap(),
                ProviderKind::ProjectX(ProjectXTenant::Topstep),
            ),
        );
        self.engine = Some(h);
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

    fn accounts(&self) -> Vec<AccountKey> {
        let target_account_name = "PRAC-V2-64413-98419885";
        let account = AccountKey::new(
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
            AccountName::from_str(target_account_name).unwrap(),
        );
        vec![account]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;

    let mut engine = EngineRuntime::new(bus.clone(), Some(500_000));
    let strategy = DataTestStrategy::default();
    let _handle = engine.start(strategy).await?;

    sleep(Duration::from_secs(60)).await;

    engine.stop().await?;
    Ok(())
}
