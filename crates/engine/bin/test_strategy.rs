use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;
use tt_bus::ClientMessageBus;
use tt_engine::engine::{EngineRuntime, EngineHandle, Strategy};
use tt_types::accounts::events::AccountDelta;
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire;

#[derive(Default)]
struct TestStrategy {
    engine: Option<EngineHandle>,
}

#[async_trait::async_trait]
impl Strategy for TestStrategy {
    fn desired_topics(&mut self) -> std::collections::HashSet<Topic> {
        use std::collections::HashSet;
        let mut s = HashSet::new();
        // Subscribe to all hot and account topics so router delivers them
        s.insert(Topic::Ticks);
        s
    }

    async fn on_start(&mut self, h: EngineHandle) {
        info!("strategy start");
        h.subscribe_key(Topic::MBP10, SymbolKey::new(Instrument::from_str("MNQZ25").unwrap(), ProviderKind::ProjectX(ProjectXTenant::Topstep))).await.unwrap();
        self.engine = Some(h);
    }
    async fn on_stop(&mut self) {
        info!("strategy stop");
    }
    async fn on_tick(&mut self, t: tt_types::data::core::Tick) {
        println!("{:?}", t)
    }
    async fn on_quote(&mut self, q: tt_types::data::core::Bbo) {
        println!("{:?}", q);
    }
    async fn on_bar(&mut self, _b: tt_types::data::core::Candle) {}
    async fn on_mbp10(&mut self, d: Mbp10) {
        println!("{:?}", d);
    }
    async fn on_orders_batch(&mut self, b: wire::OrdersBatch) {
        println!("{:?}", b);
    }
    async fn on_positions_batch(&mut self, b: wire::PositionsBatch) {
        println!("{:?}", b);
    }
    async fn on_account_delta(&mut self, accounts: Vec<AccountDelta>,) {
        for account_delta in accounts {
            println!("{:?}", account_delta);
        }
    }
    async fn on_subscribe(&mut self, instrument: Instrument, topic: Topic, success: bool) {
        println!("Subscribed to {} on topic {:?}: Success: {}", instrument, topic, success);
    }
    async fn on_unsubscribe(&mut self, _instrument: Instrument, topic: Topic) {
        println!("{:?}", topic);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env if available
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    // Prepare client bus and connect to server
    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;

    // Start engine runtime with our test strategy
    let mut engine = EngineRuntime::new(bus.clone());
    let strategy = Arc::new(Mutex::new(TestStrategy::default()));
    let _handle = engine.start(strategy.clone()).await?;

    // Keep running for a bit to receive live data
    sleep(Duration::from_secs(60)).await;

    // Graceful shutdown
    engine.stop().await;

    Ok(())
}
