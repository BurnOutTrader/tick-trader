use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, level_filters::LevelFilter};
use tt_bus::ClientMessageBus;
use tt_engine::engine::{DataTopic, EngineHandle, EngineRuntime, Strategy};
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::AccountDelta;
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire;
use tt_types::wire::{BracketWire, OrderTypeWire};

#[derive(Clone)]
struct StrategyConfig {
    key: SymbolKey,
    instrument: Instrument,
    account_name: AccountName,
}

struct TestLiveOrdersStrategy {
    engine: Option<EngineHandle>,
    cfg: StrategyConfig,
    count: i32,
}

impl TestLiveOrdersStrategy {
    fn new(cfg: StrategyConfig) -> Self {
        Self {
            engine: None,
            cfg,
            count: 105,
        }
    }

    // Fixed defaults: BUY 1 to avoid env dependencies
    fn side_default() -> tt_types::accounts::events::Side {
        tt_types::accounts::events::Side::Buy
    }
    fn qty_default() -> i64 {
        1
    }
}

#[async_trait::async_trait]
impl Strategy for TestLiveOrdersStrategy {
    async fn on_start(&mut self, h: EngineHandle) {
        info!("test strategy start");
        self.engine = Some(h.clone());
        let cfg = &self.cfg;
        h.subscribe_key(DataTopic::MBP10, cfg.key.clone())
            .await
            .unwrap();
    }

    async fn on_stop(&mut self) {
        info!("test strategy stop");
    }

    async fn on_tick(&mut self, _t: tt_types::data::core::Tick, provider_kind: ProviderKind) {}
    async fn on_quote(&mut self, _q: tt_types::data::core::Bbo, provider_kind: ProviderKind) {}
    async fn on_bar(&mut self, _b: tt_types::data::core::Candle, provider_kind: ProviderKind) {}
    async fn on_mbp10(&mut self, _d: tt_types::data::mbp10::Mbp10, provider_kind: ProviderKind) {}

    async fn on_orders_batch(&mut self, b: wire::OrdersBatch) {
        for order in b.orders {
            info!("test order batch: {:?}", order);
        }
    }
    async fn on_positions_batch(&mut self, _b: wire::PositionsBatch) {
        for position in _b.positions {
            info!("test position batch: {:?}", position);
        }
    }
    async fn on_account_delta(&mut self, accounts: Vec<AccountDelta>) {
        for a in accounts {
            info!(
                "account: can_trade={} eq={} ts={}",
                a.can_trade, a.equity, a.time
            );
        }
    }
    async fn on_subscribe(
        &mut self,
        instrument: Instrument,
        data_topic: tt_engine::engine::DataTopic,
        success: bool,
    ) {
        info!(
            "Subscribed to {} on {:?}: success={}",
            instrument, data_topic, success
        );
        // Place a real Market order shortly after start
        let cfg2 = self.cfg.clone();
        sleep(Duration::from_millis(500)).await;
        let side = TestLiveOrdersStrategy::side_default();
        let qty = TestLiveOrdersStrategy::qty_default();
        self.count += 1;
        info!(
            "placing Market order: side={:?} qty={} symbol={}",
            side, qty, cfg2.instrument.0
        );
        let _ = &<Option<EngineHandle> as Clone>::clone(&self.engine)
            .unwrap()
            .place_order(wire::PlaceOrder {
                account_name: cfg2.account_name.clone(),
                key: cfg2.key.clone(),
                side,
                qty,
                r#type: wire::OrderTypeWire::Market,
                limit_price: None,
                stop_price: None,
                trail_price: None,
                custom_tag: None,
                stop_loss: Some(BracketWire {
                    ticks: -20,
                    r#type: OrderTypeWire::Stop,
                }),
                take_profit: Some(BracketWire {
                    ticks: 20,
                    r#type: OrderTypeWire::Limit,
                }),
            })
            .await;
    }
    async fn on_unsubscribe(
        &mut self,
        instrument: Instrument,
        data_topic: tt_engine::engine::DataTopic,
    ) {
        info!("Unsubscribed {} from {:?}", instrument, data_topic);
    }
    fn accounts(&self) -> Vec<AccountKey> {
        let account = AccountKey::new(
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
            self.cfg.account_name.clone(),
        );
        vec![account]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    // Keep TT_BUS_ADDR env fallback only
    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;

    let mut engine = EngineRuntime::new(bus.clone());

    // Use fixed provider/instrument/account (no env)
    let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);
    let instrument = Instrument::from_str("MNQ.Z25").unwrap();
    let key = SymbolKey::new(instrument.clone(), provider);
    let account_name = AccountName::from_str("PRAC-V2-64413-98419885").unwrap();

    // Start engine and strategy
    let strategy_placeholder = Arc::new(Mutex::new(TestLiveOrdersStrategy::new(StrategyConfig {
        key: key.clone(),
        instrument: instrument.clone(),
        account_name: account_name.clone(),
    })));
    let _handle = engine.start(strategy_placeholder.clone()).await?;

    // Subscribe to account streams
    engine
        .initialize_account_names(provider, vec![account_name])
        .await?;

    // Run briefly to see order lifecycle events
    let secs: u64 = 20;
    sleep(Duration::from_secs(secs)).await;

    let _ = engine.stop().await?;
    Ok(())
}
