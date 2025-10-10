use chrono::Utc;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tt_bus::ClientMessageBus;
use tt_engine::engine::{DataTopic, EngineHandle, EngineRuntime, Strategy};
use tt_types::accounts::events::AccountDelta;
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{OrderType, OrdersBatch, PositionsBatch, Trade};

pub struct TotalLiveTestStrategy {
    _symbol: Instrument,
    data_provider: ProviderKind,
    execution_provider: ProviderKind,
    subscribed: Vec<DataTopic>,
    last_order_type: OrderType,
    engine: Option<EngineHandle>,
}

impl Strategy for TotalLiveTestStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        info!("strategy start");
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
        info!("live strategy passed all tests");
    }

    fn on_tick(&mut self, t: &Tick, provider_kind: ProviderKind) {
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
        assert!(t.price != Decimal::ZERO);
        assert!(t.time < Utc::now())
    }

    fn on_quote(&mut self, _q: &Bbo, provider_kind: ProviderKind) {
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
    }

    fn on_bar(&mut self, _b: &Candle, provider_kind: ProviderKind) {
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
    }

    fn on_mbp10(&mut self, _d: &Mbp10, provider_kind: ProviderKind) {
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
    }

    fn on_orders_batch(&mut self, _b: &OrdersBatch) {
        // test TODOs can remain; just validating callback wiring
    }

    fn on_positions_batch(&mut self, _b: &PositionsBatch) {
        // test TODOs can remain; just validating callback wiring
    }

    fn on_account_delta(&mut self, _accounts: &[AccountDelta]) {
        // test TODOs can remain; just validating callback wiring
    }

    fn on_trades_closed(&mut self, _trades: Vec<Trade>) {
        // test TODOs can remain; just validating callback wiring
    }

    fn on_subscribe(&mut self, _instrument: Instrument, data_topic: DataTopic, success: bool) {
        if !success {
            panic!("Failed to subscribe to {:?} {:?}", _instrument, data_topic)
        }
    }

    fn on_unsubscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic) {
        // test TODOs can remain; just validating callback wiring
    }

    fn accounts(&self) -> Vec<AccountKey> {
        Vec::new()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;
    let mut engine = EngineRuntime::new(bus.clone());
    let strategy = TotalLiveTestStrategy {
        _symbol: Instrument::from_str("MNQ.Z25").unwrap(),
        data_provider: ProviderKind::ProjectX(ProjectXTenant::Topstep),
        execution_provider: ProviderKind::ProjectX(ProjectXTenant::Topstep),
        subscribed: Vec::new(),
        last_order_type: OrderType::Market,
        engine: None,
    };
    let _handle = engine.start(strategy).await?;

    sleep(Duration::from_secs(60)).await;

    let _ = engine.stop().await?;

    Ok(())
}
