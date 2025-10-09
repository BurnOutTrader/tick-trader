use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
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

#[async_trait]
impl Strategy for TotalLiveTestStrategy {
    async fn on_start(&mut self, h: EngineHandle) {
        info!("strategy start");
        h.subscribe_key(
            DataTopic::MBP10,
            SymbolKey::new(
                Instrument::from_str("MNQ.Z25").unwrap(),
                ProviderKind::ProjectX(ProjectXTenant::Topstep),
            ),
        )
        .await
        .unwrap();
        self.engine = Some(h);
    }

    async fn on_stop(&mut self) {
        info!("live strategy passed all tests");
    }

    async fn on_tick(&mut self, t: Tick, provider_kind: ProviderKind) {
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
        assert!(t.price != Decimal::ZERO);
        assert!(t.time < Utc::now())
    }

    async fn on_quote(&mut self, _q: Bbo, provider_kind: ProviderKind) {
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
    }

    async fn on_bar(&mut self, _b: Candle, provider_kind: ProviderKind) {
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
    }

    async fn on_mbp10(&mut self, _d: Mbp10, provider_kind: ProviderKind) {
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
    }

    async fn on_orders_batch(&mut self, b: OrdersBatch) {
        todo!()
    }

    async fn on_positions_batch(&mut self, _b: PositionsBatch) {
        todo!()
    }

    async fn on_account_delta(&mut self, _accounts: Vec<AccountDelta>) {
        todo!()
    }

    async fn on_trades_closed(&mut self, _trades: Vec<Trade>) {
        todo!()
    }

    async fn on_subscribe(
        &mut self,
        _instrument: Instrument,
        data_topic: DataTopic,
        success: bool,
    ) {
        if !success {
            panic!("Failed to subscribe to {:?} {:?}", _instrument, data_topic)
        }
    }

    async fn on_unsubscribe(&mut self, _instrument: Instrument, _data_topic: DataTopic) {
        todo!()
    }

    fn accounts(&self) -> Vec<AccountKey> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;
    /*
       let mut engine = EngineRuntime::new(bus.clone());
       let strategy = Arc::new(Mutex::new(DataTestStrategy::default()));
       let _handle = engine.start(strategy.clone()).await?;

       sleep(Duration::from_secs(60)).await;

       let _ = engine.stop().await?;

    */
    Ok(())
}
