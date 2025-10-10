use chrono::Utc;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info};
use tracing::level_filters::LevelFilter;
use tt_bus::ClientMessageBus;
use tt_engine::engine::{DataTopic, EngineHandle, EngineRuntime, Strategy};
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::AccountDelta;
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::mbp10::Mbp10;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{OrderType, OrdersBatch, PositionsBatch, Trade};

pub struct TotalLiveTestStrategy {
    instrument: Instrument,
    data_provider: ProviderKind,
    execution_provider: ProviderKind,
    account_name: AccountName,
    subscribed: Vec<DataTopic>,
    last_order_type: OrderType,
    h: Option<EngineHandle>,
    symbol_key: SymbolKey,
    count: i32,
    order_id: Mutex<EngineUuid>,
    first_order_done: bool,
}
//todo, we should implement a special error type for strategies and let the engine, handle depending on severity.
impl Strategy for TotalLiveTestStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        info!("on_start: strategy start");

        h.subscribe_now(DataTopic::Ticks, self.symbol_key.clone());
        // store handle
        self.h = Some(h.clone());


    }

    fn on_stop(&mut self) {
        info!("on_stop: live strategy passed all tests");
    }

    fn on_tick(&mut self, t: &Tick, provider_kind: ProviderKind) {
        self.count += 1;
        if provider_kind != self.data_provider {
            panic!("Incorrect provider kind {:?}", provider_kind)
        }
        assert!(t.price != Decimal::ZERO);
        assert!(t.time < Utc::now());
        let mut order_id = self.order_id.lock().unwrap();

        // Spawn a small workflow to exercise place -> replace -> cancel
        let account = self.account_name.clone();
        let provider = self.execution_provider;
        let exec_key = SymbolKey::new(self.instrument.clone(), provider);
        let h = self.h.clone().unwrap();
        if self.count == 1 && !self.first_order_done {
            // small delay to allow account subscriptions to come online
            // 1) Place a small JoinBid order
            info!("test flow: placing JoinBid BUY qty=1");
            *order_id = h
                .place_order(
                    account.clone(),
                    exec_key.clone(),
                    tt_types::accounts::events::Side::Buy,
                    1,
                    OrderType::JoinBid,
                    None,
                    None,
                    None,
                    Some("total_live_test".to_string()),
                    None,
                    None,
                )
                .unwrap();
            self.first_order_done = true;
        }
        if self.count == 30 {
            info!(?self.order_id, "JoinBid placed; waiting to replace");
            // Replace: bump size
            info!(?self.order_id, "replacing order: new_qty=2");
            let _ = h
                .replace_order(
                    provider,
                    account.clone(),
                    tt_types::wire::ReplaceOrder {
                        account_name: account.clone(),
                        provider_order_id: String::new(),
                        new_qty: Some(2),
                        new_limit_price: None,
                        new_stop_price: None,
                        new_trail_price: None,
                    },
                    order_id.clone(),
                )
                .unwrap();
        }

        if self.count == 38 {
            // Cancel
            info!("cancelling order: {}", order_id);
            let _ = h.cancel_order(provider, account.clone(), order_id.clone());
        }

        if self.count == 25 {
            info!("test flow: placing JoinAsk SELL qty=1");
            *order_id = h
                .place_order(
                    account.clone(),
                    exec_key.clone(),
                    tt_types::accounts::events::Side::Sell,
                    1,
                    OrderType::JoinAsk,
                    None,
                    None,
                    None,
                    Some("total_live_test".to_string()),
                    None,
                    None,
                )
                .unwrap();
        }

        if self.count == 50 {
            info!(?self.order_id, "JoinAsk placed; waiting then cancel");
            info!(?self.order_id, "cancelling JoinAsk order");
            let _ = h
                .cancel_order(provider, account.clone(), order_id.clone())
                .unwrap();
        }

        if self.count == 75 {
            // 3) Place a MARKET BUY order (fire-and-forget)
            info!("test flow: placing MARKET BUY qty=1");
            *order_id = h.place_order(
                account.clone(),
                exec_key.clone(),
                tt_types::accounts::events::Side::Buy,
                1,
                OrderType::Market,
                None,
                None,
                None,
                Some("total_live_test".to_string()),
                None,
                None,
            ).unwrap();
            info!("submitted MARKET BUY");
        }
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

    fn on_orders_batch(&mut self, b: &OrdersBatch) {
        for order in b.orders.iter() {
            println!("{:?}", order);
        }
    }

    fn on_positions_batch(&mut self, b: &PositionsBatch) {
        for position in b.positions.iter() {
            println!("{:?}", position);
        }
    }

    fn on_account_delta(&mut self, accounts: &[AccountDelta]) {
        for account in accounts {
            println!("{:?}", account);
        }
    }

    fn on_trades_closed(&mut self, trades: Vec<Trade>) {
        for trade in trades {
            println!("{:?}", trade);
        }
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
        vec![AccountKey::new(
            self.execution_provider,
            self.account_name.clone(),
        )]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,tt_bus=info,tt_engine=info,projectx.ws=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;
    let mut engine = EngineRuntime::new(bus.clone());
    let account_name = AccountName::from_str("PRAC-V2-64413-98419885").unwrap();
    let strategy = TotalLiveTestStrategy {
        instrument: Instrument::from_str("MNQ.Z25").unwrap(),
        data_provider: ProviderKind::ProjectX(ProjectXTenant::Topstep),
        execution_provider: ProviderKind::ProjectX(ProjectXTenant::Topstep),
        account_name: account_name.clone(),
        subscribed: Vec::new(),
        last_order_type: OrderType::Market,
        h: None,
        symbol_key: SymbolKey::new(
            Instrument::from_str("MNQ.Z25").unwrap(),
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
        ),
        count: 0,
        order_id: EngineUuid::new().into(),
        first_order_done: false, 
    };
    let _handle = engine.start(strategy).await?;

    sleep(Duration::from_secs(60)).await;

    let _ = engine.stop().await?;

    Ok(())
}
