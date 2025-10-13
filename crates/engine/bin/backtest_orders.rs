use chrono::Utc;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use rust_decimal::Decimal;

use tt_engine::backtest::orchestrator::{BacktestConfig, start_backtest};
use tt_engine::handle::EngineHandle;
use tt_engine::models::DataTopic;
use tt_engine::traits::Strategy;

use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::AccountDelta;
use tt_types::data::core::Candle;
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::rolling_window::RollingWindow;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{self, OrderType, Trade};

use std::collections::HashMap;

struct BacktestOrdersStrategy {
    engine: Option<EngineHandle>,
    sk: SymbolKey,
    account: AccountKey,
    placed: bool,
    last_bars: RollingWindow<Candle>,
    is_warmed_up: bool,
    bar_idx: u32,
    expect: HashMap<String, Expect>,
    done: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Expect { require_fill: bool, acked: bool, filled: bool }

impl BacktestOrdersStrategy {
    fn new() -> Self {
        let sk = SymbolKey::new(
            Instrument::from_str("MNQ.Z25").unwrap(),
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
        );
        let account = AccountKey::new(
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
            AccountName::from_str("PRAC-V2-6").unwrap(),
        );
        Self {
            engine: None,
            sk,
            account,
            placed: false,
            last_bars: RollingWindow::new(10),
            is_warmed_up: false,
            bar_idx: 0,
            expect: HashMap::new(),
            done: false,
        }
    }
}

impl Default for BacktestOrdersStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl BacktestOrdersStrategy {
    fn record_expect(&mut self, tag: &str, require_fill: bool) {
        self.expect.insert(tag.to_string(), Expect { require_fill, acked: false, filled: false });
    }
}

impl Strategy for BacktestOrdersStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        println!("backtest orders strategy start");
        // Subscribe to a modest data stream so marks update
        h.subscribe_now(DataTopic::Candles1m, self.sk.clone());
        self.engine = Some(h);
    }

    fn on_warmup_complete(&mut self) {
        println!("warmup complete; considering order placement");
        self.is_warmed_up = true;
        if self.placed {
            return;
        }
        let Some(_) = &self.engine else {
            return;
        };
    }

    fn on_stop(&mut self) {
        println!("backtest orders strategy stop");
    }

    fn on_tick(&mut self, _t: &tt_types::data::core::Tick, _provider_kind: ProviderKind) {}
    fn on_quote(&mut self, _q: &tt_types::data::core::Bbo, _provider_kind: ProviderKind) {}

    fn on_bar(&mut self, c: &tt_types::data::core::Candle, _provider_kind: ProviderKind) {
        if !self.is_warmed_up || self.done {
            return;
        }
        self.bar_idx = self.bar_idx.saturating_add(1);
        let last = c.close;
        let h = match &self.engine {
            Some(h) => h.clone(),
            None => return,
        };
        let side_buy = tt_types::accounts::events::Side::Buy;
        let side_sell = tt_types::accounts::events::Side::Sell;
        match self.bar_idx {
            1 => {
                let tag = "MKT_BUY";
                self.record_expect(tag, true);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_buy,
                    1,
                    OrderType::Market,
                    None,
                    None,
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            2 => {
                let tag = "MKT_SELL";
                self.record_expect(tag, true);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_sell,
                    1,
                    OrderType::Market,
                    None,
                    None,
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            3 => {
                let tag = "LIM_BUY";
                self.record_expect(tag, true);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_buy,
                    1,
                    OrderType::Limit,
                    Some(last + Decimal::from(5)),
                    None,
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            4 => {
                let tag = "LIM_SELL";
                self.record_expect(tag, true);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_sell,
                    1,
                    OrderType::Limit,
                    Some(last - Decimal::from(5)),
                    None,
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            5 => {
                let tag = "STP_BUY";
                self.record_expect(tag, true);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_buy,
                    1,
                    OrderType::Stop,
                    None,
                    Some(last),
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            6 => {
                let tag = "STP_SELL";
                self.record_expect(tag, true);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_sell,
                    1,
                    OrderType::Stop,
                    None,
                    Some(last),
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            7 => {
                let tag = "STPLMT_BUY";
                self.record_expect(tag, true);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_buy,
                    1,
                    OrderType::StopLimit,
                    Some(last + Decimal::from(5)),
                    Some(last),
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            8 => {
                let tag = "STPLMT_SELL";
                self.record_expect(tag, true);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_sell,
                    1,
                    OrderType::StopLimit,
                    Some(last - Decimal::from(5)),
                    Some(last),
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            9 => {
                let tag = "JOIN_BID_BUY";
                self.record_expect(tag, false);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_buy,
                    1,
                    OrderType::JoinBid,
                    None,
                    None,
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            10 => {
                let tag = "JOIN_ASK_SELL";
                self.record_expect(tag, false);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_sell,
                    1,
                    OrderType::JoinAsk,
                    None,
                    None,
                    None,
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            11 => {
                let tag = "TRAIL_BUY";
                self.record_expect(tag, false);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_buy,
                    1,
                    OrderType::TrailingStop,
                    None,
                    None,
                    Some(Decimal::from(1)),
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            12 => {
                let tag = "TRAIL_SELL";
                self.record_expect(tag, false);
                let _ = h.place_order(
                    self.account.clone(),
                    self.sk.instrument.clone(),
                    side_sell,
                    1,
                    OrderType::TrailingStop,
                    None,
                    None,
                    Some(Decimal::from(1)),
                    Some(tag.to_string()),
                    None,
                    None,
                );
            }
            _ => {}
        }
        // Keep last bars window updated for potential future use
        self.last_bars.add(c.clone());
    }

    fn on_mbp10(&mut self, _d: &Mbp10, _provider_kind: ProviderKind) {}

    fn on_orders_batch(&mut self, b: &wire::OrdersBatch) {
        use tt_types::accounts::order::OrderState;
        for o in &b.orders {
            if let Some(tag) = &o.tag {
                if let Some(exp) = self.expect.get_mut(tag) {
                    match o.state {
                        OrderState::Acknowledged => {
                            exp.acked = true;
                        }
                        OrderState::PartiallyFilled | OrderState::Filled => {
                            exp.filled = true;
                        }
                        OrderState::Rejected => {
                            panic!("Order with tag {} was rejected: {:?}", tag, o);
                        }
                        _ => {}
                    }
                }
            }
        }
        // Check completion criteria: all expectations must be acknowledged, and those requiring fill must be filled
        if !self.expect.is_empty() && self.expect.values().all(|e| e.acked && (!e.require_fill || e.filled)) {
            println!("All order-type checks passed: {:?}", self.expect);
            self.done = true;
            // Exit process cleanly; in CI this acts as a test pass
            std::process::exit(0);
        }
    }

    fn on_positions_batch(&mut self, b: &wire::PositionsBatch) {
        for p in &b.positions {
            println!("{:?}", p);
        }
    }

    fn on_account_delta(&mut self, accounts: &[AccountDelta]) {
        for a in accounts {
            println!("{:?}", a);
        }
    }

    fn on_trades_closed(&mut self, trades: Vec<Trade>) {
        for t in trades {
            info!("{:?}", t)
        }
    }

    fn on_subscribe(&mut self, instrument: Instrument, data_topic: DataTopic, _success: bool) {
        println!("Subscribed: {:?} {:?}", instrument, data_topic);
    }

    fn on_unsubscribe(&mut self, instrument: Instrument, data_topic: DataTopic) {
        println!("Unsubscribed: {:?} {:?}", instrument, data_topic);
    }

    fn accounts(&self) -> Vec<AccountKey> {
        vec![self.account.clone()]
    }
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load env for DATABASE_URL etc.
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();

    // Create DB pool from env (Postgres) and ensure schema
    let db = tt_database::init::pool_from_env().await?;
    tt_database::schema::ensure_schema(&db).await?;

    // Backtest for a recent 30-day period
    let end_date = Utc::now().date_naive();
    let start_date = end_date - chrono::Duration::days(30);

    // Configure and start backtest
    let cfg = BacktestConfig::from_to(chrono::Duration::seconds(1), start_date, end_date);
    let strategy = BacktestOrdersStrategy::default();
    let (_engine_handle, _feeder_handle) = start_backtest(db, cfg, strategy).await?;

    // Allow time for data and order lifecycle to flow
    sleep(Duration::from_secs(400)).await;

    Ok(())
}
