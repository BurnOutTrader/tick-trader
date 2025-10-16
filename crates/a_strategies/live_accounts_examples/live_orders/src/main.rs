use rust_decimal::Decimal;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::level_filters::LevelFilter;

use tt_engine::models::DataTopic;
use tt_engine::traits::Strategy;

use tt_types::accounts::account::AccountName;
use tt_types::data::core::Candle;
use tt_types::data::mbp10::Mbp10;
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::rolling_window::RollingWindow;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{self, OrderType};

use colored::Colorize;
use std::collections::HashMap;
use tracing::warn;
use tt_database::schema::ensure_schema;
use tt_engine::runtime::EngineRuntime;
use tt_engine::statics::bus::connect_live_bus;
use tt_engine::statics::clock::time_now;
use tt_engine::statics::order_placement::place_order;
use tt_engine::statics::portfolio::PORTFOLIOS;
use tt_engine::statics::subscriptions::subscribe;

struct LiveOrdersStrategy {
    sk: SymbolKey,
    account: AccountKey,
    last_bars: RollingWindow<Candle>,
    is_warmed_up: bool,
    bar_idx: u32,
    expect: HashMap<String, Expect>,
    done: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct Expect {
    require_fill: bool,
    acked: bool,
    filled: bool,
}

impl LiveOrdersStrategy {
    fn new() -> Self {
        let sk = SymbolKey::new(
            Instrument::from_str("MNQ.Z25").unwrap(),
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
        );
        let account = AccountKey::new(
            ProviderKind::ProjectX(ProjectXTenant::Topstep),
            AccountName::from_str("PRAC-V2-64413-98419885").unwrap(),
        );
        Self {
            sk,
            account,
            last_bars: RollingWindow::new(10),
            is_warmed_up: false,
            bar_idx: 0,
            expect: HashMap::new(),
            done: false,
        }
    }
}

impl Default for LiveOrdersStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveOrdersStrategy {
    fn record_expect(&mut self, tag: &str, require_fill: bool) {
        self.expect.insert(
            tag.to_string(),
            Expect {
                require_fill,
                acked: false,
                filled: false,
            },
        );
    }
}

impl Strategy for LiveOrdersStrategy {
    fn on_start(&mut self) {
        println!("backtest orders strategy start");
        // Subscribe to a modest data stream so marks update
        subscribe(DataTopic::Ticks, self.sk.clone());
    }

    fn on_warmup_complete(&mut self) {
        println!("warmup complete; considering order placement");
        self.is_warmed_up = true;
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
        let candle_msg = format!(
            "C: {}, H:{}, L:{}, O:{}, C:{}, @{}",
            c.instrument, c.high, c.low, c.open, c.close, c.time_end
        );
        if c.close > c.open {
            println!("{}", candle_msg.as_str().bright_green());
        } else if c.close < c.open {
            println!("{}", candle_msg.as_str().bright_red());
        } else {
            println!("{:?}", candle_msg);
        }

        self.bar_idx = self.bar_idx.saturating_add(1);
        let last = c.close;

        let side_buy = tt_types::accounts::events::Side::Buy;
        let side_sell = tt_types::accounts::events::Side::Sell;
        match self.bar_idx {
            1 => {
                let tag = "MKT_BUY";
                self.record_expect(tag, true);
                let _ = place_order(
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
            50 => {
                let tag = "MKT_SELL";
                self.record_expect(tag, true);
                let _ = place_order(
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
            100 => {
                let tag = "LIM_BUY";
                self.record_expect(tag, true);
                let _ = place_order(
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
            200 => {
                let tag = "LIM_SELL";
                self.record_expect(tag, true);
                let _ = place_order(
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
            300 => {
                let tag = "STP_BUY";
                self.record_expect(tag, true);
                let _ = place_order(
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
            350 => {
                let tag = "STP_SELL";
                self.record_expect(tag, true);
                let _ = place_order(
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
            400 => {
                let tag = "STPLMT_BUY";
                self.record_expect(tag, true);
                let _ = place_order(
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
            450 => {
                let tag = "STPLMT_SELL";
                self.record_expect(tag, true);
                let _ = place_order(
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
            500 => {
                let tag = "JOIN_BID_BUY";
                self.record_expect(tag, false);
                let _ = place_order(
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
            550 => {
                let tag = "JOIN_ASK_SELL";
                self.record_expect(tag, false);
                let _ = place_order(
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
            600 => {
                let tag = "TRAIL_BUY";
                self.record_expect(tag, false);
                let _ = place_order(
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
            650 => {
                let tag = "TRAIL_SELL";
                self.record_expect(tag, false);
                let _ = place_order(
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
            let o_msg = o.to_clean_string();
            println!("{}", o_msg.bright_blue());
            if let Some(tag) = &o.tag
                && let Some(exp) = self.expect.get_mut(tag)
            {
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
            if let Some(portfolio) = PORTFOLIOS.get(&self.account) {
                let ss = portfolio.positions_snapshot(time_now());
                println!("{:?}", portfolio.account_delta());
                for p in ss.positions {
                    let s = p.to_clean_string();
                    println!("{}", s.cyan());
                }
            } else {
                warn!("No portfolio found: {:?}", &self.account);
            }
        }
        // Check completion criteria only after all planned test orders have been sent.
        // We place 12 tagged orders between bars 1..=120. Require bar_idx >= 125 (past last placement)
        // AND the expectations map to contain all 12 tags, and all required conditions to be met.
        if self.bar_idx >= 125
            && self.expect.len() >= 12
            && self
                .expect
                .values()
                .all(|e| e.acked && (!e.require_fill || e.filled))
        {
            println!("All order-type checks passed: {:?}", self.expect);
            self.done = true;
            // Exit process cleanly; in CI this acts as a test pass
            std::process::exit(0);
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
    let db = tt_database::init::init_db()?;
    ensure_schema(&db).await?;
    // Backtest for a recent 30-day period
    /*   let end_date = Utc::now().date_naive();
    let start_date = end_date - chrono::Duration::days(5);

      // Configure and start backtest
    let cfg = BacktestConfig::from_to(chrono::Duration::milliseconds(250), start_date, end_date);
    let strategy = BacktestOrdersStrategy::default();
    start_backtest(db, cfg, strategy, dec!(150_000)).await?;*/

    connect_live_bus().await?;

    let mut engine = EngineRuntime::new(Some(100_000));
    let strategy = LiveOrdersStrategy::default();
    engine.start(strategy, false).await?;

    // Allow time for data and order lifecycle to flow
    sleep(Duration::from_secs(500)).await;

    Ok(())
}
