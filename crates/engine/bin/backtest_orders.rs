use chrono::Utc;
use std::str::FromStr;
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;

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

struct BacktestOrdersStrategy {
    engine: Option<EngineHandle>,
    sk: SymbolKey,
    account: AccountKey,
    placed: bool,
    last_bars: RollingWindow<Candle>,
}

#[allow(dead_code)]
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
        }
    }
}

impl Default for BacktestOrdersStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl Strategy for BacktestOrdersStrategy {
    fn on_start(&mut self, h: EngineHandle) {
        info!("backtest orders strategy start");
        // Subscribe to a modest data stream so marks update
        h.subscribe_now(DataTopic::Candles1m, self.sk.clone());
        self.engine = Some(h);
    }

    fn on_warmup_complete(&mut self) {
        info!("warmup complete; considering order placement");
        if self.placed {
            return;
        }
        let Some(_) = &self.engine else {
            return;
        };
    }

    fn on_stop(&mut self) {
        info!("backtest orders strategy stop");
    }

    fn on_tick(&mut self, _t: &tt_types::data::core::Tick, _provider_kind: ProviderKind) {}
    fn on_quote(&mut self, _q: &tt_types::data::core::Bbo, _provider_kind: ProviderKind) {}

    fn on_bar(&mut self, c: &tt_types::data::core::Candle, _provider_kind: ProviderKind) {
        if let Some(last_candle) = self.last_bars.get(0) {
            let h = self.engine.as_ref().unwrap();
            if c.close > last_candle.close && !h.is_long(&self.account, &self.sk.instrument) {
                let qty = match h.is_short(&self.account, &self.sk.instrument) {
                    true => 2,
                    false => 1,
                };
                let _ = h
                    .place_order(
                        self.account.clone(),
                        self.sk.instrument.clone(),
                        tt_types::accounts::events::Side::Buy,
                        qty,
                        OrderType::Market,
                        None,
                        None,
                        None,
                        Some("total_live_test".to_string()),
                        None,
                        None,
                    )
                    .unwrap();
            } else if c.close < last_candle.close && !h.is_short(&self.account, &self.sk.instrument)
            {
                let qty = match h.is_long(&self.account, &self.sk.instrument) {
                    true => 2,
                    false => 1,
                };
                let _ = h
                    .place_order(
                        self.account.clone(),
                        self.sk.instrument.clone(),
                        tt_types::accounts::events::Side::Sell,
                        qty,
                        OrderType::Market,
                        None,
                        None,
                        None,
                        Some("total_live_test".to_string()),
                        None,
                        None,
                    )
                    .unwrap();
            }
        }

        self.last_bars.add(c.clone())
    }

    fn on_mbp10(&mut self, _d: &Mbp10, _provider_kind: ProviderKind) {}

    fn on_orders_batch(&mut self, b: &wire::OrdersBatch) {
        info!(orders = b.orders.len(), "orders batch");
        for o in &b.orders {
            info!(
                status = ?o.state,
                qty = o.cum_qty,
                leaves = o.leaves,
                avg_px = ?o.avg_fill_px,
                provider_order_id = ?o.provider_order_id,
                instr = %o.instrument,
                "order update"
            );
        }
    }

    fn on_positions_batch(&mut self, b: &wire::PositionsBatch) {
        info!(positions = b.positions.len(), "positions batch");
        for p in &b.positions {
            info!(instr = %p.instrument, side = ?p.side, qty = p.net_qty.to_string(), avg_px = %p.average_price, "position");
        }
    }

    fn on_account_delta(&mut self, accounts: &[AccountDelta]) {
        for a in accounts {
            info!(account = %a.name, pnl = %a.day_realized_pnl, cash = %a.equity, "account delta");
        }
    }

    fn on_trades_closed(&mut self, _trades: Vec<Trade>) {}

    fn on_subscribe(&mut self, instrument: Instrument, data_topic: DataTopic, success: bool) {
        info!(%instrument, ?data_topic, success, "subscribe ack");
    }

    fn on_unsubscribe(&mut self, _instrument: Instrument, data_topic: DataTopic) {
        info!(?data_topic, "unsubscribe ack");
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
    let db = tt_database::init::pool_from_env()?;
    tt_database::schema::ensure_schema(&db).await?;

    // Backtest for a recent 5-day period
    let end_date = Utc::now().date_naive();
    let start_date = end_date - chrono::Duration::days(30);

    // Configure and start backtest
    let cfg = BacktestConfig::from_to(chrono::Duration::minutes(1), start_date, end_date);
    let strategy = BacktestOrdersStrategy::default();
    let (_engine_handle, _feeder_handle) = start_backtest(db, cfg, strategy).await?;

    // Allow time for data and order lifecycle to flow
    sleep(Duration::from_secs(60)).await;

    Ok(())
}
