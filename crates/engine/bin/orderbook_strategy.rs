
use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tt_bus::ClientMessageBus;
use tt_engine::engine::{DataTopic, EngineHandle, EngineRuntime, Strategy};
use tt_types::accounts::events::AccountDelta;
use tt_types::data::mbp10::{Action as MbpAction, BookLevels, BookSide as MbpSide, Mbp10};
use tt_types::keys::{AccountKey, SymbolKey};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire;
use tt_types::accounts::account::AccountName;

#[derive(Default, Debug, Clone)]
struct LastTrade {
    price: Option<Decimal>,
    size: Option<Decimal>,
}

#[derive(Default, Debug, Clone)]
struct OrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
    last_trade: LastTrade,
}

impl OrderBook {
    fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    fn seed_from_snapshot(&mut self, book: &BookLevels) {
        self.clear();
        for (i, px) in book.bid_px.iter().enumerate() {
            let px = *px;
            let sz = *book.bid_sz.get(i).unwrap_or(&Decimal::zero());
            if sz > Decimal::zero() {
                self.bids.insert(px, sz);
            }
        }
        for (i, px) in book.ask_px.iter().enumerate() {
            let px = *px;
            let sz = *book.ask_sz.get(i).unwrap_or(&Decimal::zero());
            if sz > Decimal::zero() {
                self.asks.insert(px, sz);
            }
        }
    }

    fn apply_modify(&mut self, side: MbpSide, price: Decimal, size: Decimal) {
        match side {
            MbpSide::Bid => {
                if size.is_zero() {
                    self.bids.remove(&price);
                } else {
                    self.bids.insert(price, size);
                }
            }
            MbpSide::Ask => {
                if size.is_zero() {
                    self.asks.remove(&price);
                } else {
                    self.asks.insert(price, size);
                }
            }
            MbpSide::None => {}
        }
    }

    fn note_trade(&mut self, price: Decimal, size: Decimal) {
        self.last_trade.price = Some(price);
        self.last_trade.size = Some(size);
    }

    fn best_bid(&self) -> Option<(Decimal, Decimal)> {
        self.bids.iter().next_back().map(|(p, s)| (p.clone(), *s))
    }
    fn best_ask(&self) -> Option<(Decimal, Decimal)> {
        self.asks.iter().next().map(|(p, s)| (p.clone(), *s))
    }

    fn print_top_n(&self, n: usize) {
        let mut bids: Vec<(Decimal, Decimal)> = self
            .bids
            .iter()
            .rev()
            .take(n)
            .map(|(p, s)| (p.clone(), *s))
            .collect();
        let asks: Vec<(Decimal, Decimal)> = self
            .asks
            .iter()
            .take(n)
            .map(|(p, s)| (p.clone(), *s))
            .collect();

        println!("-- ORDER BOOK (top {}) --", n);
        if let (Some((bb_px, bb_sz)), Some((ba_px, ba_sz))) = (self.best_bid(), self.best_ask()) {
            println!("BBO: bid {} x {} | ask {} x {}", bb_sz, bb_px, ba_px, ba_sz);
        } else {
            println!("BBO: unavailable");
        }
        if let Some(p) = self.last_trade.price.clone() {
            println!("Last trade: {} @ {:?}", p, self.last_trade.size);
        }
        println!("BIDS:");
        for (px, sz) in bids.drain(..) {
            println!("  {} x {}", sz, px);
        }
        println!("ASKS:");
        for (px, sz) in asks {
            println!("  {} x {}", sz, px);
        }
    }
}

// New: lightweight config and state for the strategy
struct StrategyConfig {
    key: SymbolKey,
    instrument: Instrument,
    provider: ProviderKind,
    account_name: AccountName,
    max_pos_abs: i64,
}

#[derive(Default)]
struct OrderBookStrategy {
    engine: Option<EngineHandle>,
    book: OrderBook,
    cfg: Option<StrategyConfig>,
    net_pos: i64,
    last_manage: Option<Instant>,
    manage_interval: Duration,
    // New: adaptive management state
    last_desired: Option<(bool, bool)>,
    last_bbo: Option<(Decimal, Decimal)>,
    last_cancel_at: Option<Instant>,
    min_cancel_interval: Duration,
}

impl OrderBookStrategy {
    fn new(cfg: StrategyConfig) -> Self {
        Self {
            engine: None,
            book: OrderBook::default(),
            cfg: Some(cfg),
            net_pos: 0,
            last_manage: None,
            manage_interval: Duration::from_millis(100),
            last_desired: None,
            last_bbo: None,
            last_cancel_at: None,
            min_cancel_interval: Duration::from_millis(1000),
        }
    }

    fn throttle_ok(&mut self) -> bool {
        let now = Instant::now();
        if let Some(prev) = self.last_manage {
            if now.duration_since(prev) < self.manage_interval {
                return false;
            }
        }
        self.last_manage = Some(now);
        true
    }

    async fn cancel_all_for_instrument(&mut self) {
        if let (Some(h), Some(cfg)) = (&self.engine, &self.cfg) {
            // Rate-limit cancels aggressively
            if let Some(prev) = self.last_cancel_at {
                if prev.elapsed() < self.min_cancel_interval {
                    return;
                }
            }
            let open = h.orders_for_instrument(&cfg.instrument).await;
            for ou in open {
                if let Some(poid) = ou.provider_order_id {
                    let _ = h
                        .cancel_order(wire::CancelOrder {
                            account_name: cfg.account_name.clone(),
                            provider_order_id: Some(poid.0),
                            client_order_id: None,
                        })
                        .await;
                }
            }
            self.last_cancel_at = Some(Instant::now());
        }
    }

    async fn ensure_quotes(&mut self) {
        if self.engine.is_none() || self.cfg.is_none() {
            return;
        }
        if !self.throttle_ok() {
            return;
        }
        let h = self.engine.as_ref().unwrap().clone();
        let (key_clone, account_name_clone, max_pos_abs) = {
            let cfg = self.cfg.as_ref().unwrap();
            (cfg.key.clone(), cfg.account_name.clone(), cfg.max_pos_abs)
        };
        // Only quote when we have a valid BBO
        let (bb_px, ba_px) = match (self.book.best_bid(), self.book.best_ask()) {
            (Some((bb_px, _)), Some((ba_px, _))) if ba_px > bb_px => (bb_px, ba_px),
            _ => return,
        };

        // Decide which sides to quote based on current net position
        let mut want_bid = true;
        let mut want_ask = true;
        if self.net_pos >= max_pos_abs { want_bid = false; }
        if self.net_pos <= -max_pos_abs { want_ask = false; }
        if self.net_pos > 0 { want_bid = false; }
        if self.net_pos < 0 { want_ask = false; }
        let desired = (want_bid, want_ask);

        // Skip if desired state and BBO didn't change
        if let Some(prev_desired) = self.last_desired {
            if prev_desired == desired {
                if let Some(prev_bbo) = self.last_bbo.clone() {
                    if prev_bbo == (bb_px, ba_px) {
                        return;
                    }
                }
            }
        }
        self.last_desired = Some(desired);
        self.last_bbo = Some((bb_px, ba_px));

        // Clear existing working orders to avoid stale or wrong-side resting orders (rate-limited)
        self.cancel_all_for_instrument().await;

        // Re-place desired sides using join orders to stick to BBO
        if want_bid {
            let order = wire::PlaceOrder {
                account_name: account_name_clone.clone(),
                key: key_clone.clone(),
                side: tt_types::accounts::events::Side::Buy,
                qty: 1,
                r#type: wire::OrderTypeWire::JoinBid,
                limit_price: None,
                stop_price: None,
                trail_price: None,
                custom_tag: Some("orderbook-scalper:bid".to_string()),
                stop_loss: None,
                take_profit: None,
            };
            info!("Placing Order: {:?}", order);
            let _ = h
                .place_order(order)
                .await;
        }
        if want_ask {
            let order = wire::PlaceOrder {
                account_name: account_name_clone.clone(),
                key: key_clone.clone(),
                side: tt_types::accounts::events::Side::Sell,
                qty: 1,
                r#type: wire::OrderTypeWire::JoinAsk,
                limit_price: None,
                stop_price: None,
                trail_price: None,
                custom_tag: Some("orderbook-scalper:ask".to_string()),
                stop_loss: None,
                take_profit: None,
            };
            info!("Placing Order: {:?}", order);
            let _ = h
                .place_order(order)
                .await;
        }
    }
}

#[async_trait::async_trait]
impl Strategy for OrderBookStrategy {
    async fn on_start(&mut self, h: EngineHandle) {
        info!("strategy start");
        self.engine = Some(h.clone());

        // If cfg is not yet set (should be set by main), build a default for MNQ.Z25 on Topstep
        if self.cfg.is_none() {
            let instrument = Instrument::from_str("MNQ.Z25").unwrap();
            let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);
            let key = SymbolKey::new(instrument.clone(), provider);
            self.cfg = Some(StrategyConfig { key, instrument, provider, account_name: AccountName::from_str("UNKNOWN").unwrap(), max_pos_abs: 1 });
        }

        let cfg = self.cfg.as_ref().unwrap();
        h.subscribe_key(DataTopic::MBP10, cfg.key.clone()).await.unwrap();
        h.subscribe_key(DataTopic::Ticks, cfg.key.clone()).await.unwrap();
    }
    async fn on_stop(&mut self) {
        info!("strategy stop");
    }
    async fn on_tick(&mut self, t: tt_types::data::core::Tick) { println!("{:?}", t) }
    async fn on_quote(&mut self, q: tt_types::data::core::Bbo) { println!("{:?}", q); }
    async fn on_bar(&mut self, _b: tt_types::data::core::Candle) {}

    async fn on_mbp10(&mut self, d: Mbp10) {
        let ob = &mut self.book;
        if let Some(ref book) = d.book { ob.seed_from_snapshot(book); }
        match d.action {
            MbpAction::Clear => ob.clear(),
            MbpAction::Modify | MbpAction::Add | MbpAction::Cancel => ob.apply_modify(d.side, d.price, d.size),
            MbpAction::Trade | MbpAction::Fill => ob.note_trade(d.price, d.size),
            MbpAction::None => {}
        }
       /* println!(
            "MBP10 evt: action={:?} side={:?} px={} sz={} flags={:?} ts_event={} ts_recv={}",
            d.action, d.side, d.price, d.size, d.flags, d.ts_event, d.ts_recv
        );*/
        //ob.print_top_n(5);

        // Opportunistically (throttled) manage our quotes
        self.ensure_quotes().await;
    }

    async fn on_orders_batch(&mut self, b: wire::OrdersBatch) {
        // For visibility; could also reconcile here if desired
        println!("orders batch: {} updates", b.orders.len());
    }
    async fn on_positions_batch(&mut self, b: wire::PositionsBatch) {
        if let Some(cfg) = &self.cfg {
            if let Some(p) = b.positions.iter().find(|p| p.instrument == cfg.instrument) {
                self.net_pos = p.net_qty_after;
            }
        }
        println!("positions batch (net_pos={}): {} entries", self.net_pos, b.positions.len());
    }
    async fn on_account_delta(&mut self, accounts: Vec<AccountDelta>) {
        for account_delta in accounts { println!("{:?}", account_delta); }
    }
    async fn on_subscribe(&mut self, instrument: Instrument, data_topic: DataTopic, success: bool) {
        println!("Subscribed to {} on topic {:?}: Success: {}", instrument, data_topic, success);
        if !success {
            eprintln!("Warning: subscribe failed for {:?} on {} — upstream may be unavailable; engine will call on_stop if connection closes.", data_topic, instrument);
        }
    }
    async fn on_unsubscribe(&mut self, _instrument: Instrument, data_topic: DataTopic) { println!("{:?}", data_topic); }
    fn accounts(&self) -> Vec<AccountKey> {
        let account = AccountKey::new(ProviderKind::ProjectX(ProjectXTenant::Topstep), self.cfg.account_name.clone());
        vec![account]
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_max_level(LevelFilter::INFO).init();

    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;

    let mut engine = EngineRuntime::new(bus.clone());

    // Provider and instrument
    let provider = ProviderKind::ProjectX(ProjectXTenant::Topstep);
    let instrument = Instrument::from_str("MNQ.Z25").unwrap();
    let key = SymbolKey::new(instrument.clone(), provider);

    // Target account name we will use for placing orders
    let account_name = tt_types::accounts::account::AccountName::from_str("PRAC-V2-64413-98419885").unwrap();
    engine
        .initialize_account_names(provider, vec![])
        .await?;
    // Create strategy with placeholder account_id and known account_name; we'll set id after engine start
    let strategy = Arc::new(Mutex::new(OrderBookStrategy::new(StrategyConfig {
        key: key.clone(),
        instrument: instrument.clone(),
        provider,
        account_name,
        max_pos_abs: 1,
    })));

    // Start engine to obtain a sub_id and begin processing responses
    let _handle = engine.start(strategy.clone()).await?;

    // Resolve account id for the provided account name on ProjectX Topstep and subscribe to exec streams
    let target_account_name = "PRAC-V2-64413-98419885";

    // Initialize account interest so we receive orders/positions/account deltas
    engine
        .initialize_account_names(provider, vec![tt_types::accounts::account::AccountName::from_str(target_account_name).unwrap()])
        .await?;


    // Run for a while; adjust as needed
    sleep(Duration::from_secs(10000)).await;

    let _ = engine.stop().await?;
    Ok(())
}