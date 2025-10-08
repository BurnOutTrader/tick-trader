use rust_decimal::Decimal;
use rust_decimal::prelude::Zero;
use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::info;
use tracing::level_filters::LevelFilter;
use tt_bus::ClientMessageBus;
use tt_engine::engine::{DataTopic, EngineHandle, EngineRuntime, Strategy};
use tt_types::accounts::events::AccountDelta;
use tt_types::data::mbp10::{Action as MbpAction, BookLevels, BookSide as MbpSide, Mbp10};
use tt_types::keys::SymbolKey;
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire;

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

#[derive(Default)]
struct TestStrategy {
    engine: Option<EngineHandle>,
    // Since MBP10 events don't carry the instrument explicitly in the payload routed to strategies,
    // and this test subscribes to a single instrument, keep a single rolling book.
    book: OrderBook,
}

#[async_trait::async_trait]
impl Strategy for TestStrategy {
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
        h.subscribe_key(
            DataTopic::Ticks,
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
        let ob = &mut self.book;

        if let Some(ref book) = d.book {
            ob.seed_from_snapshot(book);
        }

        match d.action {
            MbpAction::Clear => ob.clear(),
            MbpAction::Modify | MbpAction::Add | MbpAction::Cancel => {
                ob.apply_modify(d.side, d.price, d.size);
            }
            MbpAction::Trade | MbpAction::Fill => ob.note_trade(d.price, d.size),
            MbpAction::None => {}
        }

        println!(
            "MBP10 evt: action={:?} side={:?} px={} sz={} flags={:?} ts_event={} ts_recv={}",
            d.action, d.side, d.price, d.size, d.flags, d.ts_event, d.ts_recv
        );
        ob.print_top_n(10);
    }

    async fn on_orders_batch(&mut self, b: wire::OrdersBatch) {
        println!("{:?}", b);
    }
    async fn on_positions_batch(&mut self, b: wire::PositionsBatch) {
        println!("{:?}", b);
    }
    async fn on_account_delta(&mut self, accounts: Vec<AccountDelta>) {
        for account_delta in accounts {
            println!("{:?}", account_delta);
        }
    }
    async fn on_subscribe(&mut self, instrument: Instrument, data_topic: DataTopic, success: bool) {
        println!(
            "Subscribed to {} on topic {:?}: Success: {}",
            instrument, data_topic, success
        );
    }
    async fn on_unsubscribe(&mut self, _instrument: Instrument, data_topic: DataTopic) {
        println!("{:?}", data_topic);
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
    let strategy = Arc::new(Mutex::new(TestStrategy::default()));
    let _handle = engine.start(strategy.clone()).await?;

    sleep(Duration::from_secs(60)).await;

    let _ = engine.stop().await?;
    Ok(())
}
