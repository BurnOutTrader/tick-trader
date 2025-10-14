use std::str::FromStr;
use std::sync::{Arc, Mutex};
use rust_decimal::{dec};
use tokio::time::{Duration, sleep, timeout};
use tt_bus::ClientMessageBus;
use tt_engine::runtime::EngineRuntime;
use tt_engine::traits::Strategy;
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::{AccountDelta, OrderUpdate, PositionDelta, PositionSide, Side};
use tt_types::accounts::order::OrderState;
use tt_types::data::core::{Bbo, Candle};
use tt_types::engine_id::EngineUuid;
use tt_types::keys::Topic;
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{AccountDeltaBatch, OrdersBatch, PositionsBatch, Response, Trade};

// Simple probe strategy to capture callbacks
#[derive(Default, Clone)]
struct Probe {
    last_positions: Arc<Mutex<Option<PositionsBatch>>>,
    last_orders: Arc<Mutex<Option<OrdersBatch>>>,
    last_accounts: Arc<Mutex<Vec<AccountDelta>>>,
    on_bar_count: Arc<Mutex<usize>>, // counts bars received (including from consolidators)
}

struct ProbeStrategy(Probe);
impl Strategy for ProbeStrategy {
    fn on_bar(&mut self, _b: &Candle, _provider: ProviderKind) {
        let mut c = self.0.on_bar_count.lock().unwrap();
        *c += 1;
    }
    fn on_orders_batch(&mut self, b: &OrdersBatch) {
        *self.0.last_orders.lock().unwrap() = Some(b.clone());
    }
    fn on_positions_batch(&mut self, b: &PositionsBatch) {
        *self.0.last_positions.lock().unwrap() = Some(b.clone());
    }
    fn on_account_delta(&mut self, a: &[AccountDelta]) {
        *self.0.last_accounts.lock().unwrap() = a.to_vec();
    }
}

fn pd(
    instr: &str,
    qty: i64,
    acct: &AccountName,
    provider: ProviderKind,
    avg: i64,
) -> PositionDelta {
    PositionDelta {
        provider_kind: provider,
        instrument: Instrument::from_str(instr).unwrap(),
        account_name: acct.clone(),
        net_qty: rust_decimal::Decimal::from(qty),
        average_price: rust_decimal::Decimal::from(avg),
        open_pnl: rust_decimal::Decimal::ZERO,
        time: chrono::Utc::now(),
        side: if qty >= 0 {
            PositionSide::Long
        } else {
            PositionSide::Short
        },
    }
}

fn ou(
    name: &AccountName,
    instr: &Instrument,
    provider: ProviderKind,
    order_id: EngineUuid,
    leaves: i64,
) -> OrderUpdate {
    OrderUpdate {
        name: name.clone(),
        instrument: instr.clone(),
        provider_kind: provider,
        provider_order_id: None,
        order_id,
        state: if leaves > 0 {
            OrderState::Acknowledged
        } else {
            OrderState::Filled
        },
        leaves,
        cum_qty: 1,
        avg_fill_px: rust_decimal::Decimal::from(100),
        tag: None,
        time: chrono::Utc::now(),
        side: Side::Buy,
    }
}

#[tokio::test]
async fn portfolio_positions_and_marks_integration() {
    let (req_tx, mut _req_rx) = tokio::sync::mpsc::channel::<tt_types::wire::Request>(8);
    let bus = ClientMessageBus::new_with_transport(req_tx);

    let mut rt = EngineRuntime::new(bus.clone(), Some(1));
    let probe = Probe::default();
    let strategy = ProbeStrategy(probe.clone());
    let handle = rt.start(strategy).await.expect("engine start");

    let provider = ProviderKind::ProjectX(ProjectXTenant::Demo);
    let account = AccountName::new("TEST-ACCT".to_string());
    let es = Instrument::from_str("ES.Z25").unwrap();
    let nq = Instrument::from_str("NQ.Z25").unwrap();
    let ym = Instrument::from_str("YM.Z25").unwrap();

    // Seed a quote mid as mark for ES/NQ/YM
    for (instr, bid, ask) in [
        (&es, 105i64, 107i64),
        (&nq, 200i64, 202i64),
        (&ym, 99i64, 101i64),
    ] {
        let bbo = Bbo {
            symbol: instr.to_string(),
            instrument: instr.clone(),
            bid: bid.into(),
            bid_size: 1.into(),
            ask: ask.into(),
            ask_size: 1.into(),
            time: chrono::Utc::now(),
            bid_orders: None,
            ask_orders: None,
            venue_seq: None,
            is_snapshot: None,
        };
        let _ = bus
            .route_response(Response::QuoteBatch(tt_types::wire::QuoteBatch {
                topic: Topic::Quotes,
                seq: 1,
                quotes: vec![bbo],
                provider_kind: provider,
            }))
            .await;
    }

    // Apply positions for the account
    let long = pd("ES.Z25", 5, &account, provider, 100);
    let short = pd("NQ.Z25", -3, &account, provider, 200);
    let flat = pd("YM.Z25", 0, &account, provider, 100);
    let pb = PositionsBatch {
        topic: Topic::Positions,
        seq: 1,
        positions: vec![long.clone(), short.clone(), flat.clone()],
    };

    let _ = bus.route_response(Response::PositionsBatch(pb)).await;

    // Allow engine loop to process
    sleep(Duration::from_millis(20)).await;

    // Validate via handle (account-scoped)
    let ak = tt_types::keys::AccountKey::new(provider, account.clone());
    assert!(handle.is_long(&ak, &es));
    assert!(handle.is_short(&ak, &nq));
    assert!(handle.is_flat(&ak, &ym));

    // And ensure strategy saw a positions batch
    let got = timeout(Duration::from_millis(200), async {
        loop {
            if probe.last_positions.lock().unwrap().is_some() {
                break true;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or(false);
    assert!(got, "strategy should receive positions batch");

    let _ = tokio::time::timeout(Duration::from_secs(2), rt.stop())
        .await
        .expect("engine stop should not hang");
}

#[tokio::test]
async fn orders_open_orders_integration() {
    let (req_tx, mut _req_rx) = tokio::sync::mpsc::channel::<tt_types::wire::Request>(8);
    let bus = ClientMessageBus::new_with_transport(req_tx);

    let mut rt = EngineRuntime::new(bus.clone(), Some(1));
    let probe = Probe::default();
    let strategy = ProbeStrategy(probe.clone());
    let handle = rt.start(strategy).await.expect("engine start");

    let provider = ProviderKind::ProjectX(ProjectXTenant::Demo);
    let account = AccountName::new("TEST-ACCT".to_string());
    let es = Instrument::from_str("ES.Z25").unwrap();

    let o1 = ou(&account, &es, provider, EngineUuid::new(), 5); // open
    let o2 = ou(&account, &es, provider, EngineUuid::new(), 0); // filled

    let ob = OrdersBatch {
        topic: Topic::Orders,
        seq: 1,
        orders: vec![o1.clone(), o2.clone()],
    };
    let _ = bus.route_response(Response::OrdersBatch(ob)).await;

    sleep(Duration::from_millis(20)).await;

    // Open orders should include only o1
    let open = handle.open_orders_for_instrument(&es);
    assert_eq!(open.len(), 1);
    assert_eq!(open[0].order_id, o1.order_id);

    // Strategy should have seen orders batch cached
    let got = probe.last_orders.lock().unwrap().is_some();
    assert!(got, "strategy should receive orders batch");

    let _ = tokio::time::timeout(Duration::from_secs(2), rt.stop())
        .await
        .expect("engine stop should not hang");
}

#[tokio::test]
async fn account_delta_autofill_open_and_day_pnl() {
    let (req_tx, mut _req_rx) = tokio::sync::mpsc::channel::<tt_types::wire::Request>(8);
    let bus = ClientMessageBus::new_with_transport(req_tx);

    let mut rt = EngineRuntime::new(bus.clone(), Some(1));
    let probe = Probe::default();
    let strategy = ProbeStrategy(probe.clone());
    let _handle = rt.start(strategy).await.expect("engine start");

    let provider = ProviderKind::ProjectX(ProjectXTenant::Demo);
    let account = AccountName::new("TEST-ACCT".to_string());
    let mnq = Instrument::from_str("MNQ.Z25").unwrap();

    // Seed mark 105, avg 100, qty 2 => open_pnl = (105-100)*2 = 10
    let bbo = Bbo {
        symbol: mnq.to_string(),
        instrument: mnq.clone(),
        bid: dec!(21000),
        bid_size: 1.into(),
        ask: dec!(21000.25),
        ask_size: 1.into(),
        time: chrono::Utc::now(),
        bid_orders: None,
        ask_orders: None,
        venue_seq: None,
        is_snapshot: None,
    };
    let _ = bus
        .route_response(Response::QuoteBatch(tt_types::wire::QuoteBatch {
            topic: Topic::Quotes,
            seq: 1,
            quotes: vec![bbo],
            provider_kind: provider,
        }))
        .await;

    // Position applied first
    let pos = pd("ES.Z25", 2, &account, provider, 100);
    let _ = bus
        .route_response(Response::PositionsBatch(PositionsBatch {
            topic: Topic::Positions,
            seq: 2,
            positions: vec![pos],
        }))
        .await;

    // Closed trade today realizes pnl 7
    let tr = Trade {
        id: EngineUuid::new(),
        provider,
        account_name: account.clone(),
        instrument: mnq.clone(),
        creation_time: chrono::Utc::now(),
        price: 0.into(),
        profit_and_loss: 7.into(),
        fees: 0.into(),
        side: tt_types::accounts::events::Side::Buy,
        size: 1.into(),
        voided: false,
        order_id: "x".into(),
    };
    let _ = bus.route_response(Response::ClosedTrades(vec![tr])).await;

    // Now send an AccountDelta with zeros; engine should autofill
    let ad = AccountDelta {
        provider_kind: provider,
        name: account.clone(),
        equity: 1000.into(),
        day_realized_pnl: 0.into(),
        open_pnl: 0.into(),
        time: chrono::Utc::now(),
        can_trade: true,
    };
    let _ = bus
        .route_response(Response::AccountDeltaBatch(AccountDeltaBatch {
            topic: Topic::AccountEvt,
            seq: 1,
            accounts: vec![ad],
        }))
        .await;

    // Wait for callback and assert non-zero values
    let got = timeout(Duration::from_millis(500), async {
        loop {
            let v = probe.last_accounts.lock().unwrap().clone();
            if !v.is_empty() {
                break v;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("accounts callback");

    let filled = got
        .into_iter()
        .find(|a| a.name == account)
        .expect("account present");
    assert_eq!(filled.open_pnl, rust_decimal::Decimal::from(10));
    assert_eq!(filled.day_realized_pnl, rust_decimal::Decimal::from(7));

    let _ = tokio::time::timeout(Duration::from_secs(2), rt.stop())
        .await
        .expect("engine stop should not hang");
}
