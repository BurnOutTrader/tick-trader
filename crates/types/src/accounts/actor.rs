use super::events::*;
use super::order::{Order, OrderState};
use super::position::PositionLedger;
use crate::rkyv_types::DecimalDef;
use crate::securities::symbols::Instrument;
use ahash::{AHashMap, AHashSet};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use tokio::sync::mpsc;

#[derive(Archive, RkyvDeserialize, RkyvSerialize, Debug, Clone, Default, PartialEq)]
pub struct AccountState {
    #[rkyv(with = DecimalDef)]
    pub equity: Decimal,
    #[rkyv(with = DecimalDef)]
    pub day_realized_pnl: Decimal,
    #[rkyv(with = DecimalDef)]
    pub open_pnl: Decimal,
}

pub struct AccountActorHandle {
    tx: mpsc::Sender<AccountEvent>,
}
impl AccountActorHandle {
    pub async fn send(&self, ev: AccountEvent) {
        let _ = self.tx.send(ev).await;
    }
}

pub struct AccountActor {
    pub orders_by_provider: AHashMap<ProviderOrderId, Order>,
    pub orders_by_client: AHashMap<ClientOrderId, Order>,
    pub exec_by_id: AHashSet<ExecId>,
    pub positions: PositionLedger,
    pub marks: AHashMap<Instrument, Decimal>,
    pub state: AccountState,
    pub wal: Vec<Vec<u8>>, // in-memory WAL (rkyv-serialized)
}

impl AccountActor {
    pub fn spawn() -> (AccountActorHandle, tokio::task::JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<AccountEvent>(4096);
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let task = tokio::spawn(async move {
            while let Some(ev) = rx.recv().await {
                // WAL append is handled inside apply() to avoid double-logging in tests
                actor.apply(ev);
            }
        });
        (AccountActorHandle { tx }, task)
    }

    fn append_wal(&mut self, ev: &AccountEvent) {
        // Serialize an event using rkyv into an aligned byte buffer and store in-memory.
        // In a full implementation this would be persisted to disk as a WAL.
        match rkyv::to_bytes::<rkyv::rancor::BoxedError>(ev) {
            Ok(buf) => {
                // AlignedVec -> Vec<u8>
                self.wal.push(buf.as_ref().to_vec());
            }
            Err(_) => {
                // On serialization failure, skip WAL append in minimal impl.
            }
        }
    }

    fn apply(&mut self, ev: AccountEvent) {
        // Append to WAL first, then apply side effects.
        self.append_wal(&ev);
        match ev {
            AccountEvent::Order(oe) => self.apply_order_event(oe),
            AccountEvent::Exec(exe) => self.apply_exec(exe),
            AccountEvent::Correction(c) => self.apply_correction(c),
            AccountEvent::Admin(_) => { /* ignore minimal */ }
            AccountEvent::Mark(m) => {
                self.marks.insert(m.instrument.clone(), m.mark_px);
                self.recompute_open_pnl();
            }
        }
    }

    fn resolve_order_mut(
        &mut self,
        provider: &Option<ProviderOrderId>,
        client: &Option<ClientOrderId>,
    ) -> Option<&mut Order> {
        if let Some(p) = provider {
            if let Some(o) = self.orders_by_provider.get_mut(p) {
                return Some(o);
            }
        }
        if let Some(c) = client {
            if let Some(o) = self.orders_by_client.get_mut(c) {
                return Some(o);
            }
        }
        None
    }

    fn apply_order_event(&mut self, oe: OrderEvent) {
        match oe.kind {
            OrderEventKind::NewAck {
                instrument,
                side,
                qty,
            } => {
                // create or update
                let mut ord = self.resolve_order_mut(&oe.provider_order_id, &oe.client_order_id);
                if ord.is_none() {
                    let mut o = Order::new(instrument.clone(), side, qty);
                    o.provider_order_id = oe.provider_order_id.clone();
                    o.client_order_id = oe.client_order_id.clone();
                    o.last_provider_seq = oe.provider_seq;
                    o.state = OrderState::Acknowledged;
                    self.insert_order(o);
                } else if let Some(o) = ord.as_mut() {
                    if o.can_apply(oe.provider_seq, OrderState::Acknowledged) {
                        o.last_provider_seq = oe.provider_seq;
                        o.state = OrderState::Acknowledged;
                        if let Some(l) = oe.leaves_qty {
                            o.leaves = l;
                        }
                    }
                }
            }
            OrderEventKind::Replaced { new_qty } => {
                if let Some(o) = self.resolve_order_mut(&oe.provider_order_id, &oe.client_order_id)
                {
                    if o.can_apply(oe.provider_seq, o.state) {
                        o.version += 1;
                        if let Some(q) = new_qty {
                            o.qty = q;
                            o.leaves = q - o.cum_qty;
                        }
                    }
                }
            }
            OrderEventKind::Canceled => {
                if let Some(o) = self.resolve_order_mut(&oe.provider_order_id, &oe.client_order_id)
                {
                    if o.can_apply(oe.provider_seq, OrderState::Canceled) {
                        o.last_provider_seq = oe.provider_seq;
                        o.state = OrderState::Canceled;
                        o.leaves = 0;
                    }
                }
            }
            OrderEventKind::Rejected { .. } => {
                if let Some(o) = self.resolve_order_mut(&oe.provider_order_id, &oe.client_order_id)
                {
                    if o.can_apply(oe.provider_seq, OrderState::Rejected) {
                        o.last_provider_seq = oe.provider_seq;
                        o.state = OrderState::Rejected;
                    }
                }
            }
            OrderEventKind::Expired => {
                if let Some(o) = self.resolve_order_mut(&oe.provider_order_id, &oe.client_order_id)
                {
                    // treat as cancel terminal
                    if o.can_apply(oe.provider_seq, OrderState::Canceled) {
                        o.last_provider_seq = oe.provider_seq;
                        o.state = OrderState::Canceled;
                        o.leaves = 0;
                    }
                }
            }
        }
    }

    fn insert_order(&mut self, o: Order) {
        if let Some(p) = &o.provider_order_id {
            self.orders_by_provider.insert(p.clone(), o.clone());
        }
        if let Some(c) = &o.client_order_id {
            self.orders_by_client.insert(c.clone(), o.clone());
        }
    }

    fn apply_exec(&mut self, exe: ExecutionEvent) {
        // Dedupe
        if self.exec_by_id.contains(&exe.exec_id) {
            return;
        }
        self.exec_by_id.insert(exe.exec_id.clone());
        // Update order
        if let Some(o) = self.resolve_order_mut(&exe.provider_order_id, &exe.client_order_id) {
            let new_cum = o.cum_qty + exe.qty.abs();
            let total_px = o.avg_fill_px
                * Decimal::from_i64(o.cum_qty.abs()).unwrap_or(Decimal::ZERO)
                + exe.price * Decimal::from_i64(exe.qty.abs()).unwrap();
            o.cum_qty = new_cum;
            o.leaves = (o.qty - o.cum_qty).max(0);
            o.avg_fill_px = if new_cum > 0 {
                total_px / Decimal::from_i64(new_cum).unwrap()
            } else {
                o.avg_fill_px
            };
            o.state = if o.leaves == 0 {
                OrderState::Filled
            } else {
                OrderState::PartiallyFilled
            };
        }
        // Update position
        let (_net_after, realized_delta) =
            self.positions
                .apply_fill(&exe.instrument, exe.side, exe.qty.abs(), exe.price, exe.fee);
        if realized_delta != Decimal::ZERO {
            self.state.day_realized_pnl += realized_delta;
        }
        self.recompute_open_pnl();
    }

    fn apply_correction(&mut self, _c: CorrectionEvent) {
        // Minimal stub: non-trivial to reverse prior effects; left for full impl.
    }

    fn recompute_open_pnl(&mut self) {
        // Sum across open segments: net_qty * (mark - open_vwap) * side_sign
        let mut open = Decimal::ZERO;
        for (inst, seg) in self.positions.segments.iter() {
            if seg.net_qty == 0 {
                continue;
            }
            if let Some(mark) = self.marks.get(inst) {
                let sign = match seg.side {
                    Side::Buy => 1,
                    Side::Sell => -1,
                };
                open += Decimal::from_i64(seg.net_qty.abs()).unwrap()
                    * (*mark - seg.open_vwap_px)
                    * Decimal::from_i32(sign).unwrap();
            }
        }
        self.state.open_pnl = open;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::securities::symbols::Instrument;

    fn inst(code: &str) -> Instrument {
        Instrument::try_from(code).unwrap()
    }

    #[test]
    fn order_lifecycle_ack_replace_cancel() {
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let prov = ProviderOrderId("P1".to_string());
        let cli = ClientOrderId::new();
        // Ack new order
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("ESZ5"),
                side: Side::Buy,
                qty: 10,
            },
            provider_order_id: Some(prov.clone()),
            client_order_id: Some(cli.clone()),
            provider_seq: Some(1),
            leaves_qty: Some(10),
            ts_ns: 1,
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.leaves, 10);
        assert_eq!(o.state, OrderState::Acknowledged);
        // Replace reduces qty to 6
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::Replaced { new_qty: Some(6) },
            provider_order_id: Some(prov.clone()),
            client_order_id: Some(cli.clone()),
            provider_seq: Some(2),
            leaves_qty: None,
            ts_ns: 2,
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.qty, 6);
        assert_eq!(o.leaves, 6 - o.cum_qty);
        // Cancel terminal
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::Canceled,
            provider_order_id: Some(prov.clone()),
            client_order_id: Some(cli.clone()),
            provider_seq: Some(3),
            leaves_qty: None,
            ts_ns: 3,
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.state, OrderState::Canceled);
        assert_eq!(o.leaves, 0);
    }

    #[test]
    fn exec_updates_order_and_positions_and_dedupe() {
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let prov = ProviderOrderId("P2".to_string());
        // Create an order via ack
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("MNQZ5"),
                side: Side::Buy,
                qty: 3,
            },
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(10),
            leaves_qty: Some(3),
            ts_ns: 1,
        }));
        // Exec 1 @ 100
        let ex1 = ExecId::new();
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ex1.clone(),
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            side: Side::Buy,
            qty: 1,
            price: Decimal::from_i32(100).unwrap(),
            fee: Decimal::from_f32(0.5).unwrap(),
            ts_ns: 2,
            provider_seq: Some(11),
            instrument: inst("MNQZ5"),
        }));
        // Duplicate exec should be ignored
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ex1.clone(),
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            side: Side::Buy,
            qty: 1,
            price: Decimal::from_i32(100).unwrap(),
            fee: Decimal::from_f32(0.5).unwrap(),
            ts_ns: 2,
            provider_seq: Some(11),
            instrument: inst("MNQZ5"),
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.cum_qty, 1);
        assert_eq!(o.leaves, 2);
        assert_eq!(o.state, OrderState::PartiallyFilled);
        // Mark price to compute open pnl on a remaining position (long 1 @ 100, mark 105 => +5)
        actor.apply(AccountEvent::Mark(MarkEvent {
            instrument: inst("MNQZ5"),
            mark_px: Decimal::from_i32(105).unwrap(),
            ts_ns: 3,
        }));
        assert_eq!(actor.state.open_pnl, Decimal::from_i32(5).unwrap());
        // Fill the remaining 2 @ 110; realized pnl accumulates when crossing the opposite side later
        let ex2 = ExecId::new();
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ex2.clone(),
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            side: Side::Buy,
            qty: 2,
            price: Decimal::from_i32(110).unwrap(),
            fee: Decimal::ZERO,
            ts_ns: 4,
            provider_seq: Some(12),
            instrument: inst("MNQZ5"),
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.leaves, 0);
        assert_eq!(o.state, OrderState::Filled);
        // Now sell 3 @ 120 to close and realize PnL: avg cost = (100*1 + 110*2)/3 = 106.666.. -> realized = (120-106.666..)*3 ~= 40
        let ex3 = ExecId::new();
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ex3.clone(),
            provider_order_id: None,
            client_order_id: None,
            side: Side::Sell,
            qty: 3,
            price: Decimal::from_i32(120).unwrap(),
            fee: Decimal::ZERO,
            ts_ns: 5,
            provider_seq: None,
            instrument: inst("MNQZ5"),
        }));
        // Open PnL should be 0 (flat); day realized > 39.9
        assert_eq!(actor.state.open_pnl, Decimal::ZERO);
        assert!(actor.state.day_realized_pnl > Decimal::from_f32(39.9).unwrap());
    }

    #[test]
    fn cancel_after_fill_with_no_seq_rules_overrides_in_current_impl() {
        // Document current behavior: since exec does not set last_provider_seq, a later cancel without seq can override
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let prov = ProviderOrderId("P3".to_string());
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("ESZ5"),
                side: Side::Buy,
                qty: 1,
            },
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(1),
            leaves_qty: Some(1),
            ts_ns: 1,
        }));
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ExecId::new(),
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            side: Side::Buy,
            qty: 1,
            price: Decimal::from_i32(100).unwrap(),
            fee: Decimal::ZERO,
            ts_ns: 2,
            provider_seq: Some(2),
            instrument: inst("ESZ5"),
        }));
        // Now send a cancel with no seq; can_apply returns true (current rules) and sets Canceled
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::Canceled,
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: None,
            leaves_qty: None,
            ts_ns: 3,
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.state, OrderState::Canceled);
    }

    #[test]
    fn out_of_order_cancel_older_seq_is_ignored() {
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let prov = ProviderOrderId("P4".to_string());
        // Ack at seq 5
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("ESZ5"),
                side: Side::Buy,
                qty: 2,
            },
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(5),
            leaves_qty: Some(2),
            ts_ns: 1,
        }));
        // Older cancel at seq 4 (should be ignored by can_apply)
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::Canceled,
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(4),
            leaves_qty: None,
            ts_ns: 2,
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.state, OrderState::Acknowledged);
        assert_eq!(o.leaves, 2);
    }

    #[test]
    fn equal_seq_precedence_cancel_beats_filled() {
        // Document precedence behavior: with equal seq, Cancel > Filled
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let prov = ProviderOrderId("P5".to_string());
        // Ack at seq 2
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("MNQZ5"),
                side: Side::Buy,
                qty: 1,
            },
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(2),
            leaves_qty: Some(1),
            ts_ns: 1,
        }));
        // Fill via exec (order last seq stays at 2)
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ExecId::new(),
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            side: Side::Buy,
            qty: 1,
            price: Decimal::from_i32(100).unwrap(),
            fee: Decimal::ZERO,
            ts_ns: 2,
            provider_seq: Some(3),
            instrument: inst("MNQZ5"),
        }));
        // Equal seq cancel (2) should override due to precedence
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::Canceled,
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(2),
            leaves_qty: None,
            ts_ns: 3,
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.state, OrderState::Canceled);
        assert_eq!(o.leaves, 0);
    }

    #[test]
    fn late_fill_after_cancel_applies_and_updates_positions_current_impl() {
        // Current implementation applies executions regardless of order cancel state/seq.
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let prov = ProviderOrderId("P6".to_string());
        // Ack then Cancel with higher seq
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("ESZ5"),
                side: Side::Sell,
                qty: 2,
            },
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(10),
            leaves_qty: Some(2),
            ts_ns: 1,
        }));
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::Canceled,
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(11),
            leaves_qty: None,
            ts_ns: 2,
        }));
        // Late fill shows up (older seq) -> position updates to short 1
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ExecId::new(),
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            side: Side::Sell,
            qty: 1,
            price: Decimal::from_i32(200).unwrap(),
            fee: Decimal::ZERO,
            ts_ns: 3,
            provider_seq: Some(9),
            instrument: inst("ESZ5"),
        }));
        let seg = actor.positions.segments.get(&inst("ESZ5")).cloned();
        assert!(seg.is_some());
        let seg = seg.unwrap();
        assert_eq!(seg.net_qty, -1);
        // Order will show PartiallyFilled despite prior cancel
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.state, OrderState::PartiallyFilled);
        assert_eq!(o.leaves, 1);
    }

    #[test]
    fn duplicate_same_seq_lower_precedence_ignored() {
        // A New event with the same seq should not override a higher-precedence Canceled
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let prov = ProviderOrderId("P7".to_string());
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("CLZ5"),
                side: Side::Buy,
                qty: 1,
            },
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(7),
            leaves_qty: Some(1),
            ts_ns: 1,
        }));
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::Canceled,
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(8),
            leaves_qty: None,
            ts_ns: 2,
        }));
        // Duplicate NewAck at same seq as first should not downgrade state
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("CLZ5"),
                side: Side::Buy,
                qty: 1,
            },
            provider_order_id: Some(prov.clone()),
            client_order_id: None,
            provider_seq: Some(7),
            leaves_qty: Some(1),
            ts_ns: 3,
        }));
        let o = actor.orders_by_provider.get(&prov).unwrap();
        assert_eq!(o.state, OrderState::Canceled);
    }

    #[test]
    fn correction_event_is_noop_in_minimal_impl() {
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let before = actor.state.clone();
        actor.apply(AccountEvent::Correction(CorrectionEvent {
            exec_id_ref: ExecId::new(),
            delta_qty: -1,
            ts_ns: 0,
        }));
        let after = actor.state.clone();
        assert_eq!(before.equity, after.equity);
        assert_eq!(before.day_realized_pnl, after.day_realized_pnl);
        assert_eq!(before.open_pnl, after.open_pnl);
    }

    #[test]
    fn multi_instrument_marks_recompute_open_pnl() {
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        // Long 2 ES @ 100, Short 1 NQ @ 200
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ExecId::new(),
            provider_order_id: None,
            client_order_id: None,
            side: Side::Buy,
            qty: 2,
            price: Decimal::from_i32(100).unwrap(),
            fee: Decimal::ZERO,
            ts_ns: 1,
            provider_seq: None,
            instrument: inst("ESZ5"),
        }));
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ExecId::new(),
            provider_order_id: None,
            client_order_id: None,
            side: Side::Sell,
            qty: 1,
            price: Decimal::from_i32(200).unwrap(),
            fee: Decimal::ZERO,
            ts_ns: 2,
            provider_seq: None,
            instrument: inst("MNQZ5"),
        }));
        // Marks: ES 105 => +5*2 = +10; NQ 195 on short @200 => +5*1 = +5; total +15
        actor.apply(AccountEvent::Mark(MarkEvent {
            instrument: inst("ESZ5"),
            mark_px: Decimal::from_i32(105).unwrap(),
            ts_ns: 3,
        }));
        actor.apply(AccountEvent::Mark(MarkEvent {
            instrument: inst("MNQZ5"),
            mark_px: Decimal::from_i32(195).unwrap(),
            ts_ns: 4,
        }));
        assert_eq!(actor.state.open_pnl, Decimal::from_i32(15).unwrap());
    }

    #[test]
    fn wal_appends_for_events_and_admin_noop() {
        let mut actor = AccountActor {
            orders_by_provider: AHashMap::new(),
            orders_by_client: AHashMap::new(),
            exec_by_id: AHashSet::new(),
            positions: PositionLedger::new(),
            marks: AHashMap::new(),
            state: AccountState::default(),
            wal: Vec::new(),
        };
        let base_wal = actor.wal.len();
        // Order -> Ack
        actor.apply(AccountEvent::Order(OrderEvent {
            kind: OrderEventKind::NewAck {
                instrument: inst("ESZ5"),
                side: Side::Buy,
                qty: 1,
            },
            provider_order_id: Some(ProviderOrderId("PX1".into())),
            client_order_id: Some(ClientOrderId::new()),
            provider_seq: Some(1),
            leaves_qty: Some(1),
            ts_ns: 1,
        }));
        // Exec
        actor.apply(AccountEvent::Exec(ExecutionEvent {
            exec_id: ExecId::new(),
            provider_order_id: Some(ProviderOrderId("PX1".into())),
            client_order_id: None,
            side: Side::Buy,
            qty: 1,
            price: Decimal::from_i32(100).unwrap(),
            fee: Decimal::ZERO,
            ts_ns: 2,
            provider_seq: Some(2),
            instrument: inst("ESZ5"),
        }));
        // Admin (noop)
        let before_state = actor.state.clone();
        actor.apply(AccountEvent::Admin(AdminEvent::ClockSync));
        let after_state = actor.state.clone();
        // Mark
        actor.apply(AccountEvent::Mark(MarkEvent {
            instrument: inst("ESZ5"),
            mark_px: Decimal::from_i32(101).unwrap(),
            ts_ns: 3,
        }));

        assert_eq!(actor.wal.len(), base_wal + 4);
        assert_eq!(before_state, after_state); // Admin did not change the state
    }
}
