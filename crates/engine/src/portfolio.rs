use ahash::AHashMap;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rust_decimal::Decimal;
use std::sync::{Arc, RwLock};
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::{
    AccountDelta, OrderUpdate, PositionDelta, PositionSide, ProviderOrderId,
};
use tt_types::engine_id::EngineUuid;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{AccountDeltaBatch, OrdersBatch, PositionsBatch, Response, Trade};

/// Key for tracking an order in the open orders map
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OrderKeyId {
    Client(EngineUuid),
    #[allow(dead_code)]
    Provider(ProviderOrderId),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderKey {
    pub instrument: Instrument,
    pub id: OrderKeyId,
}

impl OrderKey {
    fn from_update(o: &OrderUpdate) -> Option<Self> {
        Some(OrderKey {
            instrument: o.instrument.clone(),
            id: OrderKeyId::Client(o.order_id),
        })
    }
}

/// Aggregates the latest portfolio state as seen by the engine.
/// Concurrency: uses DashMap for hot paths and a Mutex-protected AHashMap for closed positions.
#[derive(Default)]
pub struct PortfolioManager {
    // Latest snapshot by provider + account -> instrument: real positions, in real provider accounts.
    positions_by_account: DashMap<(ProviderKind, AccountName), DashMap<Instrument, PositionDelta>>,
    // Total position delta across all accounts (synthetic), open pnl + average price calculated as net-sum of real positions
    positions_total_delta: DashMap<Instrument, PositionDelta>,
    // Latest account deltas keyed by provider + account name
    accounts_by_name: DashMap<(ProviderKind, AccountName), AccountDelta>,
    // All currently open orders (keyed by instrument + best available order id)
    open_orders: DashMap<OrderKey, OrderUpdate>,
    // Completed (closed/filled) orders kept for later reference
    completed_orders: RwLock<AHashMap<OrderKey, OrderUpdate>>,
    // Closed trades (from provider/execution layer)
    closed_trades: RwLock<Vec<Trade>>,
    // Optional last snapshots (for pass-through APIs and debugging)
    last_orders: RwLock<Option<OrdersBatch>>,
    last_positions: RwLock<Option<PositionsBatch>>,
    last_accounts: RwLock<Option<AccountDeltaBatch>>,
    // Vendor securities cache
    #[allow(dead_code)]
    securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>,

    // Last mark/price per (provider, instrument)
    last_price: DashMap<(ProviderKind, Instrument), Decimal>,
}

#[allow(dead_code)]
impl PortfolioManager {
    pub fn new(securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>) -> Self {
        Self {
            securities_by_provider,
            ..Default::default()
        }
    }

    /// Centralized pre-strategy processing of incoming responses.
    /// This applies portfolio updates and returns a possibly adjusted response
    /// (e.g., with recalculated open_pnl for positions or filled PnL fields for accounts).
    pub fn process_response(&self, resp: Response) -> Response {
        match resp {
            Response::MBP10Batch(ob) => {
                let ev = &ob.event;
                let mark = if let Some(book) = &ev.book {
                    if let (Some(b0), Some(a0)) = (book.bid_px.first(), book.ask_px.first()) {
                        (*b0 + *a0) / Decimal::from(2)
                    } else {
                        ev.price
                    }
                } else {
                    ev.price
                };
                self.update_apply_last_price(ob.provider_kind, &ev.instrument, mark);
                Response::MBP10Batch(ob)
            }
            Response::OrdersBatch(ob) => {
                self.apply_orders_batch(ob.clone());
                Response::OrdersBatch(ob)
            }
            Response::PositionsBatch(pb) => {
                // Group by (provider, account) and apply per-account
                let mut by_acct: AHashMap<(ProviderKind, AccountName), Vec<PositionDelta>> =
                    AHashMap::new();
                for p in &pb.positions {
                    by_acct
                        .entry((p.provider_kind, p.account_name.clone()))
                        .or_default()
                        .push(p.clone());
                }
                for ((provider, account), positions) in by_acct.into_iter() {
                    let batch = PositionsBatch {
                        topic: pb.topic,
                        seq: pb.seq,
                        positions,
                    };
                    self.apply_positions_batch_for(provider, account, batch);
                }
                // Return adjusted open_pnl values based on last_price
                let adj = self.adjust_positions_batch_open_pnl(pb.clone());
                Response::PositionsBatch(adj)
            }
            Response::AccountDeltaBatch(mut ab) => {
                self.apply_account_delta_batch(ab.clone());
                for a in ab.accounts.iter_mut() {
                    if a.day_realized_pnl.is_zero() && a.open_pnl.is_zero() {
                        let open = self.account_open_pnl_sum(&a.provider_kind, &a.name);
                        let day = self.account_day_realized_pnl_utc(
                            &a.provider_kind,
                            &a.name,
                            Utc::now(),
                        );
                        a.open_pnl = open;
                        a.day_realized_pnl = day;
                    }
                }
                Response::AccountDeltaBatch(ab)
            }
            Response::ClosedTrades(trades) => {
                self.apply_closed_trades(trades.clone());
                Response::ClosedTrades(trades)
            }
            other => other,
        }
    }

    // Helper: compute open PnL from last_price for a given position
    #[allow(dead_code)]
    fn compute_open_pnl(
        &self,
        provider: &ProviderKind,
        instr: &Instrument,
        avg_px: Decimal,
        qty: Decimal,
    ) -> Option<Decimal> {
        self.last_price
            .get(&(*provider, instr.clone()))
            .map(|p| (*p - avg_px) * qty)
    }

    #[allow(dead_code)]
    // Helper: recompute synthetic totals for one instrument across all accounts/providers
    fn recompute_synthetic_for(&self, instrument: &Instrument) {
        let mut sum_qty = Decimal::ZERO;
        let mut sum_pq = Decimal::ZERO; // sum(avg_price * qty_signed)
        let mut sum_open = Decimal::ZERO;
        let mut latest_ts = None;
        let mut any_provider: Option<ProviderKind> = None;
        for acct_entry in self.positions_by_account.iter() {
            let map = acct_entry.value();
            if let Some(p) = map.get(instrument) {
                let pd = p.value();
                sum_qty += pd.net_qty;
                sum_pq += pd.average_price * pd.net_qty;
                sum_open += pd.open_pnl;
                latest_ts = Some(latest_ts.map_or(pd.time, |t: DateTime<Utc>| t.max(pd.time)));
                if any_provider.is_none() {
                    any_provider = Some(pd.provider_kind);
                }
            }
        }
        if sum_qty == Decimal::ZERO && sum_pq == Decimal::ZERO && sum_open == Decimal::ZERO {
            // No active positions for this instrument: remove synthetic if present
            self.positions_total_delta.remove(instrument);
            return;
        }
        let avg_px = if sum_qty != Decimal::ZERO {
            sum_pq / sum_qty
        } else {
            Decimal::ZERO
        };
        let side = if sum_qty >= Decimal::ZERO {
            PositionSide::Long
        } else {
            PositionSide::Short
        };
        let pd = PositionDelta {
            instrument: instrument.clone(),
            account_name: AccountName::new("ALL".to_string()),
            provider_kind: any_provider.unwrap_or(ProviderKind::ProjectX(
                tt_types::providers::ProjectXTenant::Topstep,
            )),
            net_qty: sum_qty,
            average_price: avg_px,
            open_pnl: sum_open,
            time: latest_ts.unwrap_or_else(Utc::now),
            side,
        };
        self.positions_total_delta.insert(instrument.clone(), pd);
    }

    #[allow(dead_code)]
    /// Apply an OrdersBatch to update the open orders map.
    /// Heuristic: if leaves > 0 we consider the order open; otherwise remove it if present.
    pub fn apply_orders_batch(&self, batch: OrdersBatch) {
        for o in &batch.orders {
            if let Some(key) = OrderKey::from_update(o) {
                if o.leaves > 0 {
                    self.open_orders.insert(key, o.clone());
                } else {
                    // Order completed: move from open -> completed store
                    self.open_orders.remove(&key);
                    let mut comp = self.completed_orders.write().expect("poisoned");
                    comp.insert(key, o.clone());
                }
            }
        }
        *self.last_orders.write().expect("poisoned") = Some(batch);
    }

    /// Apply a PositionsBatch scoped to a specific provider+account (when account attribution is available).
    /// We recompute open_pnl from last_price, update the per-account map, and then recompute the synthetic totals.
    pub fn apply_positions_batch_for(
        &self,
        provider_kind: ProviderKind,
        account: AccountName,
        batch: PositionsBatch,
    ) {
        // Track which instruments changed to recompute synthetic totals afterward
        let mut touched: ahash::AHashSet<Instrument> = ahash::AHashSet::new();

        {
            // Limit the lifetime of the entry guard to this block to avoid deadlocks
            let acc_key = (provider_kind, account.clone());
            let entry = self.positions_by_account.entry(acc_key).or_default();

            for p in &batch.positions {
                let mut pd = p.clone();
                // Ensure provider_kind is consistent
                pd.provider_kind = provider_kind;
                // Update open pnl if we have a last price
                if let Some(new_open) = self.compute_open_pnl(
                    &provider_kind,
                    &pd.instrument,
                    pd.average_price,
                    pd.net_qty,
                ) {
                    pd.open_pnl = new_open;
                }
                touched.insert(pd.instrument.clone());
                if pd.net_qty == Decimal::ZERO {
                    entry.remove(&pd.instrument);
                } else {
                    entry.insert(pd.instrument.clone(), pd);
                }
            }
            // entry guard dropped here
        }

        // Recompute synthetic totals for changed instruments (no entry guard held now)
        for instr in touched.iter() {
            self.recompute_synthetic_for(instr);
        }
        *self.last_positions.write().expect("poisoned") = Some(batch);
    }

    /// Apply AccountDeltaBatch updates (keyed by provider + account name).
    pub fn apply_account_delta_batch(&self, batch: AccountDeltaBatch) {
        for a in &batch.accounts {
            self.accounts_by_name
                .insert((a.provider_kind, a.name.clone()), a.clone());
        }
        *self.last_accounts.write().expect("poisoned") = Some(batch);
    }

    // Queries - positions (aggregated by instrument)
    pub fn position_total_delta_for(&self, instrument: &Instrument) -> Option<PositionDelta> {
        self.positions_total_delta
            .get(instrument)
            .map(|r| r.value().clone())
    }

    /// True if any account currently has at least one non-flat position tracked.
    pub fn has_open_positions(&self) -> bool {
        for acct_map in self.positions_by_account.iter() {
            if !acct_map.value().is_empty() {
                return true;
            }
        }
        false
    }

    /// This is how we update open pnl from average entry price
    pub fn update_apply_last_price(
        &self,
        provider_kind: ProviderKind,
        instrument: &Instrument,
        price: Decimal,
    ) {
        let key = (provider_kind, instrument.clone());
        self.last_price.insert(key, price);

        // Update all real account positions for this provider+instrument
        for acct_entry in self.positions_by_account.iter() {
            let (pk, _acct) = acct_entry.key();
            if pk != &provider_kind {
                continue;
            }
            let map = acct_entry.value();
            if let Some(mut pd_ref) = map.get_mut(instrument) {
                let pd = pd_ref.value_mut();
                pd.open_pnl = (price - pd.average_price) * pd.net_qty;
                // side stays consistent with net_qty sign
                pd.side = if pd.net_qty >= Decimal::ZERO {
                    PositionSide::Long
                } else {
                    PositionSide::Short
                };
                // time stays as received
            }
        }
        // Recompute synthetic totals after updates
        self.recompute_synthetic_for(instrument);
    }

    pub fn net_qty(&self, instrument: &Instrument) -> Decimal {
        self.position_total_delta_for(instrument)
            .map(|p| p.net_qty)
            .unwrap_or(Decimal::ZERO)
    }

    pub fn is_long(&self, instrument: &Instrument) -> bool {
        self.net_qty(instrument) > Decimal::ZERO
    }
    pub fn is_short(&self, instrument: &Instrument) -> bool {
        self.net_qty(instrument) < Decimal::ZERO
    }
    pub fn is_flat(&self, instrument: &Instrument) -> bool {
        self.position_total_delta_for(instrument)
            .map(|p| p.net_qty == Decimal::ZERO)
            .unwrap_or(true)
    }

    // Queries - positions by account (requires attribution)
    pub fn position_for_account(
        &self,
        provider_kind: &ProviderKind,
        account: &AccountName,
        instrument: &Instrument,
    ) -> Option<PositionDelta> {
        self.positions_by_account
            .get(&(*provider_kind, account.clone()))
            .and_then(|map| map.get(instrument).map(|r| r.value().clone()))
    }
    pub fn net_qty_account(
        &self,
        provider_kind: &ProviderKind,
        account: &AccountName,
        instrument: &Instrument,
    ) -> Decimal {
        self.position_for_account(provider_kind, account, instrument)
            .map(|p| p.net_qty)
            .unwrap_or(Decimal::ZERO)
    }
    pub fn is_long_account(
        &self,
        provider_kind: &ProviderKind,
        account: &AccountName,
        instrument: &Instrument,
    ) -> bool {
        self.net_qty_account(provider_kind, account, instrument) > Decimal::ZERO
    }
    pub fn is_short_account(
        &self,
        provider_kind: &ProviderKind,
        account: &AccountName,
        instrument: &Instrument,
    ) -> bool {
        self.net_qty_account(provider_kind, account, instrument) < Decimal::ZERO
    }
    pub fn is_flat_account(
        &self,
        provider_kind: &ProviderKind,
        account: &AccountName,
        instrument: &Instrument,
    ) -> bool {
        self.position_for_account(provider_kind, account, instrument)
            .map(|p| p.net_qty == Decimal::ZERO)
            .unwrap_or(true)
    }

    #[allow(dead_code)]
    // Queries - accounts
    pub fn account_delta(
        &self,
        provider_kind: &ProviderKind,
        name: &AccountName,
    ) -> Option<AccountDelta> {
        self.accounts_by_name
            .get(&(*provider_kind, name.clone()))
            .map(|r| r.value().clone())
    }
    #[allow(dead_code)]
    pub fn can_trade(&self, provider_kind: &ProviderKind, name: &AccountName) -> Option<bool> {
        self.account_delta(provider_kind, name).map(|a| a.can_trade)
    }
    #[allow(dead_code)]
    pub fn equity(
        &self,
        provider_kind: &ProviderKind,
        name: &AccountName,
    ) -> Option<rust_decimal::Decimal> {
        self.account_delta(provider_kind, name).map(|a| a.equity)
    }
    #[allow(dead_code)]
    // Queries - open orders
    pub fn open_order_count(&self) -> usize {
        self.open_orders.len()
    }
    #[allow(dead_code)]
    pub fn open_orders_for_instrument(&self, instrument: &Instrument) -> Vec<OrderUpdate> {
        self.open_orders
            .iter()
            .filter(|kv| &kv.key().instrument == instrument)
            .map(|kv| kv.value().clone())
            .collect()
    }

    #[allow(dead_code)]
    // Queries - closed positions: no longer stored; use trades instead
    pub fn closed_for_instrument(&self, _instrument: &Instrument) -> Vec<PositionDelta> {
        Vec::new()
    }

    #[allow(dead_code)]
    // Snapshots
    pub fn last_orders(&self) -> Option<OrdersBatch> {
        self.last_orders.read().expect("poisoned").clone()
    }
    #[allow(dead_code)]
    pub fn last_positions(&self) -> Option<PositionsBatch> {
        self.last_positions.read().expect("poisoned").clone()
    }
    #[allow(dead_code)]
    pub fn last_accounts(&self) -> Option<AccountDeltaBatch> {
        self.last_accounts.read().expect("poisoned").clone()
    }

    /// Build a snapshot of all known per-account positions with open_pnl recomputed from last marks.
    /// The position times are set to `now` to reflect the snapshot emission time.
    pub fn positions_snapshot(&self, now: DateTime<Utc>) -> PositionsBatch {
        let mut out: Vec<PositionDelta> = Vec::new();
        for acct_entry in self.positions_by_account.iter() {
            for p in acct_entry.value().iter() {
                let mut pd = p.value().clone();
                // Recompute open_pnl from last mark if available
                if let Some(mark) = self
                    .last_price
                    .get(&(pd.provider_kind, pd.instrument.clone()))
                {
                    pd.open_pnl = (*mark - pd.average_price) * pd.net_qty;
                }
                // Update side based on qty and set time to snapshot time
                pd.side = if pd.net_qty.is_zero() {
                    PositionSide::Flat
                } else if pd.net_qty > Decimal::ZERO {
                    PositionSide::Long
                } else {
                    PositionSide::Short
                };
                pd.time = now;
                out.push(pd);
            }
        }
        PositionsBatch {
            topic: tt_types::keys::Topic::Positions,
            seq: 0,
            positions: out,
        }
    }

    /// Build a snapshot of account deltas (equity may be taken from last known values if present),
    /// recomputing open_pnl and day_realized_pnl using current state. Includes accounts that have
    /// either prior deltas or active positions.
    pub fn accounts_snapshot(&self, now: DateTime<Utc>) -> AccountDeltaBatch {
        use ahash::AHashSet;
        let mut out: Vec<AccountDelta> = Vec::new();
        let mut seen: AHashSet<(ProviderKind, AccountName)> = AHashSet::new();

        // Include all accounts that currently have positions
        for acct_entry in self.positions_by_account.iter() {
            let (provider, name) = acct_entry.key();
            let provider = *provider;
            let name = name.clone();
            seen.insert((provider, name.clone()));
            let open = self.account_open_pnl_sum(&provider, &name);
            let day = self.account_day_realized_pnl_utc(&provider, &name, now);
            let base = self
                .accounts_by_name
                .get(&(provider, name.clone()))
                .map(|r| r.value().clone());
            let (equity, can_trade) = base
                .map(|b| (b.equity, b.can_trade))
                .unwrap_or((Decimal::ZERO, true));
            out.push(AccountDelta {
                provider_kind: provider,
                name,
                equity,
                day_realized_pnl: day,
                open_pnl: open,
                time: now,
                can_trade,
            });
        }
        // Include any accounts we have prior deltas for (even if flat now)
        for acct in self.accounts_by_name.iter() {
            let key = acct.key();
            if seen.contains(key) {
                continue;
            }
            let base = acct.value().clone();
            let open = self.account_open_pnl_sum(&base.provider_kind, &base.name);
            let day = self.account_day_realized_pnl_utc(&base.provider_kind, &base.name, now);
            out.push(AccountDelta {
                provider_kind: base.provider_kind,
                name: base.name,
                equity: base.equity,
                day_realized_pnl: day,
                open_pnl: open,
                time: now,
                can_trade: base.can_trade,
            });
        }
        AccountDeltaBatch {
            topic: tt_types::keys::Topic::AccountEvt,
            seq: 0,
            accounts: out,
        }
    }

    #[allow(dead_code)]
    /// Adjust a PositionsBatch by recomputing open_pnl using last known marks in the portfolio.
    /// Does not mutate internal state; purely transforms the batch for downstream consumers.
    pub fn adjust_positions_batch_open_pnl(&self, mut batch: PositionsBatch) -> PositionsBatch {
        for p in batch.positions.iter_mut() {
            if let Some(mark) = self
                .last_price
                .get(&(p.provider_kind, p.instrument.clone()))
            {
                // open_pnl = (mark - avg_entry) * signed_qty
                p.open_pnl = (*mark - p.average_price) * p.net_qty;
            }
        }
        batch
    }

    /// Record a batch of closed trades (appended; de-duplication by id is out of scope here).
    pub fn apply_closed_trades(&self, trades: Vec<Trade>) {
        let mut ct = self.closed_trades.write().expect("poisoned");
        ct.extend(trades);
    }

    /// Compute total open PnL for a specific provider+account, summing across its instruments.
    /// Returns Decimal::ZERO if we have no tracked positions for this account.
    pub fn account_open_pnl_sum(
        &self,
        provider_kind: &ProviderKind,
        name: &AccountName,
    ) -> Decimal {
        if let Some(map) = self
            .positions_by_account
            .get(&(*provider_kind, name.clone()))
        {
            let mut sum = Decimal::ZERO;
            for p in map.iter() {
                sum += p.value().open_pnl;
            }
            sum
        } else {
            Decimal::ZERO
        }
    }

    /// Compute day realized PnL for the given provider+account based on closed trades with
    /// creation_time on the same UTC date as `now`. Voided trades are ignored.
    pub fn account_day_realized_pnl_utc(
        &self,
        provider_kind: &ProviderKind,
        name: &AccountName,
        now: DateTime<Utc>,
    ) -> Decimal {
        let day = now.date_naive();
        let ct = self.closed_trades.read().expect("poisoned");
        let mut sum = Decimal::ZERO;
        for t in ct.iter() {
            if &t.provider == provider_kind
                && &t.account_name == name
                && !t.voided
                && t.creation_time.date_naive() == day
            {
                // Sum reported PnL; fees handling is left to provider semantics
                sum += t.profit_and_loss;
            }
        }
        sum
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use tt_types::providers::ProjectXTenant;
    use tt_types::wire::PositionsBatch;

    fn pd(instr: &str, qty: i64, acct: &AccountName) -> PositionDelta {
        PositionDelta {
            provider_kind: ProviderKind::ProjectX(ProjectXTenant::Topstep),
            instrument: Instrument::validate_len(instr).unwrap(),
            account_name: acct.clone(),
            net_qty: Decimal::from(qty),
            average_price: Decimal::from(100),
            open_pnl: Decimal::ZERO,
            time: Utc::now(),
            side: if qty >= 0 {
                PositionSide::Long
            } else {
                PositionSide::Short
            },
        }
    }

    #[test]
    fn default_is_flat() {
        let pm = PortfolioManager::new(Arc::new(DashMap::new()));
        let i = Instrument::validate_len("TEST.X1").unwrap();
        assert!(pm.is_flat(&i));
        assert!(!pm.is_long(&i));
        assert!(!pm.is_short(&i));
    }

    #[test]
    fn long_short_flat_detection() {
        let pm = PortfolioManager::new(Arc::new(DashMap::new()));
        let provider = ProviderKind::ProjectX(ProjectXTenant::Demo);
        let account = AccountName::new("TEST-ACCT".to_string());
        let long = pd("ES.Z25", 5, &account);
        let short = pd("NQ.Z25", -3, &account);
        let flat = pd("YM.Z25", 0, &account);
        let batch = PositionsBatch {
            topic: tt_types::keys::Topic::Positions,
            seq: 1,
            positions: vec![long.clone(), short.clone(), flat.clone()],
        };
        pm.apply_positions_batch_for(provider, account.clone(), batch);

        assert!(pm.is_long(&long.instrument));
        assert!(!pm.is_short(&long.instrument));
        assert!(!pm.is_flat(&long.instrument));

        assert!(pm.is_short(&short.instrument));
        assert!(!pm.is_long(&short.instrument));
        assert!(!pm.is_flat(&short.instrument));

        assert!(pm.is_flat(&flat.instrument));
        assert!(!pm.is_long(&flat.instrument));
        assert!(!pm.is_short(&flat.instrument));
    }

    #[test]
    fn account_open_pnl_and_day_realized() {
        let pm = PortfolioManager::new(Arc::new(DashMap::new()));
        let provider = ProviderKind::ProjectX(ProjectXTenant::Demo);
        let account = AccountName::new("TEST-ACCT".to_string());
        // Seed a per-account position with open_pnl
        let instr = Instrument::validate_len("ES.Z25").unwrap();
        let pd = PositionDelta {
            instrument: instr.clone(),
            account_name: account.clone(),
            provider_kind: provider,
            net_qty: Decimal::from(2),
            average_price: Decimal::from(100),
            open_pnl: Decimal::from(5),
            time: Utc::now(),
            side: PositionSide::Long,
        };
        pm.apply_positions_batch_for(
            provider,
            account.clone(),
            PositionsBatch {
                topic: tt_types::keys::Topic::Positions,
                seq: 1,
                positions: vec![pd],
            },
        );
        assert_eq!(
            pm.account_open_pnl_sum(&provider, &account),
            Decimal::from(5)
        );

        // Seed a closed trade today with PnL 7
        let tr = Trade {
            id: EngineUuid::new(),
            provider,
            account_name: account.clone(),
            instrument: instr,
            creation_time: Utc::now(),
            price: Decimal::from(0),
            profit_and_loss: Decimal::from(7),
            fees: Decimal::ZERO,
            side: tt_types::accounts::events::Side::Buy,
            size: Decimal::from(1),
            voided: false,
            order_id: "x".into(),
        };
        pm.apply_closed_trades(vec![tr]);
        assert_eq!(
            pm.account_day_realized_pnl_utc(&provider, &account, Utc::now()),
            Decimal::from(7)
        );
    }
}
