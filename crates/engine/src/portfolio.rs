use std::sync::{Arc};
use chrono::Utc;
use dashmap::DashMap;
use ahash::AHashMap;
use rust_decimal::Decimal;
use tokio::sync::{Mutex, RwLock};
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::{AccountDelta, ClientOrderId, OrderUpdate, PositionDelta, ProviderOrderId};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{AccountDeltaBatch, OrdersBatch, PositionsBatch, Trade};

/// Key for tracking an order in the open orders map
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OrderKeyId {
    Client(ClientOrderId),
    Provider(ProviderOrderId),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OrderKey {
    pub instrument: Instrument,
    pub id: OrderKeyId,
}

impl OrderKey {
    fn from_update(o: &OrderUpdate) -> Option<Self> {
        if let Some(coid) = &o.client_order_id {
            return Some(OrderKey { instrument: o.instrument.clone(), id: OrderKeyId::Client(coid.clone()) });
        }
        if let Some(poid) = &o.provider_order_id {
            return Some(OrderKey { instrument: o.instrument.clone(), id: OrderKeyId::Provider(poid.clone()) });
        }
        None
    }
}

pub enum Result {
    Win,
    Loss,
    BreakEven
}



/// Aggregates the latest portfolio state as seen by the engine.
/// Concurrency: uses DashMap for hot paths and a Mutex-protected AHashMap for closed positions.
#[derive(Default)]
pub struct PortfolioManager {
    // Latest snapshot by account -> instrument: real positions, in real provider accounts.
    positions_by_account: DashMap<(ProviderKind, AccountName), DashMap<Instrument, PositionDelta>>,
    // Total position delta across all accounts (synthetic), open pnl + average price calculated as net-sum of real positions
    positions_total_delta: DashMap<Instrument, PositionDelta>,
    // Latest account deltas keyed by provider + account name
    accounts_by_name: DashMap<(ProviderKind, AccountName), AccountDelta>,
    // All currently open orders (keyed by instrument + best available order id)
    open_orders: DashMap<OrderKey, OrderUpdate>,
    // Completed (closed/filled) orders kept for later reference
    completed_orders: Mutex<AHashMap<OrderKey, OrderUpdate>>,
    // Closed trades (from provider/execution layer)
    closed_trades: RwLock<Vec<Trade>>,
    // Optional last snapshots (for pass-through APIs and debugging)
    last_orders: Mutex<Option<OrdersBatch>>,
    last_positions: Mutex<Option<PositionsBatch>>,
    last_accounts: Mutex<Option<AccountDeltaBatch>>,
    // Vendor securities cache
    securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>,

    last_price: DashMap<(ProviderKind, Instrument), Decimal>,
}

impl PortfolioManager {
    pub fn new(securities_by_provider: Arc<DashMap<ProviderKind, Vec<Instrument>>>) -> Self {
        let mut pm = Self::default();
        pm.securities_by_provider = securities_by_provider;
        pm
    }



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
                    let mut comp = self.completed_orders.lock().expect("poisoned");
                    comp.insert(key, o.clone());
                }
            }
        }
        *self.last_orders.lock().expect("poisoned") = Some(batch);
    }

    /// Apply a PositionsBatch (aggregated update; no account identity).
    pub fn apply_positions_batch(&self, batch: PositionsBatch) {
        for p in &batch.positions {
            //todo
        }
        *self.last_positions.lock().expect("poisoned") = Some(batch);
    }

    /// Apply a PositionsBatch scoped to a specific account (when account attribution is available).
    pub fn apply_positions_batch_for_account(&self, account: AccountName, batch: PositionsBatch) {
        let entry = self
            .positions_by_account
            .entry(account.clone())
            .or_insert_with(DashMap::new);
        for p in &batch.positions {
            if p.net_qty > Decimal::ZERO {
                entry.remove(&p.instrument);
                self.positions_by_instrument.remove(&(account.clone(), p.instrument.clone()));
            } else {
                entry.insert(p.instrument.clone(), p.clone());
                // Maintain per-account view as tuple key
                self.positions_by_instrument.insert((account.clone(), p.instrument.clone()), p.clone());
            }
        }
        *self.last_positions.lock().expect("poisoned") = Some(batch);
    }

    /// Apply AccountDeltaBatch updates.
    pub fn apply_account_delta_batch(&self, batch: AccountDeltaBatch) {
        for a in &batch.accounts {
            self.accounts_by_name.insert(a.name.clone(), a.clone());
        }
        *self.last_accounts.lock().expect("poisoned") = Some(batch);
    }

    // Queries - positions (aggregated by instrument)
    pub fn position_total_delta_for(&self, instrument: &Instrument) -> Option<PositionDelta> {
        // Aggregate net qty across all accounts for this instrument
        let mut total_after: Decimal = Decimal::ZERO;
        let mut total_before: Decimal = Decimal::ZERO;
        let mut provider_kind: Option<ProviderKind> = None;
        let ts = Utc::now(); //todo, use engine clock
        for kv in self.positions_by_instrument.iter() {
            let (_acct, instr) = kv.key();
            if instr == instrument {
                let p = kv.value();
                total_after += p.net_qty;
                total_before += p.net_qty;
                provider_kind = provider_kind.or(Some(p.provider_kind.clone()));
                if p.time > ts { ts = p.time; }
            }
        }

        if total_after == Decimal::ZERO && total_before == Decimal::ZERO && provider_kind.is_none() {
            None
        } else {
            Some(PositionDelta {
                instrument: instrument.clone(),
                provider_kind: provider_kind.unwrap_or(ProviderKind::ProjectX(tt_types::providers::ProjectXTenant::Topstep)), // fallback
                net_qty: total_after,
                average_price:,
                open_pnl: ,
                time: ts,
                side: ,
            })
        }
    }

    /// This is how we update open pnl from average entry price
    pub fn update_apply_last_price(&self, provider_kind: ProviderKind, instrument: &Instrument, price: Decimal) {
        let key = (provider_kind, instrument.clone());
        self.last_price.insert(key, price);
        if let Some(position) = self.positions_by_account

    }


    pub fn net_qty(&self, instrument: &Instrument) -> Decimal {
        self.position_for(instrument).map(|p| p.net_qty).unwrap_or(Decimal::ZERO)
    }

    pub fn is_long_delta(&self, instrument: &Instrument) -> bool {
        self.net_qty(instrument) > 0
    }
    pub fn is_short_delta(&self, instrument: &Instrument) -> bool {
        self.net_qty(instrument) < 0
    }
    pub fn is_flat_delta(&self, instrument: &Instrument) -> bool {
        self.position_for(instrument)
            .map(|p| p.net_qty_after == 0)
            .unwrap_or(true)
    }

    // Queries - positions by account (requires attribution)
    pub fn position_for_account(&self, account: &AccountName, instrument: &Instrument) -> Option<PositionDelta> {
        self.positions_by_account
            .get(account)
            .and_then(|map| map.get(instrument).map(|r| r.value().clone()))
    }
    pub fn net_qty_account(&self, account: &AccountName, instrument: &Instrument) -> i64 {
        self.position_for_account(account, instrument)
            .map(|p| p.net_qty_after)
            .unwrap_or(0)
    }
    pub fn is_long_account(&self, account: &AccountName, instrument: &Instrument) -> bool {
        self.net_qty_account(account, instrument) > 0
    }
    pub fn is_short_account(&self, account: &AccountName, instrument: &Instrument) -> bool {
        self.net_qty_account(account, instrument) < 0
    }
    pub fn is_flat_account(&self, account: &AccountName, instrument: &Instrument) -> bool {
        self.position_for_account(account, instrument)
            .map(|p| p.net_qty_after == 0)
            .unwrap_or(true)
    }

    // Queries - accounts
    pub fn account_delta(&self, name: &AccountName) -> Option<AccountDelta> {
        self.accounts_by_name.get(name).map(|r| r.value().clone())
    }
    pub fn can_trade(&self, name: &AccountName) -> Option<bool> {
        self.account_delta(name).map(|a| a.can_trade)
    }
    pub fn equity(&self, name: &AccountName) -> Option<rust_decimal::Decimal> {
        self.account_delta(name).map(|a| a.equity)
    }

    // Queries - open orders
    pub fn open_order_count(&self) -> usize {
        self.open_orders.len()
    }
    pub fn open_orders_for_instrument(&self, instrument: &Instrument) -> Vec<OrderUpdate> {
        self.open_orders
            .iter()
            .filter(|kv| &kv.key().instrument == instrument)
            .map(|kv| kv.value().clone())
            .collect()
    }

    // Queries - closed positions: no longer stored; use trades instead
    pub fn closed_for_instrument(&self, _instrument: &Instrument) -> Vec<PositionDelta> {
        Vec::new()
    }

    // Snapshots
    pub fn last_orders(&self) -> Option<OrdersBatch> {
        self.last_orders.lock().expect("poisoned").clone()
    }
    pub fn last_positions(&self) -> Option<PositionsBatch> {
        self.last_positions.lock().expect("poisoned").clone()
    }
    pub fn last_accounts(&self) -> Option<AccountDeltaBatch> {
        self.last_accounts.lock().expect("poisoned").clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rust_decimal::Decimal;
    use tt_types::providers::ProjectXTenant;
    use tt_types::wire::PositionsBatch;

    fn pd(instr: &str, before: i64, after: i64) -> PositionDelta {
        PositionDelta {
            provider_kind: ProviderKind::ProjectX(ProjectXTenant::Topstep),
            instrument: Instrument::validate_len(instr).unwrap(),
            net_qty_before: before,
            net_qty_after: after,
            realized_delta: Decimal::ZERO,
            open_pnl: Decimal::ZERO,
            time: Utc::now(),
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
        let long = pd("ES.Z25", 0, 5);
        let short = pd("NQ.Z25", 0, -3);
        let flat = pd("YM.Z25", 2, 0);
        let batch = PositionsBatch {
            topic: tt_types::keys::Topic::Positions,
            seq: 1,
            positions: vec![long.clone(), short.clone(), flat.clone()],
        };
        pm.apply_positions_batch(batch);

        assert!(pm.is_long(&long.instrument));
        assert!(!pm.is_short(&long.instrument));
        assert!(!pm.is_flat(&long.instrument));

        assert!(pm.is_short(&short.instrument));
        assert!(!pm.is_long(&short.instrument));
        assert!(!pm.is_flat(&short.instrument));

        assert!(pm.is_flat(&flat.instrument));
        assert!(!pm.is_long(&flat.instrument));
        assert!(!pm.is_short(&flat.instrument));

        // closed history recorded for flat entries
        let closed = pm.closed_for_instrument(&flat.instrument);
        assert_eq!(closed.len(), 1);
    }
}
