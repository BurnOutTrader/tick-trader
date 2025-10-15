use ahash::AHashMap;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use rust_decimal::{Decimal, dec};
use std::sync::{LazyLock, RwLock};
use tt_types::accounts::account::AccountName;
use tt_types::accounts::events::{
    AccountDelta, OrderUpdate, PositionDelta, PositionSide, ProviderOrderId,
};
use tt_types::engine_id::EngineUuid;
use tt_types::keys::{AccountKey};
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::securities::security::FuturesContract;
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


pub(crate) static PORTFOLIOS: LazyLock<DashMap<AccountKey, Portfolio>> = LazyLock::new(|| DashMap::new());

// Global contracts registry: provider -> instrument -> contract
static CONTRACTS: LazyLock<DashMap<ProviderKind, AHashMap<Instrument, FuturesContract>>> = LazyLock::new(|| DashMap::new());

/// Initialize/replace contracts for a provider
pub fn initialize_contracts(provider: ProviderKind, contracts: Vec<FuturesContract>) {
    let mut map: AHashMap<Instrument, FuturesContract> = AHashMap::new();
    for c in contracts.into_iter() {
        map.insert(c.instrument.clone(), c);
    }
    CONTRACTS.insert(provider, map);
}

/// Reset all static stores
pub fn clear_all() {
    PORTFOLIOS.clear();
    CONTRACTS.clear();
}

/// Reset all data for a single provider
pub fn clear_provider(provider: ProviderKind) {
    PORTFOLIOS.retain(|_k, v| v.provider() != provider);
    CONTRACTS.remove(&provider);
}

/// Reset a single account
pub fn clear_account(key: &AccountKey) {
    PORTFOLIOS.remove(key);
}

fn default_account_delta_for(key: &AccountKey, now: DateTime<Utc>) -> AccountDelta {
    AccountDelta {
        provider_kind: key.provider,
        name: key.account_name.clone(),
        key: key.clone(),
        equity: Decimal::ZERO,
        day_realized_pnl: Decimal::ZERO,
        open_pnl: Decimal::ZERO,
        time: now,
        can_trade: true,
    }
}

fn ensure_portfolio(key: &AccountKey) {
    if !PORTFOLIOS.contains_key(key) {
        let now = Utc::now();
        let init = default_account_delta_for(key, now);
        PORTFOLIOS.insert(key.clone(), Portfolio::new(key.clone(), init));
    }
}

// ===== Public write API (single writer expected) =====

pub fn apply_account_delta_batch(batch: AccountDeltaBatch) {
    for a in batch.accounts.into_iter() {
        let key = a.key.clone();
        ensure_portfolio(&key);
        if let Some(pf) = PORTFOLIOS.get(&key) {
            pf.apply_account_delta_batch(a);
        }
    }
}

pub fn apply_positions_batch(account_key: AccountKey, batch: PositionsBatch, now: DateTime<Utc>) {
    ensure_portfolio(&account_key);
    if let Some(pf) = PORTFOLIOS.get(&account_key) {
        pf.apply_positions_batch(batch, now);
    }
}

pub fn apply_orders_batch(account_key: AccountKey, batch: OrdersBatch) {
    ensure_portfolio(&account_key);
    if let Some(pf) = PORTFOLIOS.get(&account_key) {
        pf.apply_orders_batch(batch);
    }
}

pub fn apply_closed_trades(account_key: AccountKey, trades: Vec<Trade>) {
    ensure_portfolio(&account_key);
    if let Some(pf) = PORTFOLIOS.get(&account_key) {
        pf.apply_closed_trades(trades);
    }
}

pub fn apply_mark(provider: ProviderKind, instrument: &Instrument, price: Decimal, now: DateTime<Utc>) {
    for kv in PORTFOLIOS.iter() {
        if kv.value().provider() == provider {
            kv.update_apply_last_price(instrument, price, now);
        }
    }
}

// ===== Public read API =====

// Accounts
pub fn balance(account_key: &AccountKey) -> Decimal {
    PORTFOLIOS
        .get(account_key)
        .map(|p| p.equity())
        .unwrap_or(Decimal::ZERO)
}

pub fn can_trade(account_key: &AccountKey) -> bool {
    PORTFOLIOS
        .get(account_key)
        .map(|p| p.can_trade())
        .unwrap_or(true)
}

pub fn open_pnl(account_key: &AccountKey) -> Decimal {
    PORTFOLIOS
        .get(account_key)
        .map(|p| p.account_open_pnl_sum())
        .unwrap_or(Decimal::ZERO)
}

pub fn booked_pnl(account_key: &AccountKey, now: DateTime<Utc>) -> Decimal {
    PORTFOLIOS
        .get(account_key)
        .map(|p| p.account_day_realized_pnl_utc(now))
        .unwrap_or(Decimal::ZERO)
}

// Positions
pub fn qty(account_key: &AccountKey, instrument: &Instrument) -> Decimal {
    PORTFOLIOS
        .get(account_key)
        .map(|p| p.net_qty(instrument))
        .unwrap_or(Decimal::ZERO)
}

pub fn long_qty(account_key: &AccountKey, instrument: &Instrument) -> Decimal {
    let q = qty(account_key, instrument);
    if q > Decimal::ZERO { q } else { Decimal::ZERO }
}

pub fn short_qty(account_key: &AccountKey, instrument: &Instrument) -> Decimal {
    let q = qty(account_key, instrument);
    if q < Decimal::ZERO { -q } else { Decimal::ZERO }
}

pub fn is_long(account_key: &AccountKey, instrument: &Instrument) -> bool {
    qty(account_key, instrument) > Decimal::ZERO
}

pub fn is_short(account_key: &AccountKey, instrument: &Instrument) -> bool {
    qty(account_key, instrument) < Decimal::ZERO
}

pub fn is_flat(account_key: &AccountKey, instrument: &Instrument) -> bool {
    qty(account_key, instrument) == Decimal::ZERO
}

pub fn position_open_pnl(account_key: &AccountKey, instrument: &Instrument) -> Decimal {
    PORTFOLIOS
        .get(account_key)
        .and_then(|p| p.position(instrument))
        .map(|pd| pd.open_pnl)
        .unwrap_or(Decimal::ZERO)
}

pub fn open_positions(account_key: &AccountKey, now: DateTime<Utc>) -> PositionsBatch {
    if let Some(p) = PORTFOLIOS.get(account_key) {
        p.positions_snapshot(now)
    } else {
        PositionsBatch { topic: tt_types::keys::Topic::Positions, seq: 0, positions: vec![] }
    }
}

// Orders
pub fn open_order_count(account_key: &AccountKey) -> usize {
    PORTFOLIOS
        .get(account_key)
        .map(|p| p.open_order_count())
        .unwrap_or(0)
}

pub fn open_orders_for_instrument(account_key: &AccountKey, instrument: &Instrument) -> Vec<OrderUpdate> {
    PORTFOLIOS
        .get(account_key)
        .map(|p| p.open_orders_for_instrument(instrument))
        .unwrap_or_default()
}

pub fn open_orders(account_key: &AccountKey) -> Vec<OrderUpdate> {
    if let Some(p) = PORTFOLIOS.get(account_key) {
        p.open_orders.iter().map(|kv| kv.value().clone()).collect()
    } else {
        vec![]
    }
}

/// Aggregates the latest portfolio state for a *single* provider+account.
/// Concurrency: DashMap for hot paths; Mutex/RwLock for colder data.
pub struct Portfolio {
    // The single account this portfolio represents
    account_key: AccountKey,

    // Positions for this account only: instrument -> position
    positions: DashMap<Instrument, PositionDelta>,

    // Latest account delta for this account (if we've received one)
    account: RwLock<AccountDelta>,

    // All currently open orders (keyed by instrument + best available order id)
    open_orders: DashMap<OrderKey, OrderUpdate>,
    // Completed (closed/filled) orders kept for later reference
    completed_orders: RwLock<AHashMap<OrderKey, OrderUpdate>>,

    // Closed trades (from provider/execution layer)
    closed_trades: RwLock<Vec<Trade>>,

    // Last mark/price per instrument (single provider, so instrument key is enough)
    last_price: DashMap<Instrument, Decimal>,
}

impl Portfolio {
    pub fn new(
        account_key: AccountKey,
        account_init: AccountDelta
    ) -> Self {
        Self {
            account_key,
            positions: Default::default(),
            account:RwLock::new(account_init),
            open_orders: Default::default(),
            completed_orders: Default::default(),
            closed_trades: Default::default(),
            last_price: Default::default(),
        }
    }

    #[inline]
    fn provider(&self) -> ProviderKind { self.account_key.provider }

    #[inline]
    fn is_our_account(&self, provider: ProviderKind, name: &AccountName) -> bool {
        provider == self.account_key.provider && name == &self.account_key.account_name
    }

    /// Centralized pre-strategy processing of incoming responses.
    /// Applies updates and returns possibly adjusted responses.
    pub fn process_response(&self, resp: Response, now: DateTime<Utc>) -> Response {
        match resp {
            //todo, we no longer process raw data here, instead we just update open pnl if someone asks for an item in the portfolio and on creation and destruction to log stats
            Response::OrdersBatch(ob) => {
                self.apply_orders_batch(ob.clone());
                Response::OrdersBatch(ob)
            }
            Response::PositionsBatch(pb) => {
                // Keep only positions for our account, then apply
                let ours: Vec<PositionDelta> = pb.positions
                    .into_iter()
                    .filter(|p| self.is_our_account(p.provider_kind, &p.account_name))
                    .collect();

                if !ours.is_empty() {
                    let batch = PositionsBatch { topic: pb.topic, seq: pb.seq, positions: ours };
                    self.apply_positions_batch(batch.clone(), now);
                    // Return adjusted open_pnl values based on last_price
                    let adj = self.adjust_positions_batch_open_pnl(batch);
                    Response::PositionsBatch(adj)
                } else {
                    // Nothing for us — pass through unchanged
                    Response::PositionsBatch(PositionsBatch { topic: pb.topic, seq: pb.seq, positions: vec![] })
                }
            }
            Response::AccountDeltaBatch(mut ab) => {
                // Keep only our account's deltas
                ab.accounts.retain(|a| self.is_our_account(a.provider_kind, &a.name));
                if !ab.accounts.is_empty() {
                    for a in ab.accounts.iter().cloned() {
                        self.apply_account_delta_batch(a);
                    }
                    for a in ab.accounts.iter_mut() {
                        if a.day_realized_pnl.is_zero() && a.open_pnl.is_zero() {
                            a.open_pnl = self.account_open_pnl_sum();
                            a.day_realized_pnl = self.account_day_realized_pnl_utc(now);
                        }
                        a.time = now;
                    }
                }
                Response::AccountDeltaBatch(ab)
            }
            Response::ClosedTrades(trades) => {
                // Keep only our account's trades
                let ours: Vec<Trade> = trades.into_iter()
                    .filter(|t| self.is_our_account(t.provider, &t.account_name))
                    .collect();
                if !ours.is_empty() {
                    self.apply_closed_trades(ours.clone());
                }
                Response::ClosedTrades(ours)
            }
            other => other,
        }
    }

    // ===== Mutators =====

    /// Apply an OrdersBatch to update the open orders map (single account).
    /// Heuristic: if leaves > 0 we consider the order open; otherwise move to completed.
    pub fn apply_orders_batch(&self, batch: OrdersBatch) {
        for o in &batch.orders {
            // If orders carry account attribution, you can filter here as well
            if let Some(key) = OrderKey::from_update(o) {
                if o.leaves > 0 {
                    self.open_orders.insert(key, o.clone());
                } else {
                    self.open_orders.remove(&key);
                    let mut comp = self.completed_orders.write().expect("poisoned completed_orders");
                    comp.insert(key, o.clone());
                }
            }
        }
    }

    /// Apply a PositionsBatch for *this* account (assumes caller already filtered to our account).
    pub fn apply_positions_batch(&self, batch: PositionsBatch, _now: DateTime<Utc>) {
        for p in &batch.positions {
            // Normalize provider/account to ours to avoid mismatches
            let mut pd = p.clone();
            pd.provider_kind = self.provider();

            // Update open pnl if we have a last price
            if let Some(new_open) = self.compute_open_pnl(&pd.instrument, pd.average_price, pd.net_qty, pd.side) {
                pd.open_pnl = new_open;
            }

            if pd.net_qty.is_zero() {
                self.positions.remove(&pd.instrument);
            } else {
                self.positions.insert(pd.instrument.clone(), pd);
            }
        }
    }

    /// Store the latest account delta (single account).
    pub fn apply_account_delta_batch(&self, account_delta: AccountDelta) {
        if account_delta.key == self.account_key {
            let mut guard = self.account.write().expect("poisoned account");
            *guard = account_delta;
        }
    }

    /// Record closed trades (already filtered to our account).
    pub fn apply_closed_trades(&self, trades: Vec<Trade>) {
        let mut ct = self.closed_trades.write().expect("poisoned closed_trades");
        ct.extend(trades);
    }

    //todo there is no point doing this on every incoming data point, we can just calculate if open_pnl is called.
    /// Update mark for an instrument and recompute that instrument's open_pnl.
    pub fn update_apply_last_price(&self, instrument: &Instrument, price: Decimal, _now: DateTime<Utc>) {
        self.last_price.insert(instrument.clone(), price);

        if let Some(mut pd_ref) = self.positions.get_mut(instrument) {
            let pd = pd_ref.value_mut();
            if let Some(new_open) = self.compute_open_pnl(instrument, pd.average_price, pd.net_qty, pd.side) {
                pd.open_pnl = new_open;
            }
            pd.side = if pd.net_qty.is_zero() {
                PositionSide::Flat
            } else if pd.net_qty > Decimal::ZERO {
                PositionSide::Long
            } else {
                PositionSide::Short
            };
        }
    }

    // ===== Helpers =====

    /// Compute open PnL using last mark, tick_size and value_per_tick for the single provider.
    fn compute_open_pnl(
        &self,
        instr: &Instrument,
        avg_px: Decimal,
        qty: Decimal,
        side: PositionSide,
    ) -> Option<Decimal> {
        if let Some(contract_map) = CONTRACTS.get(&self.provider()) {
            if let Some(contract) = contract_map.get(instr)
                && let Some(last_price) = self.last_price.get(instr)
            {
                let tick_size = contract.tick_size;
                let value_per_tick = contract.value_per_tick;
                let lp = last_price.value().clone();
                return match side {
                    PositionSide::Long  => Some(((lp - avg_px) / tick_size) * value_per_tick * qty),
                    PositionSide::Short => Some(((avg_px - lp) / tick_size) * value_per_tick * qty),
                    PositionSide::Flat  => Some(dec!(0)),
                };
            }
        }
        None
    }

    pub fn has_open_positions(&self) -> bool {
        !self.positions.is_empty()
    }

    // ===== Queries (single account) =====

    pub fn position(&self, instrument: &Instrument) -> Option<PositionDelta> {
        self.positions.get(instrument).map(|r| r.value().clone())
    }

    pub fn net_qty(&self, instrument: &Instrument) -> Decimal {
        self.position(instrument).map(|p| p.net_qty).unwrap_or(Decimal::ZERO)
    }

    pub fn is_long(&self, instrument: &Instrument) -> bool  { self.net_qty(instrument) >  Decimal::ZERO }
    pub fn is_short(&self, instrument: &Instrument) -> bool { self.net_qty(instrument) <  Decimal::ZERO }
    pub fn is_flat(&self, instrument: &Instrument) -> bool  { self.net_qty(instrument) == Decimal::ZERO }

    pub fn account_delta(&self) -> AccountDelta {
        self.account.read().expect("poisoned account").clone()
    }

    pub fn can_trade(&self) -> bool {
        self.account.read().expect("poisoned account").can_trade
    }

    pub fn equity(&self) -> Decimal {
        self.account.read().expect("poisoned account").equity
    }

    pub fn open_order_count(&self) -> usize { self.open_orders.len() }

    pub fn open_orders_for_instrument(&self, instrument: &Instrument) -> Vec<OrderUpdate> {
        self.open_orders
            .iter()
            .filter(|kv| &kv.key().instrument == instrument)
            .map(|kv| kv.value().clone())
            .collect()
    }

    /// Build a snapshot of all known positions with fresh open_pnl and snapshot time.
    pub fn positions_snapshot(&self, now: DateTime<Utc>) -> PositionsBatch {
        let mut out = Vec::with_capacity(self.positions.len());
        for p in self.positions.iter() {
            let mut pd = p.value().clone();

            if let Some(new_open) = self.compute_open_pnl(&pd.instrument, pd.average_price, pd.net_qty, pd.side) {
                pd.open_pnl = new_open;
            }
            pd.side = if pd.net_qty.is_zero() {
                PositionSide::Flat
            } else if pd.net_qty > Decimal::ZERO {
                PositionSide::Long
            } else {
                PositionSide::Short
            };
            pd.provider_kind = self.provider();
            pd.account_name = self.account_key.account_name.clone();
            pd.time = now;
            out.push(pd);
        }
        PositionsBatch { topic: tt_types::keys::Topic::Positions, seq: 0, positions: out }
    }

    /// Build a snapshot batch with a single AccountDelta (this account), recomputing open/day PnL.
    pub fn accounts_snapshot(&self, now: DateTime<Utc>) -> AccountDeltaBatch {
        let base = self.account_delta();
        let equity = base.equity;
        let can_trade = base.can_trade;

        let open = self.account_open_pnl_sum();
        let day  = self.account_day_realized_pnl_utc(now);

        let delta = AccountDelta {
            provider_kind: self.provider(),
            name: self.account_key.account_name.clone(),
            key: self.account_key.clone(),
            equity,
            day_realized_pnl: day,
            open_pnl: open,
            time: now,
            can_trade,
        };

        AccountDeltaBatch { topic: tt_types::keys::Topic::AccountEvt, seq: 0, accounts: vec![delta] }
    }

    /// Adjust a PositionsBatch by recomputing open_pnl using last known marks (pure transform).
    pub fn adjust_positions_batch_open_pnl(&self, mut batch: PositionsBatch) -> PositionsBatch {
        for p in batch.positions.iter_mut() {
            if let Some(new_open) = self.compute_open_pnl(&p.instrument, p.average_price, p.net_qty, p.side) {
                p.open_pnl = new_open;
            }
        }
        batch
    }

    /// Sum of open PnL across all instruments for this account.
    pub fn account_open_pnl_sum(&self) -> Decimal {
        let mut sum = Decimal::ZERO;
        for p in self.positions.iter() {
            sum += p.value().open_pnl;
        }
        sum
    }

    /// Day realized PnL for this account based on closed trades with UTC date == `now` date.
    pub fn account_day_realized_pnl_utc(&self, now: DateTime<Utc>) -> Decimal {
        let day = now.date_naive();
        let ct = self.closed_trades.read().expect("poisoned closed_trades");
        let mut sum = Decimal::ZERO;
        for t in ct.iter() {
            if t.provider == self.provider()
                && t.account_name == self.account_key.account_name
                && !t.voided
                && t.creation_time.date_naive() == day
            {
                sum += t.profit_and_loss;
            }
        }
        sum
    }
}

