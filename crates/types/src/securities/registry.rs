use crate::securities::security::FuturesContract;
use crate::securities::symbols::{Exchange, Instrument};
use ahash::AHashMap;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

///The **security registry** is the canonical, in-memory catalog of
/// all [`FuturesContract`] instruments known to the engine at runtime.
///
/// ### Responsibilities
/// - Stores instruments, keyed by `(Exchange, SymbolId)`.
/// - Emits [`RegistryEvent`]s on add/update/remove, allowing universes,
///   strategies, or GUIs to react to symbol availability changes.
/// - Provides snapshot queries to fetch the current set of all known
///   instruments.
///
/// ### Concurrency
/// - Backed by a [`DashMap`] of exchanges → symbol maps for lock-free
///   concurrent access.
/// - Each symbol entry is reference-counted (`Arc<Security>`).
///
/// ### Usage
/// - Use [`upsert`] to insert or update a `Security`. This automatically
///   notifies subscribers with `RegistryEvent::Added` or
///   `RegistryEvent::Updated`.
/// - Use [`remove`] to delete an instrument; subscribers will receive
///   `RegistryEvent::Removed`.
/// - Use [`subscribe`] to receive a live feed of add/update/remove events.
/// - Use [`snapshot`] to fetch a point-in-time copy of all instruments
///   (no further updates will be pushed).
///
/// Typically, the registry is seeded at startup by providers/resolvers
/// (e.g. Rithmic, FIX gateways), and then maintained automatically as
/// the universe changes.
///
#[derive(Clone)]
pub enum RegistryEvent {
    /// A new [`FuturesContract`] was inserted into the registry.
    Added(Instrument),
    /// An existing [`FuturesContract`] was updated in place.
    Updated(Instrument),
    /// A [`FuturesContract`] was removed entirely from the registry.
    Removed(Instrument),
}

/// Central registry of all known [`FuturesContract`]s, organized per [`Exchange`].
pub struct SecurityRegistry {
    inner: DashMap<Exchange, AHashMap<Instrument, Arc<FuturesContract>>>,
    tx: broadcast::Sender<RegistryEvent>,
}

impl SecurityRegistry {
    /// Create a new, empty [`SecurityRegistry`].
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(256);
        Self {
            inner: DashMap::new(),
            tx,
        }
    }

    /// Look up a [`FuturesContract`] by `(SymbolId, Exchange)`.
    ///
    /// Returns `None` if the symbol is unknown.
    pub fn get(
        &self,
        instrument: &Instrument,
        exchange: &Exchange,
    ) -> Option<Arc<FuturesContract>> {
        self.inner
            .get(exchange)
            .and_then(|map| map.get(instrument).cloned())
    }

    /// Insert or update a [`FuturesContract`] in the registry.
    ///
    /// - If the symbol does not exist yet, emits `RegistryEvent::Added`.
    /// - If the symbol already exists, replaces it and emits
    ///   `RegistryEvent::Updated`.
    ///
    /// Returns an `Arc<Security>` handle to the stored entry.
    pub fn upsert(&self, sec: FuturesContract) -> Arc<FuturesContract> {
        let instrument = sec.instrument.clone();
        let exch = sec.exchange;
        let arc = Arc::new(sec);

        let mut entry = self.inner.entry(exch).or_default();

        let ev = if entry.contains_key(&instrument) {
            RegistryEvent::Updated(instrument.clone())
        } else {
            RegistryEvent::Added(instrument.clone())
        };

        entry.insert(instrument.clone(), arc.clone());
        let _ = self.tx.send(ev);
        arc
    }

    /// Remove a [`FuturesContract`] from the registry.
    ///
    /// Emits `RegistryEvent::Removed` if the symbol was present.
    ///
    /// Returns the removed entry (if any).
    pub fn remove(
        &self,
        instrument: &Instrument,
        exchange: &Exchange,
    ) -> Option<Arc<FuturesContract>> {
        let out = self
            .inner
            .get_mut(exchange)
            .and_then(|mut map| map.remove(instrument));
        if out.is_some() {
            let _ = self.tx.send(RegistryEvent::Removed(instrument.clone()));
        }
        out
    }

    /// Subscribe to a broadcast stream of [`RegistryEvent`]s.
    ///
    /// Each call returns a new receiver that will see all future events
    /// (but not past history).
    pub fn subscribe(&self) -> broadcast::Receiver<RegistryEvent> {
        self.tx.subscribe()
    }

    /// Returns a point-in-time snapshot of all [`FuturesContract`]s currently
    /// in the registry.
    ///
    /// This is a static view — it will not reflect future updates.
    pub fn snapshot(&self) -> Vec<Arc<FuturesContract>> {
        let mut all = Vec::new();
        for map in self.inner.iter() {
            all.extend(map.value().values().cloned());
        }
        all
    }
}

impl Default for SecurityRegistry {
    fn default() -> Self {
        Self::new()
    }
}
