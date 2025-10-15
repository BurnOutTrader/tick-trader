use crate::models::DataTopic;
use dashmap::DashMap;
use std::sync::LazyLock;
use tt_types::consolidators::{Consolidator, ConsolidatorKey};
use tt_types::keys::SymbolKey;

pub(crate) static CONSOLIDATORS: LazyLock<DashMap<ConsolidatorKey, Box<dyn Consolidator>>> =
    LazyLock::new(DashMap::new);

// === REGISTRATION ===
/// Register a consolidator to be driven by the engine for the given data stream key.
/// The consolidator will be invoked whenever data for (topic,key) arrives.
/// The data will be sent to the strategies on_bar(&mut self, candle: Candle) function.
#[inline]
pub fn add_consolidator(
    from_data_topic: DataTopic,
    for_key: SymbolKey,
    cons: Box<dyn tt_types::consolidators::Consolidator + Send>,
) {
    let topic = from_data_topic.to_topic_or_err().unwrap();
    let key = ConsolidatorKey::new(for_key.instrument, for_key.provider, topic);
    CONSOLIDATORS.insert(key, cons);
}

// === Removal ===
/// Remove a consolidator bring driven by the engine.
#[inline]
pub fn remove_consolidator(from_data_topic: DataTopic, for_key: SymbolKey) {
    let topic = from_data_topic.to_topic_or_err().unwrap();
    let key = ConsolidatorKey::new(for_key.instrument, for_key.provider, topic);
    CONSOLIDATORS.remove(&key);
}
