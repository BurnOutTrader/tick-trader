use crate::models::DataTopic;
use dashmap::DashMap;
use std::sync::{Arc, LazyLock};
use tt_types::consolidators::{Consolidator, ConsolidatorKey, HybridTickOrCandleToCandles};
use tt_types::data::models::Resolution;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::securities::hours::market_hours::MarketHours;

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

/// Register a hybrid consolidator that can accept either historical candles or live ticks.
/// This registers two entries sharing the same state: one under the specified candles topic
/// and another under Topic::Ticks.
#[inline]
pub fn add_hybrid_tick_or_candle(
    candle_source_topic: DataTopic,
    for_key: SymbolKey,
    dst: Resolution,
    out_symbol: String,
    hours: Option<Arc<MarketHours>>,
) {
    let cons = HybridTickOrCandleToCandles::new(dst, out_symbol, hours, for_key.instrument.clone());
    let cons2 = cons.clone();
    let tick_key = ConsolidatorKey::new(for_key.instrument.clone(), for_key.provider, Topic::Ticks);
    let candle_topic = candle_source_topic.to_topic_or_err().unwrap();
    let candle_key =
        ConsolidatorKey::new(for_key.instrument.clone(), for_key.provider, candle_topic);
    CONSOLIDATORS.insert(tick_key, Box::new(cons));
    CONSOLIDATORS.insert(candle_key, Box::new(cons2));
}

// === Removal ===
/// Remove a consolidator bring driven by the engine.
#[inline]
pub fn remove_consolidator(from_data_topic: DataTopic, for_key: SymbolKey) {
    let topic = from_data_topic.to_topic_or_err().unwrap();
    let key = ConsolidatorKey::new(for_key.instrument, for_key.provider, topic);
    CONSOLIDATORS.remove(&key);
}

/// Remove both entries of a previously registered hybrid consolidator (ticks and candles).
#[inline]
pub fn remove_hybrid_tick_or_candle(candle_source_topic: DataTopic, for_key: SymbolKey) {
    let tick_key = ConsolidatorKey::new(for_key.instrument.clone(), for_key.provider, Topic::Ticks);
    let candle_topic = candle_source_topic.to_topic_or_err().unwrap();
    let candle_key = ConsolidatorKey::new(for_key.instrument, for_key.provider, candle_topic);
    CONSOLIDATORS.remove(&tick_key);
    CONSOLIDATORS.remove(&candle_key);
}
