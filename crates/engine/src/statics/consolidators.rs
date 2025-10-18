use crate::models::DataTopic;
use chrono::TimeZone;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use std::sync::{Arc, LazyLock};
use tt_types::consolidators::{Consolidator, ConsolidatorKey, HybridTickOrCandleToCandles};
use tt_types::data::core::{Bbo, Candle, Tick};
use tt_types::data::models::Resolution;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::hours::market_hours::MarketHours;

//todo[consolidators] consolidators need to align times with the resolution of the input data, so that bars have an closer expected close time.
pub(crate) static CONSOLIDATORS: LazyLock<DashMap<ConsolidatorKey, Box<dyn Consolidator>>> =
    LazyLock::new(DashMap::new);
//todo
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
    hours: Option<Arc<MarketHours>>,
) {
    let cons = HybridTickOrCandleToCandles::new(dst, hours, for_key.instrument.clone());
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

// === Event-driven driver API ===
use tt_types::consolidators::ConsolidatedOut;

/// Normalize vendor-provided candle times to exact resolution boundaries.
/// For time-based resolutions, floors time_start to the boundary and sets time_end
/// to start + duration (exclusive end).
#[inline]
pub fn normalize_candle_window(mut b: Candle) -> Candle {
    use tt_types::data::models::Resolution;
    let dur = match b.resolution {
        Resolution::Seconds(n) => Some(chrono::Duration::seconds(n as i64)),
        Resolution::Minutes(n) => Some(chrono::Duration::minutes(n as i64)),
        Resolution::Hours(n) => Some(chrono::Duration::hours(n as i64)),
        _ => None,
    };
    if let Some(d) = dur {
        let ts = b.time_start.timestamp();
        let step = d.num_seconds();
        if step > 0 {
            let floored = ts - ts.rem_euclid(step);
            let start = chrono::Utc.timestamp_opt(floored, 0).unwrap();
            let end = start + d;
            b.time_start = start;
            b.time_end = end;
        }
    }
    b
}

/// Drive all registered consolidators with a time tick.
/// Returns any candles produced, paired with their provider.
#[inline]
pub fn drive_time(now: DateTime<Utc>) -> Vec<(ProviderKind, Candle)> {
    if CONSOLIDATORS.is_empty() {
        return Vec::new();
    }
    let mut outs: Vec<(ProviderKind, Candle)> = Vec::new();
    for mut entry in CONSOLIDATORS.iter_mut() {
        let provider = entry.key().provider;
        if let Some(ConsolidatedOut::Candle(c)) = entry.value_mut().on_time(now) {
            outs.push((provider, c));
        }
    }
    outs
}

/// Drive the tick consolidator for the given instrument/provider if present.
#[inline]
pub fn drive_tick(t: &Tick, provider: ProviderKind) -> Option<(ProviderKind, Candle)> {
    let key = ConsolidatorKey::new(t.instrument.clone(), provider, Topic::Ticks);
    if let Some(mut cons) = CONSOLIDATORS.get_mut(&key)
        && let Some(ConsolidatedOut::Candle(c)) = cons.on_tick(t)
    {
        return Some((provider, c));
    }
    None
}

/// Drive the BBO consolidator for the given instrument/provider if present.
#[inline]
pub fn drive_bbo(b: &Bbo, provider: ProviderKind) -> Option<(ProviderKind, Candle)> {
    let key = ConsolidatorKey::new(b.instrument.clone(), provider, Topic::Quotes);
    if let Some(mut cons) = CONSOLIDATORS.get_mut(&key)
        && let Some(ConsolidatedOut::Candle(c)) = cons.on_bbo(b)
    {
        return Some((provider, c));
    }
    None
}

/// Drive candle-to-candle consolidators using the incoming topic.
#[inline]
pub fn drive_candle(
    topic: Topic,
    b: &Candle,
    provider: ProviderKind,
) -> Option<(ProviderKind, Candle)> {
    let key = ConsolidatorKey::new(b.instrument.clone(), provider, topic);
    if let Some(mut cons) = CONSOLIDATORS.get_mut(&key)
        && let Some(ConsolidatedOut::Candle(c)) = cons.on_candle(b)
    {
        return Some((provider, c));
    }
    None
}
