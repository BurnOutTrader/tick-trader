use std::sync::LazyLock;
use crossbeam::queue::ArrayQueue;
use tt_types::keys::SymbolKey;
use crate::models::{Command, DataTopic};

pub(crate) static CMD_Q: LazyLock<ArrayQueue<Command>> = LazyLock::new(|| ArrayQueue::new(4096));

// === FIRE-AND-FORGET ===
/// Enqueue a subscribe request to be handled by the engine task.
///
/// Parameters:
/// - topic: Logical data stream to subscribe to (e.g., Ticks, Quotes, MBP10, Candles).
/// - key: SymbolKey including instrument and provider.
#[inline]
pub fn subscribe(topic: DataTopic, key: SymbolKey) {
    let _ = CMD_Q.push(Command::Subscribe {
        topic: topic.to_topic_or_err().unwrap(),
        key,
    });
}

/// Enqueue an unsubscribe request for a previously subscribed stream.
///
/// Parameters:
/// - topic: Logical data stream to unsubscribe from.
/// - key: SymbolKey including instrument and provider.
#[inline]
pub fn unsubscribe(topic: DataTopic, key: SymbolKey) {
    let _ = CMD_Q.push(Command::Unsubscribe {
        topic: topic.to_topic_or_err().unwrap(),
        key,
    });
}