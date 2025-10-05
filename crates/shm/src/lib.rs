use tt_types::keys::{SymbolKey, Topic};

/// Minimal SHM helper scaffold.
/// In a future pass this will create and manage OS shared memory segments with seqlock snapshots.
/// For now we provide naming helpers and default sizes so providers can announce SHM streams.
pub const DEFAULT_QUOTE_SNAPSHOT_SIZE: u64 = 64 * 1024; // 64 KiB placeholder
pub const DEFAULT_DEPTH_SNAPSHOT_SIZE: u64 = 256 * 1024; // 256 KiB placeholder

pub fn suggest_name(topic: Topic, key: &SymbolKey) -> String {
    // Keep names filesystem/OS friendly; no spaces
    // Example: ttshm.Depth-PROJX-MNQZ25
    let inst = key.instrument.to_string();
    let prov = format!("{:?}", key.provider);
    format!("ttshm.{:?}.{}.{}", topic, prov, inst)
}
