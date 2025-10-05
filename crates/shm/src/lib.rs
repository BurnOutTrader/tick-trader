use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use dashmap::DashMap;
use memmap2::{MmapMut};
use tt_types::keys::{SymbolKey, Topic};

/// Minimal SHM helper providing file-backed shared memory segments and a simple seqlock writer.
/// This is a pragmatic implementation that works on all platforms by using a file in /dev/shm
/// (if available) or /tmp as the shared backing object. Readers can mmap the same path read-only.
/// Layout: [u32 seq][u32 len][payload... up to capacity]
/// Writer increments seq to odd, writes length+payload, then increments to even.
pub const DEFAULT_TICK_SNAPSHOT_SIZE: u64 = 64 * 1024; // 64 KiB placeholder
pub const DEFAULT_QUOTE_SNAPSHOT_SIZE: u64 = 64 * 1024; // 64 KiB placeholder
pub const DEFAULT_DEPTH_SNAPSHOT_SIZE: u64 = 256 * 1024; // 256 KiB placeholder

pub fn suggest_name(topic: Topic, key: &SymbolKey) -> String {
    // Keep names filesystem/OS friendly; no spaces
    // Example: ttshm.Depth-PROJX-MNQZ25
    let inst = key.instrument.to_string();
    let prov = format!("{:?}", key.provider);
    format!("ttshm.{:?}.{}.{}", topic, prov, inst)
}

fn backing_path(name: &str) -> PathBuf {
    let mut base = PathBuf::from("/dev/shm");
    if !base.exists() {
        base = PathBuf::from("/tmp");
    }
    base.push(name);
    base
}

pub struct ShmWriter {
    mmap: MmapMut,
}

impl ShmWriter {
    pub fn new(name: &str, size: u64) -> std::io::Result<Self> {
        let path = backing_path(name);
        let parent = path.parent().unwrap_or_else(|| std::path::Path::new("/tmp"));
        std::fs::create_dir_all(parent).ok();
        let file_exists = path.exists();
        let mut file: File = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;
        if !file_exists {
            file.set_len(size)?;
            // initialize header to zeros
            file.seek(SeekFrom::Start(0))?;
            file.write_all(&[0u8; 8])?; // seq=0,len=0
        }
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { mmap })
    }

    /// Write a payload using seqlock protocol into the mmap.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        if self.mmap.len() < bytes.len() + 8 { return; }
        // read current seq
        let seq0 = u32::from_le_bytes(self.mmap[0..4].try_into().unwrap_or([0,0,0,0]));
        let mut seq = if seq0 % 2 == 0 { seq0 + 1 } else { seq0 + 2 };
        // set odd seq
        self.mmap[0..4].copy_from_slice(&seq.to_le_bytes());
        // write len
        let len_u32 = bytes.len() as u32;
        self.mmap[4..8].copy_from_slice(&len_u32.to_le_bytes());
        // write payload
        let end = 8 + bytes.len();
        self.mmap[8..end].copy_from_slice(bytes);
        // bump to even
        seq += 1;
        self.mmap[0..4].copy_from_slice(&seq.to_le_bytes());
        // flush (best-effort)
        let _ = self.mmap.flush_async();
    }
}

// Global writers registry to reuse mappings
static WRITERS: once_cell::sync::Lazy<DashMap<(Topic, SymbolKey), Arc<std::sync::Mutex<ShmWriter>>>> = once_cell::sync::Lazy::new(|| DashMap::new());

pub fn ensure_writer(topic: Topic, key: &SymbolKey, size: u64) -> Arc<std::sync::Mutex<ShmWriter>> {
    if let Some(w) = WRITERS.get(&(topic, key.clone())) { return w.value().clone(); }
    let name = suggest_name(topic, key);
    let writer = ShmWriter::new(&name, size).expect("create shm writer");
    let arc = Arc::new(std::sync::Mutex::new(writer));
    WRITERS.insert((topic, key.clone()), arc.clone());
    arc
}

pub fn write_snapshot(topic: Topic, key: &SymbolKey, bytes: &[u8]) {
    let size = match topic { Topic::Depth => DEFAULT_DEPTH_SNAPSHOT_SIZE, Topic::Quotes => DEFAULT_QUOTE_SNAPSHOT_SIZE, _ => DEFAULT_TICK_SNAPSHOT_SIZE };
    let w = ensure_writer(topic, key, size);
    if let Ok(mut guard) = w.lock() { guard.write_bytes(bytes); }
}
