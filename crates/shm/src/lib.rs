use dashmap::DashMap;
use memmap2::{Mmap, MmapMut};
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tt_types::keys::{SymbolKey, Topic};

/// Minimal SHM helper providing file-backed shared memory segments and a simple seqlock writer.
/// This is a pragmatic implementation that works on all platforms by using a file in /dev/shm
/// (if available) or /tmp as the shared backing object. Readers can mmap the same path read-only.
/// Layout (little-endian):
///   [u32 magic][u32 version][u32 capacity][u32 seq][u32 len][payload ... up to capacity]
/// Writer increments seq to odd, writes length+payload, then increments to even.
///
/// Notes on correctness:
/// - Use native usize sizes for buffer-capacity math.
/// - Insert memory fences (Release/Acquire) around header/payload writes to form a proper seqlock.
pub const DEFAULT_TICK_SNAPSHOT_SIZE: usize = 64 * 1024; // 64 KiB placeholder
pub const DEFAULT_QUOTE_SNAPSHOT_SIZE: usize = 64 * 1024; // 64 KiB placeholder
pub const DEFAULT_DEPTH_SNAPSHOT_SIZE: usize = 256 * 1024; // 256 KiB placeholder

const HEADER_MAGIC: u32 = u32::from_le_bytes(*b"TSHM");
const HEADER_VERSION: u32 = 1;
#[inline]
const fn header_len() -> usize {
    20
} // 5 * u32

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
    pub fn new(name: &str, size: usize) -> std::io::Result<Self> {
        let path = backing_path(name);
        let parent = path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("/tmp"));
        std::fs::create_dir_all(parent).ok();
        let file_exists = path.exists();
        #[cfg(unix)]
        use std::os::unix::fs::OpenOptionsExt;
        let mut oo = OpenOptions::new();
        oo.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            oo.mode(0o600);
        }
        let mut file: File = oo.open(&path)?;
        if !file_exists {
            file.set_len(size as u64)?;
            // initialize header
            file.seek(SeekFrom::Start(0))?;
            let mut hdr = Vec::with_capacity(header_len());
            hdr.extend_from_slice(&HEADER_MAGIC.to_le_bytes());
            hdr.extend_from_slice(&HEADER_VERSION.to_le_bytes());
            hdr.extend_from_slice(&(size as u32).to_le_bytes());
            hdr.extend_from_slice(&0u32.to_le_bytes()); // seq
            hdr.extend_from_slice(&0u32.to_le_bytes()); // len
            file.write_all(&hdr)?;
        }
        let mmap = unsafe { MmapMut::map_mut(&file)? };
        Ok(Self { mmap })
    }

    /// Write a payload using seqlock protocol into the mmap.
    pub fn write_bytes(&mut self, bytes: &[u8]) {
        use std::sync::atomic::{Ordering, fence};
        // Capacity from header (offset 8..12)
        if self.mmap.len() < header_len() {
            return;
        }
        let cap = u32::from_le_bytes(self.mmap[8..12].try_into().unwrap_or([0, 0, 0, 0])) as usize;
        if bytes.len() > cap {
            return;
        }
        // read current seq at offset 12..16
        let seq0 = u32::from_le_bytes(self.mmap[12..16].try_into().unwrap_or([0, 0, 0, 0]));
        // next odd sequence (writer-enter)
        let mut seq = if seq0 % 2 == 0 { seq0 + 1 } else { seq0 + 2 };
        // set odd seq (publish writer-enter)
        self.mmap[12..16].copy_from_slice(&seq.to_le_bytes());
        fence(Ordering::Release); // ensure subsequent writes are not reordered before the odd seq
        // write len at 16..20
        let len_u32 = bytes.len() as u32;
        self.mmap[16..20].copy_from_slice(&len_u32.to_le_bytes());
        // write payload starting at header_len()
        let start = header_len();
        let end = start + bytes.len();
        self.mmap[start..end].copy_from_slice(bytes);
        fence(Ordering::Release); // make payload visible before flipping to even
        // bump to even (writer-exit)
        seq = seq.wrapping_add(1);
        self.mmap[12..16].copy_from_slice(&seq.to_le_bytes());
        // best-effort flush (not strictly required for readers in same host)
        let _ = self.mmap.flush_async();
        // Acquire fence optional for writers; readers should use Acquire around reads.
        let _ = fence(Ordering::Acquire);
    }
}

// Global writers registry to reuse mappings
static WRITERS: once_cell::sync::Lazy<
    DashMap<(Topic, SymbolKey), Arc<std::sync::Mutex<ShmWriter>>>,
> = once_cell::sync::Lazy::new(|| DashMap::new());

pub fn ensure_writer(
    topic: Topic,
    key: &SymbolKey,
    size: usize,
) -> Arc<std::sync::Mutex<ShmWriter>> {
    if let Some(w) = WRITERS.get(&(topic, key.clone())) {
        return w.value().clone();
    }
    let name = suggest_name(topic, key);
    let writer = ShmWriter::new(&name, size).expect("create shm writer");
    let arc = Arc::new(std::sync::Mutex::new(writer));
    WRITERS.insert((topic, key.clone()), arc.clone());
    arc
}

pub fn write_snapshot(topic: Topic, key: &SymbolKey, bytes: &[u8]) {
    let size = match topic {
        Topic::MBP10 => DEFAULT_DEPTH_SNAPSHOT_SIZE,
        Topic::Quotes => DEFAULT_QUOTE_SNAPSHOT_SIZE,
        _ => DEFAULT_TICK_SNAPSHOT_SIZE,
    };
    let w = ensure_writer(topic, key, size);
    if let Ok(mut guard) = w.lock() {
        guard.write_bytes(bytes);
    }
}

/// Read-only SHM reader with seqlock semantics.
pub struct ShmReader {
    mmap: Mmap,
}

impl ShmReader {
    pub fn open(name: &str) -> std::io::Result<Self> {
        let path = backing_path(name);
        let file = OpenOptions::new().read(true).open(&path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self { mmap })
    }

    /// Attempt a single seqlock read. Returns (seq, payload) on success.
    pub fn read_with_seq(&self) -> Option<(u32, Vec<u8>)> {
        use std::sync::atomic::{fence, Ordering};
        if self.mmap.len() < header_len() {
            return None;
        }
        // Loop a few times to avoid transient writer windows
        for _ in 0..3 {
            let seq1 = u32::from_le_bytes(self.mmap[12..16].try_into().ok()?);
            if seq1 % 2 == 1 {
                // writer active
                continue;
            }
            let len = u32::from_le_bytes(self.mmap[16..20].try_into().ok()?) as usize;
            let cap = u32::from_le_bytes(self.mmap[8..12].try_into().ok()?) as usize;
            if len == 0 || len > cap || header_len() + len > self.mmap.len() {
                return None;
            }
            let start = header_len();
            let end = start + len;
            let mut buf = vec![0u8; len];
            buf.copy_from_slice(&self.mmap[start..end]);
            fence(Ordering::Acquire);
            let seq2 = u32::from_le_bytes(self.mmap[12..16].try_into().ok()?);
            if seq1 == seq2 && seq2 % 2 == 0 {
                return Some((seq2, buf));
            }
        }
        None
    }

    pub fn read_bytes(&self) -> Option<Vec<u8>> {
        self.read_with_seq().map(|(_, b)| b)
    }
}

// Global readers registry
static READERS: once_cell::sync::Lazy<
    DashMap<(Topic, SymbolKey), Arc<ShmReader>>,
> = once_cell::sync::Lazy::new(|| DashMap::new());

pub fn ensure_reader(topic: Topic, key: &SymbolKey) -> Option<Arc<ShmReader>> {
    if let Some(r) = READERS.get(&(topic, key.clone())) {
        return Some(r.value().clone());
    }
    let name = suggest_name(topic, key);
    match ShmReader::open(&name) {
        Ok(reader) => {
            let arc = Arc::new(reader);
            READERS.insert((topic, key.clone()), arc.clone());
            Some(arc)
        }
        Err(_) => None,
    }
}

/// Try a one-shot read of a snapshot for (topic,key); returns payload bytes if available.
pub fn read_snapshot(topic: Topic, key: &SymbolKey) -> Option<Vec<u8>> {
    ensure_reader(topic, key).and_then(|r| r.read_bytes())
}

/// Remove a snapshot segment for a given (topic,key) and evict it from the registry.
pub fn remove_snapshot(topic: Topic, key: &SymbolKey) {
    if let Some((_k, _v)) = WRITERS.remove(&(topic, key.clone())) {
        // Best-effort unlink of the backing file
        let name = suggest_name(topic, key);
        let path = backing_path(&name);
        let _ = std::fs::remove_file(path);
    }
    let _ = READERS.remove(&(topic, key.clone()));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writer_initializes_header_and_writes() {
        // Use a unique temp name to avoid collisions
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let name = format!("ttshm.test.{}", ts);
        let size = 4096usize;
        let mut w = ShmWriter::new(&name, size).expect("create shm");
        // Header should be present
        assert!(w.mmap.len() >= 20);
        let magic = u32::from_le_bytes(w.mmap[0..4].try_into().unwrap());
        let ver = u32::from_le_bytes(w.mmap[4..8].try_into().unwrap());
        let cap = u32::from_le_bytes(w.mmap[8..12].try_into().unwrap());
        let seq = u32::from_le_bytes(w.mmap[12..16].try_into().unwrap());
        let len = u32::from_le_bytes(w.mmap[16..20].try_into().unwrap());
        assert_eq!(magic, u32::from_le_bytes(*b"TSHM"));
        assert_eq!(ver, 1);
        assert_eq!(cap as usize, size);
        assert_eq!(seq % 2, 0); // even when idle
        assert_eq!(len, 0);
        // Write payload and verify header changes
        let payload = vec![1u8; 32];
        w.write_bytes(&payload);
        let seq_after = u32::from_le_bytes(w.mmap[12..16].try_into().unwrap());
        let len_after = u32::from_le_bytes(w.mmap[16..20].try_into().unwrap());
        assert_eq!(seq_after % 2, 0); // even after write
        assert_eq!(len_after as usize, payload.len());
    }
}
