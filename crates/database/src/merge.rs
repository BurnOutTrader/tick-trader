use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use tt_types::base_data::{Bbo, Candle, OrderBook, Tick};

/// Iterator wrapper around `KMerge` so you can hand it to for-loops easily.
pub struct KMergeIter<'a, T: TimeKey> {
    inner: KMerge<'a, T>,
}

impl<'a, T: TimeKey> KMergeIter<'a, T> {
    #[inline]
    pub fn new(slices: Vec<&'a [T]>) -> Self {
        Self {
            inner: KMerge::new(slices),
        }
    }
}

impl<'a, T: TimeKey> Iterator for KMergeIter<'a, T> {
    type Item = &'a T;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

/// Ordering keys for time-series merge.
///
/// - `time_ns()` must be monotonic enough for ordering (nanoseconds since epoch).
/// - `tie_key()` provides a stable tiebreaker when multiple events share the same time.
/// - `time_key()` is the primary key used in the heap (default: `(time_ns, tie_seq)`).
pub trait TimeKey {
    fn time_ns(&self) -> i128;

    /// (sequence, exec_id, maker_id, taker_id) by default; override as needed.
    fn tie_key(&self) -> u32 {
        u32::MAX
    }

    #[inline]
    fn time_key(&self) -> (i128, u32) {
        let seq = self.tie_key();
        (self.time_ns(), seq)
    }
}

/// Make references usable in the heap: `&T` inherits `TimeKey` from `T`.
impl<'a, T: TimeKey + ?Sized> TimeKey for &'a T {
    #[inline]
    fn time_ns(&self) -> i128 {
        (**self).time_ns()
    }
    #[inline]
    fn tie_key(&self) -> u32 {
        (**self).tie_key()
    }
    #[inline]
    fn time_key(&self) -> (i128, u32) {
        (**self).time_key()
    }
}

// ---------- Concrete TimeKey impls for your types ----------

impl TimeKey for Tick {
    #[inline]
    fn time_ns(&self) -> i128 {
        // unwrap(): assume your ticks always carry a timestamp
        self.time.timestamp_nanos_opt().unwrap() as i128
    }
    #[inline]
    fn tie_key(&self) -> u32 {
        self.venue_seq.unwrap_or(u32::MAX)
    }
}

impl TimeKey for Candle {
    #[inline]
    fn time_ns(&self) -> i128 {
        // As requested: candles key by time_end (close of the bar).
        self.time_end.timestamp_nanos_opt().unwrap() as i128
    }
}

impl TimeKey for Bbo {
    #[inline]
    fn time_ns(&self) -> i128 {
        self.time.timestamp_nanos_opt().unwrap() as i128
    }
}

impl TimeKey for OrderBook {
    #[inline]
    fn time_ns(&self) -> i128 {
        self.time.timestamp_nanos_opt().unwrap() as i128
    }
}

// ---------- Merge machinery ----------

#[derive(Clone, Copy)]
struct MergeNode<T: TimeKey> {
    src: usize, // which slice
    idx: usize, // index within that slice
    item: T,    // e.g. &T in our heap
}

impl<T: TimeKey> Eq for MergeNode<T> {}

impl<T: TimeKey> PartialEq for MergeNode<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.item.time_key() == other.item.time_key() && self.item.tie_key() == other.item.tie_key()
    }
}

impl<T: TimeKey> PartialOrd for MergeNode<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: TimeKey> Ord for MergeNode<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        let (ta, sa) = self.item.time_key();
        let (tb, sb) = other.item.time_key();
        match (ta, sa).cmp(&(tb, sb)) {
            Ordering::Equal => self.item.tie_key().cmp(&other.item.tie_key()),
            o => o,
        }
    }
}

/// K-way merge over multiple already-sorted slices.
/// Pops the globally next event each time you call `next()`.
pub struct KMerge<'a, T: TimeKey> {
    sources: Vec<&'a [T]>,
    heap: BinaryHeap<Reverse<MergeNode<&'a T>>>, // min-heap via Reverse
}

impl<'a, T: TimeKey> KMerge<'a, T> {
    pub fn new(sorted_slices: Vec<&'a [T]>) -> Self {
        let mut heap = BinaryHeap::new();
        for (i, s) in sorted_slices.iter().enumerate() {
            if let Some(first) = s.first() {
                heap.push(Reverse(MergeNode {
                    src: i,
                    idx: 0,
                    item: first,
                }));
            }
        }
        Self {
            sources: sorted_slices,
            heap,
        }
    }

    /// Returns the next item in global time order (ascending).
    pub fn next(&mut self) -> Option<&'a T> {
        let Reverse(MergeNode { src, idx, item }) = self.heap.pop()?;
        let next_idx = idx + 1;
        if let Some(next_item) = self.sources[src].get(next_idx) {
            self.heap.push(Reverse(MergeNode {
                src,
                idx: next_idx,
                item: next_item,
            }));
        }
        Some(item)
    }

    /// Convenience: turn into an iterator.
    #[inline]
    #[allow(unused)]
    pub fn into_iter(self) -> KMergeIter<'a, T> {
        KMergeIter { inner: self }
    }
}

// ---------- Helpers for ergonomic use ----------

/// Merge an arbitrary set of sorted slices as an iterator.
/// Usage:
/// ```ignore
/// let it = kmerge(vec![ticks_a.as_slice(), ticks_b.as_slice()]);
/// for t in it { /* ... */ }
/// ```
#[allow(dead_code)]
#[inline]
pub fn kmerge<'a, T: TimeKey>(slices: Vec<&'a [T]>) -> KMergeIter<'a, T> {
    KMergeIter::new(slices)
}
