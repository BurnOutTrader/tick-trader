use crate::models::DataKind;
use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::time::Duration;
use tokio::time::sleep;

// === Small helpers ===
#[inline]
fn ts_ns(dt: &DateTime<Utc>) -> i128 {
    (dt.timestamp() as i128) * 1_000_000_000i128 + (dt.timestamp_subsec_nanos() as i128)
}

// ---------- Replay abstraction ----------

/// Everything a source needs to implement to participate in the global replay.
#[async_trait]
trait ReplaySource {
    /// Earliest *next* timestamp this source can emit (None when exhausted).
    fn peek_ts(&self) -> Option<DateTime<Utc>>;
    /// Emit everything at `peek_ts()` into the hub and advance.
    async fn step(&mut self, hub: &SubscriptionManager) -> Result<()>;
    #[allow(unused)]
    /// Human tag (for logs).
    fn name(&self) -> &str;
}

/// One symbol-kind replayer that holds data in memory and feeds the hub step-by-step.
struct SymbolKindReplayer {
    #[allow(unused)]
    tag: String, // e.g. "Rithmic MNQ Ticks"
    kind: DataKind,

    // Data buffered (already sorted) — we batch “all at same ts”
    ticks: VecDeque<Tick>,
    candles: VecDeque<Candle>,
    bbo: VecDeque<Bbo>,
    books: VecDeque<OrderBook>,
}

impl SymbolKindReplayer {
    pub fn new_ticks(tag: String, mut rows: Vec<Tick>) -> Self {
        rows.sort_by_key(|t| ts_ns(&t.time));
        Self {
            tag,
            kind: DataKind::Tick,
            ticks: rows.into(),
            candles: [].into(),
            bbo: [].into(),
            books: [].into(),
        }
    }

    /// IMPORTANT: we order candles by **end** time (your chosen convention).
    pub fn new_candles(tag: String, mut rows: Vec<Candle>) -> Self {
        rows.sort_by_key(|c| ts_ns(&c.time_end));
        Self {
            tag,
            kind: DataKind::Candle,
            ticks: [].into(),
            candles: rows.into(),
            bbo: [].into(),
            books: [].into(),
        }
    }

    pub fn new_bbo(tag: String, mut rows: Vec<Bbo>) -> Self {
        rows.sort_by_key(|q| ts_ns(&q.time));
        Self {
            tag,
            kind: DataKind::Bbo,
            ticks: [].into(),
            candles: [].into(),
            bbo: rows.into(),
            books: [].into(),
        }
    }

    pub fn new_books(tag: String, mut rows: Vec<OrderBook>) -> Self {
        rows.sort_by_key(|b| ts_ns(&b.time));
        Self {
            tag,
            kind: DataKind::Depth,
            ticks: [].into(),
            candles: [].into(),
            bbo: [].into(),
            books: rows.into(),
        }
    }

    fn drain_eq_ts<T, G>(q: &mut VecDeque<T>, ts: DateTime<Utc>, get_ts: G, out: &mut Vec<T>)
    where
        G: Fn(&T) -> DateTime<Utc>,
    {
        while let Some(front) = q.front() {
            if get_ts(front) == ts {
                out.push(q.pop_front().unwrap());
            } else {
                break;
            }
        }
    }
}

#[async_trait]
impl ReplaySource for SymbolKindReplayer {
    fn peek_ts(&self) -> Option<DateTime<Utc>> {
        match self.kind {
            DataKind::Tick => self.ticks.front().map(|t| t.time),
            DataKind::Candle => self.candles.front().map(|c| c.time_end),
            DataKind::Depth => self.books.front().map(|b| b.time),
            DataKind::Bbo => self.bbo.front().map(|q| q.time),
        }
    }

    async fn step(&mut self, hub: &SubscriptionManager) -> Result<()> {
        let Some(next_ts) = self.peek_ts() else {
            return Ok(());
        };

        match self.kind {
            DataKind::Tick => {
                let mut batch = Vec::with_capacity(16);
                Self::drain_eq_ts(&mut self.ticks, next_ts, |t| t.time, &mut batch);
                for v in batch {
                    hub.broadcast_ticks(v);
                } // single-event publish (your hub)
            }
            DataKind::Candle => {
                let mut batch = Vec::with_capacity(16);
                Self::drain_eq_ts(&mut self.candles, next_ts, |c| c.time_end, &mut batch);
                for v in batch {
                    hub.broadcast_candles(v);
                }
            }
            DataKind::Depth => {
                let mut batch = Vec::with_capacity(4);
                Self::drain_eq_ts(&mut self.books, next_ts, |b| b.time, &mut batch);
                for v in batch {
                    hub.broadcast_book(v);
                }
            }
            DataKind::Bbo => {
                let mut batch = Vec::with_capacity(16);
                Self::drain_eq_ts(&mut self.bbo, next_ts, |q| q.time, &mut batch);
                for v in batch {
                    hub.broadcast_bbo(v);
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        &self.tag
    }
}

// ---------- Global coordinator (multi-source) ----------

/// Min-heap entry (inverted via Ord for BinaryHeap).
#[derive(Eq)]
struct QItem {
    ts: DateTime<Utc>,
    idx: usize, // index into sources vec
}
impl PartialEq for QItem {
    fn eq(&self, o: &Self) -> bool {
        self.ts == o.ts && self.idx == o.idx
    }
}
impl Ord for QItem {
    fn cmp(&self, o: &Self) -> Ordering {
        // reverse for min-heap behavior via BinaryHeap
        o.ts.cmp(&self.ts).then_with(|| o.idx.cmp(&self.idx))
    }
}
impl PartialOrd for QItem {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}

/// Coordinator: globally time-ordered replay across many sources.
/// Every step, it picks the minimum next timestamp across sources and
/// emits *all* events at that instant for those sources, keeping streams synchronized.
#[allow(unused)]
pub struct ReplayCoordinator {
    sources: Vec<Box<dyn ReplaySource + Send>>,
}

#[allow(unused)]
impl ReplayCoordinator {
    pub fn new() -> Self {
        Self {
            sources: Vec::new(),
        }
    }

    pub fn add_ticks(&mut self, tag: impl Into<String>, rows: Vec<Tick>) {
        self.sources
            .push(Box::new(SymbolKindReplayer::new_ticks(tag.into(), rows)));
    }
    pub fn add_candles(&mut self, tag: impl Into<String>, rows: Vec<Candle>) {
        self.sources
            .push(Box::new(SymbolKindReplayer::new_candles(tag.into(), rows)));
    }
    pub fn add_bbo(&mut self, tag: impl Into<String>, rows: Vec<Bbo>) {
        self.sources
            .push(Box::new(SymbolKindReplayer::new_bbo(tag.into(), rows)));
    }
    pub fn add_books(&mut self, tag: impl Into<String>, rows: Vec<OrderBook>) {
        self.sources
            .push(Box::new(SymbolKindReplayer::new_books(tag.into(), rows)));
    }

    /// Run until all sources exhaust. Optional `pace` for throttling playback.
    pub async fn run(mut self, hub: SubscriptionManager, pace: Option<Duration>) -> Result<()> {
        let mut heap: BinaryHeap<QItem> = BinaryHeap::new();

        // seed heap
        for (i, s) in self.sources.iter().enumerate() {
            if let Some(ts) = s.peek_ts() {
                heap.push(QItem { ts, idx: i });
            }
        }

        while let Some(head) = heap.pop() {
            let target_ts = head.ts;

            // gather all sources whose next ts == target_ts
            let mut to_step = vec![head.idx];
            while let Some(peek) = heap.peek() {
                if peek.ts == target_ts {
                    to_step.push(heap.pop().unwrap().idx);
                } else {
                    break;
                }
            }

            // step them; they will emit all events at this timestamp
            for idx in &to_step {
                self.sources[*idx].step(&hub).await?;
            }

            // re-seed
            for idx in to_step {
                if let Some(ts) = self.sources[idx].peek_ts() {
                    heap.push(QItem { ts, idx });
                }
            }

            if let Some(d) = pace {
                sleep(d).await;
            }
        }

        Ok(())
    }
}
