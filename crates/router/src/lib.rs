use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{trace, warn};
use tt_types::keys::{Topic, SymbolKey};
use tt_types::wire::{Request, Response, WireMessage};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubId(u64);

#[derive(Debug, Clone)]
pub struct SubscriberMeta {
    pub id: SubId,
    // control-plane topics currently subscribed (coarse)
    pub topics: HashSet<Topic>,
    // per-topic credits (temporary until key-level protocol lands)
    pub credits: HashMap<Topic, u32>,
}

#[derive(Clone)]
pub struct Router {
    subscribers: Arc<DashMap<SubId, mpsc::Sender<Response>>>,
    meta: Arc<DashMap<SubId, SubscriberMeta>>,
    next_id: Arc<AtomicU64>,
    // sequences per topic
    seq_by_topic: Arc<DashMap<Topic, u64>>,

    // Sharded indices for key-based routing
    shards: usize,
    sub_index: Vec<DashMap<(Topic, SymbolKey), HashSet<SubId>>>,
    credits: Vec<DashMap<(SubId, Topic, SymbolKey), u32>>,
}

impl Router {
    pub fn new(shards: usize) -> Self {
        let shards = shards.max(1);
        let mut sub_index = Vec::with_capacity(shards);
        let mut credits = Vec::with_capacity(shards);
        for _ in 0..shards {
            sub_index.push(DashMap::new());
            credits.push(DashMap::new());
        }
        Self {
            subscribers: Arc::new(DashMap::new()),
            meta: Arc::new(DashMap::new()),
            next_id: Arc::new(AtomicU64::new(1)),
            seq_by_topic: Arc::new(DashMap::new()),
            shards,
            sub_index,
            credits,
        }
    }

    pub async fn attach_client(&self, sock: UnixStream) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<Response>(1024);
        let id = SubId(self.next_id.fetch_add(1, Ordering::SeqCst));
        self.subscribers.insert(id.clone(), tx);
        self.meta.insert(id.clone(), SubscriberMeta { id: id.clone(), topics: HashSet::new(), credits: HashMap::new() });

        let (read_half, write_half) = sock.into_split();
        let codec = LengthDelimitedCodec::builder().max_frame_length(8 * 1024 * 1024).new_codec();
        let mut framed_reader = FramedRead::new(read_half, codec.clone());
        let mut framed_writer = FramedWrite::new(write_half, codec);

        // writer task: drain per-subscriber queue
        let subs = self.subscribers.clone();
        let writer = tokio::spawn(async move {
            while let Some(resp) = rx.recv().await {
                let wire = WireMessage::Response(resp);
                let buf = tt_types::wire::codec::encode(&wire);
                if let Err(_e) = framed_writer.send(Bytes::from(buf)).await {
                    break;
                }
            }
            // on writer exit, nothing specific to clean here; reader will handle detach
            drop(subs);
        });

        // reader loop with timeout and heartbeat support
        let idle = Duration::from_secs(15);
        loop {
            let next = timeout(idle, framed_reader.next()).await;
            let maybe_item = match next { Ok(v) => v, Err(_) => { warn!("router: client idle timeout"); break; } };
            let Some(item) = maybe_item else { break };
            let Ok(bytes) = item else { break };
            match tt_types::wire::codec::decode(&bytes) {
                Ok(WireMessage::Request(req)) => {
                    self.handle_request(&id, req).await?;
                }
                Ok(_) => { /* ignore unknown directions for now */ }
                Err(_) => break,
            }
        }
        // Clean detach: unsubscribe-all and drop sender
        self.unsubscribe_all(&id).await;
        self.subscribers.remove(&id);
        let _ = writer.await;
        Ok(())
    }

    async fn handle_request(&self, id: &SubId, req: Request) -> Result<()> {
        match req {
            Request::Subscribe(s) => {
                // Track topic and initialize per-topic credits (coarse path)
                if let Some(mut meta) = self.meta.get_mut(id) {
                    meta.topics.insert(s.topic());
                    meta.credits.entry(s.topic()).or_insert(64);
                }
                trace!(sub_id = %id.0, topic = ?s.topic(), "router.subscribe");
            }
            Request::SubscribeKey(s) => {
                let shard = self.shard_idx(s.topic, &s.key);
                let mut entry = self.sub_index[shard].entry((s.topic, s.key.clone())).or_insert(HashSet::new());
                entry.insert(id.clone());
                self.credits[shard].entry((id.clone(), s.topic, s.key.clone())).or_insert(64);
                trace!(sub_id = %id.0, topic = ?s.topic, "router.subscribe_key");
            }
            Request::UnsubscribeKey(u) => {
                let shard = self.shard_idx(u.topic, &u.key);
                let key1 = u.key.clone();
                if let Some(mut set) = self.sub_index[shard].get_mut(&(u.topic, key1)) {
                    set.remove(id);
                    if set.is_empty() { drop(set); let _ = self.sub_index[shard].remove(&(u.topic, u.key.clone())); }
                }
                self.credits[shard].remove(&(id.clone(), u.topic, u.key));
                trace!(sub_id = %id.0, topic = ?u.topic, "router.unsubscribe_key");
            }
            Request::FlowCredit(fc) => {
                if let Some(mut meta) = self.meta.get_mut(id) {
                    let e = meta.credits.entry(fc.topic()).or_insert(0);
                    *e = e.saturating_add(fc.credits as u32).min(1000);
                }
                trace!(sub_id = %id.0, topic = ?fc.topic(), credits = fc.credits, "router.flow_credit");
            }
            Request::FlowCreditKey(fc) => {
                let shard = self.shard_idx(fc.topic, &fc.key);
                let mut e = self.credits[shard].entry((id.clone(), fc.topic, fc.key)).or_insert(0);
                *e = e.saturating_add(fc.credits as u32).min(1_000);
                trace!(sub_id = %id.0, topic = ?fc.topic, credits = fc.credits, "router.flow_credit_key");
            }
            Request::Ping(p) => {
                let _ = self.send_to(id, Response::Pong(tt_types::wire::Pong { ts_ns: p.ts_ns })).await;
            }
            Request::UnsubscribeAll(_u) => {
                trace!(sub_id = %id.0, "router.unsubscribe_all");
                self.unsubscribe_all(id).await;
            }
            _ => {}
        }
        Ok(())
    }

    async fn unsubscribe_all(&self, id: &SubId) {
        if let Some(mut meta) = self.meta.get_mut(id) {
            meta.topics.clear();
            meta.credits.clear();
        }
        // Remove from key-based indices and credits across all shards
        for shard in 0..self.shards {
            // Remove sub from all sets
            for mut entry in self.sub_index[shard].iter_mut() {
                entry.value_mut().remove(id);
            }
            // Drop any empty sets to keep maps tidy
            let empty_keys: Vec<_> = self
                .sub_index[shard]
                .iter()
                .filter(|e| e.value().is_empty())
                .map(|e| e.key().clone())
                .collect();
            for k in empty_keys { let _ = self.sub_index[shard].remove(&k); }

            // Remove all credits entries for this sub
            let to_remove: Vec<_> = self
                .credits[shard]
                .iter()
                .filter(|e| e.key().0 == *id)
                .map(|e| e.key().clone())
                .collect();
            for k in to_remove { let _ = self.credits[shard].remove(&k); }
        }
    }

    fn shard_idx(&self, topic: Topic, key: &SymbolKey) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        topic.hash(&mut h);
        key.hash(&mut h);
        (h.finish() as usize) % self.shards
    }

    pub async fn send_to(&self, id: &SubId, resp: Response) -> Result<()> {
        if let Some(tx) = self.subscribers.get(id).map(|r| r.value().clone()) {
            let _ = tx.send(resp).await;
        }
        Ok(())
    }

    pub async fn publish_for_topic<F>(&self, topic: Topic, build: F) -> Result<()>
    where F: FnOnce(u64) -> Response {
        // assign sequence
        let seq = {
            let mut e = self.seq_by_topic.entry(topic).or_insert(0);
            *e += 1; *e
        };
        let resp = build(seq);

        // Collect eligible recipients
        let mut to_send: Vec<(SubId, mpsc::Sender<Response>)> = Vec::new();
        for entry in self.subscribers.iter() {
            let sid = entry.key().clone();
            let tx = entry.value().clone();
            if let Some(meta) = self.meta.get(&sid) {
                if !meta.topics.contains(&topic) { continue; }
                let has_credit = *meta.credits.get(&topic).unwrap_or(&0) > 0;
                if !has_credit { continue; }
                to_send.push((sid, tx));
            }
        }
        let mut sent = 0usize;
        for (sid, tx) in to_send {
            if let Some(mut meta) = self.meta.get_mut(&sid) {
                if let Some(c) = meta.credits.get_mut(&topic) {
                    if *c > 0 { *c -= 1; }
                }
                if tx.try_send(resp.clone()).is_ok() { sent += 1; }
            }
        }
        trace!(?topic, seq, sent, "router.publish_for_topic");
        Ok(())
    }

    pub async fn publish_for_key<F>(&self, topic: Topic, key: &SymbolKey, build: F) -> Result<()>
    where F: FnOnce(u64) -> Response {
        // assign sequence using per-topic counter to keep ordering per topic
        let seq = {
            let mut e = self.seq_by_topic.entry(topic).or_insert(0);
            *e += 1; *e
        };
        let resp = build(seq);
        let shard = self.shard_idx(topic, key);
        let mut to_send: Vec<(SubId, mpsc::Sender<Response>)> = Vec::new();
        if let Some(set) = self.sub_index[shard].get(&(topic, key.clone())) {
            for sid in set.iter() {
                if let Some(tx) = self.subscribers.get(sid).map(|r| r.value().clone()) {
                    // check credits
                    if let Some(c) = self.credits[shard].get(&(sid.clone(), topic, key.clone())) {
                        if *c.value() > 0 { to_send.push((sid.clone(), tx)); }
                    }
                }
            }
        }
        let mut sent = 0usize;
        for (sid, tx) in to_send {
            if let Some(mut e) = self.credits[shard].get_mut(&(sid.clone(), topic, key.clone())) {
                let c = e.value_mut();
                if *c > 0 { *c -= 1; }
            }
            if tx.try_send(resp.clone()).is_ok() { sent += 1; }
        }
        trace!(?topic, seq, sent, "router.publish_for_key");
        Ok(())
    }
}
