use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, atomic::{AtomicU64, Ordering}};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{trace, warn};
use tt_types::keys::Topic;
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
}

impl Router {
    pub fn new(_shards: usize) -> Self {
        // Shards will be introduced later; start with single actor-like map.
        Self {
            subscribers: Arc::new(DashMap::new()),
            meta: Arc::new(DashMap::new()),
            next_id: Arc::new(AtomicU64::new(1)),
            seq_by_topic: Arc::new(DashMap::new()),
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
                // Track topic and initialize per-topic credits
                if let Some(mut meta) = self.meta.get_mut(id) {
                    meta.topics.insert(s.topic());
                    meta.credits.entry(s.topic()).or_insert(64);
                }
                // In a later pass, we will notify ProviderManager on first sub.
            }
            Request::FlowCredit(fc) => {
                if let Some(mut meta) = self.meta.get_mut(id) {
                    let e = meta.credits.entry(fc.topic()).or_insert(0);
                    *e = e.saturating_add(fc.credits as u32).min(1000);
                }
            }
            Request::Ping(p) => {
                let _ = self.send_to(id, Response::Pong(tt_types::wire::Pong { ts_ns: p.ts_ns })).await;
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
}
