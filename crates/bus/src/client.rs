use anyhow::Result;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use std::collections::HashSet;
use std::sync::{Arc, atomic::{AtomicU64, Ordering}, LazyLock};
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{info, warn};
use tt_types::keys::Topic;
use tt_types::wire::Bytes as WireBytes;
use tt_types::wire::{Request, Response, WireMessage};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientSubId(pub(crate) u64);

#[derive(Debug, Clone)]
pub struct ClientSubscriber {
    pub id: ClientSubId,
    pub topics: HashSet<Topic>,
}

/// Client-side message bus used by the engine. It maintains local subscriptions
/// (topics/credits) and forwards requests to the server. Incoming responses from
/// the server are routed to local subscribers according to their interest and credits.
#[derive(Debug, Clone)]
pub struct ClientMessageBus {
    subscribers: Arc<DashMap<ClientSubId, mpsc::Sender<Response>>>,
    meta: Arc<DashMap<ClientSubId, ClientSubscriber>>,
    next_id: Arc<AtomicU64>,
    req_tx: mpsc::Sender<Request>,
    next_corr_id: Arc<AtomicU64>,
    pending: Arc<DashMap<u64, oneshot::Sender<Response>>>
}

impl ClientMessageBus {
    pub fn new_with_transport(req_tx: mpsc::Sender<Request>) -> ClientMessageBus {
        Self {
            subscribers: Arc::new(DashMap::new()),
            meta: Arc::new(DashMap::new()),
            next_id: Arc::new(AtomicU64::new(1)),
            req_tx,
            next_corr_id: Arc::new(AtomicU64::new(1)),
            pending: Arc::new(DashMap::new()),
        }
    }

    /// Connect to the tick-trader server via Unix Domain Socket and spawn IO tasks.
    /// If path starts with '@' or a leading NUL ("\0"), on Linux it will use abstract namespace.
    /// On macOS and others, provide a filesystem path like "/tmp/tick-trader.sock".
    pub async fn connect(path: &str) -> Result<ClientMessageBus> {
        let sock = UnixStream::connect(path).await?;
        let (r, w) = sock.into_split();
        // Match server/router framing: allow up to 8 MiB frames
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(8 * 1024 * 1024)
            .new_codec();
        let mut reader = FramedRead::new(r, codec.clone());
        let mut writer = FramedWrite::new(w, codec);

        // Create internal request channel for the write loop
        let (req_tx, mut req_rx) = mpsc::channel::<Request>(1024);
        let bus = Self::new_with_transport(req_tx.clone());
        let bus_clone = bus.clone();

        // Read loop
        tokio::spawn(async move {
            loop {
                match reader.next().await {
                    Some(Ok(bytes)) => {
                        match WireMessage::from_bytes(bytes.as_ref()) {
                            Ok(WireMessage::Response(resp)) => {
                                if let Err(e) = bus_clone.route_response(resp).await {
                                    warn!("route_response error: {e:?}");
                                }
                            }
                            Ok(WireMessage::Request(_)) => {
                                // Clients should not receive Requests; ignore
                                warn!("client received unexpected Request frame; ignoring");
                            }
                            Err(e) => {
                                warn!("decode error: {e:?}");
                            }
                        }
                    }
                    Some(Err(e)) => {
                        warn!("bus read error: {e:?}");
                        break;
                    }
                    None => {
                        // EOF
                        info!("bus disconnected by server");
                        break;
                    }
                }
            }
        });

        // Write loop
        tokio::spawn(async move {
            while let Some(req) = req_rx.recv().await {
                let wire = WireMessage::Request(req);
                let aligned = wire.to_aligned_bytes();
                let vec: Vec<u8> = aligned.into();
                if let Err(e) = writer.send(bytes::Bytes::from(vec)).await {
                    warn!("bus write error: {e:?}");
                    break;
                }
            }
        });

        Ok(bus)
    }

    pub async fn add_client(&self, tx: mpsc::Sender<Response>) -> ClientSubId {
        let id = ClientSubId(self.next_id.fetch_add(1, Ordering::SeqCst));
        self.subscribers.insert(id.clone(), tx);
        self.meta.insert(
            id.clone(),
            ClientSubscriber {
                id: id.clone(),
                topics: HashSet::new(),
            },
        );
        id
    }

    pub async fn handle_request(&self, req: Request, id: &ClientSubId) -> Result<()> {
        // Maintain local interest/credits mirroring server controls so route_response can gate by topic.
        match &req {
            Request::Subscribe(s) => {
                if let Some(mut meta) = self.meta.get_mut(id) {
                    meta.topics.insert(s.topic());
                }
            }
            Request::SubscribeKey(s) => {
                // Key-based subscriptions imply interest in the coarse topic for gating purposes
                if let Some(mut meta) = self.meta.get_mut(id) {
                    meta.topics.insert(s.topic);
                }
            }
            Request::UnsubscribeKey(u) => {
                // Optionally remove the topic if there are no more key subscriptions tracked locally.
                // We don't track per-key locally, so leave it in to keep receiving data across resubscribes.
                let _ = u; // no-op
            }
            Request::Ping(_) => {}
            Request::SubscribeAccount(_sa) => {
                // Mirror coarse topic interest for account execution streams locally so gating works before server ack
                if let Some(mut meta) = self.meta.get_mut(id) {
                    meta.topics.insert(Topic::Orders);
                    meta.topics.insert(Topic::Positions);
                    meta.topics.insert(Topic::AccountEvt);
                    meta.topics.insert(Topic::Fills);
                }
            }
            _ => {}
        }
        // Forward to server
        let _ = self.req_tx.send(req).await;
        Ok(())
    }

    pub async fn send_to(&self, id: &ClientSubId, resp: Response) -> Result<()> {
        if let Some(tx) = self.subscribers.get(id).map(|r| r.value().clone()) {
            let _ = tx.send(resp).await;
        }
        Ok(())
    }

    /// Send a correlated request and receive a oneshot Response matching the generated corr_id.
    /// The provided closure constructs the Request given the corr_id.
    pub async fn request_with_corr<F>(&self, make: F) -> oneshot::Receiver<Response>
    where
        F: FnOnce(u64) -> Request,
    {
        let corr_id = self.next_corr_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = oneshot::channel();
        self.pending.insert(corr_id, tx);
        let _ = self.req_tx.send(make(corr_id)).await;
        rx
    }

    /// Route a server Response to all local subscribers interested in the topic,
    /// decrementing credits.
    pub async fn route_response(&self, resp: Response) -> Result<()> {
        // First, handle correlated responses to fulfill pending oneshot callbacks and stop propagation
        match &resp {
            Response::InstrumentsResponse(ir) => {
                if let Some((_k, tx)) = self.pending.remove(&ir.corr_id) {
                    let _ = tx.send(resp.clone());
                    return Ok(());
                }
            }
            Response::InstrumentsMapResponse(imr) => {
                if let Some((_k, tx)) = self.pending.remove(&imr.corr_id) {
                    let _ = tx.send(resp.clone());
                    return Ok(());
                }
            }
            Response::AccountInfoResponse(air) => {
                if let Some((_k, tx)) = self.pending.remove(&air.corr_id) {
                    let _ = tx.send(resp.clone());
                    return Ok(());
                }
            }
            // When we receive a SubscribeResponse(success), mark the topic as interested
            // for all local subscribers to ensure subsequent data for that topic is delivered.
            Response::SubscribeResponse { topic, success, .. } => {
                if *success {
                    for mut meta in self.meta.iter_mut() {
                        meta.value_mut().topics.insert(*topic);
                    }
                }
                // Control messages are broadcast to all local subscribers.
                for entry in self.subscribers.iter() {
                    let tx = entry.value().clone();
                    let _ = tx.try_send(resp.clone());
                }
                return Ok(());
            }
            // Treat AnnounceShm similarly: insert topic interest and broadcast.
            Response::AnnounceShm(ann) => {
                for mut meta in self.meta.iter_mut() {
                    meta.value_mut().topics.insert(ann.topic);
                }
                for entry in self.subscribers.iter() {
                    let tx = entry.value().clone();
                    let _ = tx.try_send(resp.clone());
                }
                return Ok(());
            }
            _ => {}
        }

        let topic_opt = match &resp {
            Response::TickBatch(b) => Some(b.topic),
            Response::QuoteBatch(b) => Some(b.topic),
            Response::BarBatch(b) => Some(b.topic),
            Response::MBP10Batch(b) => Some(b.topic),
            Response::VendorData(v) => Some(v.topic),
            Response::OrdersBatch(b) => Some(b.topic),
            Response::PositionsBatch(b) => Some(b.topic),
            Response::AccountDeltaBatch(b) => Some(b.topic),
            Response::ClosedTrades(_) => Some(Topic::Fills),
            // Control/instruments and single items don't have a topic gating; deliver to all
            _ => None,
        };

        if topic_opt.is_none() {
            // broadcast controls (e.g., Pong, instruments) to all
            for entry in self.subscribers.iter() {
                let tx = entry.value().clone();
                let _ = tx.try_send(resp.clone());
            }
            return Ok(());
        }
        let topic = topic_opt.unwrap();

        let mut to_send: Vec<(ClientSubId, mpsc::Sender<Response>)> = Vec::new();
        for entry in self.subscribers.iter() {
            let sid = entry.key().clone();
            let tx = entry.value().clone();
            if let Some(meta) = self.meta.get(&sid) {
                if !meta.topics.contains(&topic) {
                    continue;
                }
                to_send.push((sid, tx));
            }
        }
        if to_send.is_empty() {
            // Fallback: if no local subscribers have recorded interest for this topic yet,
            // broadcast to all local subscribers. This prevents silent drops when the caller
            // bypasses handle_request() or defers Subscribe bookkeeping.
            for entry in self.subscribers.iter() {
                let tx = entry.value().clone();
                let _ = tx.try_send(resp.clone());
            }
        } else {
            for (_sid, tx) in to_send {
                let _ = tx.try_send(resp.clone());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tokio::time::{Duration, timeout};
    use tt_types::data::core::Decimal;
    use tt_types::keys::Topic;
    use tt_types::providers::{ProjectXTenant, ProviderKind};
    use tt_types::securities::symbols::Instrument;

    fn make_mbp10_batch(instr: Instrument) -> Response {
        let now_ns = tt_types::data::mbp10::Utc::now()
            .timestamp_nanos_opt()
            .unwrap_or(0) as u64;
        let ev = tt_types::data::mbp10::make_mbp10(
            instr,
            now_ns,
            now_ns,
            10,
            0,
            0,
            tt_types::data::mbp10::Action::Modify.into(),
            tt_types::data::mbp10::BookSide::None.into(),
            0,
            0,
            Decimal::from(0u32),
            tt_types::data::mbp10::Flags::F_MBP,
            0,
            0,
            None,
        )
        .expect("build mbp10");
        Response::MBP10Batch(tt_types::wire::MBP10Batch {
            topic: Topic::MBP10,
            seq: 1,
            event: ev,
            provider_kind: ProviderKind::ProjectX(ProjectXTenant::Demo),
        })
    }

    #[tokio::test]
    async fn route_response_broadcasts_when_no_local_interest() {
        let (req_tx, mut _req_rx) = mpsc::channel::<Request>(8);
        let bus = ClientMessageBus::new_with_transport(req_tx);

        // Register two local clients without any Subscribe bookkeeping
        let (tx1, mut rx1) = mpsc::channel::<Response>(4);
        let _ = bus.add_client(tx1).await;
        let (tx2, mut rx2) = mpsc::channel::<Response>(4);
        let _id2 = bus.add_client(tx2).await;

        let resp = make_mbp10_batch(Instrument::from_str("MNQ.Z25").unwrap());
        let _ = bus.route_response(resp).await;

        // Both should receive due to broadcast fallback
        let got1 = timeout(Duration::from_millis(200), async {
            loop {
                if let Some(Response::MBP10Batch(_)) = rx1.recv().await {
                    break true;
                }
            }
        })
        .await
        .unwrap_or(false);
        let got2 = timeout(Duration::from_millis(200), async {
            loop {
                if let Some(Response::MBP10Batch(_)) = rx2.recv().await {
                    break true;
                }
            }
        })
        .await
        .unwrap_or(false);
        assert!(
            got1 && got2,
            "both clients should receive when no local interest recorded"
        );
    }

    #[tokio::test]
    async fn route_response_respects_local_interest_after_subscribe() {
        let (req_tx, mut _req_rx) = mpsc::channel::<Request>(8);
        let bus = ClientMessageBus::new_with_transport(req_tx);

        let (tx1, mut rx1) = mpsc::channel::<Response>(4);
        let id1 = bus.add_client(tx1).await;
        let (tx2, mut rx2) = mpsc::channel::<Response>(4);
        let _id2 = bus.add_client(tx2).await;

        // Record topic interest locally for client1
        let _ = bus
            .handle_request(Request::Subscribe(tt_types::wire::Subscribe {
                    topic: Topic::MBP10,
                    latest_only: false,
                    from_seq: 0,
                }), &id1)
            .await;

        let resp = make_mbp10_batch(Instrument::from_str("MNQ.Z25").unwrap());
        let _ = bus.route_response(resp).await;

        // Client1 should receive; client2 should not
        let got1 = timeout(Duration::from_millis(200), async {
            loop {
                if let Some(Response::MBP10Batch(_)) = rx1.recv().await {
                    break true;
                }
            }
        })
        .await
        .unwrap_or(false);
        let got2_timeout = timeout(Duration::from_millis(200), async { rx2.recv().await }).await;
        assert!(got1, "client1 should receive after subscribing locally");
        assert!(
            got2_timeout.is_err(),
            "client2 should not receive within timeout"
        );
    }
}
