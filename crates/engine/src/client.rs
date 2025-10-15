use anyhow::Result;
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::error;
use tt_types::wire::{Request, Response};

/// Client-side message bus used by the engine. It maintains local subscriptions
/// (topics/credits) and forwards requests to the server. Incoming responses from
/// the server are routed to local subscribers according to their interest and credits.
#[derive(Debug)]
pub struct ClientMessageBus {
    broadcaster: broadcast::Sender<Response>,
    pub(crate) req_tx: mpsc::Sender<Request>,
    next_corr_id: AtomicU64,
    pending: DashMap<u64, oneshot::Sender<Response>>,
}

impl ClientMessageBus {
    pub fn new_with_transport(req_tx: mpsc::Sender<Request>) -> ClientMessageBus {
        Self {
            broadcaster: Sender::new(10000),
            req_tx,
            next_corr_id: AtomicU64::new(1),
            pending: DashMap::new(),
        }
    }

    pub fn add_client(&self) -> Receiver<Response> {
        self.broadcaster.subscribe()
    }

    #[inline]
    pub(crate) async fn handle_request(&self, req: Request) -> Result<()> {
        let _ = self.req_tx.send(req).await;
        Ok(())
    }

    #[inline]
    pub(crate) fn broadcast(&self, resp: Response) -> Result<()> {
        self.broadcaster.send(resp)?;
        Ok(())
    }

    pub fn route_response(&self, response: Response, cuid: u64) {
        if let Some((_, sender)) = self.pending.remove(&cuid) {
            if let Err(e) = sender.send(response) {
                error!("route_response: failed to route response: {:?}", e);
            }
        }
    }

    #[inline]
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
}
