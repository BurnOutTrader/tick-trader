use futures_util::stream::BoxStream;
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite;
use tokio_tungstenite::tungstenite::{Message, http};

#[derive(Debug, Clone)]
#[allow(unused)]
pub struct WebSocketConfig {
    pub url: String,
    pub headers: Vec<(String, String)>,
    #[allow(dead_code)]
    pub heartbeat: Option<u64>,
    pub heartbeat_msg: Option<String>,
    pub ping_handler: Option<()>,
    pub reconnect_timeout_ms: Option<u64>,
    pub reconnect_delay_initial_ms: Option<u64>,
    pub reconnect_delay_max_ms: Option<u64>,
    pub reconnect_backoff_factor: Option<f64>,
    pub reconnect_jitter_ms: Option<u64>,
}

#[derive(Debug)]
pub struct WebSocketClient {
    write_tx: mpsc::Sender<Message>,
    recv_task: Mutex<Option<JoinHandle<()>>>,
}

impl WebSocketClient {
    pub async fn connect_stream(
        config: WebSocketConfig,
        _protocols: Vec<String>,
        _proxy: Option<()>,
        _tls: Option<()>,
    ) -> anyhow::Result<(
        BoxStream<'static, Result<Message, tungstenite::Error>>,
        WebSocketClient,
    )> {
        // Build request using IntoClientRequest to ensure proper websocket defaults (Host, Upgrade, etc.)
        // Then attach any custom headers like Authorization.
        use futures_util::{SinkExt, StreamExt};
        use tokio::sync::{Mutex, mpsc};
        use tokio_tungstenite::connect_async;
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = config.url.clone().into_client_request()?;
        if !config.headers.is_empty() {
            let headers = request.headers_mut();
            for (k, v) in &config.headers {
                headers.insert(
                    http::header::HeaderName::from_bytes(k.as_bytes())?,
                    http::header::HeaderValue::from_str(v)?,
                );
            }
        }
        let (ws_stream, _response) = connect_async(request).await?;
        let (mut write, read) = ws_stream.split();
        // Create a bounded channel and spawn a dedicated writer task to serialize sends
        let (tx, mut rx) = mpsc::channel::<Message>(8192);
        let recv_task = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Err(_e) = write.send(msg).await {
                    break;
                }
            }
        });
        let client = WebSocketClient {
            write_tx: tx,
            recv_task: Mutex::new(Some(recv_task)),
        };
        let reader: BoxStream<'static, Result<Message, tungstenite::Error>> = Box::pin(read);
        Ok((reader, client))
    }

    pub async fn send_text(&self, data: String, _request_id: Option<u64>) -> anyhow::Result<()> {
        // Fast path: try to enqueue without awaiting to minimize latency on hot path
        match self.write_tx.try_send(Message::Text(data.clone().into())) {
            Ok(_) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_m)) => {
                // Apply backpressure by awaiting when queue is full
                self.write_tx
                    .send(Message::Text(data.into()))
                    .await
                    .map_err(|e| anyhow::anyhow!(e.to_string()))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_m)) => {
                Err(anyhow::anyhow!("websocket writer closed"))
            }
        }
    }

    pub async fn send_pong(&self, payload: tungstenite::Bytes) -> anyhow::Result<()> {
        // Respond to a Ping with a Pong frame.
        match self.write_tx.try_send(Message::Pong(payload.clone())) {
            Ok(_) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_m)) => {
                self.write_tx
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|e| anyhow::anyhow!(e.to_string()))
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_m)) => {
                Err(anyhow::anyhow!("websocket writer closed"))
            }
        }
    }

    pub async fn kill(&self) {
        let mut lock = self.recv_task.lock().await;
        if let Some(recv_task) = lock.take() {
            let _ = recv_task.abort();
        }
    }
}
