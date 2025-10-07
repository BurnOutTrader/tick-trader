use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{info, warn};
use tt_bus::ClientMessageBus;
use tt_engine::engine::EngineRuntime;
use tt_engine::engine::Strategy;
use tt_types::data::core::OrderBookSnapShot;
use tt_types::keys::Topic;
use tt_types::securities::symbols::Instrument;
use tt_types::wire;
use tt_types::wire::{Kick, Request, WireMessage};

#[derive(Clone, Default)]
struct TestStrategy;

#[async_trait::async_trait]
impl Strategy for TestStrategy {
    fn desired_topics(&self) -> std::collections::HashSet<Topic> {
        use std::collections::HashSet;
        let mut s = HashSet::new();
        // Subscribe to all hot and account topics so router delivers them
        s.insert(Topic::Ticks);
        s.insert(Topic::Quotes);
        s.insert(Topic::Depth);
        s.insert(Topic::Candles1s);
        s.insert(Topic::Orders);
        s.insert(Topic::Positions);
        s.insert(Topic::AccountEvt);
        s
    }
    async fn on_start(&self) {
        info!("strategy start");
    }
    async fn on_stop(&self) {
        info!("strategy stop");
    }
    async fn on_tick(&self, t: tt_types::data::core::Tick) {
        println!("{:?}", t)
    }
    async fn on_quote(&self, q: tt_types::data::core::Bbo) {
        println!("{:?}", q);
    }
    async fn on_bar(&self, _b: tt_types::data::core::Candle) {}
    async fn on_depth(&self, d: OrderBookSnapShot) {
        println!("{:?}", d);
    }
    async fn on_orders_batch(&self, b: wire::OrdersBatch) {
        println!("{:?}", b);
    }
    async fn on_positions_batch(&self, b: wire::PositionsBatch) {
        println!("{:?}", b);
    }
    async fn on_account_delta_batch(&self, b: wire::AccountDeltaBatch) {
        println!("{:?}", b);
    }
    async fn on_subscribe(&self, _instrument: Instrument, topic: Topic, success: bool) {
        println!("{:?}: {}", topic, success);
    }
    async fn on_unsubscribe(&self, _instrument: Instrument, topic: Topic) {
        println!("{:?}", topic);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env if available
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    // Prepare client bus and transport channel
    let (req_tx, mut req_rx) = mpsc::channel::<Request>(1024);
    let bus = ClientMessageBus::new_with_transport(req_tx.clone());

    // Connect to Router over UDS (filesystem path)
    // Note: for Linux abstract UDS, set TT_BUS_ADDR to a filesystem path like /tmp/tick-trader.sock for this test binary.
    let default_addr =
        std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let sock = match tokio::net::UnixStream::connect(&default_addr).await {
        Ok(s) => s,
        Err(e) => {
            warn!(path = %default_addr, error = %e, "failed to connect to server UDS");
            return Err(e.into());
        }
    };

    let (read_half, write_half) = sock.into_split();
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(8 * 1024 * 1024)
        .new_codec();
    let mut framed_reader = FramedRead::new(read_half, codec.clone());
    let mut framed_writer = FramedWrite::new(write_half, codec);

    // Writer bridge: forward Requests from ClientMessageBus to Router
    let writer = tokio::spawn(async move {
        while let Some(req) = req_rx.recv().await {
            let wire = WireMessage::Request(req);
            let buf = wire::codec::encode(&wire);
            if let Err(e) = framed_writer.send(Bytes::from(buf)).await {
                warn!(error = %e, "writer send failed");
                break;
            }
        }
    });

    // Reader bridge: route Responses from Router to ClientMessageBus
    let bus_reader = bus.clone();
    let reader = tokio::spawn(async move {
        while let Some(item) = framed_reader.next().await {
            let Ok(bytes) = item else { break };
            match wire::codec::decode(&bytes) {
                Ok(WireMessage::Response(resp)) => {
                    let _ = bus_reader.route_response(resp).await;
                }
                Ok(WireMessage::Request(_)) => { /* ignore */ }
                Err(e) => {
                    warn!(%e, "decode error");
                    break;
                }
            }
        }
    });

    // Start engine runtime with our test strategy
    let mut engine = EngineRuntime::new(bus.clone());
    let strategy = Arc::new(TestStrategy::default());
    engine.start(strategy.clone()).await?;

    // After the engine is running (topic-level subscribe/credits are in place),
    // request a specific key subscription for MNQZ5 ticks via ProjectX tenant from env.
    use std::str::FromStr;
    use tt_types::providers::{ProjectXTenant, ProviderKind};
    use tt_types::securities::symbols::Instrument;

    let tenant = ProjectXTenant::Topstep;
    let provider = ProviderKind::ProjectX(tenant);
    let instrument = Instrument::from_str("MNQZ25").expect("valid instrument MNQZ5");
    let key = tt_types::keys::SymbolKey {
        instrument,
        provider,
    };

    // Send key-based subscribe and initial credits directly over the transport channel.
    let _ = req_tx
        .send(Request::SubscribeKey(tt_types::wire::SubscribeKey {
            topic: Topic::Quotes,
            key: key.clone(),
            latest_only: false,
            from_seq: 0,
        }))
        .await;
    /* let _ = req_tx
    .send(Request::SubscribeKey(tt_types::wire::SubscribeKey {
        topic: Topic::Depth,
        key: key.clone(),
        latest_only: false,
        from_seq: 0,
    }))
    .await;*/
    info!(
        ?key,
        "sent SubscribeKey for MNQZ5 ticks + depth (server-managed backpressure; no credits)"
    );

    // Keep running for a bit to receive live data
    sleep(Duration::from_secs(60)).await;

    let _ = req_tx
        .send(Request::Kick(Kick {
            reason: Some("Want to".to_string()),
        }))
        .await;

    // Graceful shutdown
    engine.stop().await;
    // Drop sender to stop writer
    drop(req_tx);
    let _ = writer.await;
    let _ = reader.await;

    Ok(())
}
