use std::str::FromStr;
// removed unused imports: sleep, TokioInstant
// use tokio::time::{Duration}; // Duration is imported below with timeout/TInstant
use std::sync::atomic::{AtomicBool, Ordering as Ato};
use tokio::time::{Duration, Instant as TInstant, timeout};
use tracing::level_filters::LevelFilter;
use tt_bus::ClientMessageBus;
use tt_types::data::core::Utc;
use tt_types::keys::{SymbolKey, Topic};
use tt_types::providers::{ProjectXTenant, ProviderKind};
use tt_types::securities::symbols::Instrument;
use tt_types::wire::Response;
use tt_types::wire::{Request, SubscribeKey};
// This test requires a running router/backend and valid PX credentials.
// It does NOT use EngineRuntime or any Strategy. It drives the ClientMessageBus directly.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .init();
    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let bus = ClientMessageBus::connect(&addr).await?;

    // register client
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Response>(1024);
    let sub_id = bus.add_client(tx).await;

    // simple ping sanity check (optional but useful)
    bus.handle_request(
        &sub_id,
        Request::Ping(tt_types::wire::Ping { ts_ns: Utc::now() }),
    )
    .await?;

    // flags
    let got_sub_ok = std::sync::Arc::new(AtomicBool::new(false));
    let got_data = std::sync::Arc::new(AtomicBool::new(false));
    let got_sub_ok2 = got_sub_ok.clone();
    let got_data2 = got_data.clone();
    // Request instruments list via correlated request (avoid Decimal-heavy map for older routers)
    let corr_rx = bus
        .request_with_corr(|corr_id| {
            Request::InstrumentsMapRequest(tt_types::wire::InstrumentsMapRequest {
                provider: ProviderKind::ProjectX(ProjectXTenant::Topstep),
                corr_id,
            })
        })
        .await;
    let resp = corr_rx.await?;
    if let Response::InstrumentsMapResponse(ir) = resp {
        tracing::info!(n = ir.instruments.len(), provider = ?ir.provider, "received InstrumentsResponse");
    } else {
        tracing::warn!("unexpected correlated response: {:?}", resp);
    }

    // drain task
    let drain = tokio::spawn(async move {
        let deadline = TInstant::now() + Duration::from_secs(20);
        while TInstant::now() < deadline {
            match rx.recv().await {
                Some(resp) => {
                    match resp {
                        Response::Pong(_) => {
                            tracing::info!("Pong received");
                        }
                        Response::SubscribeResponse {
                            topic,
                            instrument,
                            success,
                        } => {
                            tracing::info!(
                                "Subscribed: topic={:?} instrument={} success={}",
                                topic,
                                instrument,
                                success
                            );
                            if success {
                                got_sub_ok2.store(true, Ato::SeqCst);
                            }
                        }
                        Response::AnnounceShm(ann) => {
                            // If your server uses SHM for MBP10, this may be the only thing you get.
                            tracing::info!("AnnounceShm: topic={:?} key={:?}", ann.topic, ann.key);
                            got_data2.store(true, Ato::SeqCst);
                        }
                        Response::MBP10Batch(batch) => {
                            let e = batch.event;
                            tracing::info!(
                                "Mbp10: action={:?} side={:?} px={} sz={} flags={:?} ts_event={} ts_recv={}",
                                e.action,
                                e.side,
                                e.price,
                                e.size,
                                e.flags,
                                e.ts_event,
                                e.ts_recv
                            );
                            got_data2.store(true, Ato::SeqCst);
                        }
                        // You can log others or ignore
                        _ => {}
                    }
                }
                None => {
                    // channel closed by writer; stop rather than spinning
                    tracing::warn!("rx closed");
                    break;
                }
            }
        }
    });

    // subscribe AFTER the drain task is running
    let key = SymbolKey::new(
        Instrument::from_str("MNQ.Z25").unwrap(),
        ProviderKind::ProjectX(ProjectXTenant::Topstep),
    );
    bus.handle_request(
        &sub_id,
        Request::SubscribeKey(SubscribeKey {
            topic: Topic::MBP10,
            key,
            latest_only: false,
            from_seq: 0,
        }),
    )
    .await?;

    // wait for subscribe ack -> success
    timeout(Duration::from_secs(5), async {
        while !got_sub_ok.load(Ato::SeqCst) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timeout waiting for SubscribeResponse(success=true)"))?;

    // wait for either framed data OR AnnounceShm (SHM mode)
    timeout(Duration::from_secs(10), async {
        while !got_data.load(Ato::SeqCst) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timeout waiting for MBP10 or AnnounceShm"))?;

    let _ = drain.await;
    Ok(())
}
