use std::sync::{LazyLock, OnceLock};
use crossbeam::queue::ArrayQueue;
use futures_util::{SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio::sync::mpsc;
use tokio_util::bytes;
use tracing::{info, warn};
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{Bytes, Request, WireMessage};
use crate::client::ClientMessageBus;
use crate::models::{Command, DataTopic};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};

// Single global bus client instance; initialized once when transport is ready.
pub(crate) static BUS_CLIENT: OnceLock<ClientMessageBus> = OnceLock::new();

// Internal command queue owned by the bus layer. Engine runtime drains this.
static CMD_Q: LazyLock<ArrayQueue<Command>> = LazyLock::new(|| ArrayQueue::new(2048));

/// Connect to the tick-trader server via Unix Domain Socket and spawn IO tasks.
/// If path starts with '@' or a leading NUL ("\0"), on Linux it will use abstract namespace.
/// On macOS and others, provide a filesystem path like "/tmp/tick-trader.sock".
/// ONLY FOR LIVE MODE
pub async fn connect_live_bus() -> anyhow::Result<()> {
    let addr = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| "/tmp/tick-trader.sock".to_string());
    let sock = UnixStream::connect(addr).await?;
    let (r, w) = sock.into_split();
    // Match server/router framing: allow up to 8 MiB frames
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(8 * 1024 * 1024)
        .new_codec();
    let mut reader = FramedRead::new(r, codec.clone());
    let mut writer = FramedWrite::new(w, codec);

    // Create internal request channel for the write loop
    let (req_tx, mut req_rx) = mpsc::channel::<Request>(1024);
    let bus = ClientMessageBus::new_with_transport(req_tx.clone());
    BUS_CLIENT.set(bus).expect("bus client already initialized");

    // Read loop //todo, probably better to use while loop
    tokio::spawn(async move {
        loop {
            let bus = BUS_CLIENT.get().unwrap();
            match reader.next().await {
                Some(Ok(bytes)) => {
                    match WireMessage::from_bytes(bytes.as_ref()) {
                        Ok(WireMessage::Response(resp)) => {
                            if let Err(e) = bus.broadcast(resp) {
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
                    continue; //dont stop for anything
                }
                None => {
                    // EOF
                    info!("bus disconnected by server");
                    continue; //dont stop for anything
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
                continue; //dont stop for anything
            }
        }
    });

    Ok(())
}


/// Get a reference to the global bus client. Panics if not initialized.
#[inline]
pub fn bus() -> &'static ClientMessageBus {
    BUS_CLIENT.get().expect("BUS_CLIENT not initialized; call init_bus() first")
}

/// Enqueue a command for the engine to process (fire-and-forget).
#[inline]
pub fn send_command(cmd: Command) {
    let _ = CMD_Q.push(cmd);
}

/// Expose the command queue for the engine runtime to drain synchronously.
#[inline]
pub fn command_queue() -> &'static ArrayQueue<Command> {
    &CMD_Q
}

pub async fn list_instruments(
    provider: ProviderKind,
    pattern: Option<String>,
) -> anyhow::Result<Vec<Instrument>> {
    use std::time::Duration;
    use tokio::time::timeout;
    use tt_types::wire::{InstrumentsRequest, Response as WireResp};
    let rx = crate::statics::bus::bus()
        .request_with_corr(|corr_id| {
            Request::InstrumentsRequest(InstrumentsRequest {
                provider,
                pattern,
                corr_id,
            })
        })
        .await;
    // Wait briefly for the server to respond; if unsupported, return empty
    match timeout(Duration::from_millis(750), rx).await {
        Ok(Ok(WireResp::InstrumentsResponse(ir))) => Ok(ir.instruments),
        Ok(Ok(_other)) => Ok(vec![]),
        _ => Ok(vec![]),
    }
}

pub async fn get_instruments_map(
    provider: ProviderKind,
) -> anyhow::Result<Vec<FuturesContract>> {
    use std::time::Duration;
    use tokio::time::timeout;
    use tt_types::wire::{InstrumentsMapRequest, Response as WireResp};
    let rx = crate::statics::bus::bus().request_with_corr(|corr_id| {
        Request::InstrumentsMapRequest(InstrumentsMapRequest { provider, corr_id })
    })
        .await;
    match timeout(Duration::from_secs(2), rx).await {
        Ok(Ok(WireResp::InstrumentsMapResponse(imr))) => Ok(imr.contracts),
        Ok(Ok(_other)) => Ok(vec![]),
        _ => Ok(vec![]),
    }
}

pub async fn get_account_info(
    provider: ProviderKind,
) -> anyhow::Result<tt_types::wire::AccountInfoResponse> {
    use std::time::Duration;
    use tokio::time::timeout;
    use tt_types::wire::{AccountInfoRequest, Response as WireResp};
    let rx = crate::statics::bus::bus()
        .request_with_corr(|corr_id| {
            Request::AccountInfoRequest(AccountInfoRequest { provider, corr_id })
        })
        .await;
    match timeout(Duration::from_secs(2), rx).await {
        Ok(Ok(WireResp::AccountInfoResponse(air))) => Ok(air),
        Ok(Ok(_other)) => Err(anyhow::anyhow!(
                "unexpected response for AccountInfoRequest"
            )),
        _ => Err(anyhow::anyhow!("timeout waiting for AccountInfoResponse")),
    }
}

pub async fn subscribe_symbol(topic: Topic, key: SymbolKey) -> anyhow::Result<()> {
    // Forward to server
    crate::statics::bus::bus()
        .handle_request(
            Request::SubscribeKey(tt_types::wire::SubscribeKey {
                topic,
                key,
                latest_only: false,
                from_seq: 0,
            }),
        )
        .await?;
    Ok(())
}

pub async fn unsubscribe_symbol(topic: Topic, key: SymbolKey) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            Request::UnsubscribeKey(tt_types::wire::UnsubscribeKey { topic, key }),
        )
        .await?;
    Ok(())
}

// Convenience: subscribe/unsubscribe by key without exposing sender
pub async fn subscribe_key(data_topic: DataTopic, key: SymbolKey) -> anyhow::Result<()> {
    // Ensure vendor securities refresh is active for this provider
    let topic = data_topic.to_topic_or_err()?;
    crate::statics::bus::bus()
        .handle_request(
            Request::SubscribeKey(tt_types::wire::SubscribeKey {
                topic,
                key,
                latest_only: false,
                from_seq: 0,
            }),
        )
        .await?;
    Ok(())
}

pub async fn unsubscribe_key(
    data_topic: DataTopic,
    key: SymbolKey,
) -> anyhow::Result<()> {
    let topic = data_topic.to_topic_or_err()?;
    unsubscribe_symbol(topic, key, ).await
}

// Orders API helpers
pub async fn send_order_for_execution(
    spec: tt_types::wire::PlaceOrder,
) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::PlaceOrder(spec),
        )
        .await?;
    Ok(())
}

pub async fn cancel_order(spec: tt_types::wire::CancelOrder) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::CancelOrder(spec),
        )
        .await?;
    Ok(())
}

pub async fn replace_order(spec: tt_types::wire::ReplaceOrder) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::ReplaceOrder(spec),
        )
        .await?;
    Ok(())
}

// Account interest: auto-subscribe all execution streams for an account
pub async fn activate_account_interest(
    key: tt_types::keys::AccountKey,
) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::SubscribeAccount(tt_types::wire::SubscribeAccount { key }),
        )
        .await?;
    Ok(())
}

pub async fn deactivate_account_interest(
    key: tt_types::keys::AccountKey,
) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::UnsubscribeAccount(tt_types::wire::UnsubscribeAccount {
                key,
            }),
        )
        .await?;
    Ok(())
}

/// Initialize one or more accounts at engine startup by subscribing to all
/// account-related streams (orders, positions, account events). This is a
/// convenience wrapper around `activate_account_interest`.
pub async fn initialize_accounts<I>(accounts: I) -> anyhow::Result<()>
where
    I: IntoIterator<Item = AccountKey>,
{
    for key in accounts {
        activate_account_interest(key).await?;
    }
    Ok(())
}

/// Convenience: initialize by account names for a given provider kind.
pub async fn initialize_account_names<I>(
    provider: ProviderKind,
    names: I,
) -> anyhow::Result<()>
where
    I: IntoIterator<Item = tt_types::accounts::account::AccountName>,
{
    let keys = names.into_iter().map(|account_name| AccountKey {
        provider,
        account_name,
    });
    initialize_accounts(keys).await
}
