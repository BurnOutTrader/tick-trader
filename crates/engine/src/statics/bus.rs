use std::sync::{LazyLock, OnceLock};
use crossbeam::queue::ArrayQueue;
use tokio::sync::mpsc;
use tt_bus::{ClientMessageBus, ClientSubId};
use tt_types::keys::{AccountKey, SymbolKey, Topic};
use tt_types::providers::ProviderKind;
use tt_types::securities::security::FuturesContract;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::Request;
use crate::models::{Command, DataTopic};

// Single global bus client instance; initialized once when transport is ready.
static BUS_CLIENT: OnceLock<ClientMessageBus> = OnceLock::new();

// Internal command queue owned by the bus layer. Engine runtime drains this.
static CMD_Q: LazyLock<ArrayQueue<Command>> = LazyLock::new(|| ArrayQueue::new(2048));

/// Initialize the global bus client with the provided request transport.
/// Safe to call exactly once; subsequent calls are ignored.
#[inline]
pub fn init_bus(req_tx: mpsc::Sender<Request>) {
    let _ = BUS_CLIENT.set(ClientMessageBus::new_with_transport(req_tx));
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

pub async fn subscribe_symbol(topic: Topic, key: SymbolKey, id: &ClientSubId) -> anyhow::Result<()> {
    // Forward to server
    crate::statics::bus::bus()
        .handle_request(
            Request::SubscribeKey(tt_types::wire::SubscribeKey {
                topic,
                key,
                latest_only: false,
                from_seq: 0,
            }),
            id
        )
        .await?;
    Ok(())
}
pub async fn unsubscribe_symbol(topic: Topic, key: SymbolKey, id: &ClientSubId) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            Request::UnsubscribeKey(tt_types::wire::UnsubscribeKey { topic, key }),
            id
        )
        .await?;
    Ok(())
}

// Convenience: subscribe/unsubscribe by key without exposing sender
pub async fn subscribe_key(data_topic: DataTopic, key: SymbolKey, id: &ClientSubId) -> anyhow::Result<()> {
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
            id
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
    id: &ClientSubId
) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::PlaceOrder(spec),
            id
        )
        .await?;
    Ok(())
}

pub async fn cancel_order(spec: tt_types::wire::CancelOrder, id: &ClientSubId) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::CancelOrder(spec),
            id
        )
        .await?;
    Ok(())
}

pub async fn replace_order(spec: tt_types::wire::ReplaceOrder, id: &ClientSubId) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::ReplaceOrder(spec),
            id
        )
        .await?;
    Ok(())
}

// Account interest: auto-subscribe all execution streams for an account
pub async fn activate_account_interest(
    key: tt_types::keys::AccountKey,
    id: &ClientSubId
) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::SubscribeAccount(tt_types::wire::SubscribeAccount { key }),
            id
        )
        .await?;
    Ok(())
}

pub async fn deactivate_account_interest(
    key: tt_types::keys::AccountKey,
    id: &ClientSubId,
) -> anyhow::Result<()> {
    crate::statics::bus::bus()
        .handle_request(
            tt_types::wire::Request::UnsubscribeAccount(tt_types::wire::UnsubscribeAccount {
                key,
            }),
            id,
        )
        .await?;
    Ok(())
}

/// Initialize one or more accounts at engine startup by subscribing to all
/// account-related streams (orders, positions, account events). This is a
/// convenience wrapper around `activate_account_interest`.
pub async fn initialize_accounts<I>(accounts: I, id: &ClientSubId) -> anyhow::Result<()>
where
    I: IntoIterator<Item = AccountKey>,
{
    for key in accounts {
        activate_account_interest(key, id).await?;
    }
    Ok(())
}

/// Convenience: initialize by account names for a given provider kind.
pub async fn initialize_account_names<I>(
    provider: ProviderKind,
    names: I,
    id: &ClientSubId,
) -> anyhow::Result<()>
where
    I: IntoIterator<Item = tt_types::accounts::account::AccountName>,
{
    let keys = names.into_iter().map(|account_name| AccountKey {
        provider,
        account_name,
    });
    initialize_accounts(keys, id).await
}
