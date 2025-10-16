use crate::models::Command;
use crate::statics::subscriptions::CMD_Q;
use dashmap::DashMap;
use rust_decimal::prelude::ToPrimitive;
use std::sync::LazyLock;
use tt_types::engine_id::EngineUuid;
use tt_types::keys::AccountKey;
use tt_types::securities::symbols::Instrument;

pub(crate) static PROVIDER_ORDER_IDS: LazyLock<DashMap<EngineUuid, String>> =
    LazyLock::new(DashMap::new);
pub(crate) static ENGINE_ORDER_ACCOUNTS: LazyLock<DashMap<EngineUuid, AccountKey>> =
    LazyLock::new(DashMap::new);

#[allow(clippy::too_many_arguments)]
/// Convenience: construct a `PlaceOrder` from parameters and enqueue it.
/// This keeps strategies from having to allocate and own a full `PlaceOrder` struct.
/// Do not use +oId: as part of your order's custom tag, it is reserved for internal order management
pub fn place_order(
    account_key: tt_types::keys::AccountKey,
    instrument: Instrument,
    side: tt_types::accounts::events::Side,
    qty: i64,
    order_type: tt_types::wire::OrderType,
    limit_price: Option<rust_decimal::Decimal>,
    stop_price: Option<rust_decimal::Decimal>,
    trail_price: Option<rust_decimal::Decimal>,
    custom_tag: Option<String>,
    stop_loss: Option<tt_types::wire::BracketWire>,
    take_profit: Option<tt_types::wire::BracketWire>,
) -> anyhow::Result<EngineUuid> {
    let engine_uuid = EngineUuid::new();
    let tag = EngineUuid::append_engine_tag(custom_tag, engine_uuid);
    let account_key_clone = account_key.clone();
    // Normalize quantity sign to platform standard
    let qty_norm = side.normalize_qty(qty);
    let spec = tt_types::wire::PlaceOrder {
        account_key,
        instrument,
        side,
        qty: qty_norm,
        order_type,
        limit_price: limit_price.and_then(|d| d.to_f64()),
        stop_price: stop_price.and_then(|d| d.to_f64()),
        trail_price: trail_price.and_then(|d| d.to_f64()),
        custom_tag: Some(tag),
        stop_loss,
        take_profit,
        order_id: engine_uuid,
    };
    // Record mapping so we can cancel/replace with minimal info later
    ENGINE_ORDER_ACCOUNTS.insert(engine_uuid, account_key_clone);
    // Fire-and-forget: enqueue; engine task will send to execution provider
    let _ = CMD_Q.push(Command::Place(spec));
    Ok(engine_uuid)
}
