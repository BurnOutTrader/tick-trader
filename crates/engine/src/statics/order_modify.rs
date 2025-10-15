use crate::models::Command;
use crate::statics::order_placement::{ENGINE_ORDER_ACCOUNTS, PROVIDER_ORDER_IDS};
use crate::statics::subscriptions::CMD_Q;
use anyhow::anyhow;
use dashmap::DashMap;
use std::sync::LazyLock;
use tt_types::engine_id::EngineUuid;
use tt_types::wire::{CancelOrder, ReplaceOrder};

pub(crate) static PENDING_CANCELS: LazyLock<DashMap<EngineUuid, CancelOrder>> =
    LazyLock::new(DashMap::new);

// Fire-and-forget: enqueue cancel; engine task performs I/O
pub fn cancel_order(order_id: EngineUuid) -> anyhow::Result<()> {
    // Look up provider order ID from map populated by order updates
    if let Some(pid) = PROVIDER_ORDER_IDS.get(&order_id) {
        // Resolve account from our engine_order_accounts map
        if let Some(ak) = ENGINE_ORDER_ACCOUNTS.get(&order_id) {
            let spec = tt_types::wire::CancelOrder {
                account_name: ak.account_name.clone(),
                provider_order_id: pid.value().clone(),
            };
            let _ = CMD_Q.push(Command::Cancel(spec));
            Ok(())
        } else {
            Err(anyhow!("account not known for engine order {}", order_id))
        }
    } else {
        // Provider id not yet known: record a pending cancel with full spec to be sent when first order update arrives
        if let Some(ak) = ENGINE_ORDER_ACCOUNTS.get(&order_id) {
            let spec = tt_types::wire::CancelOrder {
                account_name: ak.account_name.clone(),
                provider_order_id: String::new(),
            };
            PENDING_CANCELS.insert(order_id, spec);
            Ok(())
        } else {
            Err(anyhow!("account not known for engine order {}", order_id))
        }
    }
}

pub(crate) static PENDING_REPLACES: LazyLock<DashMap<EngineUuid, ReplaceOrder>> =
    LazyLock::new(DashMap::new);

// Fire-and-forget: enqueue replace; engine task performs I/O
pub fn replace_order(
    incoming: tt_types::wire::ReplaceOrder,
    order_id: EngineUuid,
) -> anyhow::Result<()> {
    if let Some(pid) = PROVIDER_ORDER_IDS.get(&order_id) {
        // Resolve account from our engine_order_accounts map
        if let Some(ak) = ENGINE_ORDER_ACCOUNTS.get(&order_id) {
            let spec = tt_types::wire::ReplaceOrder {
                account_name: ak.account_name.clone(),
                provider_order_id: pid.value().clone(),
                new_qty: incoming.new_qty,
                new_limit_price: incoming.new_limit_price,
                new_stop_price: incoming.new_stop_price,
                new_trail_price: incoming.new_trail_price,
            };
            let _ = CMD_Q.push(Command::Replace(spec));
            Ok(())
        } else {
            Err(anyhow!("account not known for engine order {}", order_id))
        }
    } else {
        // Provider id not yet known: record a pending replace with full spec to be sent when first order update arrives
        if let Some(ak) = ENGINE_ORDER_ACCOUNTS.get(&order_id) {
            let spec = tt_types::wire::ReplaceOrder {
                account_name: ak.account_name.clone(),
                provider_order_id: String::new(),
                new_qty: incoming.new_qty,
                new_limit_price: incoming.new_limit_price,
                new_stop_price: incoming.new_stop_price,
                new_trail_price: incoming.new_trail_price,
            };
            PENDING_REPLACES.insert(order_id, spec);
            Ok(())
        } else {
            Err(anyhow!("account not known for engine order {}", order_id))
        }
    }
}
