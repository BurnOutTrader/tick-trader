// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Live execution client implementation for the ProjectX adapter.

use std::{cell::Ref, future::Future, sync::Mutex};

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures_util::{StreamExt, pin_mut};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::{
    common::consts::PROJECT_X_VENUE,
    http::{client::PxHttpClient, credentials::PxCredential},
    websocket::{NautilusWsMessage, PxWebSocketClient},
};

#[derive(Debug)]
pub struct PxExecutionClient {
    core: ExecutionClientCore,
    http_client: PxHttpClient,
    ws_client: Option<PxWebSocketClient>,
    started: bool,
    connected: bool,
    ws_stream_handle: Option<JoinHandle<()>>,
    pending_tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl PxExecutionClient {
    /// Creates a new [`PxExecutionClient`].
    ///
    /// The ProjectX adapter uses environment variables for REST auth (see PxCredential::from_env).
    /// WebSocket (user hub) will be initialized on connect() after REST authentication.
    pub fn new(core: ExecutionClientCore) -> Result<Self> {
        // Require the ProjectX venue
        if core.venue != *PROJECT_X_VENUE {
            bail!("ProjectX execution client must use PROJECT_X venue");
        }

        let creds = PxCredential::from_env().context("Missing ProjectX env credentials")?;
        let http_client = PxHttpClient::new(creds, None, None, None, None)
            .context("failed to construct ProjectX HTTP client")?;

        Ok(Self {
            core,
            http_client,
            ws_client: None,
            started: false,
            connected: false,
            ws_stream_handle: None,
            pending_tasks: Mutex::new(Vec::new()),
        })
    }

    fn spawn_task<F>(&self, label: &'static str, fut: F)
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let handle = tokio::spawn(async move {
            if let Err(err) = fut.await {
                tracing::error!(target = "projectx.exec", "{label}: {err:?}");
            }
        });
        self.pending_tasks
            .lock()
            .expect("pending task lock poisoned")
            .push(handle);
    }

    fn abort_pending_tasks(&self) {
        let mut guard = self
            .pending_tasks
            .lock()
            .expect("pending task lock poisoned");
        for handle in guard.drain(..) {
            handle.abort();
        }
    }

    fn start_ws_stream(&mut self) -> Result<()> {
        if let Some(ws) = &mut self.ws_client {
            if self.ws_stream_handle.is_some() {
                return Ok(());
            }
            let mut stream = ws.clone().stream();
            let handle = tokio::spawn(async move {
                pin_mut!(stream);
                while let Some(message) = stream.next().await {
                    dispatch_ws_message(message);
                }
            });
            self.ws_stream_handle = Some(handle);
        }
        Ok(())
    }
}

impl ExecutionClient for PxExecutionClient {
    fn is_connected(&self) -> bool {
        self.connected
    }

    fn client_id(&self) -> nautilus_model::identifiers::ClientId {
        self.core.client_id
    }

    fn account_id(&self) -> AccountId {
        self.core.account_id
    }

    fn venue(&self) -> nautilus_model::identifiers::Venue {
        self.core.venue
    }

    fn oms_type(&self) -> nautilus_model::enums::OmsType {
        self.core.oms_type
    }

    fn get_account(&self) -> Option<nautilus_model::accounts::AccountAny> {
        self.core.get_account()
    }

    fn generate_account_state(
        &self,
        balances: Vec<nautilus_model::types::AccountBalance>,
        margins: Vec<nautilus_model::types::MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
    ) -> Result<()> {
        self.core
            .generate_account_state(balances, margins, reported, ts_event)
    }

    fn start(&mut self) -> Result<()> {
        if self.started {
            return Ok(());
        }
        self.started = true;
        info!(
            target = "projectx.exec",
            "ProjectX execution client {} started", self.core.client_id
        );
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if !self.started {
            return Ok(());
        }
        self.started = false;
        self.connected = false;
        if let Some(handle) = self.ws_stream_handle.take() {
            handle.abort();
        }
        self.abort_pending_tasks();
        info!(
            target = "projectx.exec",
            "ProjectX execution client {} stopped", self.core.client_id
        );
        Ok(())
    }

    fn submit_order(&self, cmd: &nautilus_common::messages::execution::SubmitOrder) -> Result<()> {
        let order = cmd.order.clone();
        if order.is_closed() {
            warn!("Cannot submit closed order {}", order.client_order_id());
            return Ok(());
        }

        self.core.generate_order_submitted(
            order.strategy_id(),
            order.instrument_id(),
            order.client_order_id(),
            cmd.ts_init,
        );

        // Map to ProjectX REST place order using HTTP client inner models
        let http = self.http_client.clone();
        let trader_id = self.core.trader_id;
        let strategy_id = order.strategy_id();
        let instrument_id = order.instrument_id();
        let account_id = self.core.account_id;
        let client_order_id = order.client_order_id();
        let ts_event = cmd.ts_init;

        // Extract basic params
        let side = order.order_side();
        let ord_type = order.order_type();
        let qty = order.quantity();
        let price = order.price();
        let trigger_price = order.trigger_price();

        self.spawn_task("px_submit_order", async move {
            use crate::http::models::PlaceOrderReq;
            // For ProjectX we use instrument_id string as contract_id for now.
            // Upstream already uses contract IDs like CON.F.US.MNQ.Z25 for data; execution expects the same.
            let req = PlaceOrderReq {
                // Required
                account_id: parse_account_numeric(account_id)?,
                contract_id: instrument_id.to_string(),
                side: match side.as_ref() {
                    "BUY" => 0,
                    _ => 1,
                },
                type_: map_order_type(ord_type),
                size: qty.as_f64() as i64,
                // Optional
                limit_price: price.map(|p| p.as_f64()),
                stop_price: trigger_price.map(|p| p.as_f64()),
                trail_price: None,
                custom_tag: Some(client_order_id.to_string()),
                stop_loss_bracket: None,
                take_profit_bracket: None,
            };

            match http.inner.place_order(&req).await {
                Ok(_resp) => {
                    // TODO: Convert to OrderStatusReport once mapping helpers are in place
                    tracing::info!(target = "projectx.exec", "order placed");
                }
                Err(err) => {
                    let event = OrderRejected::new(
                        trader_id,
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        account_id,
                        format!("submit-order-error: {err}").into(),
                        nautilus_core::UUID4::new(),
                        ts_event,
                        get_atomic_clock_realtime().get_time_ns(),
                        false,
                        order.is_post_only(),
                    );
                    dispatch_order_event(OrderEventAny::Rejected(event));
                }
            }
            Ok(())
        });

        Ok(())
    }

    fn submit_order_list(
        &self,
        cmd: &nautilus_common::messages::execution::SubmitOrderList,
    ) -> Result<()> {
        warn!(
            "submit_order_list not implemented for ProjectX ({} orders)",
            cmd.order_list.orders.len()
        );
        Ok(())
    }

    fn modify_order(&self, cmd: &nautilus_common::messages::execution::ModifyOrder) -> Result<()> {
        let http = self.http_client.clone();
        let instrument_id = cmd.instrument_id;
        let venue_order_id = cmd.venue_order_id;
        let client_order_id = cmd.client_order_id;
        let quantity = cmd.quantity;
        let price = cmd.price;
        let trigger_price = cmd.trigger_price;
        let account_id_num = parse_account_numeric(self.core.account_id)?;

        self.spawn_task("px_modify_order", async move {
            use crate::http::models::ModifyOrderReq;
            let req = ModifyOrderReq {
                account_id: account_id_num,
                order_id: venue_order_id.as_str().parse::<i64>().unwrap_or_default(),
                size: quantity.map(|q| q.as_f64() as i64),
                limit_price: price.map(|p| p.as_f64()),
                stop_price: trigger_price.map(|p| p.as_f64()),
                trail_price: None,
            };
            match http.inner.modify_order(&req).await {
                Ok(_resp) => {
                    tracing::info!(target = "projectx.exec", "order modified");
                }
                Err(err) => tracing::error!(target = "projectx.exec", "modify failed: {err:?}"),
            }
            Ok(())
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: &nautilus_common::messages::execution::CancelOrder) -> Result<()> {
        let http = self.http_client.clone();
        let instrument_id = cmd.instrument_id;
        let account_id = self.core.account_id;
        let order_id = cmd.venue_order_id;

        self.spawn_task("px_cancel_order", async move {
            let order_num = order_id.as_str().parse::<i64>().unwrap_or_default();
            match http
                .inner
                .cancel_order(parse_account_numeric(account_id)?, order_num)
                .await
            {
                Ok(_resp) => {
                    tracing::info!(target = "projectx.exec", "order canceled");
                }
                Err(err) => tracing::error!(target = "projectx.exec", "cancel failed: {err:?}"),
            }
            Ok(())
        });

        Ok(())
    }

    fn cancel_all_orders(
        &self,
        _cmd: &nautilus_common::messages::execution::CancelAllOrders,
    ) -> Result<()> {
        warn!("cancel_all_orders not implemented for ProjectX");
        Ok(())
    }

    fn batch_cancel_orders(
        &self,
        _cmd: &nautilus_common::messages::execution::BatchCancelOrders,
    ) -> Result<()> {
        warn!("batch_cancel_orders not implemented for ProjectX");
        Ok(())
    }

    fn query_account(
        &self,
        _cmd: &nautilus_common::messages::execution::QueryAccount,
    ) -> Result<()> {
        // We currently emit AccountState via user hub; for now no-op
        Ok(())
    }

    fn query_order(&self, _cmd: &nautilus_common::messages::execution::QueryOrder) -> Result<()> {
        warn!("query_order not implemented for ProjectX");
        Ok(())
    }
}
