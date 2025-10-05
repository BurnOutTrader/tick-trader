use std::sync::Arc;
use async_trait::async_trait;
use dashmap::DashMap;
use provider::traits::{ExecutionProvider, MarketDataProvider, ProviderSessionSpec};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio_util::codec::{FramedRead, FramedWrite};
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use projectx::client::PXClient;
use tt_bus::{server, MessageBus};
use tt_bus::server::ProviderServer;
use tt_types::providers::ProviderKind;
use tt_types::wire::Envelope;
use std::collections::HashMap;
use tt_bus::traits::ProviderBootstrap;

// ProviderFactory holds concrete shared clients per provider
struct ProviderFactory {
    md_providers: DashMap<ProviderKind, Arc<dyn MarketDataProvider>>, // PXClient implements both MD and EX traits
    ex_providers: DashMap<ProviderKind, Arc<dyn ExecutionProvider>>, // PXClient implements both MD and EX traits
    md_refcount: DashMap<ProviderKind, usize>,
    ex_refcount: DashMap<ProviderKind, usize>,
}

impl ProviderFactory {
    fn new() -> Self { Self { md_providers: DashMap::new(), md_refcount: DashMap::new(), ex_providers: DashMap::new(), ex_refcount: DashMap::new() } }

    async fn ensure_md_provider(&self, session: ProviderSessionSpec, bus: Arc<MessageBus>) -> anyhow::Result<Arc<dyn MarketDataProvider>> {
        if let Some(p) = self.md_providers.get(&session.provider_kind) { return Ok(p.clone()); }
        match session.provider_kind {
            ProviderKind::ProjectX(_) => {
                let p = PXClient::new_from_session(session.clone(), bus).await?;
                p.connect_to_market(session.clone()).await?;
                let arc = Arc::new(p);
                self.ex_providers.insert(session.provider_kind, arc.clone());
                self.md_providers.insert(session.provider_kind, arc.clone());
                Ok(arc)
            }
            ProviderKind::Rithmic(_) => { anyhow::bail!("Rithmic not implemented") }
        }
    }

    async fn ensure_ex_provider(&self, session: ProviderSessionSpec, bus: Arc<MessageBus>) -> anyhow::Result<Arc<dyn ExecutionProvider>> {
        if let Some(p) = self.ex_providers.get(&session.provider_kind) {
            return Ok(p.clone());
        }
        match session.provider_kind {
            ProviderKind::ProjectX(_) => {
                let p = PXClient::new_from_session(session.clone(), bus).await?;
                p.connect_to_broker(session.clone()).await?;
                let arc = Arc::new(p);
                self.ex_providers.insert(session.provider_kind, arc.clone());
                self.md_providers.insert(session.provider_kind, arc.clone());
                Ok(arc)
            }
            ProviderKind::Rithmic(_) => { anyhow::bail!("Rithmic not implemented") }
        }
    }

    fn incr_md(&self, kind: &ProviderKind) { let mut e = self.md_refcount.entry(kind.clone()).or_insert(0); *e += 1; }
    fn decr_md(&self, kind: &ProviderKind) -> usize { if let Some(mut e) = self.md_refcount.get_mut(kind) { if *e>0 { *e -=1; } *e } else { 0 } }
    fn incr_ex(&self, kind: &ProviderKind) { let mut e = self.ex_refcount.entry(kind.clone()).or_insert(0); *e += 1; }
    fn decr_ex(&self, kind: &ProviderKind) -> usize { if let Some(mut e) = self.ex_refcount.get_mut(kind) { if *e>0 { *e -=1; } *e } else { 0 } }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Allow overriding the UDS path via env. Defaults:
    // - Linux: abstract namespace '@tick-trader.sock' (no filesystem artifact)
    // - Others: filesystem path '/tmp/tick-trader.sock'
    #[cfg(target_os = "linux")]
    let default_addr: &str = "@tick-trader.sock";
    #[cfg(not(target_os = "linux"))]
    let default_addr: &str = "/tmp/tick-trader.sock";
    // Load environment from .env if present
    let _ = dotenvy::dotenv();
    let path = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| default_addr.to_string());
    let listener = server::bind_uds(&path)?;
    eprintln!("tick-trader server listening on UDS: {}", path);
    let bus = Arc::new(MessageBus::new());
    let provider_server = ProviderServer::new(bus.clone());
    let factory = Arc::new(ProviderFactory::new());
    let default_session: ProviderSessionSpec = Default::default();

    loop {
        let (sock, _addr) = listener.accept().await?;
        let bus_clone = bus.clone();
        let provider_server = provider_server.clone();
        let factory = factory.clone();
        let default_session_clone = default_session.clone();
        tokio::spawn(async move {
            let (tx, mut rx) = mpsc::channel::<tt_types::wire::Envelope>(1024);
            let id = bus_clone.add_client(tx.clone()).await;
            let (read_half, write_half) = sock.into_split();
            let mut framed_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
            let mut framed_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());

            // writer task: drain per-subscriber queue to socket
            let writer = tokio::spawn(async move {
                while let Some(env) = rx.recv().await {
                    let vec = tt_types::wire::codec::encode(&env);
                    if let Err(e) = framed_writer.send(Bytes::from(vec)).await { let _ = e; break; }
                }
            });

            // reader loop
            while let Some(Ok(bytes)) = framed_reader.next().await {
                match tt_types::wire::codec::decode(&bytes) {
                    Ok(env) => {
                        match &env {
                            Envelope::MdSubscribe(cmd) => {
                                // lazily ensure provider exists and is attached
                                if !bus_clone.is_provider_registered(&cmd.provider) {
                                    let mut sess = default_session_clone.clone();
                                    sess.provider_kind = cmd.provider.clone();
                                    if let Ok(p) = factory.ensure_md_provider(sess.clone(), bus_clone.clone()).await {
                                        // Attach both roles using the same client
                                        let _ = provider_server.attach_provider(cmd.provider.clone(), p.clone(), p.clone(), sess).await;
                                    }
                                }
                                factory.incr_md(&cmd.provider);
                            }
                            Envelope::MdUnsubscribe(cmd) => {
                                let n = factory.decr_md(&cmd.provider);
                                if n == 0 {
                                    // Only tear down provider if there are no execution subscriptions either
                                    if let Some(p) = factory.ex_providers.get(&cmd.provider) {
                                        let ex_active = ExecutionProvider::active_account_subscriptions(p.as_ref()).await;
                                        if ex_active.is_empty() {
                                            // best-effort disconnect and unregister provider
                                            bus_clone.unregister_provider(&cmd.provider);
                                            // remove and disconnect
                                            if let Some((_, p)) = factory.md_providers.remove(&cmd.provider) {
                                                let _ = ExecutionProvider::disconnect(p.as_ref(), provider::traits::DisconnectReason::ClientRequested).await;
                                                let _ = MarketDataProvider::disconnect(p.as_ref(), provider::traits::DisconnectReason::ClientRequested).await;
                                            }
                                        } else {
                                            // keep provider alive for execution flows
                                        }
                                    } else {
                                        // No provider instance tracked; ensure it's unregistered just in case
                                        bus_clone.unregister_provider(&cmd.provider);
                                    }
                                }
                            }
                            Envelope::InstrumentsRequest(req) => {
                                if !bus_clone.is_provider_registered(&req.provider) {
                                    let mut sess = default_session_clone.clone();
                                    sess.provider_kind = req.provider.clone();
                                    if let Ok(p) = factory.ensure_provider(sess.clone(), bus_clone.clone()).await {
                                        let _ = provider_server.attach_market_data_provider(req.provider.clone(), p.clone(), sess).await;
                                    }
                                }
                            }
                            Envelope::AuthCredentials(creds) => {
                                // Build a session spec from incoming credentials and ensure provider exists
                                if !bus_clone.is_provider_registered(&creds.provider) {
                                    let mut sess = default_session_clone.clone();
                                    sess.provider_kind = creds.provider.clone();
                                    let mut map: HashMap<String, String> = HashMap::new();
                                    if let Some(u) = &creds.username { map.insert("user_name".to_string(), u.clone()); }
                                    if let Some(pw) = &creds.password { map.insert("password".to_string(), pw.clone()); }
                                    if let Some(k) = &creds.api_key { map.insert("api_key".to_string(), k.clone()); }
                                    if let Some(s) = &creds.secret { map.insert("secret".to_string(), s.clone()); }
                                    for (k, v) in &creds.extra { map.insert(k.clone(), v.clone()); }
                                    sess.creds = map;
                                    if let Ok(p) = factory.ensure_provider(sess.clone(), bus_clone.clone()).await {
                                        // Attach both roles so subsequent commands can work
                                        let _ = provider_server.attach_provider(creds.provider.clone(), p.clone(), p.clone(), sess).await;
                                    }
                                }
                            }
                            _ => {}
                        }
                        let _ = bus_clone.handle(&id, env).await;
                    }
                    Err(_) => break,
                }
            }
            let _ = writer.await;
        });
    }
}
