use async_trait::async_trait;
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use projectx::client::PXClient;
use provider::traits::{ExecutionProvider, MarketDataProvider, ProviderSessionSpec};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Bytes;
use tokio_util::codec::length_delimited::LengthDelimitedCodec;
use tokio_util::codec::{FramedRead, FramedWrite};
use tt_bus::ServerMessageBus;
use tt_types::providers::ProviderKind;
use tt_types::wire::{Request, Response, WireMessage};

use log::log;
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::Path;
use tokio::net::UnixListener;



#[cfg(target_os = "linux")]
pub fn bind_uds(path: &str) -> io::Result<UnixListener> {
    // Accept both leading NUL ("\0...") and '@' as abstract namespace markers
    let is_abstract = path.starts_with('\0') || path.starts_with('@');
    if is_abstract {
        // Build sockaddr_un with abstract address
        let name = &path[1..]; // strip leading marker
        // Safety: we initialize all fields of sockaddr_un
        let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        // sun_path is i8 array on libc; first byte 0 marks abstract namespace
        let name_bytes = name.as_bytes();
        let max = addr.sun_path.len();
        if name_bytes.len() + 1 > max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "abstract UDS name too long",
            ));
        }
        addr.sun_path[0] = 0; // leading NUL for abstract
        for (i, b) in name_bytes.iter().enumerate() {
            addr.sun_path[i + 1] = *b as i8;
        }
        // Compute length: family + NUL + name
        let addr_len =
            (std::mem::size_of::<libc::sa_family_t>() + 1 + name_bytes.len()) as libc::socklen_t;

        // Create non-blocking, close-on-exec socket
        let fd = unsafe {
            libc::socket(
                libc::AF_UNIX,
                libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK,
                0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // Bind
        let rc = unsafe { libc::bind(fd, &addr as *const _ as *const libc::sockaddr, addr_len) };
        if rc != 0 {
            let e = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(e);
        }
        // Listen
        let rc = unsafe { libc::listen(fd, 1024) };
        if rc != 0 {
            let e = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            return Err(e);
        }
        // Wrap into std and then tokio
        let std_listener = unsafe { StdUnixListener::from_raw_fd(fd) };
        std_listener.set_nonblocking(true)?; // should already be nonblocking
        UnixListener::from_std(std_listener)
    } else {
        // Filesystem path: unlink then bind
        let _ = std::fs::remove_file(Path::new(path));
        UnixListener::bind(path)
    }
}

#[cfg(not(target_os = "linux"))]
pub fn bind_uds(path: &str) -> io::Result<UnixListener> {
    // On macOS and others: no abstract namespace; always use filesystem path
    // If caller passed an abstract marker, map to a tmp path for compatibility
    if path.starts_with('\0') || path.starts_with('@') {
        // Use a predictable fallback in /tmp
        let fallback = "/tmp/tick-trade.bus.sock";
        let _ = std::fs::remove_file(Path::new(fallback));
        UnixListener::bind(fallback)
    } else {
        let _ = std::fs::remove_file(Path::new(path));
        UnixListener::bind(path)
    }
}

// ProviderFactory holds concrete shared clients per provider
struct ProviderFactory {
    md_providers: DashMap<ProviderKind, Arc<dyn MarketDataProvider>>, // PXClient implements both MD and EX traits
    ex_providers: DashMap<ProviderKind, Arc<dyn ExecutionProvider>>, // PXClient implements both MD and EX traits
    md_refcount: DashMap<ProviderKind, usize>,
    ex_refcount: DashMap<ProviderKind, usize>,
}

impl ProviderFactory {
    fn new() -> Self {
        Self {
            md_providers: DashMap::new(),
            md_refcount: DashMap::new(),
            ex_providers: DashMap::new(),
            ex_refcount: DashMap::new(),
        }
    }

    async fn ensure_md_provider(
        &self,
        kind: ProviderKind,
        session: ProviderSessionSpec,
        bus: Arc<ServerMessageBus>,
    ) -> anyhow::Result<Arc<dyn MarketDataProvider>> {
        if let Some(p) = self.md_providers.get(&kind) {
            return Ok(p.clone());
        }
        match kind {
            ProviderKind::ProjectX(_) => {
                let p = PXClient::new_from_session(kind, session.clone(), bus).await?;
                p.connect_to_market(kind, session.clone()).await?;
                let arc = Arc::new(p);
                self.ex_providers.insert(kind, arc.clone());
                self.md_providers.insert(kind, arc.clone());
                Ok(arc)
            }
            ProviderKind::Rithmic(_) => {
                anyhow::bail!("Rithmic not implemented")
            }
        }
    }

    async fn ensure_ex_provider(
        &self,
        kind: ProviderKind,
        session: ProviderSessionSpec,
        bus: Arc<ServerMessageBus>,
    ) -> anyhow::Result<Arc<dyn ExecutionProvider>> {
        if let Some(p) = self.ex_providers.get(&kind) {
            return Ok(p.clone());
        }
        match kind {
            ProviderKind::ProjectX(_) => {
                let p = PXClient::new_from_session(kind, session.clone(), bus).await?;
                p.connect_to_broker(kind, session.clone()).await?;
                let arc = Arc::new(p);
                self.ex_providers.insert(kind, arc.clone());
                self.md_providers.insert(kind, arc.clone());
                Ok(arc)
            }
            ProviderKind::Rithmic(_) => {
                anyhow::bail!("Rithmic not implemented")
            }
        }
    }

    fn incr_md(&self, kind: &ProviderKind) {
        let mut e = self.md_refcount.entry(kind.clone()).or_insert(0);
        *e += 1;
    }
    fn decr_md(&self, kind: &ProviderKind) -> usize {
        if let Some(mut e) = self.md_refcount.get_mut(kind) {
            if *e > 0 {
                *e -= 1;
            }
            *e
        } else {
            0
        }
    }
    fn incr_ex(&self, kind: &ProviderKind) {
        let mut e = self.ex_refcount.entry(kind.clone()).or_insert(0);
        *e += 1;
    }
    fn decr_ex(&self, kind: &ProviderKind) -> usize {
        if let Some(mut e) = self.ex_refcount.get_mut(kind) {
            if *e > 0 {
                *e -= 1;
            }
            *e
        } else {
            0
        }
    }
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
    let listener = bind_uds(&path)?;
    eprintln!("tick-trader server listening on UDS: {}", path);
    let bus = Arc::new(ServerMessageBus::new());
    let factory = Arc::new(ProviderFactory::new());
    let default_session: ProviderSessionSpec = ProviderSessionSpec::from_env();

    loop {
        let (sock, _addr) = listener.accept().await?;
        let bus_clone = bus.clone();
        let factory = factory.clone();
        let default_session_clone = default_session.clone();
        tokio::spawn(async move {
            let (tx, mut rx) = mpsc::channel::<tt_types::wire::Response>(1024);
            let id = bus_clone.add_client(tx.clone()).await;
            let (read_half, write_half) = sock.into_split();
            let mut framed_reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
            let mut framed_writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());

            // writer task: drain per-subscriber queue to socket
            let writer = tokio::spawn(async move {
                while let Some(resp) = rx.recv().await {
                    let msg = tt_types::wire::WireMessage::Response(resp);
                    let vec = tt_types::wire::codec::encode(&msg);
                    if let Err(e) = framed_writer.send(Bytes::from(vec)).await {
                        let _ = e;
                        break;
                    }
                }
            });

            // reader loop
            while let Some(Ok(bytes)) = framed_reader.next().await {
                match tt_types::wire::codec::decode(&bytes) {
                    Ok(msg) => {
                        match &msg {
                            WireMessage::Request(Request::MdSubscribe(cmd)) => {
                                // lazily ensure provider exists and is attached
                                if !bus_clone.is_md_provider_registered(&cmd.provider) {
                                    let mut sess = default_session_clone.clone();
                                    if let Ok(md) = factory
                                        .ensure_md_provider(
                                            cmd.provider,
                                            sess.clone(),
                                            bus_clone.clone(),
                                        )
                                        .await
                                    {
                                        // Ensure execution side too, sharing the same underlying client
                                        if let Err(e) = factory
                                            .ensure_ex_provider(
                                                cmd.provider,
                                                sess.clone(),
                                                bus_clone.clone(),
                                            )
                                            .await
                                        {
                                            log!(
                                                log::Level::Error,
                                                "ensure_ex_provider failed: {}",
                                                e
                                            );
                                        }
                                    }
                                }
                                if let Some(p) = factory.md_providers.get(&cmd.provider) {
                                    match p.value().subscribe_md(cmd.topic, &cmd.key).await {
                                        Ok(_) => {}
                                        Err(e) => {
                                            log!(log::Level::Error, "subscribe_md failed: {}", e)
                                        }
                                    }
                                }
                                factory.incr_md(&cmd.provider);
                            }
                            WireMessage::Request(Request::MdUnsubscribe(cmd)) => {
                                let n = factory.decr_md(&cmd.provider);
                                if n == 0 {
                                    // Only tear down provider if there are no execution subscriptions either
                                    if let Some(p) = factory.ex_providers.get(&cmd.provider) {
                                        let ex_active =
                                            ExecutionProvider::active_account_subscriptions(
                                                p.as_ref(),
                                            )
                                            .await;
                                        if ex_active.is_empty() {
                                            // best-effort disconnect and unregister provider
                                            bus_clone.unregister_provider(&cmd.provider);
                                            // remove and disconnect (both roles)
                                            let md = factory.md_providers.remove(&cmd.provider);
                                            let ex = factory.ex_providers.remove(&cmd.provider);
                                            if let Some((_, ex_p)) = ex {
                                                let _ = ExecutionProvider::disconnect(ex_p.as_ref(), provider::traits::DisconnectReason::ClientRequested).await;
                                            }
                                            if let Some((_, md_p)) = md {
                                                let _ = MarketDataProvider::disconnect(md_p.as_ref(), provider::traits::DisconnectReason::ClientRequested).await;
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
                            WireMessage::Request(Request::InstrumentsRequest(req)) => {
                                if !bus_clone.is_md_provider_registered(&req.provider) {
                                    let mut sess = default_session_clone.clone();
                                    if let Err(e) = factory
                                        .ensure_md_provider(
                                            req.provider,
                                            sess.clone(),
                                            bus_clone.clone(),
                                        )
                                        .await
                                    {
                                        log!(log::Level::Error, "ensure_md_provider failed: {}", e);
                                    }
                                }
                            }
                            _ => {}
                        }
                        if let tt_types::wire::WireMessage::Request(rq) = msg.clone() {
                            let _ = bus_clone.handle_request(&id, rq.clone()).await;
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = writer.await;
        });
    }
}
