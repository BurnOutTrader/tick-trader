use dotenvy::dotenv;
use std::io;
#[cfg(target_os = "linux")]
use std::os::fd::FromRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::net::UnixListener as StdUnixListener;
use std::path::Path;
use std::sync::Arc;
use tokio::net::UnixListener;
use tracing::level_filters::LevelFilter;
use tt_bus::Router;
mod db_service;
use db_service::DuckDbService;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::INFO)
        .with_target(true)
        .with_thread_names(true)
        .with_line_number(true)
        .with_file(true)
        .with_ansi(true)
        .compact()
        .init();

    // Allow overriding the UDS path via env. Defaults:
    // - Linux: abstract namespace '@tick-trader.sock' (no filesystem artifact)
    // - Others: filesystem path '/tmp/tick-trader.sock'
    #[cfg(target_os = "linux")]
    let default_addr: &str = "@tick-trader.sock";
    #[cfg(not(target_os = "linux"))]
    let default_addr: &str = "/tmp/tick-trader.sock";
    // Load environment from .env if present
    let _ = dotenvy::dotenv();
    dotenv().ok();
    let path = std::env::var("TT_BUS_ADDR").unwrap_or_else(|_| default_addr.to_string());
    let listener = bind_uds(&path)?;
    eprintln!("tick-trader server listening on UDS: {}", path);

    // New standalone Router (initial, single-process, unsharded stub)
    let router = Arc::new(Router::new(8));
    // Wire upstream manager (providers) into the router for first/last sub notifications
    let mgr = Arc::new(tt_providers::manager::ProviderManager::new(router.clone()));

    /*for (i, c) in cs {
        let req = HistoricalRequest {
            provider_kind: ProviderKind::ProjectX(ProjectXTenant::Topstep),
            topic: Topic::Candles1m,
            instrument: i,
            exchange: c.exchange,
            start: Utc::now() - chrono::Duration::days(1500),
            end: Utc::now(),
        };
        mgr.update_historical_database(req).await?;
    }*/

    /*  let req = HistoricalRequest {
        provider_kind: ProviderKind::ProjectX(ProjectXTenant::Topstep),
        topic: Topic::Candles1m,
        instrument: Instrument::from_str("MNQZ5").unwrap(),
        exchange: Exchange::CME,
        start: Utc::now() - chrono::Duration::days(1500),
        end: Utc::now(),
    };
    mgr.update_historical_database(req).await?;*/

    router.set_backend(mgr);

    // Initialize DB service with limits/timeouts
    let max_rows: u32 = std::env::var("TT_DB_MAX_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let timeout_ms: u64 = std::env::var("TT_DB_TIMEOUT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3_000);
    let db = DuckDbService::new(max_rows, timeout_ms);
    router.set_db(db);

    // Spawn snapshot rotator (placeholder). In production, run DuckDB EXPORT and flip /data/current symlink.
    let snapshot_every_ms: u64 = std::env::var("TT_SNAPSHOT_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60_000);
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_millis(snapshot_every_ms)).await;
            tracing::debug!(
                interval_ms = snapshot_every_ms,
                "snapshot rotator tick (placeholder)"
            );
            // TODO: call DuckDB EXPORT and update symlink atomically
        }
    });

    loop {
        let (sock, _addr) = listener.accept().await?;
        let router_clone = router.clone();
        tokio::spawn(async move {
            // attach_client handles frame size cap, 15s timeout, ping/pong, unsubscribe-all on drop
            let _ = router_clone.attach_client(sock).await;
        });
    }
}
