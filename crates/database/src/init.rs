use anyhow::{anyhow, Context};
use sqlx::{Pool, Postgres};
use std::path::Path;

/// Shared database connection type for the project.
pub type Connection = Pool<Postgres>;

fn synthesize_pg_url_from_hostlike(host_like: &str) -> String {
    // Accept formats: "host", "host:port", "host:host_port:container_port"
    let parts: Vec<&str> = host_like.split(':').collect();
    let (host, port) = match parts.as_slice() {
        [h, p_host, _p_container] => (*h, *p_host),
        [h, p] => (*h, *p),
        [h] => (*h, "5432"),
        _ => ("127.0.0.1", "5432"),
    };
    let user = std::env::var("POSTGRES_USER").unwrap_or_else(|_| "postgres".to_string());
    let pass = std::env::var("POSTGRES_PASSWORD")
        .unwrap_or_else(|_| "change-this-super-secret".to_string());
    let db = std::env::var("TT_DB").unwrap_or_else(|_| "tick_trader".to_string());
    format!("postgres://{}:{}@{}:{}/{}", user, pass, host, port, db)
}

/// Initialize a Postgres connection pool.
/// Note: The `data_root` path is currently ignored for Postgres, but kept for API compatibility
/// with older call sites that passed a filesystem path for DuckDB.
pub fn init_db(_data_root: &Path) -> anyhow::Result<Connection> {
    // Primary env var is DATABASE_URL. It can be either a full postgres URL or a short host-like
    // form such as "127.0.0.1:5432:5432". In the latter case we synthesize the full URL using
    // POSTGRES_USER, POSTGRES_PASSWORD and TT_DB (see pg/.env.example).
    let raw = std::env::var("DATABASE_URL").or_else(|_| {
        // Backward-compat: if DB_PATH is set, treat it the same as the short form and emit a note.
        if let Ok(v) = std::env::var("DB_PATH") {
            eprintln!("[tt-database] WARNING: DB_PATH is deprecated; use DATABASE_URL instead. Using DB_PATH value for now.");
            Ok(v)
        } else {
            Err(anyhow!("DATABASE_URL must be set for Postgres (short form like 127.0.0.1:5432:5432 is accepted)"))
        }
    })?;

    let url = if raw.contains("://") {
        raw
    } else {
        synthesize_pg_url_from_hostlike(&raw)
    };

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(
            std::env::var("TT_DB_MAX_CONNS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8),
        )
        .connect_lazy(&url)
        .with_context(|| format!("failed to create Postgres pool (lazy) for URL '{}')", url))?;
    Ok(pool)
}

/// Convenience for components that don't have a historical path parameter.
pub fn pool_from_env() -> anyhow::Result<Connection> {
    init_db(Path::new("/"))
}
