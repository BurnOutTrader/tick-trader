use anyhow::{Context, anyhow};
use sqlx::{Pool, Postgres};

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

/// Best-effort: load environment variables from .env with fallbacks to examples.
fn load_env_best_effort() {
    // Try project root .env, then pg/.env, then root .env.example, then pg/.env.example
    let _ = dotenvy::from_filename(".env")
        .or_else(|_| dotenvy::from_filename("pg/.env"))
        .or_else(|_| dotenvy::from_filename(".env.example"))
        .or_else(|_| dotenvy::from_filename("pg/.env.example"));
}

/// Initialize a Postgres connection pool.
/// Note: The `data_root` path is currently ignored for Postgres, but kept for API compatibility
/// with older call sites that passed a filesystem path for DuckDB.
pub fn init_db() -> anyhow::Result<Connection> {
    // Ensure env vars are loaded from .env; if not present, fall back to .env.example
    load_env_best_effort();

    // Primary env var is DATABASE_URL. It can be either a full postgres URL or a short host-like
    // form such as "127.0.0.1:5432:5432". In the latter case we synthesize the full URL using
    // POSTGRES_USER, POSTGRES_PASSWORD and TT_DB (see pg/.env.example).
    let raw = std::env::var("DATABASE_URL").map_err(|_| {
        anyhow!(
            "DATABASE_URL not set. Ensure .env exists or copy from .env.example (or pg/.env.example)."
        )
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
