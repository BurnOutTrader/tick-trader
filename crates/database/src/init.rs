use anyhow::Context;
use sqlx::{Pool, Postgres};
use std::path::Path;

/// Shared database connection type for the project.
pub type Connection = Pool<Postgres>;

/// Initialize a Postgres connection pool.
/// Note: The `data_root` path is currently ignored for Postgres, but kept for API compatibility
/// with older call sites that passed a filesystem path for DuckDB.
pub fn init_db(_data_root: &Path) -> anyhow::Result<Connection> {
    let url = std::env::var("DATABASE_URL").context("DATABASE_URL must be set for Postgres")?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(
            std::env::var("TT_DB_MAX_CONNS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8),
        )
        .connect_lazy(&url)
        .context("failed to create Postgres pool (lazy)")?;
    Ok(pool)
}

/// Convenience for components that don't have a historical path parameter.
pub fn pool_from_env() -> anyhow::Result<Connection> {
    init_db(Path::new("/"))
}
