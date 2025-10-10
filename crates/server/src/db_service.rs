use async_trait::async_trait;
use std::sync::Arc;
use tokio::time::{Duration, timeout};
use tt_bus::DbService;
use tt_types::wire::{DbGetLatest, DbGetRange, DbGetSymbols, DbPageInfo, Response};

// Minimal DB service scaffolding that owns the single DuckDB connection.
// For now, methods return empty results or stubs while we wire the plumbing.
pub struct DuckDbService {
    // Single-writer connection guard placeholder. In future, wrap duckdb::Connection here.
    // conn: parking_lot::Mutex<duckdb::Connection>,
    max_rows: u32,
    timeout_ms: u64,
}

impl DuckDbService {
    pub fn new(max_rows: u32, timeout_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            max_rows,
            timeout_ms,
        })
    }
}

#[async_trait]
impl DbService for DuckDbService {
    async fn get_latest(&self, _req: DbGetLatest) -> anyhow::Result<Option<Response>> {
        // TODO: implement via database crate queries. For now, no-op.
        Ok(None)
    }

    async fn get_range(
        &self,
        mut req: DbGetRange,
    ) -> anyhow::Result<(Vec<Response>, Option<DbPageInfo>)> {
        // Enforce server-side row cap
        if req.limit == 0 || req.limit > self.max_rows {
            req.limit = self.max_rows;
        }
        let dur = Duration::from_millis(self.timeout_ms);
        let _ = timeout(dur, async { /* TODO: run query */ }).await;
        // Return empty page for now
        Ok((
            Vec::new(),
            Some(DbPageInfo {
                next: None,
                count: 0,
            }),
        ))
    }

    async fn get_symbols(&self, _req: DbGetSymbols) -> anyhow::Result<Response> {
        // For now, reuse InstrumentsResponse with empty list and corr_id 0
        // This keeps wire compatibility with existing clients.
        // Note: We do not require corr_id for DB path; 0 indicates none.
        use tt_types::wire::InstrumentsResponse;
        Ok(Response::InstrumentsResponse(InstrumentsResponse {
            provider: _req.provider,
            instruments: Vec::new(),
            corr_id: 0,
        }))
    }
}
