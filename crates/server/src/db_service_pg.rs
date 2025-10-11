use std::sync::Arc;

use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row};
use tt_bus::DbService;
use tt_types::keys::Topic;
use tt_types::providers::ProviderKind;
use tt_types::securities::symbols::Instrument;
use tt_types::wire::{DbGetLatest, DbGetRange, DbGetSymbols, DbPageInfo, Response, BarBatch};
use tt_database::schema::{ensure_schema, get_or_create_instrument_id};

pub struct PgDbService {
    pool: Pool<Postgres>,
    max_rows: u32,
}

impl PgDbService {
    pub async fn new(database_url: &str, max_rows: u32) -> anyhow::Result<Arc<Self>> {
        let pool = Pool::<Postgres>::connect(database_url).await?;
        // Ensure schema is present
        tt_database::schema::ensure_schema(&pool).await?;
        Ok(Arc::new(Self { pool, max_rows }))
    }
}

#[async_trait]
impl DbService for PgDbService {
    async fn get_latest(&self, req: DbGetLatest) -> anyhow::Result<Option<Response>> {
        // Only Candles1m is implemented via latest_bar_1m hot table.
        if req.topic != Topic::Candles1m {
            return Ok(None);
        }
        use sqlx::Row;
        use tt_types::data::core::Candle;
        let sym = req.instrument.to_string();
        let inst_id = get_or_create_instrument_id(&self.pool, &sym).await?;
        let row = sqlx::query(
            r#"SELECT ts, open, high, low, close, volume FROM latest_bar_1m WHERE symbol_id = $1"#,
        )
        .bind(inst_id)
        .fetch_optional(&self.pool)
        .await?;
        if let Some(r) = row {
            let candle = Candle {
                ts: r.get("ts"),
                open: r.get("open"),
                high: r.get("high"),
                low: r.get("low"),
                close: r.get("close"),
                volume: r.get::<i64, _>("volume").into(),
            };
            let batch = BarBatch { topic: req.topic, seq: 0, bars: vec![candle], provider_kind: req.provider };
            return Ok(Some(Response::BarBatch(batch)));
        }
        Ok(None)
    }
    async fn get_range(
        &self,
        mut req: DbGetRange,
    ) -> anyhow::Result<(Vec<Response>, Option<DbPageInfo>)> {
        // Clamp server-side rows
        if req.limit == 0 || req.limit > self.max_rows {
            req.limit = self.max_rows;
        }
        // TODO: run SELECT ... LIMIT req.limit and build appropriate *Batch response(s)
        Ok((Vec::new(), Some(DbPageInfo { next: None, count: 0 })))
    }
    async fn get_symbols(&self, _req: DbGetSymbols) -> anyhow::Result<Response> {
        // Keep wire compatibility by returning InstrumentsResponse with corr_id 0
        use tt_types::wire::InstrumentsResponse;
        Ok(Response::InstrumentsResponse(InstrumentsResponse {
            provider: _req.provider,
            instruments: Vec::new(),
            corr_id: 0,
        }))
    }
    async fn append_ticks(&self, _batch: tt_types::wire::TickBatch) -> anyhow::Result<u32> { Ok(0) }

    async fn append_quotes(&self, _batch: tt_types::wire::QuoteBatch) -> anyhow::Result<u32> { Ok(0) }

    async fn append_bars(&self, _batch: tt_types::wire::BarBatch) -> anyhow::Result<u32> { Ok(0) }
}
