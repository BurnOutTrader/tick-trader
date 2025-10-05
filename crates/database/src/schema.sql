-- Sequences (DuckDB)
CREATE SEQUENCE IF NOT EXISTS providers_seq START 1;
CREATE SEQUENCE IF NOT EXISTS symbols_seq   START 1;
CREATE SEQUENCE IF NOT EXISTS datasets_seq  START 1;
CREATE SEQUENCE IF NOT EXISTS partitions_seq START 1;

-- Providers
CREATE TABLE IF NOT EXISTS providers (
                                         provider_id   BIGINT PRIMARY KEY DEFAULT nextval('providers_seq'),
    provider_code TEXT UNIQUE NOT NULL
    );

-- Symbols
CREATE TABLE IF NOT EXISTS symbols (
                                       symbol_id   BIGINT PRIMARY KEY DEFAULT nextval('symbols_seq'),
    provider_id BIGINT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    symbol_text TEXT NOT NULL,
    UNIQUE(provider_id, symbol_text)
    );

-- Datasets
-- NOTE: add resolution_key so UNIQUE doesn’t need COALESCE()
CREATE TABLE IF NOT EXISTS datasets (
                                        dataset_id  BIGINT PRIMARY KEY DEFAULT nextval('datasets_seq'),
    provider_id BIGINT NOT NULL REFERENCES providers(provider_id) ON DELETE CASCADE,
    symbol_id   BIGINT NOT NULL REFERENCES symbols(symbol_id)   ON DELETE CASCADE,
    kind        TEXT NOT NULL,     -- 'tick' | 'quote' | 'candle' | 'orderbook'
    resolution  TEXT,              -- NULL for non-candles
    resolution_key TEXT NOT NULL DEFAULT '',  -- mirror of COALESCE(resolution,'')
    UNIQUE(provider_id, symbol_id, kind, resolution_key)
    );

-- Partitions
-- Store min/max time as BIGINT ns (or ms) as you decided; example uses ns.
CREATE TABLE IF NOT EXISTS partitions (
                                          partition_id BIGINT PRIMARY KEY DEFAULT nextval('partitions_seq'),
    dataset_id   BIGINT NOT NULL REFERENCES datasets(dataset_id) ON DELETE CASCADE,
    path         TEXT NOT NULL,
    storage      TEXT NOT NULL,       -- 'parquet' | 'duckdb'
    rows         BIGINT NOT NULL,
    bytes        BIGINT NOT NULL,
    min_ts_ns    BIGINT NOT NULL,     -- use *_ns if you’re standardizing on nanoseconds
    max_ts_ns    BIGINT NOT NULL,
    min_seq      BIGINT,
    max_seq      BIGINT,
    day_key      DATE,                -- store DATE directly (no TEXT)
    created_at   TIMESTAMP DEFAULT now(),
    UNIQUE(dataset_id, path)
    );

-- Indexes
CREATE INDEX IF NOT EXISTS idx_partitions_dataset_time
    ON partitions(dataset_id, min_ts_ns, max_ts_ns);

CREATE INDEX IF NOT EXISTS idx_symbols_provider_symbol
    ON symbols(provider_id, symbol_text);

CREATE INDEX IF NOT EXISTS idx_datasets_key
    ON datasets(provider_id, symbol_id, kind, resolution_key);

CREATE INDEX IF NOT EXISTS idx_partitions_dataset_day
    ON partitions(dataset_id, day_key);