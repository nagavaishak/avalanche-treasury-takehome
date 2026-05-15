-- Schema for the prototype, intended to be Postgres-compatible.
-- DuckDB accepts most of this dialect verbatim; the few DuckDB-specific
-- conveniences (e.g., default UUID generation) are written to be portable.

CREATE TABLE IF NOT EXISTS dex_pool_swaps (
    -- identity / idempotency
    chain_id                  INTEGER      NOT NULL DEFAULT 43114,
    block_number              BIGINT       NOT NULL,
    block_time                TIMESTAMP    NOT NULL,
    tx_hash                   VARCHAR      NOT NULL,
    log_index                 INTEGER      NOT NULL,

    -- venue
    project                   VARCHAR      NOT NULL,
    version                   VARCHAR,
    pool_address              VARCHAR      NOT NULL,
    factory_address           VARCHAR,
    fee_bps                   INTEGER,

    -- economic content
    token_sold_address        VARCHAR      NOT NULL,
    token_bought_address      VARCHAR      NOT NULL,
    token_sold_asset_id       VARCHAR,
    token_bought_asset_id     VARCHAR,
    -- uint256 max is ~10^77; DECIMAL/NUMERIC max precision varies by engine.
    -- In production Postgres this is NUMERIC(78,0) (see design doc §3.3).
    -- For the prototype we store raw amounts as VARCHAR to avoid silent overflow on
    -- engines that cap NUMERIC at 38 digits (DuckDB at the time of writing). The
    -- normalized DOUBLE columns below are what queries actually read.
    token_sold_amount_raw     VARCHAR,
    token_bought_amount_raw   VARCHAR,
    token_sold_amount         DOUBLE,
    token_bought_amount       DOUBLE,
    amount_usd                DOUBLE,

    -- attribution
    taker                     VARCHAR,
    maker                     VARCHAR,
    tx_from                   VARCHAR,
    tx_to                     VARCHAR,

    -- aggregator-context
    is_aggregator_internal    BOOLEAN      NOT NULL DEFAULT FALSE,
    aggregator_project        VARCHAR,

    -- pricing provenance
    price_source              VARCHAR,
    price_staleness_seconds   INTEGER,

    -- bookkeeping
    inserted_at               TIMESTAMP    NOT NULL DEFAULT NOW(),
    decoder_version           VARCHAR      NOT NULL,

    PRIMARY KEY (chain_id, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS idx_pool_swaps_time
    ON dex_pool_swaps (block_time);
CREATE INDEX IF NOT EXISTS idx_pool_swaps_pair_time
    ON dex_pool_swaps (token_sold_asset_id, token_bought_asset_id, block_time);
CREATE INDEX IF NOT EXISTS idx_pool_swaps_project_time
    ON dex_pool_swaps (project, block_time);
CREATE INDEX IF NOT EXISTS idx_pool_swaps_pool_time
    ON dex_pool_swaps (pool_address, block_time);


CREATE TABLE IF NOT EXISTS dim_tokens (
    token_address    VARCHAR     NOT NULL PRIMARY KEY,
    asset_id         VARCHAR     NOT NULL,
    symbol           VARCHAR,
    decimals         INTEGER     NOT NULL,
    wrapped_of       VARCHAR,
    source           VARCHAR,
    first_seen       TIMESTAMP   NOT NULL DEFAULT NOW()
);

-- Seed a few well-known Avalanche tokens so the prototype can resolve symbols/decimals
-- without an additional ABI lookup. In production this table is populated by a separate
-- pipeline against the AvaLabs token list + on-chain decimals() lookups.
INSERT INTO dim_tokens (token_address, asset_id, symbol, decimals, wrapped_of, source) VALUES
    ('0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7', 'AVAX',   'WAVAX',   18, 'AVAX', 'seed'),
    ('0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e', 'USDC',   'USDC',     6, NULL,   'seed'),
    ('0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7', 'USDT',   'USDT',     6, NULL,   'seed'),
    ('0x50b7545627a5162f82a992c33b87adc75187b218', 'WBTC',   'WBTC.e',   8, 'BTC',  'seed'),
    ('0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab', 'WETH',   'WETH.e',  18, 'ETH',  'seed'),
    ('0x6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd', 'JOE',    'JOE',     18, NULL,   'seed'),
    ('0x152b9d0fdc40c096757f570a51e494bd4b943e50', 'BTC',    'BTC.b',    8, NULL,   'seed')
ON CONFLICT (token_address) DO NOTHING;
