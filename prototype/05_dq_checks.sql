-- 05_dq_checks.sql
-- ================
-- Three live data-quality assertions. Run after every load.
-- In production these live in the dbt project as `tests/` and are run as the
-- final task in the DAG; a failure halts downstream tasks and pages.

-- ============================================================================
-- DQ-1: Ingestion lag
-- ----------------------------------------------------------------------------
-- For real-time: alert if MAX(block_time) is more than 15 min behind NOW().
-- For batch:     alert if more than 4 h behind.
-- For the prototype (running on a historical day), we just print the lag.

SELECT
    'DQ-1 ingestion_lag' AS check_name,
    MAX(block_time) AS latest_block_time,
    NOW() AS now_time,
    EXTRACT(EPOCH FROM (NOW() - MAX(block_time))) AS lag_seconds,
    COUNT(*) AS row_count
FROM dex_pool_swaps;


-- ============================================================================
-- DQ-2: Null-price rate per (hour, project)
-- ----------------------------------------------------------------------------
-- Threshold: > 20% null in any cell pages an engineer (in production).
-- For the prototype, we show the cells above the threshold; empty result = green.

SELECT
    'DQ-2 high_null_price' AS check_name,
    DATE_TRUNC('hour', block_time) AS hour,
    project,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN amount_usd IS NULL THEN 1 ELSE 0 END) AS null_rows,
    ROUND(
        SUM(CASE WHEN amount_usd IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*),
        4
    ) AS null_pct
FROM dex_pool_swaps
GROUP BY 2, 3
HAVING SUM(CASE WHEN amount_usd IS NULL THEN 1.0 ELSE 0.0 END) / COUNT(*) > 0.20
ORDER BY hour;


-- ============================================================================
-- DQ-3: Dedup invariant
-- ----------------------------------------------------------------------------
-- The primary key (chain_id, tx_hash, log_index) MUST be unique.
-- Any row in the output is a P0 — a violation means the UPSERT logic broke.

SELECT
    'DQ-3 dedup_violation' AS check_name,
    chain_id, tx_hash, log_index, COUNT(*) AS n_copies
FROM dex_pool_swaps
GROUP BY chain_id, tx_hash, log_index
HAVING COUNT(*) > 1;


-- ============================================================================
-- Bonus: a sanity readout the on-call engineer will want
-- ----------------------------------------------------------------------------
SELECT
    'summary' AS check_name,
    project,
    COUNT(*) AS swap_count,
    SUM(amount_usd) AS total_volume_usd,
    MIN(block_time) AS first_block_time,
    MAX(block_time) AS last_block_time,
    COUNT(DISTINCT pool_address) AS pool_count,
    COUNT(DISTINCT token_sold_asset_id) AS distinct_sold_assets
FROM dex_pool_swaps
GROUP BY project;
