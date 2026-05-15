# 01 — Part 1: Capital Flow Pipeline on Avalanche C-Chain

This document covers metric definition, data sources, data model, architecture (batch and real-time), quality control, and consumption layer.

---

## 1. Metric Definition

### 1.1 What "capital flow" means

The assignment asks: *how much of token A is exchanged for token B during a given time period?* This needs precision before designing the system. The pipeline supports three definitions so consumers can pick the one that fits their question:

| Metric | Definition | Use case |
|---|---|---|
| **Gross volume (A→B)** | `Σ amount_sold` over trades where the user sold A for B | Venue market share, fee estimation |
| **Gross inflows / outflows (per token)** | Sum of buys vs sum of sells, by token | Token-level demand pressure |
| **Net directional flow (A↔B)** | `Σ(A→B notional) − Σ(B→A notional)` | One-way pressure, treasury rebalancing signals |

Gross can't distinguish a $100M churn day from a $100M one-way day. Net can't distinguish a quiet day from a balanced high-volume day. Both are exposed.

Default exposed metric: `gross_volume_usd(A→B, t1, t2)`. Cohort-decomposed metric: same gross/net pair sliced by `taker_cohort ∈ {cex_deposit, smart_money, retail, mev_bot, contract}`. Cohort labels are produced by a separate enrichment pipeline (out of scope) joined against a `wallet_labels` table.

### 1.1.1 Metric contract — what each number reads from

Every exposed metric is bound to exactly one source table. The semantic layer (§6) enforces this so dashboards cannot mix them.

| Metric | Source table | Meaning | Don't use for |
|---|---|---|---|
| `pool_volume_usd` | `dex_pool_swaps` | Pool turnover — fee accrual, market share between AMM pools | Capital flow (over-counts routed trades) |
| `user_flow_usd` | `dex_user_trades` | User-intent capital movement | Pool revenue or LP fee modeling |
| `net_flow_usd(A→B)` | `dex_user_trades` | `Σ(A→B notional) − Σ(B→A notional)` | Activity level (balanced day looks like zero) |
| `liquidity_adjusted_flow` | `dex_user_trades` ⨝ pool TVL | Flow normalized by pool depth | Cross-protocol headline numbers |

If a dashboard asks for "volume", the analyst picks one of these four. The semantic layer has no generic `volume_usd` measure — that ambiguity is what causes double-counted reports.

### 1.2 The unit of observation — and why two tables

A user swapping 100 AVAX for USDC through an aggregator can produce, in one transaction:

- 1 aggregator-level event (`Paraswap.Swapped`, `LFJAggregator.Swap`, `OneInch.Swapped`) — the user-intent fill
- 3 pool-level `Swap` events — the underlying hops the aggregator routed through

Summing `amount_usd` across all four rows counts the same $50k of capital movement 4 times. This is the most common mistake in DEX analytics.

The fix: two tables.

- `dex_pool_swaps` — one row per pool-level `Swap` event. Used for pool turnover, fee accrual, liquidity utilization, market share between AMM venues.
- `dex_user_trades` — one row per user-intent fill. Used for capital flow, user cohorts, market share between routers/aggregators.

A flag on the pool-swap row (`is_aggregator_internal`) records whether the swap was an internal hop of a multi-hop route. Direct-from-EOA pool swaps also appear in `dex_user_trades` as single-hop trades, so every dollar of user capital flow appears exactly once.

**The two tables are never summed together.** The semantic layer enforces this.

### 1.3 Wrapped tokens

WAVAX and AVAX are the same asset economically; the wrapper is just an ERC-20 envelope. For flow measurement, collapse them. For pool-liquidity measurement, keep them separate (a WAVAX/USDC pool is a different liquidity object from an AVAX/USDC pool).

The model handles this with two columns:

- `token_address` — the literal ERC-20 contract that emitted the `Transfer`
- `asset_id` — the canonical economic identity (WAVAX and AVAX both map to `asset_id = 'AVAX'`)

A `tokens` dimension table records the mapping explicitly so analysts don't reverse-engineer it from strings.

### 1.4 Pricing — and reflexivity

USD is enriched at trade time using two tiers:

1. **Tier 1 (top ~5k tokens):** Coinpaprika / CoinGecko minute-resolution feeds via Dune `prices.usd`, joined on the nearest minute prior to `block_time`. Latency ~10–60 min from real time, fine for batch. Real-time uses Chainlink price feeds where available with a short-window Coingecko cache as fallback.
2. **Tier 2 (long tail):** DEX-implied price from the deepest WAVAX, USDC, or USDT pool for the token, refreshed every 5 minutes.

**The reflexivity problem:** if a token's price is computed from its own DEX pool and you then aggregate USD volume from those same swaps, wash trading inflates both. Two safeguards:

- Sandwich-attack and self-trade detection drops MEV legs and `taker == maker` rows before USD aggregation.
- Long-tail tokens carry a `price_source = 'dex_implied'` tag; dashboards expose this so analysts can filter by confidence.

`amount_usd` is allowed to be NULL when no price is available within 15 minutes. The DQ pipeline alerts when null-price % crosses 20% in any (hour, project) cell — that's a feed issue, not a correctness issue.

### 1.5 Stated assumptions

- Avalanche **C-Chain only**. P-Chain has no DEXes; X-Chain is asset-only.
- Avalanche L1s (formerly Subnets) are out of scope for v1; the architecture extends to them by adding new ingestion sources.
- On-chain DEX swaps only. CEX deposits/withdrawals, OTC, RFQ, and bridge net flows are tracked elsewhere.
- Failed transactions excluded (EVM reverts roll back logs).
- Callback-pattern attacks (malicious tokens emitting fake `Swap`-shaped logs) mitigated by filtering log emitters to a curated `factory_allowlist`.

---

## 2. Data Sources

### 2.1 Source comparison

| Source | Type | Latency | Historical | Vendor lock-in | Cost | Use |
|---|---|---|---|---|---|---|
| **Dune** (`avalanche_c.logs`, `dex.trades`) | Batch SQL | Hours | Full | Medium (SQL portable; spellbook not) | $349–$999/mo | **Primary batch source.** Already decoded and USD-normalized. |
| **BigQuery `crypto_avalanche`** | Batch SQL | Days | Full | Low | Scan-only pricing | Cost fallback; reconciliation cross-check. |
| **Allium** (`crosschain.dex.trades`, `dex.aggregator_trades`) | Batch / streaming | Sub-min to hours | Full | Medium | $10k+/mo | Scale-up option for v2; not justified at v1. |
| **Goldsky Mirror** | Streaming → Postgres/Kafka | Sub-second | Full backfill | Medium | Worker-hours + egress | **Primary real-time source.** Managed, includes backfill. |
| **QuickNode Streams** | Webhook / push | Sub-second | Full backfill | Medium | Per-payload pricing | Equivalent alternative to Goldsky. |
| **Self-hosted AvalancheGo + `eth_subscribe`** | Streaming | Lowest possible | Archive node ~5 TB | None | $500–$2000/mo + ops | No-vendor-lock-in option. Not chosen for v1 because ops burden isn't justified at our SLO. |

### 2.2 Source redundancy

The pipeline is **two-source-redundant by design**: Batch uses Dune (primary) + BigQuery (reconciliation). Real-time uses Goldsky (primary) + self-hosted node (cold-standby). A failure of any single vendor degrades freshness but does not stop the system. Two interchangeable real-time vendors removes single-vendor risk; batch SQL is portable across Dune, BigQuery, and self-hosted Postgres.

---

## 3. Data Model

### 3.1 Tables

```
raw layer:        avalanche_c.logs            (Dune source, read-only)
                  prices.usd                   (Dune source, read-only)

staging:          stg_dex_pool_swaps          (decoded, not yet enriched)

core (the marts): dex_pool_swaps              (pool-level swap events, USD-enriched)
                  dex_user_trades             (user-intent trades, USD-enriched)
                  dim_tokens                  (token_address → asset_id, decimals, wrapped_of)
                  dim_protocols               (router/factory → project, version)
                  dim_wallet_labels           (cohort attribution; out of scope)

aggregates:       mart_capital_flow_hourly    (token_a, token_b, hour, gross_volume_usd, n_trades)
                  mart_capital_flow_daily     (rollup of hourly)
                  mart_pool_turnover_hourly   (pool_address, hour, swap_count, fee_usd)
```

### 3.2 Why this layering

- **Raw → staging → core → marts** is the standard dbt convention. Each transition is idempotent and re-derivable.
- **Wide marts** for the most-queried slice (hourly capital flow) — analysts get fast dashboards without writing JOINs.
- **Star-schema dims** for join-light access. Wallet labels live in their own dim because they change on a different cadence (weekly) than the swap data (real-time).

### 3.3 `dex_pool_swaps` schema

```sql
CREATE TABLE dex_pool_swaps (
    -- identity / idempotency
    block_number              BIGINT       NOT NULL,
    block_time                TIMESTAMPTZ  NOT NULL,
    tx_hash                   TEXT         NOT NULL,
    log_index                 INTEGER      NOT NULL,
    chain_id                  INTEGER      NOT NULL DEFAULT 43114,  -- Avalanche C-Chain

    -- venue
    project                   TEXT         NOT NULL,   -- 'lfj', 'pangolin', 'uniswap', 'curve', ...
    version                   TEXT,                    -- 'v1', 'v2', 'v3', 'stable', ...
    pool_address              TEXT         NOT NULL,
    factory_address           TEXT,                    -- for trust-model filtering
    fee_bps                   INTEGER,                 -- nullable: V3 / Balancer are dynamic

    -- economic content
    token_sold_address        TEXT         NOT NULL,
    token_bought_address      TEXT         NOT NULL,
    token_sold_asset_id       TEXT,                    -- canonical (e.g. 'AVAX' for WAVAX)
    token_bought_asset_id     TEXT,
    token_sold_amount_raw     NUMERIC(78, 0) NOT NULL, -- uint256-safe
    token_bought_amount_raw   NUMERIC(78, 0) NOT NULL,
    token_sold_amount         NUMERIC,                 -- decimal-normalized
    token_bought_amount       NUMERIC,
    amount_usd                NUMERIC,                 -- nullable when no price feed

    -- attribution
    taker                     TEXT,                    -- swap recipient (often router for routed flow)
    maker                     TEXT,                    -- emitter of the Swap event = pool
    tx_from                   TEXT NOT NULL,           -- the EOA that initiated the tx
    tx_to                     TEXT,                    -- the contract the EOA called

    -- aggregator context
    is_aggregator_internal    BOOLEAN      NOT NULL DEFAULT FALSE,
    aggregator_project        TEXT,                    -- 'paraswap', 'lfj_aggregator', '1inch', NULL

    -- pricing provenance
    price_source              TEXT,                    -- 'coinpaprika' | 'coingecko' | 'dex_implied' | NULL
    price_staleness_seconds   INTEGER,

    -- bookkeeping
    inserted_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decoder_version           TEXT NOT NULL,           -- semver of the decoder that produced this row

    PRIMARY KEY (chain_id, tx_hash, log_index)
);

CREATE INDEX idx_pool_swaps_time              ON dex_pool_swaps (block_time);
CREATE INDEX idx_pool_swaps_pair_time         ON dex_pool_swaps (token_sold_asset_id, token_bought_asset_id, block_time);
CREATE INDEX idx_pool_swaps_project_time      ON dex_pool_swaps (project, block_time);
CREATE INDEX idx_pool_swaps_pool_time         ON dex_pool_swaps (pool_address, block_time);

-- Partition by block_time, monthly. Postgres native or pg_partman.
```

Design notes:

- **`NUMERIC(78, 0)` for raw amounts.** uint256 maxes at ~78 decimal digits. Storing raw alongside normalized lets us recover exact amounts if a decimals correction is needed.
- **`amount_usd` nullable, not 0.** Distinguishes "no price" from "$0 trade".
- **`decoder_version`** lets us re-run only the rows decoded by a buggy version when a fix ships.
- **PRIMARY KEY = `(chain_id, tx_hash, log_index)`.** Globally unique because EVM log index is unique per tx and tx hashes are unique per chain. This is the idempotency key for UPSERTs.
- **Indexes on filter columns** (`block_time`, asset pair, project, pool), not on amounts. The composite index `(token_sold_asset_id, token_bought_asset_id, block_time)` is what the capital-flow query hits.

### 3.4 `dex_user_trades` schema

```sql
CREATE TABLE dex_user_trades (
    block_number              BIGINT       NOT NULL,
    block_time                TIMESTAMPTZ  NOT NULL,
    tx_hash                   TEXT         NOT NULL,
    trade_index               INTEGER      NOT NULL,  -- 0 if one trade per tx; >0 only for multicall
    chain_id                  INTEGER      NOT NULL DEFAULT 43114,

    -- router / aggregator context
    venue                     TEXT         NOT NULL,  -- 'lfj_v1', 'paraswap_v6', 'direct_pool', ...
    is_aggregator             BOOLEAN      NOT NULL,
    aggregator_project        TEXT,                   -- NULL when is_aggregator=false
    router_address            TEXT         NOT NULL,
    n_hops                    INTEGER      NOT NULL DEFAULT 1,

    -- the user intent
    taker                     TEXT NOT NULL,
    token_sold_address        TEXT NOT NULL,
    token_bought_address      TEXT NOT NULL,
    token_sold_asset_id       TEXT,
    token_bought_asset_id     TEXT,
    token_sold_amount         NUMERIC NOT NULL,
    token_bought_amount       NUMERIC NOT NULL,
    amount_usd                NUMERIC,

    -- realized economics
    effective_price           NUMERIC,
    price_impact_bps          INTEGER,
    gas_used                  BIGINT,
    gas_fee_usd               NUMERIC,

    -- derivation provenance
    derivation_method         TEXT NOT NULL,          -- 'aggregator_event' | 'pool_swap_collapse' | 'direct_pool'
    source_log_indexes        INTEGER[] NOT NULL,     -- which pool-swap log_indexes built this row
    derivation_confidence     TEXT NOT NULL,          -- 'high' | 'medium' | 'low'

    -- bookkeeping
    inserted_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    decoder_version           TEXT NOT NULL,

    PRIMARY KEY (chain_id, tx_hash, trade_index)
);
```

The key fields:

- **`derivation_method`** is the audit trail. `aggregator_event` (saw the aggregator's own log) is high-confidence; `pool_swap_collapse` (grouped same-tx pool swaps by router) is medium; `direct_pool` (single Swap, no router) is high.
- **`source_log_indexes`** records which pool-swap rows produced this user-trade row. Analysts can `JOIN dex_pool_swaps USING (tx_hash) WHERE log_index = ANY(source_log_indexes)` to see the underlying events. Every aggregated row traces back to its raw lineage.
- **`derivation_confidence`** propagates to dashboards. Board decisions use `high` only; analysts looking at long-tail aggregators see `medium` clearly labelled.

### 3.5 Aggregator handling — the algorithm

For each transaction, user-trade derivation runs in priority order:

1. **Aggregator event present** (`Paraswap.Swapped`, `LFJAggregator.Swap`, `OneInch.Swapped`, `0x.RfqOrderFilled`, `CoWSettlement.Trade`): use that event as the user trade. All pool-swap rows in the same tx are flagged `is_aggregator_internal=true`. `derivation_method='aggregator_event'`, confidence `high`.
2. **Known router but no aggregator event** (older routers, smart-contract wallets): collapse pool swaps in the tx by (router, token_sold, token_bought, taker) into one user-trade row. `derivation_method='pool_swap_collapse'`, confidence `medium`.
3. **Direct pool interaction** (`tx_to` is a pool, or any unmatched contract): emit one user-trade row per pool swap. `derivation_method='direct_pool'`, confidence `high`.

The `dim_protocols` table holds the "known aggregator" and "known router" allowlists, refreshed weekly.

---

## 4. Architecture

### 4.1 Batch pipeline

See `diagrams/batch_architecture.md`.

Components:

- **Ingestion.** Airflow DAG runs hourly. Tasks: pull decoded swaps from Dune (per protocol), pull aggregator events from Dune, pull prices. Output: parquet to S3, partitioned `dt=YYYY-MM-DD/hour=HH`.
- **Storage (raw).** S3 is the system of record. Every warehouse row is rewindable to the parquet that produced it.
- **Transformation.** dbt against Postgres (RDS Multi-AZ). Models flow: `stg_*` (one per source) → `dex_pool_swaps` (incremental merge) → `dex_user_trades` (incremental, applies derivation) → `mart_capital_flow_*` (incremental rollups).
- **Orchestration.** MWAA running Airflow. DAG: `ingest_dune >> stg_models >> core_models >> mart_models >> dq_checks >> reconciliation`. Failures halt downstream tasks and page.
- **Serving.** See §6.
- **Observability.** CloudWatch metrics on every task (duration, rows-emitted, null-price-%); alarms to PagerDuty. dbt test results in `dbt_artifacts` table.

**Key choices:** dbt over Python ETL (work is SQL-on-SQL). MWAA over self-hosted Airflow (ops burden of self-hosted exceeds what it appears). RDS Multi-AZ for automatic failover.

**Failure modes covered:**

| Failure | Mitigation |
|---|---|
| Dune returns partial results | Run idempotently; next run replays the window |
| Price feed stale | `amount_usd = NULL`, DQ alarm at >20% nulls in any (hour, project) cell |
| Duplicate rows from re-runs | `PRIMARY KEY (chain_id, tx_hash, log_index)` + `ON CONFLICT DO UPDATE` guarded by `decoder_version` |
| Wrong decimals | DQ check: `AVG(token_sold_amount) BETWEEN 1e-6 AND 1e9` per token |
| New DEX deployed mid-window | "Unknown project" alert; analyst onboards the decoder |
| Schema migration during load | Migrations are additive-first; never DROP COLUMN on a live table |
| Backfill hammers warehouse | Backfill DAG runs against a read replica; throttled concurrency |

### 4.2 Real-time pipeline

See `diagrams/realtime_architecture.md`.

Components:

- **Ingestion.** Goldsky Mirror subscribed to Avalanche C-Chain logs filtered by topic0 for every DEX `Swap` event and aggregator event. Pushes directly into Postgres.
- **Storage (raw).** Same Postgres cluster, separate schema `raw_realtime`.
- **Decode + load.** A single Python worker reads `raw_realtime.logs` using an **ingestion-sequence cursor**, not a block-height watermark. Goldsky can deliver an older block after a newer one (replay, catch-up), and a `(block_number, log_index) > watermark` filter would skip those rows.
    - `raw_realtime.logs` has `ingested_at_seq BIGSERIAL` and nullable `processed_at TIMESTAMPTZ`. Worker selects `WHERE processed_at IS NULL ORDER BY ingested_at_seq LIMIT 5000`.
    - Decode → UPSERT into `dex_pool_swaps` → `UPDATE processed_at = NOW()` — all in one transaction. Crash mid-batch rolls back both; next tick re-reads the same rows. UPSERT idempotency absorbs any redundant write.
    - Goldsky delivers **at-least-once**; the UPSERT makes the sink idempotent. The composition is effectively exactly-once at the warehouse.
    - Why a Python worker: ABI decoding of variable-length data is awkward in Postgres without an extension. The worker is ~150 lines.
- **Aggregations.** dbt-incremental every 1 minute on the marts, using `block_time > (last_max - 5 minutes)` for late arrivals.
- **Orchestration.** Goldsky pipeline declared as code. Python worker runs as a systemd service on EC2 with auto-restart. dbt-incremental runs in MWAA on a 1-minute schedule.

**Latency budget:**

- Block produced → Goldsky receives: ~1.5s (Avalanche finality)
- Goldsky → Postgres: <1s
- Python decoder catches up: <2s steady state
- dbt-incremental refresh: 60s
- **Total: ~90s end-to-end at p50, ~3min at p95**

This meets "minutes of lag at most" comfortably.

**Why no Kafka, no Flink:**

The argument for Kafka/Flink is exactly-once stateful streaming with windowed joins. Our workload has neither:

- No stateful joins — aggregations happen in dbt-incremental, idempotently, on a relational substrate.
- No exactly-once needed at the streaming layer — the warehouse layer is idempotent via PRIMARY KEY + `ON CONFLICT DO UPDATE`.
- Avalanche TPS at peak is ~100; a 10x burst is hundreds of events/sec, well within what one Python worker on Postgres handles.

We'd add Kafka if (a) event volume rises 50–100x sustained, (b) we add stateful in-flight joins (live MEV detection), or (c) we go multi-chain and the consumer becomes the bottleneck.

### 4.3 Reconciliation between batch and real-time

Real-time optimizes for freshness; batch optimizes for correctness. Where they disagree, **batch wins**.

A reconciliation DAG runs nightly at 02:00 UTC. It re-derives the last 48 hours from Dune and overwrites the corresponding rows. UPSERT is keyed on `(chain_id, tx_hash, log_index)` — strict overwrite, no orphans. Drift > 0.5% in any (project, token_pair, hour) cell pages an engineer the morning of.

This uses the same idempotency property as everywhere else. No special-case reconciliation code, just a re-run.

---

### 4.4 The control plane

The control plane is a set of small tables in the `control` schema that record, for every number the warehouse produces, the conditions under which it was produced. Without this, every disagreement about a number becomes archaeology.

| Table | What it records | Read by |
|---|---|---|
| `pipeline_runs` | DAG run id, start/end, rows in/out per task, status | Reviewers diagnosing a bad number; SLA dashboards |
| `source_freshness` | Per source: last successful pull, lag, error count last 24h | DQ checks; status page |
| `decoder_versions` | Active `decoder_version` per `(project, version)`; deployment timestamp | Decoder-scoped re-derive flow |
| `backfill_state` | Window, requested by, started, completed, rows rewritten, drift before/after | Audit trail for corrected numbers |
| `reconciliation_drift` | Per (project, token_pair, hour): drift vs Dune, drift vs DeFiLlama | Dashboards expose this as a confidence band |
| `dq_check_results` | Every DQ assertion last 7 days: passed/failed, threshold | DQ dashboard |
| `metric_versions` | Each exposed metric: SQL hash, schema, owner, last changed | Board-grade reproducibility |

The control plane answers questions like:

- *"Was the AVAX/USDC number on May 12 trustworthy?"* → `pipeline_runs` (clean run?), `dq_check_results` (all green?), `reconciliation_drift` (within 0.5%?), `decoder_versions` (no decoder bumped since?).
- *"This number changed — why?"* → `backfill_state` (window rewritten?), `decoder_versions` (decoder bumped?), `metric_versions` (definition changed?).
- *"Can I trust the real-time number right now?"* → `source_freshness` (lag), `dq_check_results` (recent failures).

---

### 4.5 Idempotency contract and failure recovery

Every write path satisfies: **re-running the same window produces the same final state.**

- S3 partitioned writes (`dt=YYYY-MM-DD/hour=HH`) are atomic per object — re-runs overwrite cleanly.
- `dex_pool_swaps` UPSERT: `ON CONFLICT (chain_id, tx_hash, log_index) DO UPDATE WHERE excluded.decoder_version >= current.decoder_version`. Same-version re-runs are no-ops. Bumped decoder versions are correction-safe overwrites.
- Real-time worker marks `processed_at` in the same transaction as the UPSERT. Crash mid-batch rolls back both; next tick re-processes the same rows.
- Failed decoder logs go to `raw_logs_quarantine` with `error_reason`. DAG never crashes on undecodable logs.

**Alerting severity:**

- P0 = correctness failures (dedup violated, drift > 5%)
- P1 = freshness failures (lag > 5min, null-price > 20%)
- P2 = operational (batch DAG failure, quarantine > 0.5%)

A lagging pipeline is annoying. A double-counted dashboard is a bad treasury decision. P0 is reserved for correctness.

---

## 5. Quality Control and Correctness

### 5.1 Strategy

Three layers, increasing in cost:

1. **Schema/contract tests** — dbt schema tests + pydantic validators on every ingestion. Caught at CI. Free.
2. **Live DQ checks** — SQL assertions run inside the DAG after every materialization. Failure halts the DAG and pages. Cheap.
3. **Reconciliation against external ground truth** — nightly comparison of our totals vs Dune `dex.trades`. Most expensive, most informative.

### 5.2 Live DQ checks

The prototype ships three. The design specifies fifteen.

```sql
-- 1. Ingestion lag
SELECT EXTRACT(EPOCH FROM (NOW() - MAX(block_time))) AS lag_seconds
FROM dex_pool_swaps;
-- alert if lag_seconds > 900 (15 min) for real-time, > 14400 (4 h) for batch

-- 2. Null-price rate
SELECT
    DATE_TRUNC('hour', block_time) AS hour,
    project,
    SUM(CASE WHEN amount_usd IS NULL THEN 1 ELSE 0 END)::float / COUNT(*) AS null_pct
FROM dex_pool_swaps
WHERE block_time > NOW() - INTERVAL '24 hours'
GROUP BY 1, 2
HAVING SUM(CASE WHEN amount_usd IS NULL THEN 1 ELSE 0 END)::float / COUNT(*) > 0.20;

-- 3. Dedup invariant — same PK must never have two rows
SELECT chain_id, tx_hash, log_index, COUNT(*) AS n
FROM dex_pool_swaps
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;
-- expected: zero rows. Any output is a P0.

-- 4. Wrong-decimals heuristic
SELECT token_sold_address, AVG(token_sold_amount) AS avg_normalized
FROM dex_pool_swaps
WHERE block_time > NOW() - INTERVAL '24 hours'
GROUP BY 1
HAVING AVG(token_sold_amount) > 1e9 OR AVG(token_sold_amount) < 1e-9;

-- 5. Aggregator double-count check
WITH user_volume AS (
    SELECT SUM(amount_usd) v FROM dex_user_trades
    WHERE block_time > NOW() - INTERVAL '24 hours'
),
non_internal_pool_volume AS (
    SELECT SUM(amount_usd) v FROM dex_pool_swaps
    WHERE block_time > NOW() - INTERVAL '24 hours' AND is_aggregator_internal = FALSE
)
SELECT ABS(user_volume.v - non_internal_pool_volume.v) / user_volume.v AS drift
FROM user_volume, non_internal_pool_volume;
-- alert if drift > 0.01
```

### 5.2.1 Business-plausibility checks

Schema-shape DQ catches broken pipelines. Business-plausibility DQ catches broken numbers from pipelines that look fine. Both classes must exist.

```sql
-- 6. Protocol disappearance: a protocol with >$1M/day median over the last 7 days
-- suddenly produces <10% of that. Catches silent decoder regressions.
WITH baseline AS (
    SELECT project, percentile_cont(0.5) WITHIN GROUP (ORDER BY daily_v) AS median_v
    FROM (
        SELECT project, DATE_TRUNC('day', block_time) d, SUM(amount_usd) daily_v
        FROM dex_pool_swaps
        WHERE block_time BETWEEN NOW() - INTERVAL '8 days' AND NOW() - INTERVAL '1 day'
          AND is_aggregator_internal = FALSE
        GROUP BY 1, 2
    ) GROUP BY 1 HAVING percentile_cont(0.5) WITHIN GROUP (ORDER BY daily_v) > 1e6
),
today AS (
    SELECT project, SUM(amount_usd) v
    FROM dex_pool_swaps
    WHERE block_time > NOW() - INTERVAL '24 hours' AND is_aggregator_internal = FALSE
    GROUP BY 1
)
SELECT baseline.project, baseline.median_v, today.v
FROM baseline LEFT JOIN today USING (project)
WHERE COALESCE(today.v, 0) < 0.1 * baseline.median_v;

-- 7. USD-vs-units divergence: if USD volume moved >30% day-over-day but token-unit
-- volume moved <5%, the price feed is misbehaving. Page.

-- 8. External reconciliation drift > 0.5% vs Dune dex.trades.
-- This is the most important DQ check; it's the only one with external ground truth.

-- 9. New-pool alert: a factory address not in the allowlist produced >$1M/day.
-- Queue item, not a page.

-- 10. Price-confidence band collapse: share of amount_usd tagged price_source='dex_implied'
-- crossed 30% of total USD volume. Engineering investigates the Tier-1 feed.
```

### 5.3 Pre-deployment tests

- **Decoder unit tests:** 30 fixture logs per protocol checked into the repo, asserted bit-for-bit against expected output.
- **Schema migration tests:** every migration runs in CI against a snapshot of production schema. Non-additive migrations require a second-engineer approval.
- **End-to-end backtest:** last 7 days, our pipeline must produce within 0.5% of Dune `dex.trades` on every (project, token_pair, hour) cell.
- **Performance tests:** hourly DAG completes in < 30 minutes against production warehouse.

### 5.4 Benchmarks

| Benchmark | Frequency | What we check |
|---|---|---|
| Dune `dex.trades` (Avalanche) | Nightly | (project, token_pair, hour) USD volume; tolerance 0.5% |
| DeFiLlama Avalanche DEX volume | Daily | Daily total volume by protocol; tolerance 1% |
| Protocol-published volumes (LFJ analytics, GMX dashboards) | Weekly | Sanity only |
| Block explorer log counts (Snowtrace) | During incidents | Per-block log count; forensic only |

The first two are wired into the DQ pipeline. The last two are runbook tools.

---

## 6. Consumption Layer

Different users need different surfaces. Treating them the same produces a layer no one is happy with.

### 6.1 Persona-driven surfaces

| Persona | Need | Surface | Example query |
|---|---|---|---|
| Treasury strategist (SQL-fluent) | Ad-hoc exploration | **BI tool** (Metabase / Hex) on Postgres marts | "Net AVAX→USDC flow by hour, last 30 days, smart-money only" |
| Quant / economist (Python-fluent) | Batch pull for modeling | **S3 parquet snapshots** + thin Python client | One year of hourly capital flow for a regime-shift study |
| Internal dashboard consumer (non-technical) | Pre-built views | **Cube / Hasura** on top of marts, Superset front-end | "How are we doing this week?" |
| External partner | Read-only API | **REST API** in front of marts, rate-limited | Polls `/capital_flow?token_a=AVAX&token_b=USDC` |

### 6.2 The semantic layer

Cube/Hasura between the warehouse and dashboards does two things:

- Enforces the "never sum across the two tables" rule at the API layer. The semantic layer exposes `pool_volume_usd` (from `dex_pool_swaps WHERE is_aggregator_internal=false`) and `user_volume_usd` (from `dex_user_trades`) as separate measures.
- Provides cache invalidation tied to mart writes — fast queries without staleness drift.

### 6.3 Self-service vs governed

The Treasury team is small, so the bar for self-service is high — analysts should answer their own questions without engineer time. The bar for governed (board reports) is also high — those numbers need provenance, version-locked SQL, and audit trails. The semantic layer handles both: a Cube-versioned model is the contract, and a board-grade metric means "this metric was version-pinned on date X."

### 6.4 Decision surfaces

Personas describe who reads the data. **Decision surfaces** describe what the team is deciding when they read it. Each surface combines a small number of metrics from the contract and surfaces control-plane confidence signals next to them.

| Decision | Surface | Metrics |
|---|---|---|
| "Where is liquidity moving this week?" | Net flow heatmap by asset pair, last 7d | `net_flow_usd` by token pair |
| "Which pools are strategically important?" | Protocol market share + volume/TVL ratio per pool | `pool_volume_usd` ⨝ pool TVL |
| "Are our incentive programs working?" | Per-incentivized-pool: volume change vs control pools | `user_flow_usd` per pool × time |
| "Where is LP risk rising?" | Slippage percentiles, liquidity depth, anomaly alerts | Price impact, pool TVL, anomaly flags |
| "Anything weird in the last 24h?" | Protocol-disappearance alerts, drift band, freshness | `control.dq_check_results` + `control.reconciliation_drift` |

Each surface is a Cube model with metrics version-pinned. The control plane records which version of which metric was on screen when a decision was made.

---

## 7. Cost ballpark

| Component | Approx cost / month |
|---|---|
| Goldsky Mirror | $250–$500 |
| RDS Postgres (db.t4g.medium, Multi-AZ, 200 GB) | $180 |
| MWAA (smallest worker tier) | $100 |
| EC2 t4g.small for Python worker | $15 |
| CloudWatch, S3, Secrets Manager | $30 |
| Dune API plan | $349–$999 |
| **Total** | **~$900–$1,800/mo** |

Single-node Postgres handles current Avalanche DEX volume comfortably. Scale-up path is in `03_tradeoffs_and_next_steps.md`.

### 7.1 Cost discipline

Three choices that keep this number low:

1. **No Kafka/Flink at v1** — saves $1.5–3k/mo, trigger conditions documented.
2. **S3 as system of record + Postgres as serving layer** — hot data (90 days) in Postgres, older partitions served from S3+Athena on demand.
3. **Goldsky over self-hosted node** — break-even on self-hosting is ~10x current Avalanche volume.

Retention: 90 days hot in Postgres, 18 months in S3 Standard, indefinite in S3 Glacier-IR.
