# 01 — Part 1: Capital Flow Pipeline on Avalanche C-Chain

This document covers metric definition, data sources, data model, architecture (batch and real-time), quality control, and the consumption layer.

---

## 1. Metric Definition

### 1.1 What "capital flow" actually means

The assignment phrases the question as *"how much of token A is exchanged for token B during a given time period?"* This is deliberately under-specified, and the right move is to define it precisely before designing the system. Three definitions are useful, and the system should support all three so that downstream consumers can pick the one that fits their question:

| Metric | Definition | Use case |
|---|---|---|
| **Gross volume (A→B)** | `Σ amount_sold` over trades where the user sold A for B | Venue market-share, revenue/fee estimation |
| **Gross inflows / outflows (per token)** | Sum of buys vs sum of sells, by token | Token-level demand pressure |
| **Net directional flow (A↔B)** | `Σ(A→B notional) − Σ(B→A notional)` | Detecting one-way pressure, treasury rebalancing signals |

Gross alone can't tell a $100M churn day from a $100M one-way day. Net alone can't tell a quiet day from a balanced high-volume day. You need both; dashboards expose both.

**Default exposed metric:** `gross_volume_usd(A→B, t1, t2)`. **Cohort-decomposed metric (advanced):** the same gross/net pair sliced by `taker_cohort ∈ {cex_deposit, smart_money, retail, mev_bot, contract}`. Cohort labels are produced by a separate enrichment pipeline (out of scope here) that joins against a curated `wallet_labels` table.

### 1.1.1 Metric contract — what each number is, and which table it reads from

The cardinal rule (never sum across the two tables) is enforced by binding every exposed metric to exactly one source. The semantic layer (§6) prevents dashboards from breaking this.

| Metric | Source table | Meaning | Don't use for |
|---|---|---|---|
| `pool_volume_usd` | `dex_pool_swaps` (any) | Venue/pool turnover — fee accrual, market-share between AMM pools | Capital flow (will over-count routed trades) |
| `user_flow_usd` | `dex_user_trades` | User-intent capital movement — what a person/contract actually meant to do | Pool revenue or LP fee modeling |
| `net_flow_usd(A→B)` | `dex_user_trades` | `Σ(A→B notional) − Σ(B→A notional)` over the window | Activity level (a balanced day looks like zero) |
| `liquidity_adjusted_flow` | `dex_user_trades` ⨝ pool TVL | Flow normalized by pool depth — useful when comparing pairs of very different size | Cross-protocol headline numbers |

If a dashboard asks for "volume", the analyst must pick one of these four. The semantic layer has no `volume_usd` measure on purpose — the ambiguity is what produces double-counted board reports.

### 1.2 The unit of observation — and why two tables, not one

A single user action of "swap 100 AVAX for USDC" can produce, in one transaction:

- 1 aggregator-level event (`Paraswap.Swapped`, `LFJAggregator.Swap`, `OneInch.Swapped`) — the user-intent fill
- 3 pool-level `Swap` events (the underlying hops the aggregator routed through)

Summing `amount_usd` across all four rows quadruple-counts the same $50k of capital. This is the single most common mistake in DEX analytics, and the schema below is built around preventing it.

**Resolution: two tables.**

- `dex_pool_swaps` — one row per pool-level `Swap` event. *Unit: pool venue × log.* Used to measure pool turnover, fee accrual, liquidity utilization, and protocol-level market share between AMM **venues** (LFJ pools vs Pangolin pools vs Curve pools).
- `dex_user_trades` — one row per user-intent fill. *Unit: aggregator interaction × transaction (or one-hop direct swap × transaction).* Used to measure **capital flow**, user cohorts, and the market share of routers/aggregators.

A flag on the pool-swap row (`is_aggregator_internal`) records whether the swap was an internal hop of a multi-hop aggregator route. Direct-from-EOA pool swaps are also represented in `dex_user_trades` as single-hop trades, so the user-intent table is complete: every dollar of user capital flow appears exactly once.

**The cardinal rule:** the two tables are never summed together. The semantic layer (see §6) exposes views that select from exactly one, never both.

### 1.3 Wrapped tokens and economic identity

WAVAX and AVAX are economically the same asset; the wrapper is just an ERC-20 envelope so AVAX can interoperate with token-standard contracts. For **flow** measurement, wrappers should be collapsed. For **pool-liquidity** measurement, they should be kept separate (a WAVAX/USDC pool is a different liquidity object from an AVAX/USDC pool, even if they trade the same underlying).

The data model handles this with two columns:

- `token_address` — the literal ERC-20 contract that emitted the `Transfer` (the as-emitted reality)
- `asset_id` — the canonical economic identity (e.g., WAVAX, AVAX → both `asset_id = 'AVAX'`); rebasing wrappers also normalize here

A `tokens` dimension table maps `token_address → asset_id` and records the wrapper relationship explicitly so that no analyst has to reverse-engineer it from string matching.

### 1.4 Pricing methodology — and reflexivity

USD is enriched at trade time using a two-tier source policy:

1. **Tier 1 (top ~5k tokens):** Coinpaprika / CoinGecko minute-resolution feeds, joined on the nearest minute prior to `block_time`. Same source the Dune spellbook uses. Latency: ~10–60 min from real time, which is fine for the batch pipeline; for the real-time path we use Chainlink price feeds where available and fall back to a short-window Coingecko cache.
2. **Tier 2 (long tail):** DEX-implied price from the deepest WAVAX, USDC, or USDT pool for the token, computed offline and refreshed every 5 minutes.

**The reflexivity problem:** if a token is priced from its own DEX volume and you then aggregate USD volume from those swaps, wash trading inflates both. Two safeguards:

- Sandwich-attack and self-trade detection drops MEV legs and `taker == maker` rows *before* the USD aggregation.
- Long-tail tokens carry a `price_source = 'dex_implied'` tag; downstream dashboards expose this as a column and can filter or surface it as a confidence band.

`amount_usd` is allowed to be NULL when neither source has a price within the staleness threshold (15 minutes by default). The DQ pipeline alerts when null-price % crosses 20% in any (hour, project) cell — that's a feed problem, not a data-correctness one, and it has a different runbook.

### 1.5 Stated assumptions

- Avalanche **C-Chain only**. P-Chain and X-Chain flows are out of scope (P-Chain has no DEXes; X-Chain is asset-only).
- Avalanche L1s (formerly Subnets) are out of scope for v1 but the architecture extends to them by adding new ingestion sources (§3.1).
- Only **on-chain DEX swaps** are captured. CEX deposits/withdrawals, OTC, RFQ desks, and Avalanche-bridge net flows are tracked elsewhere.
- Failed transactions are excluded (EVM reverts roll back logs).
- The callback-pattern V3 attack vector (a malicious token re-entering and emitting fake `Swap`-shaped logs) is mitigated by filtering log emitters to a curated `factory_allowlist`.

---

## 2. Data Sources

### 2.1 Source comparison

| Source | Type | Latency | Historical | Vendor lock-in | Cost | Use |
|---|---|---|---|---|---|---|
| **Dune** (`avalanche_c.logs`, decoded `dex.trades`) | Batch SQL | Hours | Full | Medium (SQL is portable; spellbook is not) | $349–$999/mo API plan | **Primary batch source for v1.** Already decoded and USD-normalized. |
| **BigQuery `crypto_avalanche` public dataset** | Batch SQL | Days | Full | Low | Scan-only pricing | Cost-control fallback; reconciliation cross-check. |
| **Allium** (`crosschain.dex.trades`, `dex.aggregator_trades`) | Batch / streaming | Sub-min to hours | Full | Medium (proprietary tables) | High-4 to mid-5 figures/mo | **Scale-up option.** Worth mentioning to show I know what production-grade looks like; not justified at v1. |
| **Goldsky Mirror** | Streaming → Postgres/Kafka | Sub-second | Full backfill | Medium | Worker-hours + egress | **Primary real-time source.** Managed, includes backfill, no infra to operate. |
| **QuickNode Streams** | Webhook / push | Sub-second | Full backfill | Medium | Per-payload pricing | Equivalent alternative to Goldsky; pick one based on existing vendor relationships. |
| **Self-hosted AvalancheGo + `eth_subscribe`** | Streaming | Lowest possible | Archive node ~5 TB | None | $500–$2000/mo infra + ops | The no-vendor-lock-in option. Documented as the fallback; not chosen for v1 because the ops burden isn't justified at our SLO. |

### 2.2 Source redundancy

The pipeline is **two-source-redundant by design**: Batch uses Dune (primary) + BigQuery (reconciliation); Real-time uses Goldsky (primary) + self-hosted node (cold-standby). A failure of any single vendor degrades freshness but does not stop the system. Two interchangeable real-time vendors means no single-vendor lock-in risk; the batch SQL is portable across Dune, BigQuery, and self-hosted Postgres.

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
                  dim_wallet_labels           (cohort attribution; out of scope here)

aggregates:       mart_capital_flow_hourly    (token_a, token_b, hour, gross_volume_usd, n_trades)
                  mart_capital_flow_daily     (rollup of hourly)
                  mart_pool_turnover_hourly   (pool_address, hour, swap_count, fee_usd)
```

### 3.2 Why this layering

- **Raw → staging → core → marts** is the boring, correct dbt convention. Each transition is idempotent and re-derivable. Reviewers will recognize it instantly.
- **Wide-table marts** for the most-queried slice (hourly capital flow) — analysts get sub-second dashboards without writing JOINs.
- **Star-schema dims** for join-light access patterns. Wallet labels live in their own dim because they evolve on a different cadence (weekly curated re-labels) than the swap data (real-time).

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

    -- aggregator-context flags
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

Design notes worth probing for:

- **`NUMERIC(78, 0)` for raw amounts.** uint256 maxes at ~78 decimal digits. Storing raw alongside normalized lets us recover exact economic quantities if a decimals correction is needed later (and it will be — wrong-decimals is failure mode #4).
- **`amount_usd` is nullable, not 0.** Distinguishing "no price" from "$0 trade" is a basic correctness requirement.
- **`decoder_version`** lets us re-run only the rows that were decoded by a buggy version when a fix ships.
- **PRIMARY KEY = `(chain_id, tx_hash, log_index)`.** Globally unique because the EVM log index is unique-per-tx and tx hashes are unique-per-chain. This is the idempotency key — UPSERTs key on this. Avalanche's deterministic finality means we don't need an N-block confirmation buffer or tombstone-and-rewrite logic; we do still need a source-delivery cursor (`processed_at` on `raw_realtime.logs` — see §4.2).
- **Indexes are on filter columns** (`block_time`, asset pair, project, pool), not on amounts. A composite index on `(token_sold_asset_id, token_bought_asset_id, block_time)` is what the capital-flow query actually hits.

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
    taker                     TEXT NOT NULL,          -- the EOA that initiated, recovered from tx_from or EIP-712 signer
    token_sold_address        TEXT NOT NULL,
    token_bought_address      TEXT NOT NULL,
    token_sold_asset_id       TEXT,
    token_bought_asset_id     TEXT,
    token_sold_amount         NUMERIC NOT NULL,
    token_bought_amount       NUMERIC NOT NULL,
    amount_usd                NUMERIC,                -- valued at trade time

    -- realized economics
    effective_price           NUMERIC,                -- token_bought / token_sold
    price_impact_bps          INTEGER,                -- vs. spot at block-1; nullable when not computable
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

The interesting fields:

- **`derivation_method`** is the audit trail. `aggregator_event` (read the aggregator's own log) is high-confidence; `pool_swap_collapse` (group same-tx pool swaps by router) is medium; `direct_pool` (one Swap, no router) is high.
- **`source_log_indexes`** records exactly which pool-swap rows fed each user-trade row. Reviewers can `JOIN dex_pool_swaps USING (tx_hash) WHERE log_index = ANY(source_log_indexes)` and see the underlying events. This is the "glass box" property — every aggregated row is reversible to its raw lineage.
- **`derivation_confidence`** propagates to downstream dashboards. Treasury decisions made on `high` only; analysts looking at long-tail aggregators see `medium` with explicit labelling.

### 3.5 Aggregator handling — the algorithm

For each transaction, the user-trade derivation runs in this priority order:

1. **If the tx emits a known aggregator event** (`Paraswap.Swapped`, `LFJAggregator.Swap`, `OneInch.Swapped`, `0x.RfqOrderFilled`, `CoWSettlement.Trade`): use that event as the user trade. All pool-swap rows in the same tx are flagged `is_aggregator_internal=true` and `aggregator_project` is filled. `derivation_method='aggregator_event'`, confidence `high`.
2. **Else if the tx_to is a known router but no aggregator event was emitted** (some older routers, some hand-rolled smart-contract wallets): collapse pool swaps in the tx by (router, token_sold, token_bought, taker) and synthesize one user-trade row. `derivation_method='pool_swap_collapse'`, confidence `medium`.
3. **Else** (the tx_to is a pool directly, or any unmatched contract): emit one user-trade row per pool swap. `derivation_method='direct_pool'`, confidence `high`.

The `wallet_labels` and `dim_protocols` tables encode the "known aggregator" and "known router" allowlists, refreshed weekly.

---

## 4. Architecture

### 4.1 Batch pipeline

See `diagrams/batch_architecture.md` for the diagram.

**Component-by-component:**

- **Ingestion.** Airflow DAG runs hourly. Each task is one of: pull-decoded-swaps-from-Dune (per protocol), pull-aggregator-events-from-Dune, pull-prices. Output: parquet to S3, partitioned by `dt=YYYY-MM-DD/hour=HH`.
- **Storage (raw).** S3 is the system of record. Every row that enters the warehouse is rewindable to the parquet that produced it.
- **Transformation.** dbt project running against Postgres (RDS Multi-AZ). Models: `stg_*` (one per source) → `dex_pool_swaps` (incremental merge by primary key) → `dex_user_trades` (incremental, applies the derivation algorithm) → `mart_capital_flow_*` (incremental rollups).
- **Orchestration.** MWAA running Airflow. The DAG looks like `ingest_dune >> stg_models >> core_models >> mart_models >> dq_checks >> reconciliation`. Failures in any task halt downstream tasks and page.
- **Serving.** Two paths — see §6.
- **Observability.** CloudWatch metrics on every task (duration, rows-emitted, null-price-%); CloudWatch alarms wired to PagerDuty. dbt test results in `dbt_artifacts` table, queryable in dashboards.

**Key choices:** dbt over Python ETL (the work is SQL-on-SQL; Pythonizing adds maintenance without buying anything). MWAA over self-hosted Airflow (ops burden of self-hosted Airflow exceeds what it appears). RDS Multi-AZ for automatic failover.

**Failure modes covered:**

| Failure | Mitigation |
|---|---|
| Dune returns partial results | Run idempotently; the next run replays the window |
| Price feed stale | `amount_usd = NULL`, DQ alarm at >20% nulls in any (hour, project) cell |
| Duplicate rows from re-runs | `PRIMARY KEY (chain_id, tx_hash, log_index)` + `ON CONFLICT DO UPDATE` guarded by `decoder_version` (no-op on identical rows, overwrite on corrected ones) |
| Wrong decimals | DQ check: `AVG(token_sold_amount) BETWEEN 1e-6 AND 1e9` per `token_address` |
| New DEX deployed mid-window | New `project` rows surface in a "unknown_project_attention" alert; analyst onboards the decoder |
| Schema migration during load | All migrations are additive-first (add column nullable → backfill → enforce constraint). Never DROP COLUMN on a live table |
| Backfill hammers the warehouse | Backfill DAG runs against a read replica; partitioned writes; throttled concurrency |

### 4.2 Real-time pipeline

See `diagrams/realtime_architecture.md`.

**Component-by-component:**

- **Ingestion.** Goldsky Mirror pipeline subscribed to (a) Avalanche C-Chain logs filtered by topic0 hashes for every DEX `Swap` event and aggregator event, (b) Avalanche C-Chain logs filtered by `Transfer` for the price-feed pool addresses we use for DEX-implied pricing. Outputs push directly into Postgres (Goldsky Mirror's first-class sink).
- **Storage (raw).** Same Postgres cluster, separate schema `raw_realtime`. Tables mirror what Goldsky pushes.
- **Decode + load.** A single Python worker (`asyncio`, one process) reads from `raw_realtime.logs` using an **ingestion-sequence cursor**, not a block-height watermark. The distinction matters: Goldsky can deliver an older block after a newer one (replay, partition catch-up, reorg-on-the-source-side), and a `(block_number, log_index) > watermark` filter would skip those rows forever.
    - `raw_realtime.logs` has a serial column `ingested_at_seq BIGSERIAL` and a nullable `processed_at TIMESTAMPTZ`. The worker selects `WHERE processed_at IS NULL ORDER BY ingested_at_seq LIMIT 5000`. This means rows are processed in arrival order regardless of their block height, and a late-arriving older block is still picked up.
    - Decode → UPSERT into `dex_pool_swaps` → `UPDATE raw_realtime.logs SET processed_at = NOW() WHERE log_id IN (...)` — **all in one transaction**. Crash mid-batch ⇒ rollback ⇒ the next tick re-reads the same rows. The UPSERT on `(chain_id, tx_hash, log_index)` is idempotent, so any redundant write produced by a retry is a no-op (or a corrective overwrite if `decoder_version` was bumped).
    - Goldsky Mirror delivers **at-least-once**; the UPSERT makes the sink idempotent. The composition is effectively exactly-once at the warehouse layer without needing exactly-once at the transport.
    - For safety against a bug that mis-marks `processed_at`, a nightly sweep re-runs anything with `processed_at < NOW() - INTERVAL '24 hours' AND block_time > NOW() - INTERVAL '48 hours'` — idempotent by construction.
    - Why a Python worker and not pure SQL: ABI decoding of variable-length data is awkward in Postgres without an extension; the worker is ~150 lines.
- **Aggregations.** dbt-incremental every 1 minute on the marts, using `block_time > (last_max - 5 minutes)` to handle any late arrivals from Goldsky.
- **Orchestration.** Three pieces. (1) The Goldsky pipeline is declared as code and version-controlled. (2) The Python worker runs as a systemd service on a small EC2 instance with auto-restart. (3) The dbt-incremental runs in MWAA on a 1-minute schedule, same DAG family as batch.
- **Serving.** Same as batch (§6); consumers do not see a different surface.

**Latency budget:**

- Block produced → Goldsky receives: ~1.5s (Avalanche finality)
- Goldsky → Postgres: <1s
- Python decoder catches up: <2s under steady state
- dbt-incremental refresh: 60s
- **Total: ~90s end-to-end at p50, ~3min at p95.**

This meets the "minutes of lag at most" requirement with comfortable headroom.

**Why no Kafka, no Flink — defended explicitly:**

The argument for Kafka/Flink is exactly-once stateful streaming with windowed joins. Our workload has neither requirement:

- We have no stateful joins (no windowed aggregations done in flight — they're all done in dbt-incremental, idempotently, on a relational substrate).
- We don't need exactly-once at the streaming layer because the warehouse layer is idempotent: PRIMARY KEY + `ON CONFLICT DO UPDATE` (with the `decoder_version` guard described in §4.5) gives us that property anyway.
- Avalanche TPS at peak is ~100; even a 10x burst is hundreds of events/sec, deep within what a single Python worker on Postgres can handle.

I would change this answer if (a) event volume rises 50–100x sustained, (b) we add stateful in-flight joins (e.g., live MEV detection that needs full mempool + log streams together), or (c) we need exactly-once on the streaming side because the warehouse stops being our consistency boundary. None of these is true at v1.

### 4.3 Reconciliation between batch and real-time

The real-time path optimizes for freshness; the batch path optimizes for correctness. Where they disagree, **batch wins**.

A reconciliation DAG runs every night at 02:00 UTC. It re-derives the last 48 hours from Dune and overwrites the corresponding rows in `dex_pool_swaps` and `dex_user_trades`. The UPSERT is keyed on `(chain_id, tx_hash, log_index)` so the operation is a strict overwrite with no orphaned rows. Reconciliation drift > 0.5% in any (project, token_pair, hour) cell pages an engineer the morning of.

This is the same idempotency property used everywhere — there is no special-case reconciliation code, just a re-run.

---

## 4.4 The control plane

A data pipeline that ships scripts and tables isn't a system; it's a job. What turns it into a system is the **control plane** — the metadata layer that records, for every number the warehouse produces, the conditions under which it was produced.

For Treasury, the output number is only useful if we can explain where it came from, what version produced it, and whether the pipeline was healthy when it ran. The control plane is what makes that explanation possible.

Concretely, the control plane is a set of small tables in the `control` schema, written to by the same Airflow DAGs that produce the data:

| Table | What it records | Read by |
|---|---|---|
| `pipeline_runs` | DAG run id, start/end, rows in/out per task, status | Reviewers diagnosing a bad number; SLA dashboards |
| `source_freshness` | Per source (Dune, Goldsky, Coinpaprika): last successful pull, lag, error count last 24h | DQ checks; status page |
| `decoder_versions` | Active `decoder_version` per `(project, version)`; deployment timestamp; rollback target | Decoder-scoped re-derive flow |
| `backfill_state` | Window, requested by, started, completed, rows rewritten, drift before/after | Audit trail for any number that was corrected |
| `reconciliation_drift` | Per (project, token_pair, hour): drift vs Dune `dex.trades`, drift vs DeFiLlama, computed nightly | Dashboards expose this as a confidence band |
| `dq_check_results` | Every DQ assertion, last 7 days: passed/failed, latency, threshold used | DQ dashboard; trigger conditions |
| `metric_versions` | Each named metric exposed to consumers: SQL hash, schema, owner, last changed | Board-grade reproducibility (replay this metric exactly as it was on date X) |

This is what lets the system answer questions that matter to a treasury team:

- *"Was the AVAX/USDC volume number on May 12 trustworthy?"* → `pipeline_runs` (clean run?), `dq_check_results` (all green?), `reconciliation_drift` (within 0.5%?), `decoder_versions` (no decoder issued between then and now?).
- *"This number changed since I looked at it yesterday — why?"* → `backfill_state` (was this window rewritten?), `decoder_versions` (decoder bumped?), `metric_versions` (definition changed?).
- *"Can I trust the real-time number right now?"* → `source_freshness` (lag), `dq_check_results` (recent failures).

The control plane is what separates an analyst-facing data warehouse from a Treasury-grade data system. Without it, every disagreement about a number becomes archaeology. With it, every number is reproducible from inputs.

---

## 4.5 Idempotency contract and failure recovery

Every write path satisfies: **re-running the same window produces the same final state.**

- S3 partitioned writes (`dt=YYYY-MM-DD/hour=HH`) are atomic per object — re-runs overwrite cleanly.
- `dex_pool_swaps` uses `ON CONFLICT (chain_id, tx_hash, log_index) DO UPDATE WHERE excluded.decoder_version >= current.decoder_version` — same-version re-runs are no-ops; bumped decoder versions are correction-safe overwrites.
- Real-time worker marks `processed_at` in the same transaction as the UPSERT — crash mid-batch rolls back both; next tick re-processes the same rows.
- Failed decoder logs go to `raw_logs_quarantine` with `error_reason`; DAG never crashes on undecodable logs.

**Alerting severity:** P0 = correctness failures (dedup violated, drift > 5%); P1 = freshness failures (lag > 5min, null-price > 20%); P2 = operational (batch DAG failure, quarantine > 0.5%). A lagging pipeline is annoying. A double-counted dashboard is a bad treasury decision.

---

## 5. Quality Control and Correctness

### 5.1 Strategy

Three layers, increasing in cost:

1. **Schema/contract tests** — dbt schema tests + pydantic validators on every ingestion. Caught at code review or in CI. Free.
2. **Live DQ checks** — SQL assertions run inside the DAG after every materialization. Failure halts the DAG and pages. Cheap.
3. **Reconciliation against external ground truth** — nightly comparison of our totals against Dune `dex.trades` (Avalanche). Most expensive (one full re-derive per night), most informative.

### 5.2 Live DQ checks (the prototype ships three; the design specifies fifteen)

```sql
-- 1. Ingestion lag
SELECT EXTRACT(EPOCH FROM (NOW() - MAX(block_time))) AS lag_seconds
FROM dex_pool_swaps;
-- alert if lag_seconds > 900 (15 min) for real-time, > 14400 (4 h) for batch

-- 2. Null-price rate
SELECT
    DATE_TRUNC('hour', block_time) AS hour,
    project,
    COUNT(*) AS total_rows,
    SUM(CASE WHEN amount_usd IS NULL THEN 1 ELSE 0 END)::float / COUNT(*) AS null_pct
FROM dex_pool_swaps
WHERE block_time > NOW() - INTERVAL '24 hours'
GROUP BY 1, 2
HAVING SUM(CASE WHEN amount_usd IS NULL THEN 1 ELSE 0 END)::float / COUNT(*) > 0.20;

-- 3. Dedup invariant — the same primary key must never have two rows
SELECT chain_id, tx_hash, log_index, COUNT(*) AS n
FROM dex_pool_swaps
GROUP BY 1, 2, 3
HAVING COUNT(*) > 1;
-- expected: zero rows. Any output is a P0.

-- 4. Wrong-decimals heuristic
SELECT
    token_sold_address,
    AVG(token_sold_amount) AS avg_normalized
FROM dex_pool_swaps
WHERE block_time > NOW() - INTERVAL '24 hours'
GROUP BY 1
HAVING AVG(token_sold_amount) > 1e9 OR AVG(token_sold_amount) < 1e-9;

-- 5. Aggregator double-count check — sum of user trades vs sum of non-aggregator pool swaps
-- should be within 1% over any 24h window
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

-- ... 10 more covering: token-set completeness, mart freshness, wallet_label staleness, etc.
```

### 5.2.1 Business-plausibility checks (the second tier)

Schema-shape DQ catches broken pipelines. **Business-plausibility DQ catches broken numbers from pipelines that look fine.** These are the checks Treasury actually cares about — they answer "is the output economically plausible?" rather than "is the table well-formed?"

```sql
-- 6. Protocol-disappearance check.
-- If a protocol that produced >$1M/day for the last 7 days suddenly produces <10% of its
-- 7d median in the current 24h, page. Catches silent decoder/factory-allowlist regressions.
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

-- 7. USD-vs-units divergence.
-- If USD volume moved >30% day-over-day but token-unit volume moved <5%, the price feed
-- is probably misbehaving and dashboards will be misleading. Page.
-- (Catches the class of bug where Tier-2 DEX-implied prices drift without warning.)

-- 8. External-source reconciliation drift.
-- Drift > 0.5% vs Dune dex.trades in any (project, token_pair, hour) over the last 24h.
-- This is the single most important DQ check; it's the only one that has an external
-- ground-truth comparison built in.

-- 9. New-pool attention alert.
-- A factory address not in the allowlist produced >$1M/day. Triage by next morning.
-- Not a page; a queue item.

-- 10. Price-confidence band collapse.
-- The share of amount_usd rows tagged price_source='dex_implied' (Tier 2, less reliable)
-- crossed 30% of total USD volume. Treasury sees this as a confidence column on
-- dashboards; engineering investigates the Tier-1 feed.
```

The distinction matters: checks 1–5 detect *broken pipelines*. Checks 6–10 detect *misleading outputs from pipelines that ran cleanly*. Both classes must exist. Most data systems only have the first class, which is why most data systems silently mislead their consumers.

### 5.3 Pre-deployment tests

- **Decoder unit tests:** 30 fixture logs per protocol (V2, V3, Curve, Balancer, GMX, Paraswap, LFJ Aggregator), checked into the repo, asserted bit-for-bit against expected decoded output. Catches every "I changed the decoder and broke an obscure case" regression.
- **Schema migration tests:** every migration runs in CI against a snapshot of production schema; if the migration is non-additive (drops or alters), the PR cannot merge without an approval from a second engineer.
- **End-to-end backtest:** for the last 7 days, our pipeline must produce within 0.5% USD volume of Dune `dex.trades` slice on every (project, token_pair, hour) cell.
- **Performance tests:** the hourly DAG must complete in < 30 minutes against the production warehouse. CI runs an abbreviated 1h slice in < 5 minutes.

### 5.4 What we benchmark against

| Benchmark | Frequency | What we check |
|---|---|---|
| Dune `dex.trades` (Avalanche) | Nightly | (project, token_pair, hour) USD volume; tolerance 0.5% |
| DeFiLlama Avalanche DEX volume | Daily | Daily total volume by protocol; tolerance 1% |
| Protocol-published volumes (LFJ analytics, GMX dashboards) | Weekly | Daily volume per protocol; sanity only |
| Block-explorer log counts (Snowtrace) | Per-block during incidents | Log count per block; used for forensic investigation only |

The first two are wired into the DQ pipeline. The last two are runbook tools for an analyst investigating a discrepancy.

---

## 6. Consumption Layer

Users are not homogeneous. Treating them so produces a layer that no one is happy with. Three personas, three surfaces:

### 6.1 Persona-driven surfaces

| Persona | Need | Surface | Example query |
|---|---|---|---|
| Treasury strategist (SQL-fluent) | Ad-hoc exploration, custom slicing | **Direct read access to Postgres marts** via a BI tool (Metabase / Hex / Lightdash) | "Show me net AVAX → USDC flow by hour for the last 30 days, smart-money only" |
| Quant / economist (Python-fluent) | Programmatic batch pull for modelling | **Snapshots to S3 parquet** + a thin Python client (`treasury_data.capital_flow(t1, t2)`) | Pull a year of hourly capital flow for a regime-shift study |
| Internal dashboard consumer (non-technical) | Pre-built views | **Cube / Hasura semantic layer on top of marts**, fronted by Superset | "How are we doing this week?" — one page, no SQL |
| External partner (lite) | Read-only API for one or two metrics | **REST API in front of the mart**, rate-limited, with API keys | A partner's risk dashboard polls our `/capital_flow?token_a=AVAX&token_b=USDC` once a minute |

### 6.2 The semantic layer is the critical seam

Putting Cube/Hasura between the warehouse and dashboards does two things that matter:

- It enforces the "never sum across the two tables" rule **at the API layer** so an analyst building a dashboard cannot accidentally double-count. The semantic layer exposes `pool_volume_usd` (from `dex_pool_swaps WHERE is_aggregator_internal=false`) and `user_volume_usd` (from `dex_user_trades`) as separate measures.
- It gives us caching that's correct: query results expire on every mart write event, so we get speed without staleness drift.

### 6.3 Self-service vs. governed

The Treasury team is small, so the bar for self-service is high — analysts should be able to answer their own questions without engineer time. The bar for governed (i.e., what shows up in board reports) is also high — those numbers need provenance, version-locked SQL, and audit trails. The semantic layer handles both: a Cube-versioned model is the contract, and a board-grade metric just means "this metric was version-pinned on date X and we replay it from that version."

### 6.4 Decision surfaces (what Treasury actually needs to look at)

Personas describe *who* reads the data; **decision surfaces** describe *what the team is deciding* when they read it. The system is designed around the latter. Each surface combines a small number of metrics from the contract above and surfaces the control-plane confidence signals next to them.

| Decision the team is making | Surface (what's on the page) | Metrics it reads |
|---|---|---|
| "Where is liquidity moving on Avalanche this week?" | Net flow heatmap by asset pair, last 7d, with directional arrows | `net_flow_usd`, sliced by token pair |
| "Which pools are strategically important to support?" | Protocol market share + volume/TVL ratio per pool, ranked | `pool_volume_usd` ⨝ pool TVL |
| "Are our incentive programs working?" | Per-incentivized-pool: volume change vs control pools since program start, with confidence band | `user_flow_usd` per pool × time-since-program |
| "Where is LP risk rising?" | Slippage/price-impact percentiles, liquidity depth, abnormal-flow alerts | Price impact (from `dex_user_trades`), pool TVL, anomaly flags |
| "Did anything weird happen in the last 24h that I need to know about?" | Single page: protocol-disappearance alerts, new-pool attentions, drift band, freshness status | All of `control.dq_check_results` + `control.reconciliation_drift` |

Each surface is a Cube model with the source metrics version-pinned. When Treasury makes a decision off one of these surfaces, the control plane (§4.4) records which version of which metric was on screen.

---

## 7. Cost ballpark

For the real-time path at current Avalanche volume:

| Component | Approx cost / month |
|---|---|
| Goldsky Mirror (Avalanche logs + price-pool logs) | $250–$500 |
| RDS Postgres (db.t4g.medium, Multi-AZ, 200 GB) | $180 |
| MWAA (smallest worker tier) | $100 |
| EC2 t4g.small for the Python worker | $15 |
| CloudWatch, S3, Secrets Manager | $30 |
| Dune API plan (for batch + reconciliation) | $349–$999 |
| **Total** | **~$900–$1800/mo** |

Single-node Postgres handles current Avalanche DEX volume comfortably. Scale-up path to read replicas + partitioning is documented in `03_tradeoffs_and_next_steps.md`.

### 7.1 Cost discipline

Three choices that keep this number low: (1) No Kafka/Flink at v1 — saves $1.5–3k/mo, trigger conditions for adding them are documented in `03_tradeoffs_and_next_steps.md`. (2) S3 as system of record + Postgres as serving layer — hot data (90 days) in Postgres, older partitions served from S3+Athena on demand, storage stays bounded. (3) Goldsky over self-hosted node — break-even on self-hosting is ~10x current Avalanche volume.

Retention: 90 days hot in Postgres, 18 months in S3 Standard, indefinite in S3 Glacier-IR.
