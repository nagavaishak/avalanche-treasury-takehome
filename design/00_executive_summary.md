# 00 — Executive Summary

## The problem in one sentence

Build a system that answers *"how much of token A was exchanged for token B on Avalanche C-Chain between t1 and t2?"* — for two freshness regimes (batch and real-time) — plus an AI-assisted workflow that turns one-off dev pipelines into production ones.

## Three decisions the rest of the design follows

### 1. Two tables, not one

A user who swaps 100 AVAX → USDC via an aggregator can produce **one** user-intent event (the router-level fill) and **three** pool-level `Swap` events (the underlying hops). Summing across all of them inflates capital flow by 3-4x.

The fix: two tables.

- `dex_pool_swaps` — one row per pool-level `Swap` event. Used for pool turnover, fee accrual, liquidity utilization.
- `dex_user_trades` — one row per user-intent fill. Used for capital flow, market share between protocols.

Pool swaps that are internal to an aggregator route are flagged `is_aggregator_internal = TRUE` so they can be excluded when measuring user-facing volume.

**The two tables are never summed together.** This is the most common DEX analytics mistake. The schema and semantic layer enforce the rule.

This is the same approach Dune (`dex.trades` + `dex_aggregator.trades`) and Allium use — converged industry practice.

### 2. Avalanche finality is deterministic

Snowman consensus finalizes blocks in ~1 second. Once a block is accepted, it cannot be reorganized — there is no longest-chain rewrite path. Standard APIs (Goldsky Mirror, QuickNode Streams, RPC) only expose finalized blocks.

What this removes from a normal EVM pipeline:
- No N-block confirmation buffer
- No reorg rollback logic
- No dual watermarks (ingested vs finalized)

What still matters:
- **Idempotency.** Streaming sources deliver at-least-once. We use `PRIMARY KEY (chain_id, tx_hash, log_index)` + `ON CONFLICT DO UPDATE WHERE excluded.decoder_version >= current.decoder_version`. Same data redelivered → no-op. Bumped decoder version → correction-safe overwrite.

### 3. Boring tech for real-time

The role is a small high-ownership team. Freshness target is minutes, not seconds. That is not a Kafka-and-Flink problem.

The choice:

- Managed log stream (Goldsky Mirror or QuickNode Streams) → push to Postgres directly
- One Python worker for custom decoding the managed product can't handle
- dbt-incremental every 1–5 minutes for aggregations
- Nightly batch reconciliation re-derives the last 48h from Dune and overwrites

Total moving parts: one streaming source, one Python worker, one Postgres, one dbt project, one Airflow DAG. Kafka/Flink come in if event volume rises 50–100x or we need streaming joins.

## What I built vs designed

| Component | Status |
|---|---|
| Batch ingestion of LFJ V1 Swap logs from Dune (1 day, real Avalanche data) | **Built** |
| ABI decoding of `Swap(...)` topic + hex data | **Built** |
| USD enrichment via Dune `prices.usd` join | **Built** |
| Idempotent UPSERT into DuckDB (Postgres-compatible DDL) | **Built** |
| Three live DQ checks (null-price %, ingestion lag, dedup invariant) | **Built** |
| Multi-protocol decoder dispatch (V2, V3, Curve, Balancer, GMX) | Designed |
| Aggregator-event decoding (Paraswap, LFJ Aggregator, 1inch) | Designed |
| Real-time path (Goldsky Mirror → Postgres → dbt-incremental) | Designed |
| Consumption layer (dbt marts + Cube semantic API) | Designed |
| Part 2 — AI-assisted productionization workflow | Designed + example artifacts |

The narrowing is deliberate. The assignment prefers a practical opinionated submission. A working prototype on three pools with a clear design for the rest is more honest than a half-finished implementation across six.

## What I'd build first with more time

1. **Aggregator-event decoding for LFJ Aggregator, Paraswap, 1inch.** Without this, aggregator-routed trades fall to medium-confidence pool-swap-collapse instead of high-confidence direct attribution.
2. **Reconciliation suite against Dune `dex.trades`.** Daily comparison per (project, token_pair, hour); drift > 0.5% pages.
3. **Real-time path in production.** Design is complete; turning it on is ~1 week including alerting.
4. **`prices_dex` long-tail price model.** VWAP across top 3 pools per token, with a confidence band.
5. **Part 2 (PromoteIt) MVP.** Spec parser + one template + PR generator.

## Operating numbers

- **Batch freshness:** ≤ 6h lag at p95 (Dune materialization cadence is the bottleneck)
- **Real-time freshness:** ≤ 3 minutes lag at p95, end-to-end
- **Correctness target:** ≤ 0.5% USD-volume deviation from Dune `dex.trades` on a rolling 24h window, by (project, token_pair, hour)
- **Cost ballpark:** ~$900–$1,800/month for the real-time path (Goldsky Mirror + db.t4g.medium Postgres + MWAA)
- **On-call burden:** <2 pages/week at steady state once DQ checks are tuned
