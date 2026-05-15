# 00 — Executive Summary

## The problem in one sentence

Build a system that answers *"how much of token A was exchanged for token B on Avalanche C-Chain between t1 and t2?"* — for two freshness regimes (days-of-lag batch and minutes-of-lag real-time) — and design an AI-assisted workflow that turns one-off dev pipelines into production ones.

## Architecture principles

Six principles the rest of the system follows. If you only read one section, read this one.

1. **Separate facts from derived metrics.** Pool-level swap events are immutable on-chain facts. User-intent trades are *derived* — they require an algorithm with edge cases and a confidence level. The two never share a table or get summed together.
2. **Batch is the correctness authority; real-time is freshness.** Real-time is allowed to be temporarily wrong or incomplete; it is not allowed to be *silently* wrong. Nightly reconciliation re-derives from the batch source and overwrites any disagreement. Where they conflict, batch wins.
3. **Every metric carries lineage, version, and confidence.** `decoder_version`, `derivation_method`, `derivation_confidence`, `source_log_indexes`, and `price_source` are columns, not afterthoughts. A board-grade number must be reproducible from its inputs.
4. **User intent is derived carefully, not assumed from raw swaps.** Logs alone are insufficient for intent reconstruction in a world of aggregators, multicalls, and routers. The system explicitly attributes intent via aggregator events first, router-grouped pool collapse second, direct pool last — with confidence dropping accordingly.
5. **Reconciliation is part of the system, not an afterthought.** A drift number against an external source (Dune `dex.trades`, DeFiLlama) is published every night. Drift > 0.5% pages.
6. **Treasury surfaces need economic meaning, not just data availability.** "We have the data" is not the same as "the team can decide with it." Surfaces are designed around decisions (where is liquidity moving, which pools are strategic, where is LP risk rising), not around tables.

## The three decisions everything else hangs from

### 1. The unit of observation is two things, not one

A "trade" on a DEX is ambiguous. A user who swaps 100 AVAX → USDC via the LFJ aggregator may produce **one** user-intent event (the router-level fill) and **three** pool-level `Swap` events (the underlying hops). Summing `amount_usd` across all of them quadruple-counts the same $50k of capital movement.

The approach I chose is to model **two tables**:

- `dex_pool_swaps` — one row per pool-level `Swap` event. Used for **venue/pool turnover, fee accrual, liquidity utilization.**
- `dex_user_trades` — one row per user-intent (router/aggregator) fill. Used for **capital flow, user-cohort attribution, market-share between protocols.**

Pool swaps emitted as part of an aggregator route are flagged `is_aggregator_internal=true` so downstream consumers can exclude them when measuring user-facing volume.

**You never sum across the two tables.** The schema and the docs enforce this.

### 2. Avalanche finality is deterministic — design around that

Snowman consensus delivers deterministic finality in ~1.5 seconds. Once a block is accepted, practical reorg risk is negligible (no longest-chain rewrite path exists by construction). This is a categorical difference from Ethereum-style probabilistic finality, and it removes the single most expensive pattern in a production EVM pipeline:

- No N-block confirmation buffer
- No tombstone-and-rewrite logic
- No watermark juggling between "ingested" and "finalized" tables (source-delivery cursor only — see §4.2)
- Idempotency + correction-safety: `PRIMARY KEY (chain_id, tx_hash, log_index)` + `ON CONFLICT DO UPDATE WHERE excluded.decoder_version >= dex_pool_swaps.decoder_version` (identical re-runs are no-ops; corrected decoder versions overwrite)

The real-time pipeline subscribes to `finalized` blocks (or sets `allow-unfinalized-queries=false` on the node) and writes on first sight.

### 3. Boring tech for the real-time path

The operating point is a **small high-ownership team** (per the role description) at **minutes-of-lag freshness**. That is not a Kafka-and-Flink problem. The right answer is:

- Managed log stream (Goldsky Mirror **or** QuickNode Streams) → push to Postgres directly
- One long-running Python consumer for any custom decoding the managed product cannot do natively
- dbt-incremental every 1–5 minutes for aggregations and the `dex_user_trades` materialization
- A nightly batch reconciliation job re-derives the last 48h from Dune and overwrites

Total moving parts: one streaming source contract, one Python worker, one Postgres, one dbt project, one Airflow DAG. Kafka/Flink/Materialize are named as the scale-up path *if* event volume rises 50–100x, and the doc says exactly when that decision should fire.

## What I built vs. what I designed

| Component | Status |
|---|---|
| Batch ingestion of LFJ V1 Swap logs from Dune (1 day, real Avalanche data) | **Built** |
| ABI decoding of `Swap(...)` topic + hex data | **Built** |
| USD enrichment via Dune `prices.usd` join | **Built** |
| Idempotent UPSERT into DuckDB (Postgres-compatible DDL) | **Built** |
| Three live DQ checks (null-price %, ingestion lag, dedup invariant) | **Built** |
| Multi-protocol decoder dispatch (V2, V3, Curve, Balancer, GMX) | **Designed** |
| Aggregator-event decoding (Paraswap Swapped, LFJ Aggregator) | **Designed** |
| Real-time path (Goldsky Mirror → Postgres → dbt-incremental) | **Designed** |
| Consumption layer (dbt marts + Cube/Hasura semantic API) | **Designed** |
| Part 2 — AI-assisted productionization workflow | **Designed** + example artifacts |

This narrowing is deliberate. The assignment explicitly prefers a *practical, opinionated submission*, and a working prototype on one protocol with a clear design for the rest is more honest than a half-finished implementation across six.

## What I would build first with more time

In priority order:

1. **Aggregator-event decoding for LFJ Aggregator, Paraswap, 1inch on Avalanche.** This is the single most consequential gap because it gates the correctness of the `dex_user_trades` table.
2. **Reconciliation suite against Dune's `dex.trades`** (Avalanche slice) — automated daily comparison of total USD volume per (project, token_pair, hour). Any drift > 0.5% pages.
3. **The real-time path** in production. The design is complete; turning it on is a one-week project including alerting wiring.
4. **A `prices_dex` long-tail price model** — Coinpaprika/CoinGecko cover the top of the market well; the long-tail Avalanche memes/L1 tokens need DEX-implied pricing with explicit reflexivity safeguards.
5. **Part 2 system, MVP** — a single CLI that takes a `pipeline_spec.yaml`, generates the production scaffold from approved templates, and opens a PR for human review.

## Operating numbers

A treasury-grade pipeline must commit to operating numbers:

- **Batch freshness:** ≤ 6h lag at p95 (Dune materialization cadence is the bottleneck)
- **Real-time freshness:** ≤ 3 minutes lag at p95, end-to-end
- **Correctness target:** ≤ 0.5% USD-volume deviation from Dune `dex.trades` (Avalanche) on a rolling 24h window, by (project, token_pair, hour)
- **Cost ballpark:** ~$400–$900/month for the real-time path (Goldsky Mirror + db.t4g.medium Postgres + MWAA), excluding any team labour
- **On-call burden:** zero scheduled toil; <2 pages/week at steady state once DQ checks are tuned

Each of these is defensible and tied to a concrete component decision in the next section.
