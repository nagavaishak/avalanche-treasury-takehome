# 03 — Cross-cutting Tradeoffs and Next Steps

A short doc on the decisions I'm *least* certain about and the order I would attack them with more time. The goal is to leave the reviewer with the same view of the risk surface that I have.

---

## 1. Tradeoffs I'm least certain about

### 1.1 Dune as the primary batch source

**The risk:** every analytics platform built on Dune eventually outgrows it. Query queueing during peak, opaque cost evolution, and the spellbook's update cadence being out of our control.

**Why I still chose it:** at v1, the time-to-first-correct-number matters more than the time-to-final-architecture. Dune gets us to "we can answer treasury questions in days" instead of "in months." The data model and the dbt project are written so that swapping Dune for BigQuery, Allium, or self-extracted parquet is a source-file change, not an architecture change.

**Trigger to revisit:** Dune queries regularly take > 60 minutes to materialize *or* monthly Dune cost crosses ~$3k *or* we need a metric the spellbook does not cover (most likely: per-block ordered traces for sandwich-attack detection).

### 1.2 Two-table model with the agg-internal flag, vs. three tables

**The alternative:** keep `dex_pool_swaps`, `dex_user_trades`, and add a third `dex_aggregator_routes` that explicitly stores the route (sequence of pool addresses, fees per hop, slippage). Allium does this.

**Why I held off:** routes are useful for MEV/slippage analysis but not for the assignment's stated question. Adding a third table doubles the surface for downstream confusion. The pool-swap rows already carry `aggregator_project` and `tx_hash`, so the route is reconstructible by `SELECT * FROM dex_pool_swaps WHERE tx_hash = X ORDER BY log_index`. Materializing it is cheap to add later.

**Trigger:** the first time someone writes the route-reconstruction query for the third time.

### 1.3 Single Postgres vs. warehouse separation

**The alternative:** OLTP Postgres for serving + a separate OLAP warehouse (Snowflake, BigQuery, ClickHouse) for analytics.

**Why I held off:** at Avalanche's current volume, a single beefy Postgres handles both. Avoiding a separate warehouse keeps the operating surface small. dbt-incremental gets us most of the columnar-ish performance gains analysts want.

**Trigger:** any of (a) `dex_pool_swaps` exceeds ~200M rows and partition pruning isn't enough, (b) a board-facing dashboard query exceeds 30s p95, or (c) the team grows past one full-time data engineer.

### 1.4 No Kafka

I made this case in §4.2 of Part 1, but it is the choice most likely to draw a senior data-engineer reviewer's pushback. I want to be explicit: if event volume scales 50x, or we need streaming joins (live MEV sandwich detection that fuses mempool + log streams), or we add five more chains and the consumer becomes the bottleneck — **then I add Kafka.** Until then, a Python worker is simpler to operate, easier to hire for, and faster to change.

### 1.5 The price source is the leakiest abstraction

Tier 1 (Coinpaprika/CoinGecko via Dune `prices.usd`) is good for top tokens; Tier 2 (DEX-implied for long tail) is good but reflexive. There's a non-zero set of mid-cap Avalanche tokens that fall between — small enough that Coinpaprika is stale, large enough that DEX-implied pricing is noisy.

**What I would do with more time:** build a `prices_dex` model that VWAP-aggregates across the top 3 deepest pools per token and exposes a `confidence` band. Surface that confidence in the user-trade table so downstream consumers can filter.

### 1.6 Cohort labels are out of scope here

Capital flow without cohort decomposition is half the answer. The wallet-labels dim is named but not built in this submission. With more time, I would integrate:

- A curated list of CEX deposit addresses (Arkham, Nansen, internal)
- Heuristic clustering for smart-money (size + frequency + win rate over a rolling window)
- MEV searcher identification (Flashbots builder, common bundle patterns)
- Contract identification (any address with > 0 bytecode at block N)

These do not need to live in our Postgres if we can join against an external service. But the *interface* — `dex_user_trades.taker_cohort` — should be ours.

---

## 2. Sequencing — what I'd do next, in priority order

| # | Build | Why it's first | Effort |
|---|---|---|---|
| 1 | Aggregator-event decoder for LFJ Aggregator, Paraswap, 1inch on Avalanche | The `dex_user_trades` table is currently fed only by direct pool swaps and pool-swap-collapse heuristics. Without aggregator-event decoding, the user-trade derivation for aggregator-routed trades is the lowest-confidence row class in the system. | ~1 week |
| 2 | Reconciliation suite against Dune `dex.trades` | Automated daily comparison, drift > 0.5% pages. This is what makes the system *trustworthy*, not just runnable. | ~3 days |
| 3 | Turn on the real-time path in production | Design is done; Goldsky pipeline declared in code, Python worker is ~150 lines, dbt-incremental schedule wired in. Operational shakedown is the time consumer. | ~1 week to "watched closely" |
| 4 | `prices_dex` long-tail price model with confidence band | Captures the bottom-half of the long tail correctly. Surface the confidence band end-to-end so analysts can filter. | ~1 week |
| 5 | Wallet labels v1 | At minimum: CEX deposit addresses + smart-money via internal heuristics + contract-vs-EOA flag. Wire into `dex_user_trades.taker_cohort`. | ~2 weeks |
| 6 | Part 2 (PromoteIt) MVP | Build the spec parser, the AST analyzer, one template (Airflow batch), and the PR generator. Manually generate the first 5 pipelines through the tool to iterate. | ~3 weeks |
| 7 | Multi-protocol decoder expansion | Uniswap V3 on Avalanche, GMX V2 (the oracle-vault model is non-trivial), Curve, Balancer. | ~2 weeks |
| 8 | MEV / sandwich-attack detection module | Detect (frontrun, victim, backrun) triples. Flag rows in `dex_pool_swaps` for downstream filtering. | ~2 weeks |

The first three items are the highest-leverage; they make the existing scope production-grade rather than expanding it.

---

## 3. The "what could go wrong" honest list

In order of how badly the reviewer can rip me on each:

1. **The aggregator double-count problem.** Already addressed front-and-center, but it's the one a senior on-chain engineer will probe deepest. The two-table model + `is_aggregator_internal` flag is the industry-correct answer; making sure the derivation algorithm gets all the edge cases right (multicall, nested router calls, partial fills) is ongoing work.
2. **Pricing for long-tail tokens.** Reflexivity is a real problem. I've named the mitigations but not built them.
3. **L1s (Subnets).** The system as designed is C-Chain only. Extending to Avalanche L1s adds N data sources, each with their own decoders. Architecture handles it; volume of work is real.
4. **GMX V1 / oracle-based DEXes.** Pricing in GMX V1 is set by a Chainlink-derived oracle inside the vault, not by an AMM curve. The "Swap" semantics are different (no token0/token1; it's a delta on the vault). The decoder needs a special case.
5. **CoW Protocol / intent-based DEXes if they expand on Avalanche.** Today they're Ethereum-mainnet-only, but if they expand the user-intent table's `derivation_method` needs a new branch.
6. **Schema evolution on production.** Every additive change is reversible; the policy of "no DROP COLUMN on live tables; add new column nullable, dual-write, migrate readers, then drop in a separate change window" needs to be enforced by the platform, not by goodwill.
7. **Recovering from a bad decoder push.** Mitigated by `decoder_version` field + scoped re-derives; never tested under fire.
8. **Dune API endpoint.** The prototype uses `POST /v1/query` (create transient query) + `/execute`. Production should switch to `POST /v1/sql/execute` for raw SQL execution — narrower API scope, no saved-query quota, simpler retry semantics. A small but visible follow-up.

---

## 4. What I learned by writing this

Two things worth saying:

- **The hardest part of this problem is the metric definition, not the infrastructure.** Anyone with an AWS account and a week can build a pipeline. Defining "capital flow" precisely, with two tables that never sum, and a derivation algorithm that handles aggregators correctly — that's where treasury-grade data work earns its keep.
- **The Part 2 system is a thinly-disguised exercise in trust calibration.** The AI is doing 80% of the work, but the design has to make sure the 20% that requires human judgment is *legibly* the human's responsibility. The PR template, the high-risk file list, and the "human must write the assertions" rule are all about making the seam between AI and human review explicit and unmissable.

Both of these are the kind of opinion you only form by writing the doc. I'd rather submit them than a more polished but less honest version.
