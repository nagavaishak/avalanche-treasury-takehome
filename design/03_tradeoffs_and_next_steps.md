# 03 — Tradeoffs and Next Steps

The decisions I'm least certain about, and the order I'd attack them with more time.

---

## 1. Tradeoffs I'm least certain about

### 1.1 Dune as the primary batch source

**Risk:** every analytics platform on Dune eventually outgrows it — query queueing during peak, opaque cost evolution, spellbook update cadence out of our control.

**Why I still chose it:** at v1, time-to-first-correct-number matters more than time-to-final-architecture. Dune gets us answering treasury questions in days instead of months. The data model is written so swapping Dune for BigQuery, Allium, or self-extracted parquet is a source-file change, not an architecture change.

**Trigger to revisit:** Dune queries regularly take > 60 minutes *or* monthly Dune cost crosses ~$3k *or* we need a metric the spellbook doesn't cover.

### 1.2 Two tables vs three tables

**Alternative:** keep `dex_pool_swaps`, `dex_user_trades`, add a third `dex_aggregator_routes` storing the route (sequence of pool addresses, fees per hop, slippage). Allium does this.

**Why I held off:** routes are useful for MEV/slippage analysis but not for the assignment's stated question. A third table doubles downstream confusion. The pool-swap rows already carry `aggregator_project` and `tx_hash`, so the route is reconstructible with `SELECT * FROM dex_pool_swaps WHERE tx_hash = X ORDER BY log_index`. Materializing it is cheap to add later.

**Trigger:** first time someone writes the route-reconstruction query a third time.

### 1.3 Single Postgres vs separate warehouse

**Alternative:** OLTP Postgres for serving + separate OLAP warehouse (Snowflake, BigQuery, ClickHouse) for analytics.

**Why I held off:** at Avalanche's current volume, a single Postgres handles both. Avoiding a separate warehouse keeps the operating surface small. dbt-incremental gives us most of the performance gains analysts want.

**Trigger:** (a) `dex_pool_swaps` exceeds ~200M rows and partition pruning isn't enough, (b) board-facing dashboard query exceeds 30s p95, or (c) team grows past one full-time data engineer.

### 1.4 No Kafka

This is the choice most likely to draw a senior data-engineer's pushback. To be explicit: if event volume scales 50x, or we need streaming joins (live MEV detection fusing mempool + log streams), or we add five more chains and the consumer becomes the bottleneck — **add Kafka.** Until then, a Python worker is simpler to operate and faster to change.

### 1.5 Price sourcing for the long tail

Tier 1 (Coinpaprika/CoinGecko) covers top tokens well. Tier 2 (DEX-implied) covers the long tail but is reflexive — using a token's own pool price to value trades in that pool is circular. There's a middle band where Coinpaprika is stale and DEX-implied is noisy.

**What I'd build with more time:** a `prices_dex` model that VWAPs across the top 3 deepest pools per token and exposes a confidence band. Surface that confidence in the user-trade table so analysts can filter.

### 1.6 Cohort labels are out of scope

Capital flow without cohort decomposition is half the answer. The wallet-labels dim is named but not built. With more time:

- A curated list of CEX deposit addresses (Arkham, Nansen, internal)
- Heuristic clustering for smart-money (size + frequency + win rate over a rolling window)
- MEV searcher identification (Flashbots builder, common bundle patterns)
- Contract identification (any address with > 0 bytecode at block N)

These don't need to live in our Postgres if we can join against an external service. But the interface — `dex_user_trades.taker_cohort` — should be ours.

---

## 2. Next steps in priority order

| # | Build | Why it's first | Effort |
|---|---|---|---|
| 1 | Aggregator-event decoder for LFJ Aggregator, Paraswap, 1inch | Without this, the user-trade derivation for aggregator-routed trades is the lowest-confidence row class | ~1 week |
| 2 | Reconciliation suite vs Dune `dex.trades` | Daily comparison, drift > 0.5% pages. Makes the system trustworthy, not just runnable. | ~3 days |
| 3 | Turn on real-time path in production | Design is done; Goldsky pipeline declared in code, worker is ~150 lines. Operational shakedown is the time consumer. | ~1 week |
| 4 | `prices_dex` long-tail model with confidence band | Captures the bottom of the long tail. Surface confidence end-to-end. | ~1 week |
| 5 | Wallet labels v1 | CEX deposit addresses + smart-money heuristics + contract-vs-EOA flag. Wire into `dex_user_trades.taker_cohort`. | ~2 weeks |
| 6 | Part 2 (PromoteIt) MVP | Spec parser, AST analyzer, one template (Airflow batch), PR generator. | ~3 weeks |
| 7 | Multi-protocol decoder expansion | Uniswap V3 on Avalanche, GMX V2 (oracle-vault model is non-trivial), Curve, Balancer. | ~2 weeks |
| 8 | MEV / sandwich-attack detection | Detect (frontrun, victim, backrun) triples. Flag rows in `dex_pool_swaps`. | ~2 weeks |

The first three are the highest-leverage — they make the existing scope production-grade rather than expanding it.

---

## 3. What could go wrong

Honest risk list, in order of how hard each would be to defend:

1. **Aggregator double-count.** Addressed front-and-center. Two-table model + `is_aggregator_internal` is the industry-correct answer. Getting all derivation edge cases right (multicall, nested router calls, partial fills) is ongoing work.
2. **Pricing for long-tail tokens.** Reflexivity is real. Mitigations named, not built.
3. **L1s (Subnets).** C-Chain only. Extending to L1s adds N data sources, each with own decoders.
4. **GMX V1 / oracle-based DEXes.** Pricing in GMX V1 is from a Chainlink-derived oracle inside the vault, not an AMM curve. Swap semantics differ — decoder needs a special case.
5. **CoW Protocol / intent-based DEXes if they expand to Avalanche.** Today Ethereum-only. If they expand, `derivation_method` needs a new branch.
6. **Schema evolution on production.** Additive-first policy needs to be enforced by platform, not goodwill.
7. **Recovering from a bad decoder push.** Mitigated by `decoder_version` + scoped re-derives; never tested under fire.
8. **Dune API endpoint.** Prototype uses `POST /v1/query` + `/execute`. Production should switch to `POST /v1/sql/execute` — narrower API scope, no saved-query quota, simpler retry. Small but visible follow-up.
