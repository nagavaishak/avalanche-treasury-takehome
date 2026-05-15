# Avalanche Treasury — Take-Home Submission

**Candidate:** Naga
**Role:** Software Engineer, Treasury Team, Avalanche Foundation

---

## TL;DR

Two pipelines that answer *"how much of token A was exchanged for token B between t1 and t2 on Avalanche C-Chain?"* — one batch (hours of lag), one real-time (minutes of lag). Plus a workflow that uses AI to turn one-off dev pipelines into production ones.

## The eight ideas the rest of the submission builds from

1. **Two tables, never summed.** `dex_pool_swaps` (pool events) and `dex_user_trades` (user intent). Summing across them inflates capital flow by the number of hops. This is the single most common DEX analytics mistake.
2. **User intent is derived, not assumed.** Three-priority algorithm: aggregator event (high confidence) → pool swap collapse (medium) → direct pool (low). Confidence is a column, not an afterthought.
3. **Avalanche has deterministic finality.** Snowman consensus, ~1s finality. Standard APIs expose only finalized blocks. No reorg buffer, no tombstone-and-rewrite. Removes ~30% of an Ethereum pipeline's complexity.
4. **Batch is correctness, real-time is freshness.** Lambda architecture, one codebase, batch overwrites real-time nightly. When they conflict, batch wins.
5. **Boring tech for real-time.** Goldsky → Python worker → Postgres → dbt-incremental. No Kafka, no Flink. Trigger conditions for adding them are documented.
6. **Price sourcing has reflexivity safeguards.** Two-tier (Coinpaprika/CoinGecko + DEX-implied). VWAP across top 3 pools, minimum TVL threshold, confidence column for downstream filtering.
7. **The control plane separates trust from data.** Seven metadata tables track decoder versions, source freshness, reconciliation drift, DQ results — every number is reproducible from inputs.
8. **AI handles mechanical 80%, humans own semantic 20%.** PromoteIt generates scaffolding, suggests utility reuse, writes test stubs. Humans write test assertions, pick DQ thresholds, define metric meaning.

## What I built vs designed

| | Status |
|---|---|
| Batch ingestion of LFJ V1 Swap logs from Dune (real Avalanche data) | **Built** |
| ABI decoding + USD enrichment + idempotent UPSERT into DuckDB | **Built** |
| Three live DQ checks (null-price %, ingestion lag, dedup invariant) | **Built** |
| `dex_user_trades` derivation algorithm + aggregator-event decoders | Designed |
| Real-time path (Goldsky → Postgres → dbt-incremental) | Designed |
| Control plane + reconciliation against Dune | Designed |
| Part 2 (PromoteIt) | Designed + example artifacts |

The prototype covers **three LFJ V1 pools** for one day. The assignment says to prefer depth over breadth, so I built one path end-to-end and designed the rest.

---

## How to navigate

```
.
├── README.md                            ← you are here
├── design/
│   ├── 00_executive_summary.md          ← read this first
│   ├── 01_part1_capital_flow_pipeline.md
│   ├── 02_part2_ai_productionization.md
│   ├── 03_tradeoffs_and_next_steps.md
│   └── diagrams/
│       ├── batch_architecture.md
│       ├── realtime_architecture.md
│       ├── data_model.md
│       └── productionization_workflow.md
├── prototype/                            ← code snippets + schema + DQ checks
│   ├── pipeline_snippets.py             ← ingest → decode → enrich → UPSERT patterns
│   ├── schema.sql                       ← Postgres / DuckDB DDL
│   ├── 05_dq_checks.sql                 ← three live DQ checks
│   └── sample_output.csv                ← 5 rows of decoded LFJ V1 swaps
└── part2_workflow/                       ← PromoteIt example artifacts
    ├── pipeline_spec.example.yaml       ← example metadata contract
    ├── templates/airflow_dag_template.py.j2  ← Jinja2 production template
    ├── prompts/                         ← LLM prompts (template mapping + utility match)
    ├── examples/sample_pr_description.md ← what PromoteIt generates
    └── promotion_checklist.md           ← human review gates
```

**Suggested reading order:** executive summary → Part 1 design → skim prototype → Part 2 design → tradeoffs.

---

## Stack

- Python 3.11, `requests`, `eth_abi` (for ABI decoding), `duckdb` (local Postgres stand-in)
- Dune API for raw logs (same query runs on `avalanche_c.logs` in production)
- All code runs on a laptop with no AWS or Postgres setup

---

## AI usage

Used Claude as a thought partner — to challenge my metric definitions, enumerate edge cases I might have missed, and sanity-check the Avalanche finality model. The architecture choices are mine and I can defend them.
