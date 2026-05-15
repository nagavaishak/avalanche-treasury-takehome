# Avalanche Treasury — Take-Home Submission

**Candidate:** Naga
**Role:** Software Engineer, Treasury Team, Avalanche Foundation
**Time budget:** ~8 hours
**Date:** May 2026

---

## TL;DR

Two pipelines that answer *"How much of token A was exchanged for token B between t1 and t2 on Avalanche C-Chain?"* — one batch (hours of lag), one real-time (minutes of lag) — plus a workflow that uses AI to turn one-off dev pipelines into production pipelines without losing rigor.

Three core decisions that drive the rest of this submission:

1. **Two-table data model.** A `dex_pool_swaps` table at the pool-event level (each `Swap` log = one row) and a `dex_user_trades` table at the user-intent level (one router/aggregator interaction = one row). Never summed across. This is the single most common mistake in DEX analytics, and the schema is built around preventing it.
2. **Avalanche-specific architectural simplification.** Snowman consensus gives deterministic finality in ~1.5s. Sources expose finalized blocks by default, so we don't need an Ethereum-style N-block confirmation buffer or tombstone-and-rewrite logic — though we still need idempotent replay and source-delivery watermarks (handled by the PK on `(chain_id, tx_hash, log_index)` and the ingestion-sequence cursor described in the design). This removes ~30% of the complexity an Ethereum pipeline would carry.
3. **Boring tech for the real-time path.** Managed log stream → Python consumer → Postgres → dbt-incremental. No Kafka, no Flink. Justified by the minutes-lag operating point — explicit tradeoff discussion in the doc.

---

## How to navigate

```
.
├── README.md                            ← you are here
├── design/
│   ├── 00_executive_summary.md          ← 2-page overview, read first
│   ├── 01_part1_capital_flow_pipeline.md
│   ├── 02_part2_ai_productionization.md
│   ├── 03_tradeoffs_and_next_steps.md
│   └── diagrams/
│       ├── batch_architecture.md
│       ├── realtime_architecture.md
│       ├── data_model.md
│       └── productionization_workflow.md
├── prototype/
│   ├── README.md                        ← how to run
│   ├── requirements.txt
│   ├── schema.sql                       ← Postgres / DuckDB DDL
│   ├── 01_ingest_dune.py                ← pulls real Avalanche LFJ Swap logs
│   ├── 02_decode.py                     ← topic0 routing + ABI decoding
│   ├── 03_enrich_usd.py                 ← price join, edge-case handling
│   ├── 04_load.py                       ← idempotent UPSERT
│   ├── 05_dq_checks.sql                 ← live correctness gates
│   └── sample_output.csv                ← 5-row sample of decoded pool swaps
└── part2_workflow/
    ├── README.md
    ├── pipeline_spec.example.yaml       ← the metadata contract
    ├── templates/
    │   └── airflow_dag_template.py.j2
    └── promotion_checklist.md
```

**Suggested reading order:** `design/00_executive_summary.md` → `design/01_part1_capital_flow_pipeline.md` → skim `prototype/` → `design/02_part2_ai_productionization.md` → `design/03_tradeoffs_and_next_steps.md`.

---

## Scope I narrowed and why

- **Implemented (a deliberately narrow vertical slice):** the batch pipeline end-to-end on **three LFJ V1 pools** (WAVAX/USDC, WAVAX/USDT, JOE/WAVAX) for one day, against a local DuckDB. Produces `dex_pool_swaps` only — schemas, decoder, USD enrichment, correction-safe MERGE, three live DQ checks. This proves the decode-enrich-load-DQ contract works on real data; it is not a scaled-down version of the full system.
- **Designed but not implemented:** the `dex_user_trades` derivation algorithm and table (this is the table capital-flow metrics actually read from), the real-time pipeline, full multi-protocol decoder coverage (Uniswap V3 on Avax, GMX, Curve, Balancer), aggregator-event handling (Paraswap, LFJ Aggregator, 1inch), the dbt-incremental layer, the control plane, and the AI productionization system.
- **Reasoning:** the assignment explicitly prefers depth over breadth, and Mauricio's framing prioritizes systems thinking and architecture over working code. A narrow honest prototype paired with a complete design is the right shape for this submission.

All assumptions and narrowings are stated in-line where they appear.

---

## Stack used in the prototype

- Python 3.11, `requests`, `web3.py` (for ABI decoding), `duckdb` (local Postgres stand-in)
- Dune API for raw logs (free tier; same query runs on `avalanche_c.logs` in production)
- All code is runnable on a laptop with no AWS or Postgres setup

---

## A note on AI usage

I used Claude as a thought partner throughout. Specifically:
- To pressure-test the metric definition (gross vs net flow, aggregator double-counting)
- To enumerate edge cases I might have missed (fee-on-transfer, rebasing, callback-pattern V3 attacks)
- To sanity-check the Avalanche finality model

I did not use AI to write the design conclusions or pick the architecture; those are mine and I can defend them. Where AI is the *subject* of the design (Part 2), I have been specific about what it should and should not do, with named failure modes.
