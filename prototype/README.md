# Prototype — Code Snippets

Four illustrative patterns from the batch pipeline design, plus the schema and DQ checks.

| File | Purpose |
|---|---|
| `schema.sql` | Postgres / DuckDB DDL for `dex_pool_swaps` |
| `pipeline_snippets.py` | The four key patterns: Dune ingest → ABI decode → USD enrichment → idempotent UPSERT |
| `05_dq_checks.sql` | Three live DQ checks: ingestion lag, null-price rate, dedup invariant |
| `sample_output.csv` | 5 rows of decoded LFJ V1 swaps on Avalanche (illustrative) |

## The four patterns in `pipeline_snippets.py`

1. **Ingest** — Pull raw `Swap` logs from Dune by `topic0` for a date window.
2. **Decode** — ABI-decode the 4 uint256s in the log data; detect direction by which side is non-zero.
3. **Enrich** — Join `prices.usd` by **contract address** (not symbol — symbol collisions are a real risk), nearest minute prior.
4. **Load** — `INSERT ... ON CONFLICT DO UPDATE WHERE excluded.decoder_version >= current.decoder_version`. Idempotent + correction-safe.

## Why the schema and DQ are full files

The assignment explicitly asks for "schemas or table definitions you propose" and "live checks to guarantee health." Those two are the deliverables. The Python patterns are snippets that show how each architectural decision lands in code.

## Scope

Three LFJ V1 pools (WAVAX/USDC, WAVAX/USDT, JOE/WAVAX), one-day window, batch only. Real Avalanche C-Chain data. `dex_user_trades` derivation, real-time path, and multi-protocol decoders are designed in `design/01_part1_capital_flow_pipeline.md`, not coded here.

## Why DuckDB

Single-file warehouse with Postgres-compatible DDL. The schema and the UPSERT in `pipeline_snippets.py` run unchanged against RDS Postgres in production.
