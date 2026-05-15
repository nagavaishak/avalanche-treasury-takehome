# Prototype — Pool-Level Swap Pipeline (Batch, 3 LFJ V1 pools)

A narrow end-to-end batch pipeline that ingests, decodes, enriches, and loads the **pool-level `Swap` events** from **three LFJ V1 pools** (WAVAX/USDC, WAVAX/USDT, JOE/WAVAX) over a one-day window of Avalanche C-Chain. Real data, real decoding, real USD enrichment, idempotent MERGE into DuckDB (Postgres-compatible DDL).

## What this proves

Concretely:

1. **Decoding works.** Raw `Swap` topic + hex data from `avalanche_c.logs` is correctly decoded into typed token-amount columns, normalized by each token's `decimals` (per-token, not global).
2. **USD enrichment works.** Minute-resolution prices from Dune's `prices.usd` are joined on the nearest prior minute by token contract address, with explicit NULL handling for missing feeds and price-staleness recorded per row.
3. **Idempotency + correction-safety works.** Running the pipeline twice produces the same final state. Re-running with a bumped `decoder_version` overwrites prior rows. The PK is `(chain_id, tx_hash, log_index)`.
4. **DQ checks work.** Three live SQL assertions run after load; failures are loud.

## What this prototype deliberately does NOT prove

The design (see `design/01_part1_capital_flow_pipeline.md`) is significantly larger than what's coded. The following are **design-only** in this submission:

- **`dex_user_trades`** — the user-intent table that capital-flow metrics are actually computed from. Schema and derivation algorithm are in §3.4 / §3.5 of the design. The prototype only produces `dex_pool_swaps`, which is the foundation for `dex_user_trades` but not the final capital-flow surface.
- **Aggregator-event handling.** The dispatch table in `02_decode.py` only knows the LFJ V1 Swap topic. Paraswap, LFJ Aggregator, 1inch decoding is in §3.5 of the design, not coded.
- **Multi-protocol decoding** (V3, GMX, Curve, Balancer).
- **Pool discovery.** The decoder explicitly knows three pool addresses; any other pool's log is counted as `skipped_unknown_pool` and reported, not silently dropped. Production pool discovery is via the factory `PairCreated` event — designed, not coded.
- **The real-time path.**
- **The dbt-incremental layer.**

The prototype is a **vertical slice** that proves the decode-enrich-load-DQ contract end-to-end on real data. It is not a scaled-down version of the full system.

## Requirements

- Python 3.11+
- A free Dune API key (https://dune.com/settings/api)

## Install

```bash
cd prototype
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export DUNE_API_KEY=...
```

## Run

```bash
# Initialize the DuckDB schema
python -c "import duckdb; duckdb.connect('treasury.duckdb').execute(open('schema.sql').read())"

# Pull a day of LFJ V1 swaps + matching prices from Dune
python 01_ingest_dune.py --date 2026-05-12 --protocol lfj_v1 --out raw.parquet

# Decode raw logs into typed swap rows
python 02_decode.py --in raw.parquet --out decoded.parquet

# Enrich with USD using Dune prices.usd
python 03_enrich_usd.py --in decoded.parquet --date 2026-05-12 --out enriched.parquet

# Idempotent UPSERT into DuckDB
python 04_load.py --in enriched.parquet --db treasury.duckdb

# Run DQ checks
duckdb treasury.duckdb < 05_dq_checks.sql
```

Total runtime: ~90 seconds for one day of LFJ V1 swaps.

## Re-run idempotency demo

Run `04_load.py` twice in a row. The second run should:
- Report 0 rows inserted
- Leave the row count unchanged

This is the core safety property of the pipeline. Every script can be re-run without side effects.

## Files

| File | Purpose |
|---|---|
| `schema.sql` | The DuckDB / Postgres DDL for `dex_pool_swaps` and `dim_tokens` |
| `01_ingest_dune.py` | Pulls raw `avalanche_c.logs` rows for one day for the chosen protocol |
| `02_decode.py` | Topic0 routing → ABI decode → typed columns |
| `03_enrich_usd.py` | Joins against `prices.usd` on nearest minute; handles missing feeds |
| `04_load.py` | Idempotent UPSERT into DuckDB |
| `05_dq_checks.sql` | Three live DQ assertions (lag, null-price %, dedup) |
| `sample_output.csv` | A 5-row sample of decoded LFJ V1 swaps for inspection (full output is produced by running the pipeline) |

## A note on the Dune API choice

The prototype creates a transient saved query via `POST /v1/query` and executes it via `POST /v1/query/{id}/execute`. This requires the **"query create"** scope on the API key (free tier supports it). Dune also exposes `POST /v1/sql/execute` for direct SQL execution without saving a query — for production we'd switch to that endpoint to avoid scope surface and the per-API-key saved-query quota. Documented as a small follow-up in `design/03_tradeoffs_and_next_steps.md`.

## Why DuckDB and not Postgres

Reviewer ergonomics. DuckDB is a single-file, zero-install warehouse that accepts essentially all the Postgres DDL we'd use in production. The schema and queries in this prototype are written to be Postgres-compatible — when wired into production, the same files run against RDS without modification.

## Design choices visible in the code

- All scripts are pure functions of their inputs. No hidden state.
- Idempotency key is `(chain_id, tx_hash, log_index)` everywhere.
- `decoder_version` is hardcoded in `02_decode.py` and stored on every row, so a re-derive after a fix is one SQL statement.
- The decoder is registered per-protocol in a dispatch table; adding Uniswap V3 is one new entry plus a decoder function.
- `amount_usd` is `NULL` when no price is available, not 0.
