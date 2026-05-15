"""
04_load.py
===========

Idempotent MERGE of enriched swap rows into DuckDB (Postgres-compatible DDL).

The cardinal correctness property of the pipeline lives here:

    PRIMARY KEY (chain_id, tx_hash, log_index)
    INSERT ... ON CONFLICT DO UPDATE WHERE new.decoder_version >= old.decoder_version

Re-running with identical rows is a no-op (idempotency). Re-running with a
bumped `decoder_version` overwrites the existing row (correction-safe — see
design doc §4.4 "Decoder-scoped replay"). Older decoder_version rows never
overwrite newer ones; the WHERE guard prevents accidental regressions during
a partial backfill that races with live ingestion.

Note on DuckDB vs Postgres:
  DuckDB's UPSERT syntax matches Postgres's `INSERT ... ON CONFLICT ...`.
  Production runs the same statement against RDS without modification.
"""
from __future__ import annotations

import argparse
import sys

import duckdb
import pandas as pd


INSERT_COLUMNS = [
    "chain_id", "block_number", "block_time", "tx_hash", "log_index",
    "project", "version", "pool_address", "factory_address", "fee_bps",
    "token_sold_address", "token_bought_address",
    "token_sold_asset_id", "token_bought_asset_id",
    "token_sold_amount_raw", "token_bought_amount_raw",
    "token_sold_amount", "token_bought_amount", "amount_usd",
    "taker", "maker", "tx_from", "tx_to",
    "is_aggregator_internal", "aggregator_project",
    "price_source", "price_staleness_seconds",
    "decoder_version",
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input", required=True)
    parser.add_argument("--db", required=True)
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    # The columns expected by INSERT may include some the dataframe doesn't have
    # if a decoder didn't fill them; normalize with explicit NULLs.
    for col in INSERT_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[INSERT_COLUMNS]

    con = duckdb.connect(args.db)
    pre_count = con.execute("SELECT COUNT(*) FROM dex_pool_swaps").fetchone()[0]

    con.register("incoming", df)
    col_list = ", ".join(INSERT_COLUMNS)
    # Columns to overwrite on conflict — everything except the PK and inserted_at.
    update_cols = [c for c in INSERT_COLUMNS if c not in ("chain_id", "tx_hash", "log_index")]
    update_set = ", ".join(f"{c} = excluded.{c}" for c in update_cols)
    con.execute(
        f"""
        INSERT INTO dex_pool_swaps ({col_list})
        SELECT {col_list} FROM incoming
        ON CONFLICT (chain_id, tx_hash, log_index) DO UPDATE SET
            {update_set}
        WHERE excluded.decoder_version >= dex_pool_swaps.decoder_version
        """
    )
    post_count = con.execute("SELECT COUNT(*) FROM dex_pool_swaps").fetchone()[0]
    inserted = post_count - pre_count
    updated_or_noop = len(df) - inserted

    print(f"[load] pre={pre_count:,} post={post_count:,} inserted={inserted:,} (updated-or-noop on conflict: {updated_or_noop:,})")
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
