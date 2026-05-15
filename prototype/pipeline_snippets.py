"""
pipeline_snippets.py
=====================

The four key code patterns from the batch pipeline, in one file.
Each section is illustrative — not a runnable script, but the snippets
match the schema in `schema.sql` and the DQ checks in `05_dq_checks.sql`.

Decoder version semver:
  Major: breaking schema change (column added/removed/retyped)
  Minor: new protocol added
  Patch: decoder bugfix

Bumping the version triggers a scoped re-derive:
  WHERE decoder_version < 'X.Y.Z' AND project = 'lfj_v1'
"""
from __future__ import annotations

import duckdb
import pandas as pd
import requests
from eth_abi import decode as abi_decode

DECODER_VERSION = "0.1.0"


# =============================================================================
# 1. Ingest raw Avalanche Swap logs from Dune
# =============================================================================
#
# In production this runs as the first task in an hourly Airflow DAG.
# Same SQL works against `avalanche_c.logs` directly when migrating off Dune.

def ingest_lfj_v1_swaps(date: str, dune_api_key: str) -> pd.DataFrame:
    query = f"""
        SELECT block_number, block_time, tx_hash, log_index,
               contract_address AS pool_address, topic0, topic1, topic2, data
        FROM avalanche_c.logs
        WHERE topic0 = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822'
          AND DATE(block_time) = DATE '{date}'
    """
    # Submit query → poll for completion → fetch results.
    # Real client handles pagination, retries, and rate limits.
    headers = {"X-Dune-API-Key": dune_api_key}
    execution = requests.post("https://api.dune.com/api/v1/sql/execute",
                              json={"query_sql": query}, headers=headers).json()
    # ... poll on execution["execution_id"] until state=QUERY_STATE_COMPLETED ...
    results = requests.get(f"https://api.dune.com/api/v1/execution/{execution['execution_id']}/results",
                           headers=headers).json()
    return pd.DataFrame(results["result"]["rows"])


# =============================================================================
# 2. Decode an LFJ V1 (Uniswap-V2-style) Swap event
# =============================================================================
#
# Event signature:
#   Swap(address sender, uint amount0In, uint amount1In, uint amount0Out, uint amount1Out, address to)
#
# topic0 = keccak256 of the signature
# topic1 = sender (router or EOA)
# topic2 = to (recipient)
# data   = abi.encode(uint256, uint256, uint256, uint256)

# In production these maps come from `dim_tokens` and `dim_protocols`.
TOKEN_DECIMALS = {
    "0xb31f66aa...c7": 18,  # WAVAX
    "0xb97ef9ef...6e": 6,   # USDC
}
POOL_TOKENS = {
    "0xf4003f4e...db": ("0xb31f66aa...c7", "0xb97ef9ef...6e"),  # WAVAX/USDC
}


def decode_v2_swap(row: pd.Series) -> dict | None:
    pool = row["pool_address"].lower()
    if pool not in POOL_TOKENS:
        return None  # quarantined; counted, not silently dropped

    token0, token1 = POOL_TOKENS[pool]
    data_bytes = bytes.fromhex(row["data"][2:])
    a0in, a1in, a0out, a1out = abi_decode(
        ["uint256", "uint256", "uint256", "uint256"], data_bytes
    )

    # Determine direction: V2 swaps fill exactly one side
    if a0in > 0 and a1out > 0:
        sold, bought, sold_raw, bought_raw = token0, token1, a0in, a1out
    elif a1in > 0 and a0out > 0:
        sold, bought, sold_raw, bought_raw = token1, token0, a1in, a0out
    else:
        return None  # degenerate; skip

    return {
        "chain_id": 43114,
        "tx_hash": row["tx_hash"],
        "log_index": int(row["log_index"]),
        "block_time": row["block_time"],
        "project": "lfj", "version": "v1", "pool_address": pool,
        "token_sold_address": sold,
        "token_bought_address": bought,
        "token_sold_amount_raw": str(sold_raw),     # uint256-safe
        "token_bought_amount_raw": str(bought_raw),
        "token_sold_amount": sold_raw / 10 ** TOKEN_DECIMALS[sold],
        "token_bought_amount": bought_raw / 10 ** TOKEN_DECIMALS[bought],
        "decoder_version": DECODER_VERSION,
    }


# =============================================================================
# 3. Price enrichment — by contract_address, not symbol
# =============================================================================
#
# Two tokens can share a symbol (cross-chain, or malicious copy on the same
# chain). Contract address is the only unique on-chain identifier.

def enrich_usd(swaps: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    # Join each swap to the nearest prior price by contract address + minute.
    swaps["minute"] = swaps["block_time"].dt.floor("min")
    prices["minute"] = prices["timestamp"].dt.floor("min")
    enriched = swaps.merge(
        prices.rename(columns={"contract_address": "token_sold_address",
                               "price_usd": "sold_price_usd"}),
        on=["token_sold_address", "minute"], how="left"
    )
    enriched["amount_usd"] = enriched["token_sold_amount"] * enriched["sold_price_usd"]
    # amount_usd is NULL when no price feed within staleness threshold;
    # DQ check 2 alerts at >20% nulls per (hour, project). Never coerce to 0.
    enriched["price_source"] = enriched["sold_price_usd"].notna().map(
        {True: "coinpaprika", False: None}
    )
    return enriched


# =============================================================================
# 4. Idempotent + correction-safe UPSERT
# =============================================================================
#
# The architectural keystone: same data redelivered → no-op.
# Bumped decoder version → overwrite. Older versions never clobber newer.

UPSERT_SQL = """
INSERT INTO dex_pool_swaps (
    chain_id, tx_hash, log_index, block_time, project, version, pool_address,
    token_sold_address, token_bought_address,
    token_sold_amount_raw, token_bought_amount_raw,
    token_sold_amount, token_bought_amount, amount_usd,
    price_source, decoder_version
)
SELECT * FROM incoming
ON CONFLICT (chain_id, tx_hash, log_index) DO UPDATE SET
    token_sold_amount = excluded.token_sold_amount,
    token_bought_amount = excluded.token_bought_amount,
    amount_usd = excluded.amount_usd,
    decoder_version = excluded.decoder_version
WHERE excluded.decoder_version >= dex_pool_swaps.decoder_version;
"""


def load_swaps(df: pd.DataFrame, db_path: str) -> None:
    con = duckdb.connect(db_path)
    con.register("incoming", df)
    con.execute(UPSERT_SQL)
    con.close()
