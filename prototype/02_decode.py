"""
02_decode.py
=============

Decode raw Avalanche `Swap` logs into typed swap rows.

Design:
  - Decoder dispatch by (project, topic0). Adding a new DEX is one entry.
  - Each decoder receives the raw row and returns a normalized record.
  - Token decimals come from `dim_tokens`; rows for unknown tokens fall back
    to a runtime decimals() lookup (out of scope for this prototype — we
    just emit a WARNING and leave token_*_amount NULL).
  - Wrong-decimals is a known failure mode (see design doc §5.2 check 4),
    so we keep both `_amount_raw` and `_amount` columns. Recovery requires
    only raw + correct decimals; we never lose information.

Output schema matches the columns in `dex_pool_swaps` (see schema.sql).

DECODER_VERSION semver:
  Major: breaking schema change (column added/removed/retyped)
  Minor: new protocol added
  Patch: decoder bugfix

Bumping the version is the trigger that lets us scope a targeted re-derive
in production via `WHERE decoder_version < 'X.Y.Z' AND project = 'lfj_v1'`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
from eth_abi import decode as abi_decode


DECODER_VERSION = "0.1.0"


# Hardcoded for the prototype; in production loaded from dim_tokens at job start.
TOKEN_DECIMALS: dict[str, int] = {
    "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": 18,  # WAVAX
    "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": 6,   # USDC
    "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7": 6,   # USDT
    "0x50b7545627a5162f82a992c33b87adc75187b218": 8,   # WBTC.e
    "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab": 18,  # WETH.e
    "0x6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd": 18,  # JOE
    "0x152b9d0fdc40c096757f570a51e494bd4b943e50": 8,   # BTC.b
}

TOKEN_ASSET_ID: dict[str, str] = {
    "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": "AVAX",
    "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": "USDC",
    "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7": "USDT",
    "0x50b7545627a5162f82a992c33b87adc75187b218": "BTC",
    "0x49d5c2bdffac6ce2bfdb6640f4f80f226bc10bab": "ETH",
    "0x6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd": "JOE",
    "0x152b9d0fdc40c096757f570a51e494bd4b943e50": "BTC",
}


# In production we'd load this from a pool->token0,token1 dim table populated by
# a separate factory-event watcher pipeline. Hardcoded here so the prototype runs
# without that pipeline. These are real LFJ V1 pool addresses on Avalanche.
LFJ_V1_POOL_TOKENS: dict[str, tuple[str, str]] = {
    # WAVAX/USDC
    "0xf4003f4efbe8691b60249e6afbd307abe7758adb": (
        "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
        "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
    ),
    # WAVAX/USDT
    "0xed8cbd9f0ce3c6986b22002f03c6475ceb7a6256": (
        "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
        "0x9702230a8ea53601f5cd2dc00fdbc13d4df4a8c7",
    ),
    # JOE/WAVAX
    "0x454e67025631c065d3cfad6d71e6892f74487a15": (
        "0x6e84a6216ea6dacc71ee8e6b0a5b7322eebc0fdd",
        "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
    ),
}


def normalize_address(addr: str | None) -> str | None:
    if addr is None:
        return None
    return addr.lower()


def normalize_amount(raw: int, decimals: int | None) -> float | None:
    """Divide raw uint256 by 10^decimals. Returns None if decimals unknown."""
    if decimals is None:
        return None
    return raw / (10 ** decimals)


def decode_lfj_v1_swap(row: pd.Series) -> dict | None:
    """
    Uniswap-V2-style Swap event:
        topic1 = sender (router or EOA)
        topic2 = to (recipient)
        data   = abi.encode(uint256 a0in, uint256 a1in, uint256 a0out, uint256 a1out)

    Returns None with reason 'unknown_pool' if the emitter is not one of the
    three pools the prototype knows about. Caller quarantines and counts these.
    """
    pool = normalize_address(row["pool_address"])
    if pool not in LFJ_V1_POOL_TOKENS:
        # Production: trigger pool-discovery side-task off PairCreated factory event.
        # Prototype: surface as quarantine row so the count is loud, not silent.
        return {"_skip_reason": "unknown_pool", "pool_address": pool, "tx_hash": row["tx_hash"], "log_index": int(row["log_index"])}

    token0, token1 = LFJ_V1_POOL_TOKENS[pool]
    raw_data = row["data"]
    if raw_data.startswith("0x"):
        raw_data = raw_data[2:]
    data_bytes = bytes.fromhex(raw_data)

    try:
        a0in, a1in, a0out, a1out = abi_decode(
            ["uint256", "uint256", "uint256", "uint256"],
            data_bytes,
        )
    except Exception as exc:
        print(f"[decode] ABI decode failed for tx={row['tx_hash']}: {exc}")
        return None

    # Determine direction: exactly one of {token0 sold, token1 sold} is the case.
    if a0in > 0 and a1out > 0:
        token_sold, token_bought = token0, token1
        sold_raw, bought_raw = a0in, a1out
    elif a1in > 0 and a0out > 0:
        token_sold, token_bought = token1, token0
        sold_raw, bought_raw = a1in, a0out
    else:
        # Degenerate: zero on both sides or both sides nonzero. Skip — should be near-zero rate.
        return None

    dec_sold = TOKEN_DECIMALS.get(token_sold)
    dec_bought = TOKEN_DECIMALS.get(token_bought)
    asset_sold = TOKEN_ASSET_ID.get(token_sold)
    asset_bought = TOKEN_ASSET_ID.get(token_bought)

    sender = "0x" + row["topic1"][-40:] if row.get("topic1") else None
    recipient = "0x" + row["topic2"][-40:] if row.get("topic2") else None

    return {
        "chain_id": int(row["chain_id"]),
        "block_number": int(row["block_number"]),
        "block_time": row["block_time"],
        "tx_hash": row["tx_hash"],
        "log_index": int(row["log_index"]),
        "project": "lfj",
        "version": "v1",
        "pool_address": pool,
        "factory_address": "0x9ad6c38be94206ca50bb0d90783181662f0cfa10",  # LFJ V1 factory
        "fee_bps": 30,  # LFJ V1 / V2-fork fixed 0.3%
        "token_sold_address": token_sold,
        "token_bought_address": token_bought,
        "token_sold_asset_id": asset_sold,
        "token_bought_asset_id": asset_bought,
        "token_sold_amount_raw": str(sold_raw),  # stringified for parquet/Decimal safety
        "token_bought_amount_raw": str(bought_raw),
        "token_sold_amount": normalize_amount(sold_raw, dec_sold),
        "token_bought_amount": normalize_amount(bought_raw, dec_bought),
        "amount_usd": None,  # filled in by 03_enrich_usd.py
        "taker": normalize_address(recipient),
        "maker": pool,
        "tx_from": normalize_address(row.get("tx_from")),
        "tx_to": normalize_address(row.get("tx_to")),
        "is_aggregator_internal": False,  # TODO: set true if tx_to is a known aggregator
        "aggregator_project": None,
        "price_source": None,
        "price_staleness_seconds": None,
        "decoder_version": DECODER_VERSION,
    }


DECODER_DISPATCH = {
    ("lfj_v1", "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"): decode_lfj_v1_swap,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    df = pd.read_parquet(args.input)
    print(f"[decode] {len(df):,} raw rows; decoder_version={DECODER_VERSION}")

    decoded_rows: list[dict] = []
    quarantine_rows: list[dict] = []
    skipped_no_dispatch = 0
    skipped_degenerate = 0
    for _, row in df.iterrows():
        decoder = DECODER_DISPATCH.get((row["project"], row["topic0"]))
        if decoder is None:
            skipped_no_dispatch += 1
            continue
        result = decoder(row)
        if result is None:
            # Degenerate row (zero on both sides, or ABI decode failure already logged).
            skipped_degenerate += 1
            continue
        if "_skip_reason" in result:
            quarantine_rows.append(result)
            continue
        decoded_rows.append(result)

    out_df = pd.DataFrame(decoded_rows)
    out_df.to_parquet(args.out, index=False)
    if quarantine_rows:
        q_path = args.out.replace(".parquet", "_quarantine.parquet")
        pd.DataFrame(quarantine_rows).to_parquet(q_path, index=False)
    print(
        f"[decode] decoded={len(out_df):,} "
        f"quarantined_unknown_pool={len(quarantine_rows):,} "
        f"skipped_no_dispatch={skipped_no_dispatch:,} "
        f"skipped_degenerate={skipped_degenerate:,}"
    )
    total = len(out_df) + len(quarantine_rows) + skipped_no_dispatch + skipped_degenerate
    assert total == len(df), f"row accounting mismatch: {total} != {len(df)}"
    return 0


if __name__ == "__main__":
    sys.exit(main())
