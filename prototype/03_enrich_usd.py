"""
03_enrich_usd.py
=================

USD-enrich decoded swap rows by joining against Dune's `prices.usd` table.

Source policy (see design doc §1.4):
  Tier 1: Coinpaprika minute-resolution feeds via Dune `prices.usd`. Used here.
  Tier 2: DEX-implied prices for the long tail. Not implemented in this prototype.

Staleness:
  We join on the nearest minute prior to block_time. If the nearest price is
  more than STALENESS_THRESHOLD seconds away, we keep amount_usd NULL and
  record price_staleness_seconds. Downstream DQ alarms on > 20% null in any
  (hour, project) cell — that's a price-feed problem, not a correctness one.

Reflexivity guard:
  Tier-1 prices are CEX+DEX volume-weighted by Coinpaprika; the reflexive
  case (price a token from its own DEX volume) does not apply here. The guard
  matters for the not-yet-implemented Tier 2.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests


DUNE_API = "https://api.dune.com/api/v1"
STALENESS_THRESHOLD_SECONDS = 15 * 60  # 15 minutes


def fetch_prices(api_key: str, date_str: str, token_addresses: list[str]) -> pd.DataFrame:
    """
    Pull minute-resolution prices keyed by **token contract address** on the
    given day. Joining on symbol is unsafe on Avalanche where wrappers and
    bridged variants can share a ticker (e.g., USDC vs USDC.e).
    """
    if not token_addresses:
        return pd.DataFrame(columns=["contract_address", "minute", "price"])

    addrs_sql = ", ".join(f"'{a.lower()}'" for a in token_addresses)
    sql = f"""
        SELECT
            LOWER(contract_address) AS contract_address,
            minute,
            price
        FROM prices.usd
        WHERE blockchain = 'avalanche_c'
          AND LOWER(contract_address) IN ({addrs_sql})
          AND minute >= TIMESTAMP '{date_str} 00:00:00' - INTERVAL '1' HOUR
          AND minute <  TIMESTAMP '{date_str} 00:00:00' + INTERVAL '1' DAY + INTERVAL '1' HOUR
    """
    headers = {"X-Dune-API-Key": api_key, "Content-Type": "application/json"}
    create = requests.post(
        f"{DUNE_API}/query",
        headers=headers,
        json={"name": f"treasury_prices_{int(time.time())}", "query_sql": sql, "is_private": True},
        timeout=30,
    )
    create.raise_for_status()
    query_id = create.json()["query_id"]

    execute = requests.post(f"{DUNE_API}/query/{query_id}/execute", headers=headers, timeout=30)
    execute.raise_for_status()
    execution_id = execute.json()["execution_id"]

    deadline = time.time() + 600
    while time.time() < deadline:
        status = requests.get(f"{DUNE_API}/execution/{execution_id}/status", headers=headers, timeout=30)
        status.raise_for_status()
        state = status.json().get("state")
        if state == "QUERY_STATE_COMPLETED":
            break
        if state in {"QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"}:
            raise RuntimeError(f"Prices query {execution_id} ended in {state}")
        time.sleep(4)

    results = requests.get(f"{DUNE_API}/execution/{execution_id}/results", headers=headers, timeout=60).json()
    rows = results["result"]["rows"]
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["minute"] = pd.to_datetime(df["minute"], utc=True)
    return df


def attach_usd(swaps: pd.DataFrame, prices: pd.DataFrame) -> pd.DataFrame:
    """
    For each swap, find the nearest-prior minute price for token_sold's asset.
    amount_usd = token_sold_amount * price_sold (we standardize on the sell-side).
    """
    if prices.empty:
        swaps["amount_usd"] = None
        swaps["price_source"] = None
        swaps["price_staleness_seconds"] = None
        return swaps

    swaps = swaps.copy()
    swaps["block_time"] = pd.to_datetime(swaps["block_time"], utc=True)
    swaps["token_sold_address"] = swaps["token_sold_address"].str.lower()

    # Per token contract, sort the price series and merge_asof to find nearest prior minute.
    # We join by token contract address, not symbol, to avoid wrapper/bridged collisions.
    merged_parts: list[pd.DataFrame] = []
    for token_addr, token_prices in prices.groupby("contract_address"):
        slice_ = swaps[swaps["token_sold_address"] == token_addr].copy()
        if slice_.empty:
            continue
        slice_ = slice_.sort_values("block_time")
        ap = token_prices.sort_values("minute")[["minute", "price"]]
        merged = pd.merge_asof(
            slice_,
            ap,
            left_on="block_time",
            right_on="minute",
            direction="backward",
            tolerance=pd.Timedelta(seconds=STALENESS_THRESHOLD_SECONDS),
        )
        merged_parts.append(merged)

    if not merged_parts:
        swaps["amount_usd"] = None
        swaps["price_source"] = None
        swaps["price_staleness_seconds"] = None
        return swaps

    out = pd.concat(merged_parts, ignore_index=True)
    out["amount_usd"] = out["token_sold_amount"] * out["price"]
    out["price_source"] = out["price"].apply(lambda x: "coinpaprika" if pd.notna(x) else None)
    out["price_staleness_seconds"] = (
        (out["block_time"] - out["minute"]).dt.total_seconds().fillna(-1).astype("Int64")
    )
    out = out.drop(columns=["price", "minute"])

    # Re-attach swaps with no Tier-1 asset coverage (token_sold_asset_id NULL or unknown)
    covered_keys = set(zip(out["tx_hash"], out["log_index"]))
    uncovered = swaps[~swaps.apply(lambda r: (r["tx_hash"], r["log_index"]) in covered_keys, axis=1)].copy()
    if not uncovered.empty:
        uncovered["amount_usd"] = None
        uncovered["price_source"] = None
        uncovered["price_staleness_seconds"] = None
        out = pd.concat([out, uncovered], ignore_index=True)

    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="input", required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        sys.stderr.write("DUNE_API_KEY required\n")
        return 2

    df = pd.read_parquet(args.input)
    print(f"[enrich] {len(df):,} decoded rows")

    token_addresses = sorted({
        a.lower() for a in (
            df["token_sold_address"].dropna().tolist()
            + df["token_bought_address"].dropna().tolist()
        )
    })
    print(f"[enrich] fetching prices for {len(token_addresses)} token contracts ...")
    prices = fetch_prices(api_key, args.date, token_addresses)
    print(f"[enrich] fetched {len(prices):,} price rows")

    enriched = attach_usd(df, prices)
    null_pct = enriched["amount_usd"].isna().mean()
    print(f"[enrich] amount_usd null rate: {null_pct:.2%}")

    enriched.to_parquet(args.out, index=False)
    print(f"[enrich] wrote {len(enriched):,} rows → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
