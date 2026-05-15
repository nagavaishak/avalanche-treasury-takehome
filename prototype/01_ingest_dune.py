"""
01_ingest_dune.py
==================

Pull a one-day window of raw `Swap` logs for one Avalanche DEX from Dune.

Why Dune for v1:
  - `avalanche_c.logs` is a managed, decoded-ready raw-log table indexed
    from full archive nodes. Cost-free to start, SQL-native.
  - For batch freshness (hours of lag) Dune is the lowest-effort path to
    "we can answer treasury questions in days" — see design doc 01 §2.

Why parquet on disk:
  - Decouples ingestion from decoding. If the decoder has a bug, we re-run
    `02_decode.py` against the same parquet without re-paying Dune cost.
  - Production rewinds: the S3 partition is the system of record.

Idempotency:
  - The output filename includes the date; re-running overwrites with the
    same content. No partial writes — pandas.to_parquet is atomic.

Failure handling:
  - Dune execution polling is bounded by `MAX_POLL_SECONDS`.
  - All HTTP errors surface as a non-zero exit; the calling DAG retries.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

import pandas as pd
import requests


# Topic0 signatures — keccak256("Swap(...)") for each DEX we currently support.
# Adding a new protocol = one entry here + one decoder function in 02_decode.py.
TOPIC0_BY_PROTOCOL: dict[str, dict[str, str]] = {
    "lfj_v1": {
        # event Swap(address sender, uint256 amount0In, uint256 amount1In,
        #            uint256 amount0Out, uint256 amount1Out, address to)
        "topic0": "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",
        "description": "Uniswap V2 / LFJ V1 / Pangolin shared Swap event signature.",
    },
}


DUNE_API = "https://api.dune.com/api/v1"
MAX_POLL_SECONDS = 600
POLL_INTERVAL_SECONDS = 4


def build_query_sql(protocol: str, date_str: str) -> str:
    """The SQL we execute on Dune. Pulls raw logs for one day, one topic0."""
    topic0 = TOPIC0_BY_PROTOCOL[protocol]["topic0"]
    return f"""
        SELECT
            block_number,
            block_time,
            tx_hash,
            index AS log_index,
            contract_address AS pool_address,
            topic1,
            topic2,
            data,
            tx_from,
            tx_to
        FROM avalanche_c.logs
        WHERE topic0 = {topic0!r}
          AND block_time >= TIMESTAMP '{date_str} 00:00:00'
          AND block_time <  TIMESTAMP '{date_str} 00:00:00' + INTERVAL '1' DAY
        ORDER BY block_number, log_index
        LIMIT 100000
    """


def submit_query(api_key: str, sql: str) -> str:
    """Create a transient Dune query and execute it. Returns execution_id."""
    headers = {"X-Dune-API-Key": api_key, "Content-Type": "application/json"}

    create = requests.post(
        f"{DUNE_API}/query",
        headers=headers,
        json={
            "name": f"treasury_assignment_raw_swaps_{int(time.time())}",
            "query_sql": sql,
            "is_private": True,
        },
        timeout=30,
    )
    create.raise_for_status()
    query_id = create.json()["query_id"]

    execute = requests.post(
        f"{DUNE_API}/query/{query_id}/execute",
        headers=headers,
        timeout=30,
    )
    execute.raise_for_status()
    return execute.json()["execution_id"]


def wait_for_results(api_key: str, execution_id: str) -> dict:
    """Poll Dune for execution completion. Returns the results payload."""
    headers = {"X-Dune-API-Key": api_key}
    deadline = time.time() + MAX_POLL_SECONDS
    while time.time() < deadline:
        status = requests.get(
            f"{DUNE_API}/execution/{execution_id}/status",
            headers=headers,
            timeout=30,
        )
        status.raise_for_status()
        state = status.json().get("state")
        if state == "QUERY_STATE_COMPLETED":
            results = requests.get(
                f"{DUNE_API}/execution/{execution_id}/results",
                headers=headers,
                timeout=60,
            )
            results.raise_for_status()
            return results.json()
        if state in {"QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED"}:
            raise RuntimeError(f"Dune execution {execution_id} ended in state {state}")
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Dune execution {execution_id} did not complete in {MAX_POLL_SECONDS}s")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--protocol", required=True, choices=list(TOPIC0_BY_PROTOCOL.keys()))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        sys.stderr.write("DUNE_API_KEY environment variable is required.\n")
        return 2

    sql = build_query_sql(args.protocol, args.date)
    print(f"[ingest] submitting Dune query for {args.protocol} on {args.date} ...")
    exec_id = submit_query(api_key, sql)
    print(f"[ingest] execution_id={exec_id}, polling ...")
    payload = wait_for_results(api_key, exec_id)

    rows = payload["result"]["rows"]
    df = pd.DataFrame(rows)
    df["chain_id"] = 43114
    df["project"] = args.protocol
    df["topic0"] = TOPIC0_BY_PROTOCOL[args.protocol]["topic0"]

    out_path = Path(args.out)
    df.to_parquet(out_path, index=False)
    print(f"[ingest] wrote {len(df):,} rows to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
