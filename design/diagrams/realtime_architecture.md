# Real-Time Pipeline Architecture

```mermaid
flowchart LR
    subgraph "Chain"
        AVAX[("Avalanche C-Chain<br/>Snowman finality ~1.5s<br/>no reorgs")]
    end

    subgraph "Managed Stream"
        GS["Goldsky Mirror<br/>filtered subscription<br/>topic0 ∈ swap_signatures"]
        QN["(or QuickNode Streams)<br/>vendor-redundant alternative"]
        NODE["Self-hosted AvalancheGo<br/>eth_subscribe<br/>cold-standby"]
    end

    subgraph "Postgres (same cluster as batch)"
        RAW[("raw_realtime.logs<br/>Goldsky-managed sink")]
        WORKER["Python worker (systemd, EC2)<br/>logical-decoding consumer<br/>topic0 → decoder dispatch<br/>idempotent UPSERT"]
        CORE_POOL[("dex_pool_swaps<br/>same table as batch")]
        CORE_USER[("dex_user_trades")]
    end

    subgraph "Aggregations (dbt-incremental, every 1 min)"
        MARTS[("mart_capital_flow_hourly<br/>(reads block_time > last_max − 5min)")]
    end

    subgraph "Reconciliation (nightly batch)"
        RECON["overwrites last 48h<br/>from Dune dex.trades"]
    end

    subgraph "Serving"
        SEM["Cube semantic layer<br/>(same as batch)"]
    end

    subgraph "Observability"
        CW["CloudWatch<br/>lag, throughput, decoder errors"]
        PD["PagerDuty"]
    end

    AVAX --> GS
    AVAX -.->|alternative| QN
    AVAX -.->|cold-standby| NODE

    GS --> RAW
    RAW --> WORKER
    WORKER --> CORE_POOL
    WORKER --> CORE_USER
    CORE_POOL --> MARTS
    CORE_USER --> MARTS
    MARTS --> SEM
    RECON -.->|overwrite| CORE_POOL
    RECON -.->|overwrite| CORE_USER

    WORKER --> CW
    MARTS --> CW
    CW --> PD
```

## Latency budget (end-to-end)

| Hop | Latency |
|---|---|
| Block produced → Goldsky receives | ~1.5 s (Avalanche finality) |
| Goldsky → Postgres raw | < 1 s |
| Python decoder catches up | < 2 s steady state |
| dbt-incremental refresh | 60 s |
| **Total p50** | **~90 s** |
| **Total p95** | **~3 min** |

## Idempotency & finality property

Every row keyed by `(chain_id, tx_hash, log_index)`. UPSERT with `ON CONFLICT DO UPDATE` guarded by `decoder_version` (identical re-writes are no-ops; corrected decoder versions overwrite). Avalanche's deterministic finality means we don't need an N-block confirmation buffer or tombstone-and-rewrite logic. We do still maintain an ingestion-sequence cursor in the worker so at-least-once Goldsky delivery is correctly handled (see Part 1 §4.2).

## What is *not* in the picture (and why)

- **Kafka / Flink / Materialize** — not needed at our volume (hundreds of events/sec at peak). The Python worker + Postgres + dbt-incremental triplet handles current Avalanche DEX volume with comfortable headroom. Decision boundary: switch when sustained event volume crosses ~10k/sec or we need stateful in-flight joins.
- **Separate streaming warehouse** — analytics and operational reads sit on the same Postgres; column-store separation only justified if marts queries exceed 30s at p95.
- **Confirmation buffer** — explicitly *not* present, because Avalanche doesn't need one.
