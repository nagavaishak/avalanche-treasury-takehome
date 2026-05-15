# Batch Pipeline Architecture

```mermaid
flowchart LR
    subgraph "Sources (read-only)"
        DUNE[("Dune<br/>avalanche_c.logs<br/>dex.trades<br/>prices.usd")]
        BQ[("BigQuery<br/>crypto_avalanche<br/>(reconciliation)")]
    end

    subgraph "Ingestion (MWAA / Airflow, hourly)"
        I1["task: pull_pool_swaps<br/>(per protocol)"]
        I2["task: pull_aggregator_events<br/>(Paraswap, LFJ Agg, 1inch)"]
        I3["task: pull_prices"]
    end

    subgraph "Raw / System of Record"
        S3[("S3<br/>parquet, partitioned<br/>dt=YYYY-MM-DD/hour=HH")]
    end

    subgraph "Transform (dbt on Postgres)"
        STG["stg_dex_pool_swaps<br/>stg_aggregator_events<br/>stg_prices"]
        CORE_POOL[("dex_pool_swaps<br/>incremental UPSERT<br/>PK (chain_id, tx_hash, log_index)")]
        CORE_USER[("dex_user_trades<br/>incremental<br/>derivation algorithm")]
        MARTS[("mart_capital_flow_hourly<br/>mart_capital_flow_daily<br/>mart_pool_turnover_hourly")]
    end

    subgraph "Quality"
        DQ["dq_checks: freshness, null_pct,<br/>dedup, decimals, dual-table-drift"]
        RECON["nightly reconciliation<br/>vs Dune dex.trades"]
    end

    subgraph "Serving"
        BI["Metabase / Hex<br/>(SQL-fluent)"]
        SEM["Cube semantic layer"]
        SUPER["Superset dashboards<br/>(non-technical)"]
        API["REST API<br/>(rate-limited)"]
        PARQ["S3 parquet snapshots<br/>(quant / Python)"]
    end

    subgraph "Observability"
        CW["CloudWatch<br/>metrics, alarms"]
        PD["PagerDuty"]
    end

    DUNE --> I1 & I2 & I3
    BQ -.->|reconciliation only| RECON
    I1 & I2 & I3 --> S3
    S3 --> STG
    STG --> CORE_POOL
    CORE_POOL --> CORE_USER
    CORE_USER --> MARTS
    CORE_POOL --> MARTS
    MARTS --> DQ
    DQ --> RECON
    MARTS --> SEM
    SEM --> SUPER
    SEM --> API
    MARTS --> BI
    MARTS --> PARQ
    DQ --> CW
    CW --> PD
```

## Component cheat sheet

| Component | Why |
|---|---|
| **S3 (parquet)** | System of record. Every warehouse row is rewindable. |
| **Airflow / MWAA** | Orchestrator. AWS-managed to avoid the self-hosted-Airflow ops trap. |
| **dbt** | Transformations. Idempotent, version-controlled, testable. |
| **Postgres (RDS Multi-AZ)** | Single substrate for OLTP serving + analytical marts at current volume. |
| **Cube** | Semantic layer enforces the "never sum across two tables" rule at the API. |
| **CloudWatch + PagerDuty** | Alerting. |

## Cadence

- Hourly ingest, hourly transform, hourly DQ. Each task ~5–15 min.
- Nightly reconciliation at 02:00 UTC. Overwrites last 48h from Dune ground truth.
- Backfills run against a read replica with throttled concurrency.
