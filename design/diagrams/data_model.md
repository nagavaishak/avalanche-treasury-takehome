# Data Model — ERD

```mermaid
erDiagram
    DEX_POOL_SWAPS {
        bigint block_number
        timestamptz block_time
        text tx_hash PK
        int log_index PK
        int chain_id PK
        text project
        text version
        text pool_address
        text factory_address
        int fee_bps
        text token_sold_address FK
        text token_bought_address FK
        text token_sold_asset_id
        text token_bought_asset_id
        numeric token_sold_amount_raw
        numeric token_bought_amount_raw
        numeric token_sold_amount
        numeric token_bought_amount
        numeric amount_usd
        text taker
        text maker
        text tx_from
        text tx_to
        bool is_aggregator_internal
        text aggregator_project
        text price_source
        int price_staleness_seconds
        timestamptz inserted_at
        text decoder_version
    }

    DEX_USER_TRADES {
        bigint block_number
        timestamptz block_time
        text tx_hash PK
        int trade_index PK
        int chain_id PK
        text venue
        bool is_aggregator
        text aggregator_project
        text router_address
        int n_hops
        text taker
        text token_sold_address FK
        text token_bought_address FK
        numeric token_sold_amount
        numeric token_bought_amount
        numeric amount_usd
        numeric effective_price
        int price_impact_bps
        bigint gas_used
        numeric gas_fee_usd
        text derivation_method
        array source_log_indexes
        text derivation_confidence
    }

    DIM_TOKENS {
        text token_address PK
        text asset_id
        text symbol
        int decimals
        text wrapped_of
        text source
        timestamptz first_seen
    }

    DIM_PROTOCOLS {
        text address PK
        text project
        text role
        text version
        text factory_address
        timestamptz onboarded_at
    }

    DIM_WALLET_LABELS {
        text address PK
        text cohort
        text label_source
        timestamptz labelled_at
        numeric confidence
    }

    MART_CAPITAL_FLOW_HOURLY {
        timestamptz hour PK
        text token_a_asset_id PK
        text token_b_asset_id PK
        text taker_cohort PK
        numeric gross_volume_usd
        numeric net_flow_usd
        int n_trades
        text source_table
    }

    MART_POOL_TURNOVER_HOURLY {
        timestamptz hour PK
        text pool_address PK
        int swap_count
        numeric volume_usd
        numeric fee_usd
    }

    DEX_POOL_SWAPS }o--|| DIM_TOKENS : token_sold_address
    DEX_POOL_SWAPS }o--|| DIM_TOKENS : token_bought_address
    DEX_POOL_SWAPS }o--|| DIM_PROTOCOLS : pool_address
    DEX_USER_TRADES }o--|| DIM_TOKENS : token_sold_address
    DEX_USER_TRADES }o--|| DIM_TOKENS : token_bought_address
    DEX_USER_TRADES }o--|| DIM_PROTOCOLS : router_address
    DEX_USER_TRADES }o--|| DIM_WALLET_LABELS : taker
    DEX_USER_TRADES ||..o{ DEX_POOL_SWAPS : "source_log_indexes lineage"
    MART_CAPITAL_FLOW_HOURLY ||--o{ DEX_USER_TRADES : "aggregates"
    MART_POOL_TURNOVER_HOURLY ||--o{ DEX_POOL_SWAPS : "aggregates"
```

## The lineage edge that matters

`dex_user_trades.source_log_indexes` is an array linking back to the pool-swap log indexes that constructed it. Reviewers can audit any aggregated row by joining back. This is the "glass box" property: nothing in the aggregation is unreverseable.

## What the marts protect

- `mart_capital_flow_hourly` reads from **`dex_user_trades` only**. Never from pool swaps. This is enforced in dbt.
- `mart_pool_turnover_hourly` reads from **`dex_pool_swaps` only**. Never from user trades. This is enforced in dbt.

The semantic layer (Cube) exposes them as different measures so an analyst cannot mistakenly combine them.
