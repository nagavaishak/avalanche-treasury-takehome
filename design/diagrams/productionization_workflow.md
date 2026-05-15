# Part 2 — PromoteIt Workflow Diagram

```mermaid
flowchart TD
    DEV["Exploratory notebook / script<br/>/exploratory/&lt;author&gt;/&lt;topic&gt;/"]

    INIT["$ promoteit init<br/>(AST walk → draft spec)"]

    SPEC["pipeline_spec.yaml<br/>author fills in:<br/>• purpose<br/>• owner / oncall<br/>• SLA<br/>• DQ thresholds"]

    PROMOTE["$ promoteit promote"]

    subgraph "Code analysis"
        AST["AST walk<br/>(libcst)<br/>function graph<br/>IO classification"]
    end

    subgraph "Generation"
        TEMPLATE["template library<br/>airflow_dag_batch<br/>airflow_dag_streaming<br/>ecs_long_running<br/>lambda_event_driven"]
        UTIL["utility registry<br/>manifest embeddings<br/>RAG lookup"]
        LLM["LLM (pinned model)<br/>three call sites:<br/>1. template map<br/>2. utility match<br/>3. test scaffold"]
    end

    subgraph "Outputs (committed to branch)"
        OUT_DAG["DAG / service code"]
        OUT_TEST["test scaffolds<br/># TODO: assert"]
        OUT_DQ["DQ check SQL"]
        OUT_SCHEMA["data contract<br/>(JSON Schema)"]
        OUT_PR_DESC["PR description<br/>(what / skipped / human review)"]
    end

    PR["GitHub PR opened<br/>via gh"]

    REVIEW["human review:<br/>• assertions<br/>• thresholds<br/>• purpose accuracy<br/>• high-risk file flag"]

    CI["CI: tests, lint, schema, perf"]

    STAGING["merged → staging deploy"]

    SHIP["$ promoteit ship<br/>(separate, human-initiated)"]

    PROD["production"]

    DEV --> INIT
    INIT --> SPEC
    SPEC --> PROMOTE
    PROMOTE --> AST
    AST --> LLM
    TEMPLATE --> LLM
    UTIL --> LLM
    LLM --> OUT_DAG
    LLM --> OUT_TEST
    LLM --> OUT_DQ
    LLM --> OUT_SCHEMA
    LLM --> OUT_PR_DESC
    OUT_DAG & OUT_TEST & OUT_DQ & OUT_SCHEMA & OUT_PR_DESC --> PR
    PR --> REVIEW
    REVIEW --> CI
    CI --> STAGING
    STAGING --> SHIP
    SHIP --> PROD

    classDef ai fill:#fff2cc,stroke:#d6b656
    classDef human fill:#dae8fc,stroke:#6c8ebf
    classDef gate fill:#f8cecc,stroke:#b85450

    class LLM,AST,TEMPLATE,UTIL ai
    class SPEC,REVIEW,SHIP human
    class CI,PR gate
```

## Legend

- **Yellow** = AI-driven step
- **Blue** = human-driven step
- **Red** = gate / checkpoint

The shape of the diagram is the design statement: AI does a wide middle band of mechanical work, but every promotion crosses two human gates (the spec, then the PR review) and one technical gate (CI). Promotion to prod is a separate, human-initiated command.
