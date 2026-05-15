# 02 — Part 2: AI-Assisted Productionization Workflow

## The problem, restated honestly

A small high-ownership team will, in practice, write a lot of one-off pipelines. An analyst writes a notebook that pulls validator-set churn. An economist writes a Python script that scrapes incentive-emissions data from a contract. Most of these stay one-off forever. A meaningful fraction (maybe 1 in 5) turns out to be load-bearing, and someone has to "productionize" it — wrap it in scheduling, tests, alerts, error handling, code-style conformance, observability, and put it in version control with the rest of the platform.

Productionization is mostly **mechanical translation** of an exploratory pipeline into a conforming production scaffold. It is a near-perfect AI use case: bounded, repeatable, and amenable to template-driven generation. It is also a place where AI can silently introduce bugs that take weeks to surface, so the design must make AI helpful **and** loud about its limits.

The system I'm proposing is called **PromoteIt**. It takes a `pipeline_spec.yaml` + a directory of exploratory code, and emits a PR containing the production-ready pipeline that conforms to the platform's standards. The PR is the artifact a human reviews and merges.

---

## 1. Developer Workflow

### 1.1 Day-zero — writing the dev pipeline (no AI involvement yet)

The developer writes a Jupyter notebook or a flat Python script in `/exploratory/<author>/<topic>/`. There is no enforced structure here on purpose — exploratory code is supposed to be ugly. The only ask: name your dataframes meaningfully and keep IO and logic in different cells/functions.

### 1.2 Day-N — deciding to promote

When the pipeline proves valuable, the author runs:

```bash
promoteit init exploratory/eric/validator_churn/
```

This walks the exploratory directory and produces a **draft `pipeline_spec.yaml`** by inspecting the code (imports, function calls, IO calls, return types). The author then fills in the metadata the system can't infer — primarily intent, ownership, SLA, and consumption pattern. This is the **metadata contract**.

A real `pipeline_spec.yaml` example is in `/part2_workflow/pipeline_spec.example.yaml`. Highlights:

```yaml
name: validator_churn_hourly
owner: eric.lu@avalabs.org
oncall: treasury-data-pager
purpose: |
  Compute hourly validator-set additions and departures on
  Avalanche P-Chain; surface to the validator-economics dashboard.

freshness:
  sla: 1h
  classification: batch        # batch | streaming | hybrid

inputs:
  - kind: dune_query
    query_id: 4123456
    refresh_minutes: 60
  - kind: chain_rpc
    chain: avalanche_p
    endpoint: from_secrets_manager(name=avax_p_rpc)

outputs:
  - kind: postgres_table
    schema: treasury_marts
    table: validator_churn_hourly
    primary_key: [block_time_hour]
    write_mode: upsert

dependencies:
  - dim_validators
  - prices.usd

dq_checks:
  - kind: freshness
    threshold: 2h
  - kind: row_count
    min: 1
    window: 24h
  - kind: schema_match
    contract: contracts/validator_churn_hourly.schema.json

consumer_classifications:
  - dashboards
  - api

reuses_utilities:
  - dune_client
  - postgres_upsert
  - secrets_manager

ai_review_required: true       # forced true for this pipeline class
deploy_target: mwaa            # mwaa | ecs | lambda
runtime: python3.11
```

### 1.3 The promotion command

```bash
promoteit promote exploratory/eric/validator_churn/ --spec pipeline_spec.yaml
```

This is the one button the developer presses. Under the hood, the system:

1. Parses the spec and validates it against a JSON schema.
2. Runs a **code analysis pass** over the exploratory code (function graph, import graph, identified IO touchpoints).
3. Picks the matching **production template** (Airflow DAG vs. ECS service vs. Lambda) from the curated template library.
4. Maps the exploratory code into the template, **with AI assistance** for the non-mechanical mappings (variable name rewrites, splitting one notebook function into pre-/transform/post-, choosing existing utility modules to reuse instead of copying logic).
5. Generates dbt test stubs and DQ-check SQL from the `dq_checks` block in the spec.
6. Generates a unit-test scaffold from the function signatures, populated with `# TODO: assert` markers and one happy-path test that the AI hand-rolled from the exploratory code's example inputs.
7. Opens a PR against `main` with a structured description (what the system did, what it skipped, where human review is required).

### 1.4 The PR is the human checkpoint

The developer (and a second reviewer for high-risk classes) reads the PR. Everything is mechanical and reviewable. Comments are inline. Merging gates on:

- All generated tests pass in CI
- A second reviewer's approval if any flagged "high-risk" file changed (see §4)
- A "human acknowledgment" box ticked on the PR template

CI deploys to staging on merge. Promotion to prod requires a separate approved promotion PR. The same `promoteit` tool tracks promotion state.

---

## 2. Role of AI — specifically

The assignment explicitly asks for specifics over hand-waving. Here are the exact tasks AI does, and a parallel list of tasks it does **not** do.

### 2.1 What AI does

| Task | Why AI is well-suited | Failure mode |
|---|---|---|
| **Map exploratory code to template scaffold** | The mapping is fuzzy: function `fetch_dune_data()` in the notebook becomes the `ingest` task in the DAG. AI is good at pattern-matching the intent and refactoring accordingly. | AI mis-identifies a side-effecting function as pure |
| **Suggest reuse of shared utilities** | The platform has a registry of approved utility modules (`utilities/dune_client.py`, `utilities/postgres_upsert.py`, `utilities/secrets_manager.py`). AI semantically matches functions in the exploratory code to utilities and rewrites imports. | AI suggests a utility that's *similar* but not equivalent |
| **Generate the Airflow DAG from the template + spec** | Highly mechanical. Templates are Jinja2; AI fills in task names, dependencies, schedule, retry behavior from the spec. | Low — this is essentially deterministic |
| **Generate unit test scaffolding** | AI reads function signatures and example inputs in the exploratory code and writes one happy-path test and one TODO test per function. | AI generates a test that asserts the buggy behavior (because the test was written from the buggy code) |
| **Generate DQ-check SQL** | The `dq_checks` block in the spec maps mechanically to SQL templates; AI fills in table/column names from the schema contract. | Low if templates are tight |
| **Suggest docstrings and inline comments** | Pure rewriting. AI generates a one-line docstring per function, a top-of-file purpose comment, and a CHANGELOG line. | Verbosity creep |
| **Lint and style fixes** | Run `ruff`, `black`, `mypy`. AI fixes most type errors and formatting violations. | Type-erasure (`# type: ignore` sprinkling) |
| **Generate the PR description** | Structured: "what I did / what I skipped / where human review matters." Pure template. | Low |
| **Suggest existing tests to reuse** | If the spec says `reuses_utilities: dune_client`, AI suggests adding new tests under `tests/dune_client/test_validator_churn_query.py` rather than fresh test file. | Low |

### 2.2 What AI does *not* do (the "do not trust without human review" list)

| Task | Why AI must not | What we do instead |
|---|---|---|
| **Choose the SLA, ownership, or oncall** | These are organizational facts, not derivable from code | Author fills in `pipeline_spec.yaml` by hand; tool errors if blank |
| **Choose what the metric *means*** | "Capital flow" can mean five different things; AI guessing the intent will produce a plausible-looking pipeline measuring the wrong quantity | Spec includes a `purpose:` block reviewed in PR |
| **Write the DQ-check thresholds** | "Alert when null-price > 20%" is a business decision informed by historical noise. AI estimating from notebook output is risky. | Author sets thresholds in spec; AI fills the SQL skeleton |
| **Approve the PR** | Trivial guardrail | Second-engineer review for high-risk classes |
| **Promote staging → prod** | Two-key turn | Separate human-initiated `promoteit ship` command |
| **Modify shared utilities** | A change to `utilities/dune_client.py` impacts every pipeline; AI cannot vouch for cross-pipeline consequences | Tool refuses to edit `/utilities/`; emits a TODO comment instead and the author opens a separate PR |
| **Decide between batch and real-time** | Architectural choice with cost and ops implications | Spec specifies; AI cannot override |
| **Touch credentials or secrets** | Obvious | Code analysis is read-only on secret references; secrets are pulled at runtime via Secrets Manager, never inlined |
| **Generate the *correctness* assertions** | The TODO tests are placeholders — AI does not assert correctness for novel logic | Human writes the assertions; AI only writes the scaffolding around them |

### 2.3 The "high-risk file changed" rule

Certain files are tagged in `.promoteit/risk_index.yaml`. Any change to them in a generated PR adds a `requires-second-reviewer` label automatically:

- `/utilities/` — shared modules
- `migrations/` — schema changes
- `dbt/models/marts/` — anything board-facing
- Files matching `*.dq.*` — DQ check definitions
- `.github/workflows/` — CI itself

Promotion of a new pipeline that *uses* these files (but doesn't modify them) is single-reviewer. Promotion that *modifies* them is two-reviewer.

---

## 3. System Architecture

See `diagrams/productionization_workflow.md` for the diagram. The components:

### 3.1 Metadata / specification layer

- **`pipeline_spec.yaml`** — the contract. JSON-Schema-validated. Owned by the pipeline author.
- **`promoteit/spec.py`** — the parser. Pydantic models with validators (e.g., "freshness < 1m requires `classification: streaming`").
- **`/contracts/<table>.schema.json`** — column-level data contracts that DQ checks bind to. JSON-Schema-compatible.

### 3.2 Code analysis layer

- **AST walk** — uses `libcst` to identify functions, IO calls (boto3, requests, sqlalchemy, web3), data transformations (pandas, polars), and side effects.
- **Function dependency graph** — used by the template mapper to decide what becomes an Airflow task vs. an inline function.
- **IO classifier** — flags functions that read/write external systems (Dune, RPC, Postgres, S3). These become DAG-level tasks; pure functions become helpers.

### 3.3 Reusable template library

Stored at `/promoteit/templates/`, each is a Jinja2 directory tree.

```
templates/
  airflow_dag_batch/         <- batch pipelines
  airflow_dag_streaming/     <- streaming consumer + dbt-incremental
  ecs_long_running/          <- websocket / consumer services
  lambda_event_driven/       <- EventBridge-triggered Lambdas
```

Adding a template is a (rare) governed change reviewed by the platform owner. Templates pin the platform's opinions: which orchestrator, which logger, which metrics emitter, which secrets pattern. AI cannot fork or extend templates.

### 3.4 Utility registry

`/utilities/` is the platform's shared library. Each utility has:

- A `manifest.yaml` describing what it does and its inputs/outputs (in English, for AI to read).
- A `__init__.py` exporting only the public API.
- A `tests/` directory.

The utility-suggestion step works by embedding each manifest and the exploratory function bodies, retrieving top-K matches, then asking the LLM to confirm or reject the match with the function bodies in context. False positives are reviewed in the PR.

### 3.5 Test-generation framework

- **Unit-test scaffolds:** one stub per function, generated from signatures and the example inputs/outputs the AI extracted from the notebook.
- **DQ-check SQL:** generated from `dq_checks` block in the spec and the schema contract.
- **Integration test:** one end-to-end test stub that runs the pipeline against a small fixture dataset committed to `tests/fixtures/`.

The author finishes the assertions — see §2.2.

### 3.6 Code-generation layer

The LLM is invoked at three places, each with a tightly scoped prompt and an output schema:

1. **Template mapping** — input: spec + AST analysis + chosen template tree. Output: a JSON map of `{exploratory_function_name → template_slot}`. The Jinja2 templates are rendered deterministically from this map.
2. **Utility suggestion** — input: function body + retrieved candidate manifests. Output: `{accept: bool, utility: str, rewrite_snippet: str}`. False rejects are fine; false accepts are caught in PR review.
3. **Test/docstring scaffolding** — input: function signature + example inputs. Output: a unit test function + a one-line docstring. Marked `# AI-GENERATED, please fill in assertions`.

Each call uses the **same model and version**, pinned in `.promoteit/config.yaml`. Upgrading the model is a governed change.

### 3.7 PR generation

Final step. The system commits to a branch, opens a PR via `gh`, attaches a structured description:

```markdown
## Generated by promoteit

**Spec:** `pipeline_spec.yaml`
**Template applied:** `airflow_dag_batch`
**Utility reuse:** `dune_client`, `postgres_upsert`
**AI confidence flags:** medium (1 utility match required PR review)

### What I did
- Generated DAG at `dags/validator_churn_hourly.py` (220 lines)
- Wrote 4 unit-test stubs (assertions are TODO)
- Wrote 3 DQ checks (thresholds copied from spec)
- Wrote schema contract at `contracts/validator_churn_hourly.schema.json`

### Where I am uncertain
- `fetch_validator_set` looked like a candidate for `utilities.chain_rpc.get_validators` but the signatures differ slightly. **Please verify line 47.**
- Generated unit tests assert that the *current* output is correct. If the exploratory code has bugs, the tests will perpetuate them. **Please review assertions before merging.**

### Human review required for
- All `assert` lines in `tests/test_validator_churn.py`
- DQ thresholds (currently copied from spec; tune after first prod run)
- The `purpose:` block in the spec for accuracy
```

This kind of structured PR is the single most important UX choice: a reviewer can answer "is this correct?" in 10 minutes instead of 2 hours.

---

## 4. Where Human Review Remains Mandatory

In one place, listed:

1. **All `assert` lines in generated tests.** AI writes the scaffolding, humans write the truth.
2. **The `purpose:` block of every spec.** AI cannot judge intent.
3. **All DQ-check *thresholds* and SLAs.** AI fills SQL skeletons; humans pick numbers.
4. **Any PR touching `/utilities/`, `migrations/`, `dbt/models/marts/`, `*.dq.*`, or `.github/workflows/`.**
5. **Promotion staging → prod.**
6. **Schema additions/removals on production tables.** Migration files are AI-generated for additive cases (`ADD COLUMN nullable`) but every schema PR is human-approved.
7. **Cost-affecting decisions** — DAG schedule frequency, instance size, retention period. The spec captures these and a finance reviewer is added if they change a baseline.
8. **Any cross-pipeline change** — touching a model that another pipeline reads.

---

## 5. Evaluation and Success Metrics

We are claiming this system saves the team meaningful engineering time. The claim has to be measurable.

### 5.1 Outcome metrics (what we report quarterly)

| Metric | Target | How measured |
|---|---|---|
| **Time from `pipeline_spec.yaml` written → prod deployment** | < 1 business day at p50 | Timestamp of spec commit → timestamp of prod deploy event in DAG metadata |
| **Engineer-hours per promotion** | < 2 hours at p50 | Self-report on PR via a "time spent" comment; sanity-checked against PR cycle time |
| **% of promotions requiring two-reviewer** | < 25% | Label counter on closed PRs |
| **% of generated tests merged with non-trivial human edits** | < 40% | Diff comparison: generated test file at PR-open vs at PR-merge |
| **% of pipelines failing within 7 days of promotion** | < 5% | Incident tracker, joined on pipeline `name` |
| **DQ-check coverage** | > 90% of prod pipelines have ≥ 3 active DQ checks | Inventory query |

### 5.2 Process metrics (operational health)

| Metric | Why it matters |
|---|---|
| LLM call cost per promotion | Ensures the system stays affordable; alert if > $5 per promotion |
| Time per LLM call (P95) | UX; alert if > 60s end-to-end |
| Template version mismatch | Catches the "I'm running an old template" footgun |
| Utility-match precision / recall | Tracked manually on a 10-PR audit each quarter |

### 5.3 The leading indicators

The two leading indicators of trouble:

- Rising `% of generated tests merged with non-trivial human edits` — means the test scaffolding is degrading and developers are working around it. Fix by improving the test prompt or the templates.
- Rising `% of pipelines failing within 7 days` — means the templates or DQ-check defaults are out of date for current production reality. Fix by reviewing the failing pipelines and updating templates.

Both are tracked on a small internal dashboard. The dashboard is itself produced by `promoteit` to dogfood the system.

---

## 6. What this is not

To be explicit, because the assignment asks for opinionated design:

- **Not a full agent.** No autonomous task loops, no AI tools that touch production, no LLM-driven runtime decisions. The LLM is invoked at code-generation time only, behind a human-approved PR gate.
- **Not a no-code platform.** The output is plain Python + dbt + Airflow that a human can read, modify, and own forever. The system gets out of the way after the PR is merged.
- **Not a one-shot.** The same `promoteit` tool is used for ongoing maintenance: re-running it after a major spec change re-renders the scaffold and produces a diff PR.
- **Not married to a specific LLM.** The model is named in config, pinned, and replaceable. Templates and the utility registry do most of the heavy lifting; the LLM is a thin orchestration layer.
