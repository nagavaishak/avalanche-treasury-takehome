# Promotion PR — Reviewer Checklist

Use this every time you review a promoteit-generated PR. Estimated time: 10–20 minutes per PR. If the PR exceeds 30 minutes of review, the promoteit prompts or templates need tightening — file a feedback issue.

## Author has confirmed

- [ ] The `purpose:` block in `pipeline_spec.yaml` is accurate. (AI cannot judge intent — this is a hand-written fact.)
- [ ] The SLA, owner, and oncall fields are correct for this pipeline's actual operational profile.
- [ ] DQ-check thresholds were chosen deliberately (not just defaulted to the example values).
- [ ] All `# TODO: assert` lines in the generated tests have been replaced with real assertions.

## Generated code

- [ ] The Airflow DAG imports the utilities listed in `spec.dependencies.utilities`. If the LLM suggested a utility that does *not* fit, the suggestion was rejected and the rationale is in a PR comment.
- [ ] No utility from `/utilities/` was modified by this PR. (If yes, add a second reviewer and the `requires-second-reviewer` label.)
- [ ] No schema migration (`migrations/`) was modified. (If yes, see two-key migration policy.)
- [ ] DAG task graph matches what the author expected — the AST analyzer can miss implicit ordering.
- [ ] Retry / SLA settings match `spec.retries` and `spec.freshness.sla`.

## DQ and contracts

- [ ] At least one `freshness` check is wired.
- [ ] At least one `schema_match` check binds to a real `contracts/*.schema.json` file.
- [ ] Numeric-range checks have business-justified bounds, not LLM-defaulted ones.
- [ ] DQ severity levels are intentional: `page` only for things that need 24/7 response.

## Testing

- [ ] CI is green (lint, type, unit, integration).
- [ ] The generated unit-test scaffold has had assertions filled in for every function.
- [ ] At least one integration test runs against the fixtures in `tests/fixtures/`.
- [ ] The DAG starts up in the local docker-compose Airflow without errors.

## Cost and operations

- [ ] The pipeline's monthly cost estimate (queries × volume × source pricing) is within `spec.cost_baseline.monthly_usd_max`. If not, add a finance reviewer.
- [ ] The pipeline does not introduce a new external dependency without a `rationale:` in `spec.inputs`.
- [ ] The pipeline does not introduce a new vendor without an architecture-level sign-off.

## High-risk file flags

If the PR touches any of:

- `utilities/`
- `migrations/`
- `dbt/models/marts/`
- `*.dq.*`
- `.github/workflows/`

then the `requires-second-reviewer` label MUST be set and a second reviewer MUST approve before merge. Single-reviewer merges on these paths is a P1 incident.

## Final sign-off

- [ ] I have spent at least 10 minutes on this review.
- [ ] If anything felt off but I couldn't articulate it, I requested changes anyway. The cost of a slow review is much lower than the cost of a bad pipeline.
