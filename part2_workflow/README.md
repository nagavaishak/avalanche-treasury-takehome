# Part 2 — Example Artifacts

Concrete artifacts that make the PromoteIt design (see `design/02_part2_ai_productionization.md`) tangible. None of these are toy — they're what the real system's first version would ship with.

| File | Purpose |
|---|---|
| `pipeline_spec.example.yaml` | A full filled-in metadata contract for a real-world treasury pipeline. The system's source of truth. |
| `templates/airflow_dag_template.py.j2` | One of the production Jinja2 templates. Shows how the spec maps into deterministic scaffolding. |
| `promotion_checklist.md` | The human-side checklist a reviewer uses on every promotion PR. |
| `sample_pr_description.md` | The structured PR description PromoteIt generates. |
| `prompts/template_mapping.md` | The exact LLM prompt for the template-mapping step. |
| `prompts/utility_match.md` | The exact LLM prompt for the utility-matching step. |
