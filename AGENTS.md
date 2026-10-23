# RetailQ DAB Project — AI Agent Instructions

## Project Overview

This is a Declarative Automation Bundle (DAB) for the RetailQ retail analytics pipeline. It deploys an 8-task end-to-end job that ingests data from Azure Blob Storage, PostgreSQL (Neon), and Salesforce, processes it through Bronze-Silver-Gold layers, and refreshes dashboards.

## Key Files

- `databricks.yml` — Bundle configuration with `dev` and `prod` targets
- `resources/` — Pipeline and job YAML definitions
- `src/retail_transformation_v2/` — SDP pipeline source code (bronze_to_silver + silver_to_gold)
- `src/*.ipynb` — Ingestion, gold views, calendar, and metric view notebooks
- `unit_tests/` — Standalone unit tests (no Databricks connection needed)
- `tests/` — Integration test configuration (requires Databricks Connect)

## Important Conventions

- The catalog is parameterized via `${var.catalog}` in all YAMLs. Python files use schema-qualified names (e.g., `retail_silver.transactions`) — the catalog is resolved by the pipeline's `catalog` field.
- Notebook widget defaults use `retail_q_dab_dev` but are overridden by the job's `base_parameters: { catalog: ${var.catalog} }`.
- `src/03_calendar.py` is deprecated — the calendar is handled by `src/03_calendar.ipynb` as a separate job task.
- The `prod` target is prepared but NOT deployed. Do not run `databricks bundle deploy -t prod` without explicit approval.
- The production catalog is `retail_q`. Never modify production resources from a development branch.

## Development Commands

```bash
databricks bundle validate -t dev    # Validate dev bundle
uv run ruff check src/ unit_tests/ tests/   # Lint
uv run pytest --noconftest -c pytest-unit.ini unit_tests -v  # Unit tests
```

## Safety Rules

- Never deploy, run, or modify the production catalog (`retail_q`) without explicit approval.
- Never commit secrets, tokens, or credentials to Git.
- Never push directly to `main` — create a feature branch and open a PR.
- The Databricks CLI is pre-authenticated in the workspace environment.
