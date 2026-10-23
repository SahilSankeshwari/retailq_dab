# Contributing to RetailQ

## Development Workflow

1. Keep the `main` branch stable at all times.
2. Create a feature branch from `main` for each change (e.g., `feature/your-feature`).
3. Make and test changes locally using `uv run pytest` and `uv run ruff check`.
4. Commit changes with a clear, descriptive message.
5. Push the feature branch to GitHub.
6. Open a pull request into `main` and request review.
7. Merge only after CI checks pass and a reviewer approves.

## Databricks Bundle Validation

Always validate the bundle before deploying:

```bash
# Validate development target
databricks bundle validate -t dev

# Review the deployment plan
databricks bundle plan -t dev

# Deploy to development (requires workspace access)
databricks bundle deploy -t dev
```

Review the deployment plan before applying changes. Do not deploy to production without explicit authorization and completing the promotion checklist in README.md.

## Code Quality

* **Ruff** is used for linting and formatting (line-length 120, target py312).
* **pytest** runs unit tests in `unit_tests/` (no Databricks connection needed for most tests).
* **Integration tests** in `tests/` require Databricks Connect and a configured cluster.

```bash
# Lint and format
uv run ruff check src/ unit_tests/ tests/
uv run ruff format src/ unit_tests/ tests/

# Run unit tests
uv run pytest --noconftest -c pytest-unit.ini unit_tests -v
```

## Security

* Never commit passwords, access tokens, client secrets, or connection strings.
* Keep local credentials (`.databrickscfg`, `.env`) outside version control — they are gitignored.
* Source system connections are managed as Unity Catalog connections in the workspace.
* The production catalog (`retail_q`) must never be modified from a development branch.
