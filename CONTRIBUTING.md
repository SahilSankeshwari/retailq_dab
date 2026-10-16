# Contributing to RetailQ

## Development Workflow

1. Keep the main branch stable.
2. Create a feature branch for each change.
3. Make and test changes locally.
4. Commit changes with a clear message.
5. Push the feature branch to GitHub.
6. Open a pull request before merging into main.

## Databricks Deployment

Validate the development bundle before deploying:

```bash
databricks bundle validate -t dev --profile RETAILQ_DEV
databricks bundle plan -t dev --profile RETAILQ_DEV
```

Review the deployment plan before applying changes. Do not deploy to production without authorization.

## Security

- Never commit passwords, access tokens, or other secrets.
- Keep local credentials outside version control.
