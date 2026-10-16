# Retail Data Engineering Pipeline

An end-to-end retail data engineering project built with Databricks Declarative Automation Bundles (DAB), PySpark, Delta Lake, and multiple data sources.

## Project Overview

This project ingests retail data from Azure Blob Storage, PostgreSQL (Neon), and Salesforce. It processes the data through Bronze, Silver, and Gold layers and prepares curated datasets for business reporting and analytics.

## Technology Stack

- Databricks Declarative Automation Bundles (DAB)
- Databricks CLI
- PySpark and Delta Lake
- Azure Blob Storage
- PostgreSQL (Neon)
- Salesforce
- Unity Catalog
- Lakeflow Spark Declarative Pipelines
- Databricks Jobs, metric views, and dashboards
- Python and Git

## Pipeline Architecture
Azure Blob Storage ---+
                      |
PostgreSQL (Neon) ----+---> Bronze ---> Silver ---> Gold
                      |                         |
Salesforce -----------+                         v
                                      Metric View & Dashboards

## Repository Structure

- `databricks.yml` — bundle configuration and deployment targets
- `resources/` — job and pipeline definitions
- `src/` — notebooks and transformation code
- `dashboards/` — dashboard definitions
- `tests/` — test configuration and tests
- `fixtures/` — test fixtures

## Development Setup

Prerequisites: Python, Git, Databricks CLI, and access to a Databricks workspace with the required permissions and source connections.

Validate the development bundle:

```powershell
databricks bundle validate -t dev --profile RETAILQ_DEV
```

Preview deployment changes:

```powershell
databricks bundle plan -t dev --profile RETAILQ_DEV
```

Deploy to the development target only after reviewing the plan:

```powershell
databricks bundle deploy -t dev --profile RETAILQ_DEV
```

## Safety

- Never commit credentials, access tokens, private keys, or local secret files.
- Keep local Databricks authentication configuration outside Git.
- Review deployment plans before deploying.
- Do not deploy the production target without a separate review and explicit approval.

## Project Goal

Build a maintainable, version-controlled retail data pipeline with repeatable deployments and a clear separation between development and production.
