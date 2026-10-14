# RetailFlow Databricks Automation Bundle

This folder contains the deployable Databricks resources for the [RetailFlow portfolio project](../README.md).

## Contents

- `databricks.yml`: DEV, TEST and PROD targets, with environment-specific catalog and Bronze source table variables.
- `resources/retailflow_cicd.pipeline.yml`: Spark Declarative Pipeline definition, source configuration and PySpark source path.
- `resources/retailflow_dev_job.job.yml`: Lakeflow Job resource that references the bundle-managed pipeline. Its schedule is **paused**.
- `src/retailflow_cicd/transformations/`: PySpark Silver and Gold transformations.

## Targets

| Target | Unity Catalog | Bronze customers | Bronze orders |
|---|---|---|---|
| dev | `retailflow_dev` | `dev_customers` | `dev_orders` |
| test | `retailflow_test` | `test_customers` | `test_orders` |
| prod | `retailflow_prod` | `prod_customers` | `prod_orders` |

All targets use the **same Databricks Free Edition workspace**, with separate catalogs and deployment paths. Source Bronze tables must already exist.

## Deploy

Use the **Deployments** (rocket) panel in the Databricks workspace to select a target, validate and deploy. Alternatively, from this directory, with an authenticated Databricks CLI:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
```

Replace `dev` with `test` or `prod` when appropriate. The GitHub Actions CD workflow automatically validates and deploys `prod` after pushes to `main`.

**Deployment does not run the pipeline.** Run the deployed pipeline separately in Databricks and check its update status, data quality expectations and output tables.

## CI/CD and safety

The repository's CI currently checks YAML structure and Python syntax. Automated PySpark unit tests are not yet integrated. CD requires `DATABRICKS_HOST` (repository variable) and `DATABRICKS_TOKEN` (secret). Never commit credentials.

See the [architecture diagram](../docs/architecture.md) and [main README](../README.md) for the full workflow and verified run results.
