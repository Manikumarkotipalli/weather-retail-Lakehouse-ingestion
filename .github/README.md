# GitHub Actions CI/CD

This repository uses GitHub Actions to validate and deploy the Databricks bundle in `weather_retail_lakehouse`.

## Required GitHub environments

Create these environments in GitHub repository settings:

- `dev`
- `prod`

Add the following environment variables to each environment:

- `DATABRICKS_HOST`: the Databricks workspace URL, for example `https://dbc-ff2689c4-e54f.cloud.databricks.com`
- `DATABRICKS_CLIENT_ID`: the Databricks service principal application ID configured for GitHub OIDC federation
- `RUN_DATABRICKS_TESTS`: optional; set to `true` only when Databricks Connect test authentication and compute are ready

## Authentication

The workflows use GitHub OIDC authentication with:

```yaml
DATABRICKS_AUTH_TYPE: github-oidc
```

Configure a Databricks service principal with a GitHub Actions federation policy for this repository, then grant it permission to deploy the bundle resources.

## Workflows

- `CI`: runs on pull requests, pushes to `main`, and manual dispatch. It validates the Databricks bundle against the `dev` target and runs Python checks.
- `Deploy Databricks Bundle`: deploys the `dev` target on pushes to `main`. Manual runs can deploy either `dev` or `prod` and optionally run a bundle resource after deployment.

Useful manual resource keys:

- `wf_bronze_landing_walmart_csv`
- `wf_bronze_landing_weather_api`
- `weather_forecast_pipeline`
