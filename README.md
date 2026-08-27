# weather-retail-Lakehouse-ingestion

## CI/CD

GitHub Actions workflows live in `.github/workflows`:

- `CI` validates the Databricks bundle and runs Python checks.
- `Deploy Databricks Bundle` deploys the bundle to Databricks.

See `.github/README.md` for the required GitHub environment variables and Databricks OIDC setup.
