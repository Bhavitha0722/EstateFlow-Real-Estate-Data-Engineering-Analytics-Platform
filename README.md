# Data Operations Platform

An Apache Airflow, PostgreSQL, and Docker Compose project for the real-estate listings pipeline, its analytics API, and the Atlas dashboard.

## Local stack

Copy `.env.example` to `.env`, review the local settings, then start the LocalExecutor stack:

```powershell
docker compose --env-file .env -f docker-compose-LocalExecutor.yml up -d --build
```

- Airflow: <http://localhost:8080>
- Dashboard: <http://localhost:5173>
- API documentation: <http://localhost:8000/docs>

Keep actual credentials and webhook URLs in `.env`; do not commit them. The checked-in `.env.example` contains local placeholders only.

## Airflow workflows

### `real_estate_analytics`

This is the canonical dashboard workflow. It runs daily (`@daily`, `catchup=False`) and performs these tasks in order:

1. Check the source CSV and run the configured data-quality gate.
2. Clean the input, stamp `process_date`, and filter the records.
3. Upsert the filtered snapshot into PostgreSQL and refresh neighbourhood and room-type aggregates.
4. Check data freshness.

Task failures invoke the existing failure-notification callback. The loader maintains a current snapshot, not historical daily copies.

### `supply_chain_dag`

This is a separate manual workflow (`schedule=None`, `catchup=False`) with four tasks: `check_data`, `process_data`, `filter_data`, and `load_curated_data`. It currently reuses the real-estate CSV cleaner, ID filter, and database loader; despite its DAG ID, it is not an inventory/orders/suppliers pipeline. It does not replace or control `real_estate_analytics`.

The API's `PIPELINE_DAG_ID` is `real_estate_analytics`. Therefore, the dashboard's Run, status, history, and retry controls target that DAG. Retry is available only for a failed run.

## API endpoints

| Method and path | Purpose |
| --- | --- |
| `GET /health/live` | API process health |
| `GET /health/ready` | PostgreSQL readiness |
| `GET /api/v1/summary` | Listing, neighbourhood, room-type, price, availability, and last-load KPIs |
| `GET /api/v1/data-quality` | Record count, missing-value count, duplicate IDs, invalid prices, and validation status |
| `GET /api/v1/metrics/neighbourhoods` | Neighbourhood metrics; supports `neighbourhood_group` and `limit` |
| `GET /api/v1/metrics/room-types` | Room-type metrics |
| `GET /api/v1/listings` | Paginated listings; supports neighbourhood and room-type filters |
| `GET /api/v1/operations/pipeline` | Current DAG state, latest successful run, load time, and freshness |
| `GET /api/v1/operations/pipeline/history` | Recent DAG runs and failed-task counts |
| `POST /api/v1/operations/pipeline/run` | Trigger `real_estate_analytics` |
| `POST /api/v1/operations/pipeline/retry?dag_run_id=<id>` | Retry a failed `real_estate_analytics` run |

The dashboard uses same-origin proxy paths for the protected operations calls. In local development the proxy keeps `OPERATIONS_API_KEY` out of the browser. The API's development role override is local-only; production authentication uses OIDC.

## Dashboard behavior

The dashboard reads its KPIs, charts, listing rows, and quality figures from the API, which queries PostgreSQL. It displays the latest successful Airflow run time and current run state. The hero borough counts are summed from neighbourhood metrics. Data and status refresh every 30 seconds; the refresh button forces an immediate refresh. Sections can show their own unavailable or empty state if a request fails or returns no rows.

The quality panel reports total records, missing values across the checked listing fields, duplicate IDs, invalid prices, and validation status. The API uses `DATA_QUALITY_MAX_NULL_RATE`, `DATA_QUALITY_MIN_PRICE`, and `DATA_QUALITY_MAX_PRICE`; an empty table is reported as `empty` rather than `passed`.

## Data and database

- Source CSV: `input_files/modified_data.csv`
- Generated cleaned CSV: `input_files/modified_data_cleaned.csv`
- Generated filtered CSV and metrics: `output_files/`
- Airflow connection ID: `analytics_db`, configured for the Compose PostgreSQL service
- Tables: `real_estate_listings`, `real_estate_neighbourhood_metrics`, and `real_estate_room_type_metrics`

The LocalExecutor Compose file mounts the input and output directories into the Airflow containers under `AIRFLOW_DATA_DIR` (default `/opt/airflow`).

## Tests and checks

Run the unit suite:

```powershell
python -m pytest -q
```

Current automated coverage includes:

- `tests/test_real_estate_pipeline.py`: cleaning, required columns, empty input, filtering, CSV-to-loader parsing, duplicate IDs, and raw-file quality rules.
- `tests/test_api.py`: API summary over HTTP with a test database dependency, database quality calculation/status, room-type and listing query behavior, RBAC, and Airflow operation logic using test doubles.
- `tests/test_analytics_db_integration.py`: opt-in PostgreSQL test for idempotent upsert, stale-row removal, aggregate refresh, and transaction rollback. It is run in CI against an isolated PostgreSQL service and a unique temporary schema.
- `tests/test_notifications.py`: failure and stale-data webhook payload behavior.

The API test exercises the HTTP route but uses a fake connection; it does not prove a live API-to-PostgreSQL request. There is no browser-level dashboard integration test. The PostgreSQL integration test requires Airflow 2.10.5/Postgres provider dependencies plus `RUN_POSTGRES_INTEGRATION_TESTS=1` and a disposable `TEST_ANALYTICS_DATABASE_DSN`; it is skipped by default in a local unit-test run.

The CI workflow runs Ruff, unit and disposable-PostgreSQL integration tests with coverage, mypy for the transform modules, Python compilation, Compose configuration validation, and an API Docker image build. Equivalent local checks:

```powershell
python -m ruff check api dags tests
python -m ruff format --check api dags tests
python -m mypy --follow-imports=silent dags/clean.py dags/filter.py
python -m compileall -q dags tests
docker compose --env-file .env.example -f docker-compose-LocalExecutor.yml config --quiet
docker build --file Dockerfile.api --tag analytics-api:local .
```

## Git hygiene and deployment

`.gitignore` excludes `.env`, Airflow configuration, local logs, and coverage artifacts. Generated cleaned and output files are ignored; the source CSV remains available to the pipeline. Before committing, inspect `git status` and the staged file list in the actual Git checkout.

Production deployment still requires provider-specific OIDC, managed secret storage, HTTPS/network policies, and operational backup/restore scheduling. The repository's deployment files are templates, not a live production deployment.