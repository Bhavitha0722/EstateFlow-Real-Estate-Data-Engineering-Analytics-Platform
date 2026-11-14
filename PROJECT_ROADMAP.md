# Project Roadmap

## Product goal

Build a small but credible data operations platform that demonstrates orchestration, data quality, analytics, machine learning, observability, and delivery practices used in modern data teams.

## Milestone 1: Foundation

- [x] Remove credentials from Compose configuration.
- [x] Add safe local environment configuration.
- [x] Persist the local PostgreSQL metadata database.
- [x] Replace the legacy Airflow image with a supported Airflow release.
- [x] Add automated Python tests and formatting checks.
- [ ] Replace the inherited README with project documentation.

## Milestone 2: Real-estate analytics

- [x] Validate the raw listings schema and reject malformed files.
- [x] Make cleaning and filtering tasks deterministic and rerunnable.
- [x] Load curated records into PostgreSQL instead of relying only on CSV output.
- [x] Produce neighborhood, room-type, availability, and price metrics.
- [x] Add a read-only API over curated and aggregate tables.
- [x] Publish a dashboard showing trends and data-quality status.

## Milestone 3: Operational control plane

- [x] Expose protected pipeline status and data-freshness APIs.
- [x] Add a protected dashboard action to trigger the Airflow pipeline.
- [x] Add automatic dashboard refresh for operational status.
- [x] Add pipeline run history and task-level failure visibility.
- [x] Add retry controls for failed pipeline runs.
- [x] Add Slack or Teams-compatible alerts for failed tasks.
- [x] Add alerts for stale data.

## Milestone 4: Security and platform hardening

- [x] Add request IDs, security headers, audit-style logs, and production config checks.
- [x] Add bounded rate limiting for operational API routes.
- [x] Add provider-neutral OIDC JWT validation and role enforcement.
- [ ] Connect OIDC to the company identity provider and validate production claims.
- [ ] Move secrets to a secret manager for deployed environments.
- [ ] Move rate limiting to a distributed gateway or shared Redis service.
- [ ] Add HTTPS and network policies.
- [ ] Add database backups, restore drills, and retention policies.

## Milestone 5: Supply-chain intelligence

- [ ] Add representative orders, inventory, suppliers, and shipments datasets.
- [ ] Build ingestion, normalization, and warehouse-loading workflows.
- [ ] Track late deliveries, stockout risk, supplier performance, and order fill rate.
- [ ] Add threshold-based alerts for operational risks.

## Milestone 6: Machine learning

- [ ] Train an explainable listing-price model.
- [ ] Forecast demand and inventory risk.
- [ ] Track datasets, features, experiments, and model versions.
- [ ] Schedule batch predictions through Airflow.

## Milestone 7: Delivery and operations

- [x] Add CI checks for tests, formatting, and Compose validation.
- [ ] Add DAG import checks, security scanning, and dependency scanning.
- [ ] Add structured logs, metrics, tracing, and failure notifications.
- [ ] Document staging/production deployment and architecture decisions.
- [ ] Add release versioning, rollback, and change-approval workflows.

## Definition of done

Each production-style workflow should be idempotent, observable, testable, documented, and able to recover from a failed task without manually editing generated files.