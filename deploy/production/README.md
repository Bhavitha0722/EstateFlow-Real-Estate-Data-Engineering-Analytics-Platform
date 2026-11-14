# Production deployment guide

This directory contains the deployment scaffolding for production hardening. The repository keeps the local developer workflow intact, while the production profile expects secrets to come from a managed secret store and the provider configuration to be injected at runtime.

## 1. OIDC production wiring

The API already supports production OIDC validation through the following environment variables:

- `ENVIRONMENT=production`
- `OIDC_ISSUER`
- `OIDC_AUDIENCE`
- `OIDC_JWKS_URL`
- `OIDC_ROLES_CLAIM`
- `OPERATIONS_API_KEY`

Use the company identity provider configuration, then inject the values from a managed secret manager, for example:

- Azure Key Vault
- AWS Secrets Manager
- GCP Secret Manager
- Kubernetes Secret / External Secrets Operator
- HashiCorp Vault

The production startup checks in `api/main.py` enforce these values before the service becomes ready. The local development override remains disabled in production mode.

## 2. Secret management pattern

Do not commit `.env` or production credentials to git. For deployment, inject secret material as environment variables or mounted files from the platform's native secret store.

Recommended pattern:

- keep a non-secret `.env.example` for local developer onboarding
- use a secret manager to populate runtime values
- rotate secrets centrally and inject through CI/CD
- store only references in the repository, never the secret itself

## 3. HTTPS and ingress

A production deployment should terminate TLS at an ingress or reverse proxy and require HTTPS for all externally reachable routes.

The reference config in `traefik.yml` demonstrates an HTTPS router for the dashboard and API. It is intentionally deployment-oriented and should be adapted to the company gateway or load balancer.

## 4. Network policy

The sample policy in `network-policy.yaml` denies all ingress by default and then authorizes only the required internal flows:

- API to Postgres
- dashboard to API
- Airflow webserver to Postgres
- outbound access for OIDC and webhook endpoints only when required

It is a template for Kubernetes or other network-enforced deployments.

## 5. Database backup and restore

The repository includes backup and restore helper scripts in `scripts/`. Use them for operational recovery drills.

Typical workflow:

1. Run the backup script to create a timestamped dump
2. Validate the dump file exists and is readable
3. Run the restore script against a staging database
4. Re-run a focused smoke test on the restored data
5. Record the drill result in the incident log

## 6. Production checklist

- [ ] Set `ENVIRONMENT=production`
- [ ] Configure `OIDC_ISSUER`, `OIDC_AUDIENCE`, and `OIDC_JWKS_URL`
- [ ] Pull `OPERATIONS_API_KEY` from a secret manager
- [ ] Pull `AIRFLOW_PASSWORD`, DB credentials, and webhook secrets from a secret manager
- [ ] Enforce HTTPS and redirect HTTP to TLS
- [ ] Apply the network policy
- [ ] Run a backup/restore drill in a non-production environment
- [ ] Verify the API and dashboard still serve traffic after a restore

This scaffolding keeps the project deployable without exposing secrets in source control or local configuration files.
