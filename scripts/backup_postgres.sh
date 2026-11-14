#!/usr/bin/env bash
set -euo pipefail

DB_HOST="${PGHOST:-postgres}"
DB_PORT="${PGPORT:-5432}"
DB_NAME="${PGDATABASE:-airflow}"
DB_USER="${PGUSER:-airflow}"
BACKUP_DIR="${BACKUP_DIR:-./backups}"
RETENTION_DAYS="${RETENTION_DAYS:-30}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
DESTINATION="${BACKUP_DIR}/postgres-${DB_NAME}-${TIMESTAMP}.dump"

mkdir -p "${BACKUP_DIR}"

export PGHOST DB_PORT DB_NAME DB_USER

pg_dump --host="${DB_HOST}" --port="${DB_PORT}" --username="${DB_USER}" --format=d --file="${DESTINATION}" "${DB_NAME}"

find "${BACKUP_DIR}" -type d -name "postgres-${DB_NAME}-*.dump" -mtime +"${RETENTION_DAYS}" -exec rm -rf {} +

echo "Postgres backup created: ${DESTINATION}"
