#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <backup-directory>"
  exit 1
fi

BACKUP_PATH="$1"
DB_HOST="${PGHOST:-postgres}"
DB_PORT="${PGPORT:-5432}"
DB_NAME="${PGDATABASE:-airflow}"
DB_USER="${PGUSER:-airflow}"
RESTORE_TARGET="${RESTORE_TARGET:-${DB_NAME}_restore_test}"

if [ ! -d "${BACKUP_PATH}" ]; then
  echo "Backup directory not found: ${BACKUP_PATH}"
  exit 1
fi

export PGHOST DB_PORT DB_NAME DB_USER

createdb --host="${DB_HOST}" --port="${DB_PORT}" --username="${DB_USER}" "${RESTORE_TARGET}" || true
pg_restore --host="${DB_HOST}" --port="${DB_PORT}" --username="${DB_USER}" --clean --if-exists --dbname="${RESTORE_TARGET}" "${BACKUP_PATH}"

echo "Database restored into: ${RESTORE_TARGET}"
