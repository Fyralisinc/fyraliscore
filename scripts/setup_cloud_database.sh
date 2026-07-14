#!/usr/bin/env bash
# Provision a Postgres-compatible cloud database for Fyralis Core.
#
# The database itself must already exist and be reachable. This script verifies
# that the DSN is not local, applies core migrations with a migration ledger,
# applies extension migrations, and runs the schema drift check.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATABASE_URL_ARG="${DATABASE_URL:-}"
ALLOW_LOCAL=0
RUN_DRIFT_CHECK=1

usage() {
  cat <<'HELP'
Usage:
  scripts/setup_cloud_database.sh --database-url "postgresql://..."
  DATABASE_URL="postgresql://..." scripts/setup_cloud_database.sh

Options:
  --database-url URL     Cloud Postgres DSN to initialize.
  --allow-local          Permit localhost/127.0.0.1 DSNs.
  --skip-drift-check     Apply migrations without running check_schema_drift.py.
  -h, --help             Show this help.

The DSN is never written to disk. Use a local ignored env file, shell secret
manager, or provider dashboard to store it.
HELP
}

log() { printf "\033[1;36m[cloud-db]\033[0m %s\n" "$*"; }
fail() { printf "\033[1;31m[cloud-db]\033[0m %s\n" "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --database-url)
      [[ $# -ge 2 ]] || fail "--database-url requires a value"
      DATABASE_URL_ARG="$2"
      shift 2
      ;;
    --allow-local)
      ALLOW_LOCAL=1
      shift
      ;;
    --skip-drift-check)
      RUN_DRIFT_CHECK=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "Unknown arg: $1"
      ;;
  esac
done

[[ -n "$DATABASE_URL_ARG" ]] || fail "DATABASE_URL is required"

if [[ "$ALLOW_LOCAL" -eq 0 ]]; then
  case "$DATABASE_URL_ARG" in
    *localhost*|*127.0.0.1*|*::1*)
      fail "Refusing local DSN. Pass --allow-local only for intentional local checks."
      ;;
  esac
fi

command -v psql >/dev/null 2>&1 || fail "psql is required"

PYTHON_BIN="${PYTHON:-python}"
if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
fi

log "Checking connectivity"
psql -X "$DATABASE_URL_ARG" -v ON_ERROR_STOP=1 -qAt -c "SELECT current_database();" >/dev/null

log "Ensuring migration ledger"
psql -X "$DATABASE_URL_ARG" -v ON_ERROR_STOP=1 -q <<'SQL' >/dev/null
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
SQL

APPLIED_MIGRATIONS="$(
  psql -X "$DATABASE_URL_ARG" -qAt -v ON_ERROR_STOP=1 -c "SELECT filename FROM schema_migrations"
)"

applied=0
for migration in db/migrations/*.sql; do
  filename="$(basename "$migration")"
  if printf "%s\n" "$APPLIED_MIGRATIONS" | grep -Fxq "$filename"; then
    continue
  fi

  log "Applying ${filename}"
  psql -X "$DATABASE_URL_ARG" -v ON_ERROR_STOP=1 -q -f "$migration" >/dev/null
  psql -X "$DATABASE_URL_ARG" -qAt -v ON_ERROR_STOP=1 \
    -c "INSERT INTO schema_migrations(filename) VALUES ('${filename}') ON CONFLICT DO NOTHING" >/dev/null
  APPLIED_MIGRATIONS="${APPLIED_MIGRATIONS}
${filename}"
  applied=$((applied + 1))
done
log "Core migrations applied: ${applied} new"

log "Applying extension migrations"
DATABASE_URL="$DATABASE_URL_ARG" "$PYTHON_BIN" scripts/apply_extension_migrations.py

log "Checking required extensions"
extension_count="$(
  psql -X "$DATABASE_URL_ARG" -v ON_ERROR_STOP=1 -qAt <<'SQL'
SELECT extname
FROM pg_extension
WHERE extname IN ('vector', 'pg_trgm', 'btree_gin')
ORDER BY extname;
SQL
)"
printf "%s\n" "$extension_count"
if [[ "$(printf "%s\n" "$extension_count" | sed '/^$/d' | wc -l | tr -d ' ')" != "3" ]]; then
  fail "Missing one or more required extensions: vector, pg_trgm, btree_gin"
fi

if [[ "$RUN_DRIFT_CHECK" -eq 1 ]]; then
  log "Running schema drift check"
  "$PYTHON_BIN" scripts/check_schema_drift.py --dsn "$DATABASE_URL_ARG"
fi

log "Cloud database is ready for DB-backed metabolism tracing"
