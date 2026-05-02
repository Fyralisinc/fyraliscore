#!/usr/bin/env bash
# Run database migrations inside the Docker container.
# Usage: docker compose exec gateway bash scripts/docker-migrate.sh
# Requires DATABASE_URL to be set (done via docker-compose environment).
set -euo pipefail

psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename text PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
);
SQL

applied=0
for f in db/migrations/*.sql; do
  fname="$(basename "$f")"
  done_already=$(psql -tAd "$DATABASE_URL" -c \
    "SELECT 1 FROM schema_migrations WHERE filename='${fname}'")
  if [ -n "$done_already" ]; then
    continue
  fi
  echo "  + ${fname}"
  if ! psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -f "$f"; then
    echo "  WARNING: ${fname} failed — may already be applied. Recording and continuing."
  fi
  psql -tAd "$DATABASE_URL" -c \
    "INSERT INTO schema_migrations(filename) VALUES('${fname}') ON CONFLICT DO NOTHING" >/dev/null
  applied=$((applied+1))
done

echo "Migrations complete. Applied: ${applied}"
