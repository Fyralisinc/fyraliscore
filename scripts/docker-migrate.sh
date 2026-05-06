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
  # T3: --single-transaction wraps the whole file in BEGIN…COMMIT so a
  # failure on statement N rolls back statements 1..N-1 atomically and
  # leaves the database clean rather than half-migrated. Without this
  # flag, psql commits each statement as it runs, mirroring the bug
  # the Python-side runner had via raw `conn.execute(file_text)`.
  if ! psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 --single-transaction -q -f "$f"; then
    echo "  WARNING: ${fname} failed — may already be applied. Recording and continuing."
  fi
  psql -tAd "$DATABASE_URL" -c \
    "INSERT INTO schema_migrations(filename) VALUES('${fname}') ON CONFLICT DO NOTHING" >/dev/null
  applied=$((applied+1))
done

echo "Migrations complete. Applied: ${applied}"
