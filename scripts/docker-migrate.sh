#!/usr/bin/env bash
# Run database migrations inside the Docker container.
# Usage: docker compose exec gateway bash scripts/docker-migrate.sh
# Requires DATABASE_URL to be set (done via docker-compose environment).
set -euo pipefail

# Fail fast on duplicate numeric prefixes. Two files sharing a prefix
# (e.g. 0014_a.sql and 0014_b.sql) make apply-order depend on locale
# collation, which can diverge across environments and produce
# non-deterministic schemas. Reject before applying anything.
dupes="$(
  for f in db/migrations/*.sql; do
    basename "$f" | sed -E 's/^([0-9]+)_.*/\1/'
  done | sort | uniq -d
)"
if [ -n "$dupes" ]; then
  echo "ERROR: duplicate migration prefixes detected: ${dupes}" >&2
  echo "Each db/migrations/*.sql must have a unique numeric prefix." >&2
  exit 1
fi

# BYOC §12 G1 — formal definition lives in db/migrations/0155_schema_migrations.sql;
# this lazy bootstrap keeps shape parity (incl. the checksum column added by 0155
# for drift detection) for DBs whose first migration is applied by this runner.
psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 -q <<'SQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
  filename text PRIMARY KEY,
  checksum text,
  applied_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum text;
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
  #
  # Ingestion LLD §1.6: CREATE INDEX CONCURRENTLY cannot run inside an
  # explicit transaction. Files containing the keyword CONCURRENTLY
  # (excluding -- line comments) OR an opt-in `-- migration:no-transaction`
  # directive are run WITHOUT --single-transaction. Such files lose the
  # atomic-rollback guarantee and should contain a single CONCURRENTLY
  # statement. The grep matches token-level CONCURRENTLY only after
  # stripping line comments via sed.
  if sed 's|--.*$||' "$f" | grep -qiE '\bCONCURRENTLY\b' \
       || grep -qiE '^[[:space:]]*--[[:space:]]*migration:no-transaction\b' "$f"; then
    psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 -q -f "$f"
  else
    psql -d "$DATABASE_URL" -v ON_ERROR_STOP=1 --single-transaction -q -f "$f"
  fi
  # BYOC §12 G1 — capture the file digest so the control plane can detect a
  # silently-edited applied migration (schema drift). sha256sum is part of
  # coreutils and present in the gateway image.
  checksum="$(sha256sum "$f" | cut -d' ' -f1)"
  psql -tAd "$DATABASE_URL" -c \
    "INSERT INTO schema_migrations(filename, checksum) VALUES('${fname}', '${checksum}') ON CONFLICT DO NOTHING" >/dev/null
  applied=$((applied+1))
done

echo "Core migrations complete. Applied: ${applied}"

# ADR-0004 extension-owned schema: after the core set, apply each installed
# extension's own migrations (company_os.migrations entry-point group), each under
# its own per-extension ledger so filenames never collide with the host's. A
# no-op when no extension contributes migrations. Idempotent + ledger-tracked.
echo "Applying extension-owned migrations…"
python scripts/apply_extension_migrations.py

echo "All migrations complete."
