-- 0187_schema_migrations.sql
--
-- G1 (BYOC control-plane §12) — a durable schema-version ledger so the fleet
-- control plane can remotely answer "is this deployment on the right schema,
-- and did a migration fail?". Closes the §9.2 "Database & schema integrity"
-- gap tagged 🔴 **no `schema_migrations` table**.
--
-- The two migration runners (lib/shared/migrations.py and
-- scripts/docker-migrate.sh) already lazily create a minimal ledger
-- `schema_migrations(filename, applied_at)` via CREATE TABLE IF NOT EXISTS
-- before applying anything. This migration is the FORMAL, checked-in
-- definition of that table and additively widens it with the columns the
-- design names: the applied-migration id (= filename, the lex-ordered
-- 0NNN_ prefix that defines schema version), a content `checksum` so drift /
-- silent-edit of an already-applied file is detectable, and `applied_at`.
--
-- Idempotent + additive so it is safe whether the lazily-created table exists
-- or not: CREATE TABLE IF NOT EXISTS lays down the full shape on a fresh DB,
-- and ADD COLUMN IF NOT EXISTS back-fills the `checksum` column onto a DB that
-- was bootstrapped by the older two-column runner. Both runners record THIS
-- file (0187_schema_migrations.sql) into the ledger after applying it, so the
-- ledger becomes self-describing from this point forward.
--
-- Infra bookkeeping, NOT tenant data: it tracks schema state for the whole
-- deployment, has no tenant_id, and carries no RLS — same pattern as
-- writer_poison_attempts (0137) / workflow_states (0065). "checksum" is the
-- runner-computed digest of the migration file's bytes at apply time.

BEGIN;

CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT        PRIMARY KEY,
    checksum   TEXT,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Back-fill the checksum column onto DBs first bootstrapped by the older
-- two-column lazy CREATE TABLE in the runners. NULL is allowed: rows recorded
-- before this migration have no captured checksum and stay NULL; rows recorded
-- by an updated runner carry the digest.
ALTER TABLE schema_migrations
  ADD COLUMN IF NOT EXISTS checksum TEXT;

COMMENT ON TABLE schema_migrations IS
  'G1 (BYOC §12) — schema-version ledger: one row per successfully applied db/migrations/*.sql, keyed by filename (the 0NNN_ prefix = monotonic schema version). checksum = runner-computed digest of the file bytes at apply time (drift detection); applied_at = when it landed. Infra bookkeeping; no tenant_id / RLS. Surfaced to the fleet control plane as fyralis_schema_* Prometheus metrics.';

COMMENT ON COLUMN schema_migrations.filename IS
  'Migration file name (e.g. 0187_schema_migrations.sql). The numeric prefix is the monotonic schema version reported as fyralis_schema_version.';
COMMENT ON COLUMN schema_migrations.checksum IS
  'Runner-computed digest of the migration file bytes at apply time. NULL for rows recorded before 0155 / by a runner that does not capture it. Lets the control plane detect a silently-edited applied migration (schema drift).';

COMMIT;
