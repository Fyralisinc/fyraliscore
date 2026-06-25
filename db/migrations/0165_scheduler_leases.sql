-- =====================================================================
-- 0165_scheduler_leases.sql
-- =====================================================================
-- Deployment-global row leases for in-process schedulers that must be
-- singleton across horizontally scaled gateway replicas. The rows contain no
-- tenant data and are intentionally not tenant-scoped.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS scheduler_leases (
  lease_name   TEXT PRIMARY KEY,
  holder_id    TEXT NOT NULL,
  expires_at   TIMESTAMPTZ NOT NULL,
  acquired_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata     JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS scheduler_leases_expires_at_idx
  ON scheduler_leases (expires_at);

COMMIT;
