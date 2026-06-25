-- =====================================================================
-- 0156_post_commit_pending_tenant_index.sql
-- =====================================================================
-- Add the tenant-scoped hot-path index used by per-tenant post-commit
-- workers. The global scheduled_at partial index remains for unscoped
-- workers that drain all tenants from one process.
-- =====================================================================

BEGIN;

CREATE INDEX IF NOT EXISTS post_commit_pending_tenant_idx
  ON pending_post_commit_actions (tenant_id, scheduled_at)
  WHERE processed_at IS NULL AND dead_lettered_at IS NULL;

COMMIT;
