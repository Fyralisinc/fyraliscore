-- =====================================================================
-- 0039_viewer_state.sql — per-viewer last-seen tracking for CEO view
-- =====================================================================
-- Track A of the CEO-view redesign (Tier 1 substrate plumbing). The
-- CEO home payload will surface a `viewer_state` block so the UI can
-- render delta indicators ("what moved since you last looked"). This
-- migration stores the last-seen timestamp per (tenant_id, viewer_id).
--
-- The companion repo (services/greeting/viewer_state_repo.py) does an
-- atomic upsert-returning-previous in a single SQL statement so two
-- concurrent GET /view/ceo/home requests never race the read/write.
--
-- viewer_id is a free-form TEXT — for single-tenant dogfood it is just
-- "default"; for token-authenticated requests it is the bearer token
-- (or a derived identifier). Per-viewer/per-request, NOT per-tenant
-- cache; intentionally NOT a member of view_ceo_cache.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS viewer_state (
  tenant_id   UUID NOT NULL,
  viewer_id   TEXT NOT NULL,
  last_seen_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, viewer_id)
);

-- Per-tenant scans ("every viewer's last-seen for this tenant") are
-- the only secondary access pattern we expect. The PK already covers
-- (tenant_id, viewer_id), and (tenant_id) is its leading prefix, so
-- no extra index is needed today.

-- Apply the same RLS policy as the migration-0036 sweep. viewer_state
-- is tenant-scoped, so the `tenant_isolation` policy fits without
-- modification.
ALTER TABLE viewer_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE viewer_state FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON viewer_state;
CREATE POLICY tenant_isolation ON viewer_state
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
