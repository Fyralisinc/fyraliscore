-- =====================================================================
-- 0017_view_ceo_cache.sql — Agent-GRT (Company OS CEO view)
-- =====================================================================
-- CONTRACTS.md §3. Backing store for the pre-computed CEO view
-- (greeting, cards, query_grid, status) and prefetched query-grid
-- responses. One row per (tenant_id, cache_key). The scheduler
-- (services/greeting/scheduler.py) rewrites these rows every 15 min
-- and on trigger-driven invalidation; the HTTP endpoint
-- (services/greeting/api.py::GET /view/ceo/home) reads them.
--
-- Style mirrors 0015_post_commit_durability.sql / 0016_think_run_costs.sql:
--   * CREATE TABLE IF NOT EXISTS for idempotency
--   * tenant-scoped; secondary index on (tenant, time) for age queries
--   * single transactional migration
--   * no cross-table FKs (tenant lifecycle handled elsewhere)
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS view_ceo_cache (
  tenant_id UUID NOT NULL,
  cache_key TEXT NOT NULL,
  cached_content JSONB NOT NULL,
  cached_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  recomputed_reason TEXT,  -- 'scheduled' | 'trigger_fired' | 'manual'
  PRIMARY KEY (tenant_id, cache_key)
);

-- Age dashboards / staleness audits.
CREATE INDEX IF NOT EXISTS view_ceo_cache_tenant_time
  ON view_ceo_cache (tenant_id, cached_at);

COMMIT;
