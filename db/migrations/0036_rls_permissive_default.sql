-- =====================================================================
-- 0036_rls_permissive_default.sql — Row-Level Security with permissive default
-- =====================================================================
-- Phase 4 of the architectural overhaul. Enables RLS on every
-- tenant-scoped table and installs a single policy named
-- `tenant_isolation` that says:
--
--    "If app.current_tenant is unset (empty string), allow ALL rows.
--     Otherwise, allow only rows where tenant_id matches."
--
-- This is intentionally permissive: existing code paths that don't yet
-- use lib.shared.tenant_context.tenant_transaction() see no behavior
-- change, while any code path that DOES use it gets defense-in-depth
-- enforcement at the database — even a bug that omits `WHERE tenant_id
-- = $1` cannot return another tenant's data when the policy is active.
--
-- Once every repo and worker is on TenantContext (a follow-up plan),
-- the policy's permissive branch can be dropped and RLS becomes
-- mandatory. Until then, this is a safety net rather than a hard wall.
--
-- Tables NOT covered (intentionally):
--   * model_status_notes — inherits tenant via FK to models. Adding a
--     subquery-based policy is expensive on the hot path; skipped here.
--   * commitment_contributors / contributes_to / depends_on /
--     constrained_by / resource_deployments — junction tables without
--     tenant_id. Inherit via parent.
--   * tenants / demo_configs — global registry tables, not tenant-scoped.
--   * Partitioned parents only (RLS on observations / resource_transactions
--     cascades to partitions automatically in Postgres 12+).
-- =====================================================================

BEGIN;

DO $$
DECLARE
  t TEXT;
  -- Every base table whose primary tenancy column is `tenant_id`.
  tenant_tables TEXT[] := ARRAY[
    -- Foundation (0001)
    'actors', 'observations', 'models', 'goals', 'commitments',
    'decisions', 'resources', 'resource_transactions', 'entity_aliases',
    -- Sessions, queues, caches (0003-0008)
    'actor_sessions', 'think_trigger_queue', 'entity_review_queue',
    'relationship_maintenance_log',
    'model_reeval_queue', 'think_region_lock_log',
    'applied_triggers', 'think_runs', 'model_reeval_dead_letter',
    'think_anomalies_raw',
    -- Pattern / signal / calibration (0009-0011)
    'signal_memory_fabric', 'pattern_candidates',
    'calibration_stats', 'calibration_offsets',
    -- Realtime / orphans (0012-0013)
    'realtime_replay_cursors', 'orphan_log',
    -- Access control (0014)
    'actor_roles', 'shared_channels', 'access_override_log',
    -- Post-commit / costs / view caches (0015-0018)
    'pending_post_commit_actions',
    'think_run_costs', 'view_ceo_cache', 'view_render_costs',
    -- Review remediation (0021)
    'anomaly_thresholds', 'dedup_keys_seen',
    -- Demo (0023)
    'demo_sessions',
    -- Cards (0024)
    'card_conversations',
    -- Model watchers (0027)
    'model_watchers',
    -- Reconciliation / audit / edges (0029-0031)
    'reconciliation_events', 'audit_events', 'model_edges',
    -- Topology (0032-0033)
    'topo_dirty_queue', 'model_neighborhoods',
    'model_neighborhood_membership', 'topology_events'
  ];
BEGIN
  FOREACH t IN ARRAY tenant_tables LOOP
    -- Defensive: only proceed if the table actually exists. Some
    -- installations may have skipped optional migrations.
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = t
    ) THEN
      RAISE NOTICE 'skipping RLS for missing table %', t;
      CONTINUE;
    END IF;

    -- Enable RLS and FORCE — without FORCE, the owning role
    -- (company_os) bypasses policies, defeating the safety net for
    -- our own application code (which connects as the owner).
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);

    -- Recreate the policy each time so re-runs pick up edits.
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    -- The IS NULL branch is the permissive default: when no tenant
    -- has been bound to the connection, all rows are visible (and
    -- writable). With FORCE RLS this branch is what keeps existing
    -- code paths working until they migrate to TenantContext.
    -- current_setting('foo', true) returns NULL when unset, so we
    -- check IS NULL rather than = '' (which would be NULL = '' → NULL,
    -- treated as FALSE by the RLS evaluator).
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I '
      'USING ('
      '  current_setting(''app.current_tenant'', true) IS NULL'
      '  OR tenant_id = current_setting(''app.current_tenant'', true)::uuid'
      ') '
      'WITH CHECK ('
      '  current_setting(''app.current_tenant'', true) IS NULL'
      '  OR tenant_id = current_setting(''app.current_tenant'', true)::uuid'
      ')',
      t
    );
  END LOOP;
END $$;

COMMIT;
