-- =====================================================================
-- 0037_tenant_fks.sql — promote tenant_id from convention to constraint
-- =====================================================================
-- Phase 1b of the architectural overhaul. Every tenant-scoped table
-- gets `FOREIGN KEY (tenant_id) REFERENCES tenants(id) DEFERRABLE
-- INITIALLY IMMEDIATE`.
--
-- Why DEFERRABLE INITIALLY IMMEDIATE
-- ----------------------------------
-- IMMEDIATE = production code that forgets to register a tenant fails
-- loudly on the first INSERT, not silently with orphaned rows.
-- DEFERRABLE = tests that wrap the body in a transaction and ROLLBACK
-- can `SET CONSTRAINTS ALL DEFERRED` so the FK is checked only at
-- COMMIT (which never fires for tests). This means existing tests that
-- generate tenant_id via uuid7() without inserting a tenants row keep
-- working unchanged, as long as the test transaction is rolled back.
--
-- Backfill safety
-- ---------------
-- Before adding the FK we backfill any orphan tenant_id (a UUID seen in
-- a tenant-scoped table but not present in the tenants registry) with
-- a placeholder row. This is a no-op on a fresh DB and a one-shot
-- legalization on environments that grew up before the tenants table
-- existed (which was added in 0023). Using a deterministic name
-- (`auto_backfill_<uuid>`) keeps the operation idempotent across
-- re-runs.
--
-- Tables NOT covered (intentional)
-- --------------------------------
--   * tenants, demo_configs, demo_session_costs (not tenant-scoped)
--   * model_status_notes, commitment_contributors, etc. — junction
--     tables that inherit tenant_id from a parent.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- Backfill orphans into the tenants registry.
-- ---------------------------------------------------------------------
INSERT INTO tenants (id, name, created_at)
SELECT DISTINCT t.tenant_id,
                'auto_backfill_' || t.tenant_id::text,
                now()
FROM (
  SELECT tenant_id FROM actors
  UNION SELECT tenant_id FROM observations
  UNION SELECT tenant_id FROM models
  UNION SELECT tenant_id FROM goals
  UNION SELECT tenant_id FROM commitments
  UNION SELECT tenant_id FROM decisions
  UNION SELECT tenant_id FROM resources
  UNION SELECT tenant_id FROM resource_transactions
  UNION SELECT tenant_id FROM entity_aliases
  UNION SELECT tenant_id FROM actor_sessions
  UNION SELECT tenant_id FROM think_trigger_queue
  UNION SELECT tenant_id FROM entity_review_queue
  UNION SELECT tenant_id FROM relationship_maintenance_log
  UNION SELECT tenant_id FROM model_reeval_queue
  UNION SELECT tenant_id FROM think_region_lock_log
  UNION SELECT tenant_id FROM applied_triggers
  UNION SELECT tenant_id FROM think_runs
  UNION SELECT tenant_id FROM model_reeval_dead_letter
  UNION SELECT tenant_id FROM think_anomalies_raw
  UNION SELECT tenant_id FROM signal_memory_fabric
  UNION SELECT tenant_id FROM pattern_candidates
  UNION SELECT tenant_id FROM calibration_stats
  UNION SELECT tenant_id FROM calibration_offsets
  UNION SELECT tenant_id FROM realtime_replay_cursors
  UNION SELECT tenant_id FROM orphan_log
  UNION SELECT tenant_id FROM actor_roles
  UNION SELECT tenant_id FROM shared_channels
  UNION SELECT tenant_id FROM access_override_log
  UNION SELECT tenant_id FROM pending_post_commit_actions
  UNION SELECT tenant_id FROM think_run_costs
  UNION SELECT tenant_id FROM view_ceo_cache
  UNION SELECT tenant_id FROM view_render_costs
  UNION SELECT tenant_id FROM anomaly_thresholds
  UNION SELECT tenant_id FROM dedup_keys_seen
  UNION SELECT tenant_id FROM card_conversations
  UNION SELECT tenant_id FROM model_watchers
  UNION SELECT tenant_id FROM reconciliation_events
  UNION SELECT tenant_id FROM audit_events
  UNION SELECT tenant_id FROM model_edges
  UNION SELECT tenant_id FROM topo_dirty_queue
  UNION SELECT tenant_id FROM model_neighborhoods
  UNION SELECT tenant_id FROM model_neighborhood_membership
  UNION SELECT tenant_id FROM topology_events
) t
ON CONFLICT (id) DO NOTHING;

-- ---------------------------------------------------------------------
-- Add FK constraints. Idempotent via pg_constraint check.
-- ---------------------------------------------------------------------
DO $$
DECLARE
  t TEXT;
  fk_name TEXT;
  tenant_tables TEXT[] := ARRAY[
    'actors', 'observations', 'models', 'goals', 'commitments',
    'decisions', 'resources', 'resource_transactions', 'entity_aliases',
    'actor_sessions', 'think_trigger_queue', 'entity_review_queue',
    'relationship_maintenance_log',
    'model_reeval_queue', 'think_region_lock_log',
    'applied_triggers', 'think_runs', 'model_reeval_dead_letter',
    'think_anomalies_raw',
    'signal_memory_fabric', 'pattern_candidates',
    'calibration_stats', 'calibration_offsets',
    'realtime_replay_cursors', 'orphan_log',
    'actor_roles', 'shared_channels', 'access_override_log',
    'pending_post_commit_actions',
    'think_run_costs', 'view_ceo_cache', 'view_render_costs',
    'anomaly_thresholds', 'dedup_keys_seen',
    'card_conversations',
    'model_watchers',
    'reconciliation_events', 'audit_events', 'model_edges',
    'topo_dirty_queue', 'model_neighborhoods',
    'model_neighborhood_membership', 'topology_events'
  ];
BEGIN
  FOREACH t IN ARRAY tenant_tables LOOP
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = t
    ) THEN
      RAISE NOTICE 'skipping FK for missing table %', t;
      CONTINUE;
    END IF;

    fk_name := t || '_tenant_fk';

    IF NOT EXISTS (
      SELECT 1 FROM pg_constraint WHERE conname = fk_name
    ) THEN
      EXECUTE format(
        'ALTER TABLE %I ADD CONSTRAINT %I '
        'FOREIGN KEY (tenant_id) REFERENCES tenants(id) '
        'DEFERRABLE INITIALLY IMMEDIATE',
        t, fk_name
      );
    END IF;
  END LOOP;
END $$;

COMMIT;
