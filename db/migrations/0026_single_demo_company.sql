-- =====================================================================
-- 0026_single_demo_company.sql — narrow the demo to a single company
-- =====================================================================
-- The demo product has converged on a single example tenant (pelago,
-- seeded by 0028). The legacy truss/northwind/meridian configs seeded
-- by 0023 are retired: their snapshots are no longer maintained and the
-- gateway only re-seeds pelago (services/app/gateway/demo_seed.py:ensure_demo_seed).
--
-- Mirrors the (separately-authored) main-branch 0029 cleanup. Idempotent:
-- a re-run finds nothing. Sessions/tenants pointing at a legacy config
-- are detached/ended first, and dependent cost ledger rows are removed
-- before sessions so FK constraints hold on databases with prior demo runs.
-- =====================================================================

BEGIN;

-- End any sessions still attached to a legacy config.
UPDATE demo_sessions
   SET ended_at = COALESCE(ended_at, now()),
       end_reason = COALESCE(end_reason, 'user_ended')
 WHERE demo_config_id IN (
   SELECT id FROM demo_configs
   WHERE company_id IN ('truss', 'northwind', 'meridian')
 );

-- Detach tenants from legacy configs. Modern post-demo core schemas no
-- longer have tenants.demo_config_id (dropped by 0093), so guard the
-- historical cleanup for long-lived DBs that replay migrations.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'tenants'
      AND column_name = 'demo_config_id'
  ) THEN
    UPDATE tenants
       SET demo_config_id = NULL
     WHERE demo_config_id IN (
       SELECT id FROM demo_configs
       WHERE company_id IN ('truss', 'northwind', 'meridian')
     );
  END IF;
END $$;

DELETE FROM demo_session_costs
 WHERE demo_session_id IN (
   SELECT ds.id
   FROM demo_sessions ds
   JOIN demo_configs dc ON dc.id = ds.demo_config_id
   WHERE dc.company_id IN ('truss', 'northwind', 'meridian')
 );

DELETE FROM demo_sessions
 WHERE demo_config_id IN (
   SELECT id FROM demo_configs
   WHERE company_id IN ('truss', 'northwind', 'meridian')
 );

DELETE FROM demo_configs
 WHERE company_id IN ('truss', 'northwind', 'meridian');

COMMIT;
