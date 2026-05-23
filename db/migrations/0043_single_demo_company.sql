-- =====================================================================
-- 0043_single_demo_company.sql — narrow the demo to a single company
-- =====================================================================
-- The demo product has converged on a single example tenant (pelago,
-- seeded by 0028). The legacy truss/northwind/meridian configs seeded
-- by 0023 are retired: their snapshots are no longer maintained and the
-- gateway only re-seeds pelago (services/gateway/main.py:_ensure_demo_seed).
--
-- Mirrors the (separately-authored) main-branch 0029 cleanup. Idempotent:
-- a re-run finds nothing. Sessions/tenants pointing at a legacy config
-- are detached/ended first so the FK + NOT NULL constraints hold.
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

-- Detach tenants from legacy configs.
UPDATE tenants
   SET demo_config_id = NULL
 WHERE demo_config_id IN (
   SELECT id FROM demo_configs
   WHERE company_id IN ('truss', 'northwind', 'meridian')
 );

DELETE FROM demo_sessions
 WHERE demo_config_id IN (
   SELECT id FROM demo_configs
   WHERE company_id IN ('truss', 'northwind', 'meridian')
 );

DELETE FROM demo_configs
 WHERE company_id IN ('truss', 'northwind', 'meridian');

COMMIT;
