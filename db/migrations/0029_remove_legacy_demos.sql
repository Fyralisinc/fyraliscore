-- Remove the truss/northwind/meridian demo configs. The demo product
-- has narrowed to a single example (pelago); the legacy configs are
-- no longer surfaced in /v1/demo/companies and their snapshot files
-- have been removed from the repo.
--
-- demo_sessions and tenants reference demo_configs via FK. Sessions
-- bound to a removed config are ended first (so the FK no longer
-- points at the row), then the tenants' demo_config_id is nulled, and
-- finally the rows are deleted. Idempotent: a re-run finds nothing.

BEGIN;

WITH legacy AS (
  SELECT id FROM demo_configs WHERE company_id IN ('truss','northwind','meridian')
)
UPDATE demo_sessions
   SET ended_at = COALESCE(ended_at, now()),
       end_reason = COALESCE(end_reason, 'user_ended')
 WHERE demo_config_id IN (SELECT id FROM legacy);

UPDATE tenants
   SET demo_config_id = NULL
 WHERE demo_config_id IN (
   SELECT id FROM demo_configs
   WHERE company_id IN ('truss','northwind','meridian')
 );

DELETE FROM demo_sessions
 WHERE demo_config_id IN (
   SELECT id FROM demo_configs
   WHERE company_id IN ('truss','northwind','meridian')
 );

DELETE FROM demo_configs
 WHERE company_id IN ('truss','northwind','meridian');

COMMIT;
