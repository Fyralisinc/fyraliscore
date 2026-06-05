-- =====================================================================
-- 0093_drop_demo_scaffolding.sql — remove demo tables from the core schema
-- =====================================================================
-- The VC-pitch demo moved to its own repo (fyraliscore-demo). Core is a
-- pure production runtime and must not carry the demo's tables or seed.
--
-- This drops the three demo tables created in 0023 and the tenants ->
-- demo_configs link column. The `tenants` registry table itself stays
-- (it is core infrastructure — every tenant_id references it), as does
-- the generic `is_demo` flag column (FK-free, no core logic branches on
-- it; the demo overlay still sets it).
--
-- The demo overlay recreates demo_configs / demo_sessions /
-- demo_session_costs and re-adds tenants.demo_config_id via its own
-- migrations (fyraliscore-demo/db/migrations).
--
-- Idempotent: every drop guards with IF EXISTS.
-- =====================================================================

BEGIN;

-- Cost ledger references demo_sessions; drop first.
DROP TABLE IF EXISTS demo_session_costs CASCADE;

-- Sessions reference tenants + demo_configs.
DROP TABLE IF EXISTS demo_sessions CASCADE;

-- Remove the tenants -> demo_configs link before dropping demo_configs.
ALTER TABLE tenants DROP CONSTRAINT IF EXISTS tenants_demo_config_id_fkey;
ALTER TABLE tenants DROP COLUMN IF EXISTS demo_config_id;

-- Per-company demo settings.
DROP TABLE IF EXISTS demo_configs CASCADE;

COMMIT;
