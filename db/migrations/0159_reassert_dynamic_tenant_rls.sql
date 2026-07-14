-- 0159_reassert_dynamic_tenant_rls.sql
--
-- Reassert the dynamic tenant RLS policy after legacy/test migration replays.
-- Some long-lived dev DBs had old fixtures that truncated schema_migrations,
-- replayed early hard-coded RLS migrations, then stopped before reaching 0157.
-- This keeps the final policy deterministic.

BEGIN;

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT c.relname AS table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_attribute a ON a.attrelid = c.oid
    WHERE n.nspname = 'public'
      AND c.relkind IN ('r', 'p')
      AND c.relispartition = FALSE
      AND a.attname = 'tenant_id'
      AND NOT a.attisdropped
      AND c.relname <> 'tenants'
    ORDER BY c.relname
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', r.table_name);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', r.table_name);

    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', r.table_name);
    EXECUTE format('DROP POLICY IF EXISTS %I_tenant_isolation ON %I',
                   r.table_name, r.table_name);

    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I '
      'USING ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ') '
      'WITH CHECK ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ')',
      r.table_name
    );
  END LOOP;
END $$;

COMMIT;
