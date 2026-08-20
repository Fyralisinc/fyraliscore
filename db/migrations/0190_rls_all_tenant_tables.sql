-- 0190_rls_all_tenant_tables.sql
--
-- Earlier RLS migrations used hard-coded table lists, so later tenant-scoped
-- tables drifted out of the defense-in-depth policy. Apply the established
-- permissive tenant policy to every current base table that carries tenant_id.
--
-- Contract preserved from 0036/0134:
--   * app.current_tenant unset or empty: allow all rows for legacy/setup paths.
--   * app.current_tenant set: isolate reads/writes to that tenant.

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
