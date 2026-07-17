-- Local/admin migrations may be applied by an owner role distinct from the
-- standard Fyralis runtime role.  Preserve least-privilege DML access for that
-- runtime without making the coordination head table public.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'company_os') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE
      ON TABLE company_learning_barrier_heads TO company_os;
  END IF;
END $$;
