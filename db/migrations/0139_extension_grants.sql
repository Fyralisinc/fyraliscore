-- =====================================================================
-- 0139_extension_grants.sql — the capability store + the read-only role
-- =====================================================================
-- ADR-0004 §A.5 / roadmap E2. Two things:
--   1. `extension_grants` — per-(tenant, extension) capability grant. The
--      effective grant is intersection(manifest-declared, operator-approved);
--      an extension can never receive more than it declared.
--   2. `fyralis_ext_readonly` — a restricted Postgres role with SELECT on the
--      read substrate and NO write grants, so when the host hands an extension a
--      connection under this role a substrate write is denied *structurally*
--      (not on the honour system). The role is subject to RLS (not BYPASSRLS),
--      so tenant isolation still applies via app.current_tenant.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS extension_grants (
  tenant_id        UUID NOT NULL,
  extension_id     TEXT NOT NULL,
  granted_version  TEXT NOT NULL,             -- re-review when a new version asks for more
  capabilities     JSONB NOT NULL,            -- intersection(declared, approved)
  -- INV-6: the max trust tier this extension's edge-ingested observations may
  -- carry. Default is the honest floor for *derived* third-party signals.
  trust_ceiling    TEXT NOT NULL DEFAULT 'inferential_external'
                   CHECK (trust_ceiling IN (
                     'unvetted', 'inferential_external', 'inferential',
                     'reputable', 'attested_agent'
                   )),
  granted_by       TEXT NOT NULL,
  granted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at       TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, extension_id)
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'extension_grants'::regclass
      AND conname = 'extension_grants_tenant_fk'
  ) THEN
    ALTER TABLE extension_grants
      ADD CONSTRAINT extension_grants_tenant_fk
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
      DEFERRABLE INITIALLY IMMEDIATE;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS extension_grants_active_idx
  ON extension_grants (extension_id)
  WHERE revoked_at IS NULL;

ALTER TABLE extension_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE extension_grants FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON extension_grants;
CREATE POLICY tenant_isolation ON extension_grants
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

-- ---------------------------------------------------------------------
-- The restricted read-only role (cluster-global; idempotent).
-- ---------------------------------------------------------------------
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fyralis_ext_readonly') THEN
    CREATE ROLE fyralis_ext_readonly NOLOGIN;
  END IF;
EXCEPTION WHEN insufficient_privilege THEN
  -- Dev/test DB users often lack CREATEROLE. The schema remains valid; the
  -- structural read-only role is activated in environments that can create it.
  NULL;
END $$;

-- Schema usage + SELECT on the read substrate ONLY. No INSERT/UPDATE/DELETE is
-- granted anywhere, so any write under this role fails with "permission denied".
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fyralis_ext_readonly') THEN
    GRANT USAGE ON SCHEMA public TO fyralis_ext_readonly;
    GRANT SELECT ON observations        TO fyralis_ext_readonly;
    GRANT SELECT ON models              TO fyralis_ext_readonly;
    GRANT SELECT ON commitments         TO fyralis_ext_readonly;
    GRANT SELECT ON goals               TO fyralis_ext_readonly;
    GRANT SELECT ON decisions           TO fyralis_ext_readonly;
    GRANT SELECT ON resources           TO fyralis_ext_readonly;
    GRANT SELECT ON extension_grants    TO fyralis_ext_readonly;
  END IF;
EXCEPTION WHEN insufficient_privilege OR undefined_object THEN
  NULL;
END $$;

-- Let the role that runs the app / migrations SET ROLE into the restricted role.
-- (A superuser can SET ROLE regardless; this matters for a non-superuser app
-- role in production.)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fyralis_ext_readonly') THEN
    EXECUTE format('GRANT fyralis_ext_readonly TO %I', current_user);
  END IF;
EXCEPTION WHEN OTHERS THEN
  -- e.g. already a member, or insufficient privilege in an exotic setup —
  -- non-fatal; SET ROLE still works for superusers.
  NULL;
END $$;

COMMIT;
