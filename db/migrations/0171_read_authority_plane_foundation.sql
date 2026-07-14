-- 0171_read_authority_plane_foundation.sql
--
-- Durable substrate for authority-based reads:
--   * labels on any readable object;
--   * provenance edges from derived objects to source objects;
--   * grant epochs for cache/session fingerprints;
--   * explicit delegated read grants.
--
-- This migration is intentionally generic. Product code should not need a new
-- table every time a new read surface or derived object kind appears.

BEGIN;

CREATE TABLE IF NOT EXISTS object_access_labels (
  tenant_id UUID NOT NULL,
  object_kind TEXT NOT NULL,
  object_id UUID NOT NULL,
  label TEXT NOT NULL,
  source TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, object_kind, object_id, label, source)
);

CREATE INDEX IF NOT EXISTS object_access_labels_object_idx
  ON object_access_labels (tenant_id, object_kind, object_id);

CREATE INDEX IF NOT EXISTS object_access_labels_label_idx
  ON object_access_labels (tenant_id, label, object_kind);

CREATE INDEX IF NOT EXISTS object_access_labels_metadata_idx
  ON object_access_labels USING gin (metadata);

CREATE TABLE IF NOT EXISTS object_provenance_edges (
  tenant_id UUID NOT NULL,
  derived_kind TEXT NOT NULL,
  derived_id UUID NOT NULL,
  source_kind TEXT NOT NULL,
  source_id UUID NOT NULL,
  derivation_kind TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    tenant_id, derived_kind, derived_id,
    source_kind, source_id, derivation_kind
  )
);

CREATE INDEX IF NOT EXISTS object_provenance_edges_derived_idx
  ON object_provenance_edges (tenant_id, derived_kind, derived_id);

CREATE INDEX IF NOT EXISTS object_provenance_edges_source_idx
  ON object_provenance_edges (tenant_id, source_kind, source_id);

CREATE INDEX IF NOT EXISTS object_provenance_edges_metadata_idx
  ON object_provenance_edges USING gin (metadata);

CREATE TABLE IF NOT EXISTS access_grant_epochs (
  tenant_id UUID PRIMARY KEY,
  epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS read_authority_grants (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  grantee_actor_id UUID NOT NULL REFERENCES actors(id),
  granted_by_actor_id UUID NOT NULL REFERENCES actors(id),
  purpose TEXT NOT NULL,
  grant_kind TEXT NOT NULL,
  object_kind TEXT,
  object_id UUID,
  label TEXT,
  scope JSONB NOT NULL DEFAULT '{}'::jsonb,
  reason TEXT NOT NULL,
  granted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  revoked_by_actor_id UUID REFERENCES actors(id),
  revoked_reason TEXT,
  CONSTRAINT read_authority_grants_kind_check CHECK (
    grant_kind IN ('object', 'label', 'scope')
  ),
  CONSTRAINT read_authority_grants_object_shape_check CHECK (
    (grant_kind = 'object' AND object_kind IS NOT NULL AND object_id IS NOT NULL)
    OR grant_kind <> 'object'
  ),
  CONSTRAINT read_authority_grants_label_shape_check CHECK (
    (grant_kind = 'label' AND label IS NOT NULL)
    OR grant_kind <> 'label'
  )
);

CREATE INDEX IF NOT EXISTS read_authority_grants_actor_active_idx
  ON read_authority_grants (tenant_id, grantee_actor_id, purpose)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS read_authority_grants_object_idx
  ON read_authority_grants (tenant_id, object_kind, object_id)
  WHERE grant_kind = 'object' AND revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS read_authority_grants_label_idx
  ON read_authority_grants (tenant_id, label)
  WHERE grant_kind = 'label' AND revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS read_authority_grants_scope_idx
  ON read_authority_grants USING gin (scope);

CREATE OR REPLACE FUNCTION bump_access_grant_epoch()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO access_grant_epochs (tenant_id, epoch, updated_at)
  VALUES (NEW.tenant_id, 1, now())
  ON CONFLICT (tenant_id) DO UPDATE
    SET epoch = access_grant_epochs.epoch + 1,
        updated_at = now();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS read_authority_grants_bump_epoch
  ON read_authority_grants;
CREATE TRIGGER read_authority_grants_bump_epoch
AFTER INSERT OR UPDATE OF
  purpose, grant_kind, object_kind, object_id, label, scope,
  expires_at, revoked_at, revoked_by_actor_id
ON read_authority_grants
FOR EACH ROW
EXECUTE FUNCTION bump_access_grant_epoch();

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'object_access_labels',
    'object_provenance_edges',
    'access_grant_epochs',
    'read_authority_grants'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', table_name);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', table_name);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', table_name);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I
       USING (
         NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL
         OR tenant_id = NULLIF(
           current_setting(''app.current_tenant'', true), ''''
         )::uuid
       )
       WITH CHECK (
         NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL
         OR tenant_id = NULLIF(
           current_setting(''app.current_tenant'', true), ''''
         )::uuid
       )',
      table_name
    );
  END LOOP;
END $$;

COMMIT;
