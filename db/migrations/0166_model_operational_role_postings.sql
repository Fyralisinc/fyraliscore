-- 0166_model_operational_role_postings.sql
--
-- Normalize Model operational roles into bounded posting lists for SAGE.
-- The Model proposition remains canonical; this table is only the fast
-- query-time surface for operational-facet activation.

BEGIN;

CREATE TABLE IF NOT EXISTS model_operational_role_postings (
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  role TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, model_id, role)
);

CREATE INDEX IF NOT EXISTS model_operational_role_postings_lookup_idx
  ON model_operational_role_postings (tenant_id, role, model_id)
  WHERE status = 'active';

ALTER TABLE model_operational_role_postings ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_operational_role_postings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_operational_role_postings;
CREATE POLICY tenant_isolation ON model_operational_role_postings
USING (
  NULLIF(current_setting('app.current_tenant', true), '') IS NULL
  OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  NULLIF(current_setting('app.current_tenant', true), '') IS NULL
  OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

CREATE OR REPLACE FUNCTION refresh_model_operational_role_postings()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_operational_role_postings
    WHERE tenant_id = OLD.tenant_id
      AND model_id = OLD.id;
    RETURN OLD;
  END IF;

  DELETE FROM model_operational_role_postings
  WHERE tenant_id = NEW.tenant_id
    AND model_id = NEW.id;

  INSERT INTO model_operational_role_postings (
    tenant_id,
    model_id,
    status,
    role,
    updated_at
  )
  SELECT
    NEW.tenant_id,
    NEW.id,
    NEW.status,
    lower(trim(role.value)),
    now()
  FROM jsonb_array_elements_text(
    coalesce(NEW.proposition->'operational_roles', '[]'::jsonb)
  ) AS role(value)
  WHERE nullif(trim(role.value), '') IS NOT NULL
  ON CONFLICT (tenant_id, model_id, role) DO UPDATE SET
    status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS models_refresh_operational_role_postings
  ON models;
CREATE TRIGGER models_refresh_operational_role_postings
AFTER INSERT OR UPDATE OF tenant_id, status, proposition
OR DELETE ON models
FOR EACH ROW EXECUTE FUNCTION refresh_model_operational_role_postings();

INSERT INTO model_operational_role_postings (
  tenant_id,
  model_id,
  status,
  role,
  updated_at
)
SELECT
  m.tenant_id,
  m.id,
  m.status,
  lower(trim(role.value)),
  now()
FROM models m
CROSS JOIN LATERAL jsonb_array_elements_text(
  coalesce(m.proposition->'operational_roles', '[]'::jsonb)
) AS role(value)
WHERE nullif(trim(role.value), '') IS NOT NULL
ON CONFLICT (tenant_id, model_id, role) DO UPDATE SET
  status = EXCLUDED.status,
  updated_at = EXCLUDED.updated_at;

COMMIT;
