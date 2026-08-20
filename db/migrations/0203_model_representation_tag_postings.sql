-- 0203_model_representation_tag_postings.sql
-- destructive-migration-approved: backup=canonical-models-table rollback=rerun-migration-backfill owner=reasoning
-- Normalize Model representation tags into active posting lists for bounded
-- semantic rescue. This avoids OR-ing indexed arrays with unindexed JSONB
-- predicates over the full active Models table.

BEGIN;

CREATE TABLE IF NOT EXISTS model_representation_tag_postings (
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  tag_type TEXT NOT NULL CHECK (tag_type IN ('domain', 'retrieval', 'coverage')),
  tag TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, model_id, tag_type, tag)
);

CREATE INDEX IF NOT EXISTS model_representation_tag_postings_active_lookup_idx
  ON model_representation_tag_postings (tenant_id, tag_type, tag, model_id)
  WHERE status = 'active';

ALTER TABLE model_representation_tag_postings ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_representation_tag_postings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_representation_tag_postings;
CREATE POLICY tenant_isolation ON model_representation_tag_postings
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

CREATE OR REPLACE FUNCTION refresh_model_representation_tag_postings()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_representation_tag_postings
    WHERE tenant_id = OLD.tenant_id
      AND model_id = OLD.id;
    RETURN OLD;
  END IF;

  DELETE FROM model_representation_tag_postings
  WHERE tenant_id = NEW.tenant_id
    AND model_id = NEW.id;

  INSERT INTO model_representation_tag_postings (
    tenant_id,
    model_id,
    status,
    tag_type,
    tag,
    updated_at
  )
  SELECT
    NEW.tenant_id,
    NEW.id,
    NEW.status,
    tags.tag_type,
    tags.tag,
    now()
  FROM (
    SELECT 'domain'::text AS tag_type, lower(trim(tag.value)) AS tag
    FROM unnest(coalesce(NEW.domain_tags, '{}'::text[])) AS tag(value)

    UNION ALL

    SELECT 'retrieval'::text AS tag_type, lower(trim(tag.value)) AS tag
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(NEW.proposition->'retrieval_tags') = 'array'
        THEN NEW.proposition->'retrieval_tags'
        ELSE '[]'::jsonb
      END
    ) AS tag(value)

    UNION ALL

    SELECT 'coverage'::text AS tag_type, lower(trim(tag.value)) AS tag
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(NEW.proposition->'coverage_roles') = 'array'
        THEN NEW.proposition->'coverage_roles'
        ELSE '[]'::jsonb
      END
    ) AS tag(value)
  ) tags
  WHERE nullif(tags.tag, '') IS NOT NULL
  ON CONFLICT (tenant_id, model_id, tag_type, tag) DO UPDATE SET
    status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS models_refresh_representation_tag_postings
  ON models;
CREATE TRIGGER models_refresh_representation_tag_postings
AFTER INSERT OR UPDATE OF tenant_id, status, domain_tags, proposition
OR DELETE ON models
FOR EACH ROW EXECUTE FUNCTION refresh_model_representation_tag_postings();

INSERT INTO model_representation_tag_postings (
  tenant_id,
  model_id,
  status,
  tag_type,
  tag,
  updated_at
)
SELECT
  m.tenant_id,
  m.id,
  m.status,
  tags.tag_type,
  tags.tag,
  now()
FROM models m
CROSS JOIN LATERAL (
  SELECT 'domain'::text AS tag_type, lower(trim(tag.value)) AS tag
  FROM unnest(coalesce(m.domain_tags, '{}'::text[])) AS tag(value)

  UNION ALL

  SELECT 'retrieval'::text AS tag_type, lower(trim(tag.value)) AS tag
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(m.proposition->'retrieval_tags') = 'array'
      THEN m.proposition->'retrieval_tags'
      ELSE '[]'::jsonb
    END
  ) AS tag(value)

  UNION ALL

  SELECT 'coverage'::text AS tag_type, lower(trim(tag.value)) AS tag
  FROM jsonb_array_elements_text(
    CASE
      WHEN jsonb_typeof(m.proposition->'coverage_roles') = 'array'
      THEN m.proposition->'coverage_roles'
      ELSE '[]'::jsonb
    END
  ) AS tag(value)
) tags
WHERE nullif(tags.tag, '') IS NOT NULL
ON CONFLICT (tenant_id, model_id, tag_type, tag) DO UPDATE SET
  status = EXCLUDED.status,
  updated_at = EXCLUDED.updated_at;

ALTER TABLE model_representation_tag_postings
  ALTER COLUMN tenant_id SET STATISTICS 1000,
  ALTER COLUMN status SET STATISTICS 1000,
  ALTER COLUMN tag_type SET STATISTICS 1000,
  ALTER COLUMN tag SET STATISTICS 1000;

CREATE STATISTICS IF NOT EXISTS model_representation_tag_lookup_stats
  (dependencies, ndistinct, mcv)
  ON tenant_id, status, tag_type, tag
  FROM model_representation_tag_postings;

ANALYZE model_representation_tag_postings;

COMMIT;
