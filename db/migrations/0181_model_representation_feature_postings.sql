-- 0181_model_representation_feature_postings.sql
-- Merge lexical semantic terms and representation tags into one typed posting
-- surface. Existing semantic/tag posting tables remain as compatibility
-- sources; this table is the unified retrieval accelerator.

BEGIN;

CREATE TABLE IF NOT EXISTS model_representation_feature_postings (
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  status TEXT NOT NULL,
  feature_type TEXT NOT NULL CHECK (nullif(trim(feature_type), '') IS NOT NULL),
  feature TEXT NOT NULL CHECK (nullif(trim(feature), '') IS NOT NULL),
  weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
  source TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, model_id, feature_type, feature)
);

CREATE INDEX IF NOT EXISTS model_representation_feature_postings_active_lookup_idx
  ON model_representation_feature_postings (
    tenant_id, feature_type, feature, model_id
  )
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS model_representation_feature_postings_model_idx
  ON model_representation_feature_postings (tenant_id, model_id);

ALTER TABLE model_representation_feature_postings ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_representation_feature_postings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_representation_feature_postings;
CREATE POLICY tenant_isolation ON model_representation_feature_postings
USING (
  NULLIF(current_setting('app.current_tenant', true), '') IS NULL
  OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  NULLIF(current_setting('app.current_tenant', true), '') IS NULL
  OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

CREATE OR REPLACE FUNCTION refresh_model_representation_features_from_model()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_representation_feature_postings
    WHERE tenant_id = OLD.tenant_id
      AND model_id = OLD.id;
    RETURN OLD;
  END IF;

  IF TG_OP = 'UPDATE' AND OLD.tenant_id IS DISTINCT FROM NEW.tenant_id THEN
    DELETE FROM model_representation_feature_postings
    WHERE tenant_id = OLD.tenant_id
      AND model_id = OLD.id
      AND source = 'models';
  END IF;

  DELETE FROM model_representation_feature_postings
  WHERE tenant_id = NEW.tenant_id
    AND model_id = NEW.id
    AND source = 'models';

  INSERT INTO model_representation_feature_postings (
    tenant_id,
    model_id,
    status,
    feature_type,
    feature,
    weight,
    source,
    updated_at
  )
  SELECT
    NEW.tenant_id,
    NEW.id,
    NEW.status,
    features.feature_type,
    features.feature,
    features.weight,
    'models',
    now()
  FROM (
    SELECT DISTINCT ON (raw.feature_type, raw.feature)
      raw.feature_type,
      raw.feature,
      raw.weight
    FROM (
      SELECT 'domain'::text AS feature_type,
             lower(trim(tag.value)) AS feature,
             1.0::double precision AS weight
      FROM unnest(coalesce(NEW.domain_tags, '{}'::text[])) AS tag(value)

      UNION ALL

      SELECT 'retrieval'::text AS feature_type,
             lower(trim(tag.value)) AS feature,
             1.0::double precision AS weight
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(NEW.proposition->'retrieval_tags') = 'array'
          THEN NEW.proposition->'retrieval_tags'
          ELSE '[]'::jsonb
        END
      ) AS tag(value)

      UNION ALL

      SELECT 'coverage'::text AS feature_type,
             lower(trim(tag.value)) AS feature,
             1.0::double precision AS weight
      FROM jsonb_array_elements_text(
        CASE
          WHEN jsonb_typeof(NEW.proposition->'coverage_roles') = 'array'
          THEN NEW.proposition->'coverage_roles'
          ELSE '[]'::jsonb
        END
      ) AS tag(value)

      UNION ALL

      SELECT 'claim_role'::text AS feature_type,
             lower(trim(coalesce(NEW.claim_role, NEW.proposition->>'claim_role'))),
             0.9::double precision

      UNION ALL

      SELECT 'abstraction_level'::text AS feature_type,
             lower(trim(coalesce(
               NEW.abstraction_level,
               NEW.proposition->>'abstraction_level'
             ))),
             0.8::double precision

      UNION ALL

      SELECT 'time_mode'::text AS feature_type,
             lower(trim(coalesce(NEW.time_mode, NEW.proposition->>'time_mode'))),
             0.75::double precision

      UNION ALL

      SELECT 'modality'::text AS feature_type,
             lower(trim(coalesce(NEW.modality, NEW.proposition->>'modality'))),
             0.75::double precision

      UNION ALL

      SELECT 'polarity'::text AS feature_type,
             lower(trim(coalesce(NEW.polarity, NEW.proposition->>'polarity'))),
             0.75::double precision

      UNION ALL

      SELECT 'pressure'::text AS feature_type,
             lower(trim(NEW.proposition->>'pressure_type')),
             0.8::double precision

      UNION ALL

      SELECT 'operation'::text AS feature_type,
             lower(trim(NEW.proposition->'proposed_change'->>'operation')),
             0.8::double precision
      WHERE jsonb_typeof(NEW.proposition->'proposed_change') = 'object'

      UNION ALL

      SELECT 'act_target'::text AS feature_type,
             lower(trim(NEW.proposition->'target_act_ref'->>'type')),
             0.75::double precision
      WHERE jsonb_typeof(NEW.proposition->'target_act_ref') = 'object'

      UNION ALL

      SELECT 'state'::text AS feature_type,
             lower(trim(NEW.proposition->>'status')),
             0.7::double precision

      UNION ALL

      SELECT 'structural'::text AS feature_type,
             'recurring_invariant'::text AS feature,
             0.7::double precision
      WHERE coalesce(NEW.claim_role, NEW.proposition->>'claim_role') = 'pattern'
         OR coalesce(NEW.abstraction_level, NEW.proposition->>'abstraction_level')
            = 'pattern'
         OR coalesce(NEW.time_mode, NEW.proposition->>'time_mode') = 'recurring'

      UNION ALL

      SELECT 'structural'::text AS feature_type,
             'composite_condition'::text AS feature,
             0.7::double precision
      WHERE coalesce(NEW.claim_role, NEW.proposition->>'claim_role') = 'situation'
         OR coalesce(NEW.abstraction_level, NEW.proposition->>'abstraction_level')
            = 'composite'

      UNION ALL

      SELECT 'structural'::text AS feature_type,
             'negative_pressure'::text AS feature,
             0.7::double precision
      WHERE coalesce(NEW.claim_role, NEW.proposition->>'claim_role') = 'concern'
         OR coalesce(NEW.polarity, NEW.proposition->>'polarity') = 'negative'

      UNION ALL

      SELECT 'structural'::text AS feature_type,
             'future_expectation'::text AS feature,
             0.7::double precision
      WHERE coalesce(NEW.claim_role, NEW.proposition->>'claim_role') = 'prediction'
         OR coalesce(NEW.time_mode, NEW.proposition->>'time_mode')
            IN ('future', 'recurring')
    ) raw
    WHERE nullif(raw.feature, '') IS NOT NULL
    ORDER BY raw.feature_type, raw.feature, raw.weight DESC
  ) features
  ON CONFLICT (tenant_id, model_id, feature_type, feature) DO UPDATE SET
    status = EXCLUDED.status,
    weight = EXCLUDED.weight,
    source = EXCLUDED.source,
    updated_at = EXCLUDED.updated_at;

  UPDATE model_representation_feature_postings
  SET status = NEW.status,
      updated_at = now()
  WHERE tenant_id = NEW.tenant_id
    AND model_id = NEW.id
    AND status IS DISTINCT FROM NEW.status;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION refresh_model_representation_features_from_semantic_terms()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  model_status TEXT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_representation_feature_postings
    WHERE tenant_id = OLD.tenant_id
      AND model_id = OLD.model_id
      AND source = 'model_semantic_terms';
    RETURN OLD;
  END IF;

  IF TG_OP = 'UPDATE'
     AND (
       OLD.tenant_id IS DISTINCT FROM NEW.tenant_id
       OR OLD.model_id IS DISTINCT FROM NEW.model_id
     ) THEN
    DELETE FROM model_representation_feature_postings
    WHERE tenant_id = OLD.tenant_id
      AND model_id = OLD.model_id
      AND source = 'model_semantic_terms';
  END IF;

  SELECT status INTO model_status
  FROM models
  WHERE tenant_id = NEW.tenant_id
    AND id = NEW.model_id;

  DELETE FROM model_representation_feature_postings
  WHERE tenant_id = NEW.tenant_id
    AND model_id = NEW.model_id
    AND source = 'model_semantic_terms';

  INSERT INTO model_representation_feature_postings (
    tenant_id,
    model_id,
    status,
    feature_type,
    feature,
    weight,
    source,
    updated_at
  )
  SELECT
    NEW.tenant_id,
    NEW.model_id,
    coalesce(model_status, 'active'),
    'lexical',
    features.feature,
    1.0,
    'model_semantic_terms',
    now()
  FROM (
    SELECT DISTINCT lower(trim(term.value)) AS feature
    FROM unnest(NEW.semantic_terms) AS term(value)
    WHERE nullif(lower(trim(term.value)), '') IS NOT NULL
  ) features
  ON CONFLICT (tenant_id, model_id, feature_type, feature) DO UPDATE SET
    status = EXCLUDED.status,
    weight = EXCLUDED.weight,
    source = EXCLUDED.source,
    updated_at = EXCLUDED.updated_at;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS models_refresh_representation_feature_postings
  ON models;
CREATE TRIGGER models_refresh_representation_feature_postings
AFTER INSERT OR UPDATE OF
  tenant_id,
  status,
  domain_tags,
  proposition,
  claim_role,
  abstraction_level,
  time_mode,
  modality,
  polarity
OR DELETE ON models
FOR EACH ROW EXECUTE FUNCTION refresh_model_representation_features_from_model();

DROP TRIGGER IF EXISTS model_semantic_terms_refresh_representation_feature_postings
  ON model_semantic_terms;
CREATE TRIGGER model_semantic_terms_refresh_representation_feature_postings
AFTER INSERT OR UPDATE OF tenant_id, model_id, semantic_terms
OR DELETE ON model_semantic_terms
FOR EACH ROW EXECUTE FUNCTION refresh_model_representation_features_from_semantic_terms();

INSERT INTO model_representation_feature_postings (
  tenant_id,
  model_id,
  status,
  feature_type,
  feature,
  weight,
  source,
  updated_at
)
SELECT
  m.tenant_id,
  m.id,
  m.status,
  features.feature_type,
  features.feature,
  features.weight,
  'models',
  now()
FROM models m
CROSS JOIN LATERAL (
  SELECT DISTINCT ON (raw.feature_type, raw.feature)
    raw.feature_type,
    raw.feature,
    raw.weight
  FROM (
    SELECT 'domain'::text AS feature_type,
           lower(trim(tag.value)) AS feature,
           1.0::double precision AS weight
    FROM unnest(coalesce(m.domain_tags, '{}'::text[])) AS tag(value)

    UNION ALL

    SELECT 'retrieval'::text AS feature_type,
           lower(trim(tag.value)) AS feature,
           1.0::double precision AS weight
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(m.proposition->'retrieval_tags') = 'array'
        THEN m.proposition->'retrieval_tags'
        ELSE '[]'::jsonb
      END
    ) AS tag(value)

    UNION ALL

    SELECT 'coverage'::text AS feature_type,
           lower(trim(tag.value)) AS feature,
           1.0::double precision AS weight
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(m.proposition->'coverage_roles') = 'array'
        THEN m.proposition->'coverage_roles'
        ELSE '[]'::jsonb
      END
    ) AS tag(value)

    UNION ALL

    SELECT 'claim_role'::text AS feature_type,
           lower(trim(coalesce(m.claim_role, m.proposition->>'claim_role'))),
           0.9::double precision

    UNION ALL

    SELECT 'abstraction_level'::text AS feature_type,
           lower(trim(coalesce(
             m.abstraction_level,
             m.proposition->>'abstraction_level'
           ))),
           0.8::double precision

    UNION ALL

    SELECT 'time_mode'::text AS feature_type,
           lower(trim(coalesce(m.time_mode, m.proposition->>'time_mode'))),
           0.75::double precision

    UNION ALL

    SELECT 'modality'::text AS feature_type,
           lower(trim(coalesce(m.modality, m.proposition->>'modality'))),
           0.75::double precision

    UNION ALL

    SELECT 'polarity'::text AS feature_type,
           lower(trim(coalesce(m.polarity, m.proposition->>'polarity'))),
           0.75::double precision

    UNION ALL

    SELECT 'pressure'::text AS feature_type,
           lower(trim(m.proposition->>'pressure_type')),
           0.8::double precision

    UNION ALL

    SELECT 'operation'::text AS feature_type,
           lower(trim(m.proposition->'proposed_change'->>'operation')),
           0.8::double precision
    WHERE jsonb_typeof(m.proposition->'proposed_change') = 'object'

    UNION ALL

    SELECT 'act_target'::text AS feature_type,
           lower(trim(m.proposition->'target_act_ref'->>'type')),
           0.75::double precision
    WHERE jsonb_typeof(m.proposition->'target_act_ref') = 'object'

    UNION ALL

    SELECT 'state'::text AS feature_type,
           lower(trim(m.proposition->>'status')),
           0.7::double precision

    UNION ALL

    SELECT 'structural'::text AS feature_type,
           'recurring_invariant'::text AS feature,
           0.7::double precision
    WHERE coalesce(m.claim_role, m.proposition->>'claim_role') = 'pattern'
       OR coalesce(m.abstraction_level, m.proposition->>'abstraction_level')
          = 'pattern'
       OR coalesce(m.time_mode, m.proposition->>'time_mode') = 'recurring'

    UNION ALL

    SELECT 'structural'::text AS feature_type,
           'composite_condition'::text AS feature,
           0.7::double precision
    WHERE coalesce(m.claim_role, m.proposition->>'claim_role') = 'situation'
       OR coalesce(m.abstraction_level, m.proposition->>'abstraction_level')
          = 'composite'

    UNION ALL

    SELECT 'structural'::text AS feature_type,
           'negative_pressure'::text AS feature,
           0.7::double precision
    WHERE coalesce(m.claim_role, m.proposition->>'claim_role') = 'concern'
       OR coalesce(m.polarity, m.proposition->>'polarity') = 'negative'

    UNION ALL

    SELECT 'structural'::text AS feature_type,
           'future_expectation'::text AS feature,
           0.7::double precision
    WHERE coalesce(m.claim_role, m.proposition->>'claim_role') = 'prediction'
       OR coalesce(m.time_mode, m.proposition->>'time_mode')
          IN ('future', 'recurring')
  ) raw
  WHERE nullif(raw.feature, '') IS NOT NULL
  ORDER BY raw.feature_type, raw.feature, raw.weight DESC
) features
ON CONFLICT (tenant_id, model_id, feature_type, feature) DO UPDATE SET
  status = EXCLUDED.status,
  weight = EXCLUDED.weight,
  source = EXCLUDED.source,
  updated_at = EXCLUDED.updated_at;

INSERT INTO model_representation_feature_postings (
  tenant_id,
  model_id,
  status,
  feature_type,
  feature,
  weight,
  source,
  updated_at
)
SELECT
  mst.tenant_id,
  mst.model_id,
  coalesce(m.status, 'active'),
  'lexical',
  features.feature,
  1.0,
  'model_semantic_terms',
  now()
FROM model_semantic_terms mst
LEFT JOIN models m
  ON m.tenant_id = mst.tenant_id
 AND m.id = mst.model_id
CROSS JOIN LATERAL (
  SELECT DISTINCT lower(trim(term.value)) AS feature
  FROM unnest(mst.semantic_terms) AS term(value)
  WHERE nullif(lower(trim(term.value)), '') IS NOT NULL
) features
ON CONFLICT (tenant_id, model_id, feature_type, feature) DO UPDATE SET
  status = EXCLUDED.status,
  weight = EXCLUDED.weight,
  source = EXCLUDED.source,
  updated_at = EXCLUDED.updated_at;

ALTER TABLE model_representation_feature_postings
  ALTER COLUMN tenant_id SET STATISTICS 1000,
  ALTER COLUMN status SET STATISTICS 1000,
  ALTER COLUMN feature_type SET STATISTICS 1000,
  ALTER COLUMN feature SET STATISTICS 1000;

CREATE STATISTICS IF NOT EXISTS model_representation_feature_lookup_stats
  (dependencies, ndistinct, mcv)
  ON tenant_id, status, feature_type, feature
  FROM model_representation_feature_postings;

ANALYZE model_representation_feature_postings;

COMMIT;
