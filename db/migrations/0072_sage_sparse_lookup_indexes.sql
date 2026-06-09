-- 0072_sage_sparse_lookup_indexes.sql
-- Question-time lexical and answerability lookup should be posting-list
-- retrieval, not repeated text scans over compact search documents.

CREATE TABLE IF NOT EXISTS model_sparse_terms (
  model_id uuid NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  status text NOT NULL,
  term text NOT NULL,
  field text NOT NULL DEFAULT 'search_text',
  weight real NOT NULL DEFAULT 1.0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (model_id, term, field)
);

CREATE TABLE IF NOT EXISTS model_answerability_index (
  model_id uuid NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  status text NOT NULL,
  primitive text NOT NULL,
  term text NOT NULL,
  field text NOT NULL DEFAULT 'address',
  weight real NOT NULL DEFAULT 1.0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (model_id, primitive, term, field)
);

CREATE OR REPLACE FUNCTION sage_sparse_terms_from_text(
  value text,
  max_terms integer DEFAULT 128
)
RETURNS text[]
LANGUAGE sql
IMMUTABLE
AS $$
  WITH raw AS (
    SELECT (m.match)[1] AS term
    FROM regexp_matches(
      lower(coalesce(value, '')),
      '([a-z0-9][a-z0-9_-]{2,})',
      'g'
    ) AS m(match)
  ),
  filtered AS (
    SELECT DISTINCT term
    FROM raw
    WHERE term <> ALL(ARRAY[
      'about', 'after', 'also', 'and', 'are', 'around', 'because', 'been',
      'before', 'case', 'company', 'context', 'from', 'has', 'have', 'into',
      'need', 'needs', 'now', 'only', 'signal', 'that', 'the', 'their',
      'there', 'this', 'today', 'with', 'without', 'what', 'which', 'would',
      'should', 'actually', 'where', 'when', 'whose', 'who', 'why', 'how',
      'sure', 'here', 'current', 'fyralis'
    ]::text[])
      AND term !~ '^[0-9]+$'
    ORDER BY term
    LIMIT greatest(1, max_terms)
  )
  SELECT coalesce(array_agg(term ORDER BY term), ARRAY[]::text[])
  FROM filtered;
$$;

CREATE OR REPLACE FUNCTION refresh_model_sparse_terms()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_sparse_terms WHERE model_id = OLD.model_id;
    RETURN OLD;
  END IF;

  DELETE FROM model_sparse_terms WHERE model_id = NEW.model_id;

  INSERT INTO model_sparse_terms (
    model_id,
    tenant_id,
    status,
    term,
    field,
    weight,
    updated_at
  )
  SELECT
    NEW.model_id,
    NEW.tenant_id,
    NEW.status,
    term.value,
    'search_text',
    1.0,
    now()
  FROM unnest(sage_sparse_terms_from_text(NEW.search_text, 128)) AS term(value)
  WHERE term.value <> '';

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS model_search_documents_refresh_sparse_terms
  ON model_search_documents;
CREATE TRIGGER model_search_documents_refresh_sparse_terms
AFTER INSERT OR UPDATE OF tenant_id, status, search_text
OR DELETE ON model_search_documents
FOR EACH ROW EXECUTE FUNCTION refresh_model_sparse_terms();

CREATE OR REPLACE FUNCTION refresh_model_answerability_index()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_answerability_index WHERE model_id = OLD.model_id;
    RETURN OLD;
  END IF;

  DELETE FROM model_answerability_index WHERE model_id = NEW.model_id;

  INSERT INTO model_answerability_index (
    model_id,
    tenant_id,
    status,
    primitive,
    term,
    field,
    weight,
    updated_at
  )
  SELECT
    NEW.model_id,
    NEW.tenant_id,
    NEW.status,
    upper(primitive.value),
    term.value,
    'address',
    1.0,
    now()
  FROM unnest(NEW.answerable_primitives) AS primitive(value)
  CROSS JOIN unnest(sage_sparse_terms_from_text(NEW.search_text, 96)) AS term(value)
  WHERE nullif(trim(primitive.value), '') IS NOT NULL
    AND term.value <> '';

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS model_belief_addresses_refresh_answerability_index
  ON model_belief_addresses;
CREATE TRIGGER model_belief_addresses_refresh_answerability_index
AFTER INSERT OR UPDATE OF tenant_id, status, answerable_primitives, search_text
OR DELETE ON model_belief_addresses
FOR EACH ROW EXECUTE FUNCTION refresh_model_answerability_index();

INSERT INTO model_sparse_terms (
  model_id,
  tenant_id,
  status,
  term,
  field,
  weight,
  updated_at
)
SELECT
  msd.model_id,
  msd.tenant_id,
  msd.status,
  term.value,
  'search_text',
  1.0,
  now()
FROM model_search_documents msd
CROSS JOIN LATERAL unnest(
  sage_sparse_terms_from_text(msd.search_text, 128)
) AS term(value)
WHERE term.value <> ''
ON CONFLICT (model_id, term, field) DO UPDATE SET
  tenant_id = EXCLUDED.tenant_id,
  status = EXCLUDED.status,
  weight = EXCLUDED.weight,
  updated_at = EXCLUDED.updated_at;

INSERT INTO model_answerability_index (
  model_id,
  tenant_id,
  status,
  primitive,
  term,
  field,
  weight,
  updated_at
)
SELECT
  mba.model_id,
  mba.tenant_id,
  mba.status,
  upper(primitive.value),
  term.value,
  'address',
  1.0,
  now()
FROM model_belief_addresses mba
CROSS JOIN LATERAL unnest(mba.answerable_primitives) AS primitive(value)
CROSS JOIN LATERAL unnest(
  sage_sparse_terms_from_text(mba.search_text, 96)
) AS term(value)
WHERE nullif(trim(primitive.value), '') IS NOT NULL
  AND term.value <> ''
ON CONFLICT (model_id, primitive, term, field) DO UPDATE SET
  tenant_id = EXCLUDED.tenant_id,
  status = EXCLUDED.status,
  weight = EXCLUDED.weight,
  updated_at = EXCLUDED.updated_at;

CREATE INDEX IF NOT EXISTS model_sparse_terms_active_lookup_idx
  ON model_sparse_terms (tenant_id, term, weight DESC, model_id)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS model_sparse_terms_active_model_idx
  ON model_sparse_terms (tenant_id, model_id)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS model_answerability_index_active_lookup_idx
  ON model_answerability_index (
    tenant_id,
    primitive,
    term,
    weight DESC,
    model_id
  )
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS model_answerability_index_active_model_idx
  ON model_answerability_index (tenant_id, model_id)
  WHERE status = 'active';
