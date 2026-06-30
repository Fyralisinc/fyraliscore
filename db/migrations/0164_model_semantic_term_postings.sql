-- 0164_model_semantic_term_postings.sql
--
-- Normalize model semantic terms into bounded posting lists for retrieval.
-- The array sidecar remains the canonical hydrated Model field; this table is
-- the query-time acceleration surface used by pathway L.

BEGIN;

CREATE TABLE IF NOT EXISTS model_semantic_term_postings (
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  term TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, model_id, term)
);

CREATE INDEX IF NOT EXISTS model_semantic_term_postings_term_idx
  ON model_semantic_term_postings (tenant_id, term, model_id);

ALTER TABLE model_semantic_term_postings ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_semantic_term_postings FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_semantic_term_postings;
CREATE POLICY tenant_isolation ON model_semantic_term_postings
USING (
  NULLIF(current_setting('app.current_tenant', true), '') IS NULL
  OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  NULLIF(current_setting('app.current_tenant', true), '') IS NULL
  OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

CREATE OR REPLACE FUNCTION refresh_model_semantic_term_postings()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_semantic_term_postings
    WHERE tenant_id = OLD.tenant_id
      AND model_id = OLD.model_id;
    RETURN OLD;
  END IF;

  DELETE FROM model_semantic_term_postings
  WHERE tenant_id = NEW.tenant_id
    AND model_id = NEW.model_id;

  INSERT INTO model_semantic_term_postings (
    tenant_id,
    model_id,
    term,
    updated_at
  )
  SELECT
    NEW.tenant_id,
    NEW.model_id,
    term.value,
    now()
  FROM unnest(NEW.semantic_terms) AS term(value)
  WHERE nullif(trim(term.value), '') IS NOT NULL
  ON CONFLICT (tenant_id, model_id, term) DO UPDATE SET
    updated_at = EXCLUDED.updated_at;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS model_semantic_terms_refresh_postings
  ON model_semantic_terms;
CREATE TRIGGER model_semantic_terms_refresh_postings
AFTER INSERT OR UPDATE OF tenant_id, model_id, semantic_terms
OR DELETE ON model_semantic_terms
FOR EACH ROW EXECUTE FUNCTION refresh_model_semantic_term_postings();

INSERT INTO model_semantic_term_postings (
  tenant_id,
  model_id,
  term,
  updated_at
)
SELECT
  mst.tenant_id,
  mst.model_id,
  term.value,
  now()
FROM model_semantic_terms mst
CROSS JOIN LATERAL unnest(mst.semantic_terms) AS term(value)
WHERE nullif(trim(term.value), '') IS NOT NULL
ON CONFLICT (tenant_id, model_id, term) DO UPDATE SET
  updated_at = EXCLUDED.updated_at;

COMMIT;
