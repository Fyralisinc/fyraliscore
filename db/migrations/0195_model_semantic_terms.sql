-- 0195_model_semantic_terms.sql
--
-- Compact model-specific lexical handles for retrieval. These are not broad
-- domain tags, scope actors, scope entities, or projection labels; they are
-- normalized semantic phrases derived from the Model's own belief text.
--
-- Store them as a Model-layer sidecar instead of another physical models
-- column. The domain ModelRow still hydrates them as model state, while the
-- core models table stays under PostgreSQL's column limit.

BEGIN;

CREATE TABLE IF NOT EXISTS model_semantic_terms (
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  semantic_terms TEXT[] NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, model_id)
);

CREATE INDEX IF NOT EXISTS model_semantic_terms_model_idx
  ON model_semantic_terms (model_id);

CREATE INDEX IF NOT EXISTS model_semantic_terms_terms_gin
  ON model_semantic_terms USING gin (semantic_terms);

ALTER TABLE model_semantic_terms ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_semantic_terms FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_semantic_terms;
CREATE POLICY tenant_isolation ON model_semantic_terms
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

COMMIT;
