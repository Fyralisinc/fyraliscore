-- 0111_sage_query_text_indexes.sql
-- Fast substring search for SAGE question-only retrieval.
--
-- The SAGE reader intentionally uses conservative literal substring
-- matching over natural text and structured JSON text so Ask Fyralis can
-- recover exact user-facing labels, field names, and option text. This
-- sidecar keeps that searchable document precomputed, avoiding repeated
-- jsonb::text casts and wide-column rescans on every Ask.

CREATE TABLE IF NOT EXISTS model_search_documents (
  model_id uuid PRIMARY KEY REFERENCES models(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  status text NOT NULL,
  search_text text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION refresh_model_search_document()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_search_documents WHERE model_id = OLD.id;
    RETURN OLD;
  END IF;

  INSERT INTO model_search_documents (
    model_id,
    tenant_id,
    status,
    search_text,
    updated_at
  ) VALUES (
    NEW.id,
    NEW.tenant_id,
    NEW.status,
    lower(
      coalesce(NEW."natural", '')
      || ' '
      || coalesce(NEW.proposition::text, '')
      || ' '
      || coalesce(NEW.scope_entities::text, '')
    ),
    now()
  )
  ON CONFLICT (model_id) DO UPDATE SET
    tenant_id = EXCLUDED.tenant_id,
    status = EXCLUDED.status,
    search_text = EXCLUDED.search_text,
    updated_at = EXCLUDED.updated_at
  WHERE model_search_documents.tenant_id IS DISTINCT FROM EXCLUDED.tenant_id
     OR model_search_documents.status IS DISTINCT FROM EXCLUDED.status
     OR model_search_documents.search_text IS DISTINCT FROM EXCLUDED.search_text;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS models_refresh_search_document ON models;
CREATE TRIGGER models_refresh_search_document
AFTER INSERT OR UPDATE OF tenant_id, status, "natural", proposition, scope_entities
OR DELETE ON models
FOR EACH ROW EXECUTE FUNCTION refresh_model_search_document();

INSERT INTO model_search_documents (
  model_id,
  tenant_id,
  status,
  search_text,
  updated_at
)
SELECT
  id,
  tenant_id,
  status,
  lower(
    coalesce("natural", '')
    || ' '
    || coalesce(proposition::text, '')
    || ' '
    || coalesce(scope_entities::text, '')
  ),
  now()
FROM models
ON CONFLICT (model_id) DO UPDATE SET
  tenant_id = EXCLUDED.tenant_id,
  status = EXCLUDED.status,
  search_text = EXCLUDED.search_text,
  updated_at = EXCLUDED.updated_at
WHERE model_search_documents.tenant_id IS DISTINCT FROM EXCLUDED.tenant_id
   OR model_search_documents.status IS DISTINCT FROM EXCLUDED.status
   OR model_search_documents.search_text IS DISTINCT FROM EXCLUDED.search_text;

DROP INDEX IF EXISTS models_active_natural_trgm_idx;
DROP INDEX IF EXISTS models_active_proposition_trgm_idx;
DROP INDEX IF EXISTS models_active_scope_entities_trgm_idx;
DROP INDEX IF EXISTS models_active_tenant_natural_trgm_idx;
DROP INDEX IF EXISTS models_active_tenant_proposition_trgm_idx;
DROP INDEX IF EXISTS models_active_tenant_scope_entities_trgm_idx;
DROP INDEX IF EXISTS model_search_documents_active_tenant_search_text_trgm_idx;

CREATE INDEX IF NOT EXISTS model_search_documents_active_tenant_idx
  ON model_search_documents (tenant_id)
  WHERE status = 'active';
