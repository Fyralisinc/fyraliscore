-- 0202_model_semantic_term_posting_status.sql
-- destructive-migration-approved: backup=canonical-models-table rollback=rerun-migration-backfill owner=reasoning
-- Keep semantic-term postings active-aware so bounded posting reads do not
-- spend their per-term page on archived Models.

BEGIN;

ALTER TABLE model_semantic_term_postings
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';

UPDATE model_semantic_term_postings post
SET status = m.status
FROM models m
WHERE m.id = post.model_id
  AND m.tenant_id = post.tenant_id
  AND post.status IS DISTINCT FROM m.status;

CREATE INDEX IF NOT EXISTS model_semantic_term_postings_active_term_idx
  ON model_semantic_term_postings (tenant_id, term, model_id)
  WHERE status = 'active';

ALTER TABLE model_semantic_term_postings
  ALTER COLUMN tenant_id SET STATISTICS 1000,
  ALTER COLUMN status SET STATISTICS 1000,
  ALTER COLUMN term SET STATISTICS 1000;

CREATE STATISTICS IF NOT EXISTS model_semantic_term_postings_lookup_stats
  (dependencies, ndistinct, mcv)
  ON tenant_id, status, term
  FROM model_semantic_term_postings;

CREATE OR REPLACE FUNCTION refresh_model_semantic_term_postings()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  model_status TEXT;
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_semantic_term_postings
    WHERE tenant_id = OLD.tenant_id
      AND model_id = OLD.model_id;
    RETURN OLD;
  END IF;

  SELECT status INTO model_status
  FROM models
  WHERE tenant_id = NEW.tenant_id
    AND id = NEW.model_id;

  DELETE FROM model_semantic_term_postings
  WHERE tenant_id = NEW.tenant_id
    AND model_id = NEW.model_id;

  INSERT INTO model_semantic_term_postings (
    tenant_id,
    model_id,
    status,
    term,
    updated_at
  )
  SELECT
    NEW.tenant_id,
    NEW.model_id,
    coalesce(model_status, 'active'),
    term.value,
    now()
  FROM unnest(NEW.semantic_terms) AS term(value)
  WHERE nullif(trim(term.value), '') IS NOT NULL
  ON CONFLICT (tenant_id, model_id, term) DO UPDATE SET
    status = EXCLUDED.status,
    updated_at = EXCLUDED.updated_at;

  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION refresh_model_semantic_term_posting_status()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE model_semantic_term_postings
  SET status = NEW.status,
      updated_at = now()
  WHERE tenant_id = NEW.tenant_id
    AND model_id = NEW.id
    AND status IS DISTINCT FROM NEW.status;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS models_refresh_semantic_term_posting_status
  ON models;
CREATE TRIGGER models_refresh_semantic_term_posting_status
AFTER UPDATE OF status ON models
FOR EACH ROW EXECUTE FUNCTION refresh_model_semantic_term_posting_status();

ANALYZE model_semantic_term_postings;

COMMIT;
