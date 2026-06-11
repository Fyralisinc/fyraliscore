-- 0131_disputed_edges_and_valid_until.sql
--
-- Capability plan C3/C7: allow an active edge to be explicitly disputed
-- instead of forcing a binary accepted/rejected lifecycle. Also add a small
-- helper index for retrieval paths that filter open-ended temporal scope.

BEGIN;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'model_edges_review_status_check'
  ) THEN
    ALTER TABLE model_edges
      DROP CONSTRAINT model_edges_review_status_check;
  END IF;

  ALTER TABLE model_edges
    ADD CONSTRAINT model_edges_review_status_check
    CHECK (review_status IN (
      'accepted',
      'candidate',
      'needs_review',
      'disputed',
      'rejected',
      'retired'
    ));
END $$;

CREATE INDEX IF NOT EXISTS model_edges_active_disputed_idx
  ON model_edges (tenant_id, edge_kind, created_at DESC)
  WHERE status = 'active' AND review_status = 'disputed';

CREATE INDEX IF NOT EXISTS models_active_scope_valid_until_idx
  ON models (tenant_id, ((scope_temporal->>'valid_until')), created_at DESC)
  WHERE status = 'active' AND archived_at IS NULL;

COMMIT;
