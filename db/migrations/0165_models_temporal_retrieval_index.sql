-- Speed Pathway C model recency reads.
--
-- The temporal pathway ranks active Models by their effective retrieval
-- timestamp: last retrieval if present, otherwise creation time. A matching
-- partial expression index keeps that read bounded without changing result
-- semantics.

CREATE INDEX IF NOT EXISTS models_active_effective_retrieved_at_idx
  ON models (tenant_id, (COALESCE(last_retrieved_at, created_at)) DESC, id)
  WHERE status = 'active';
