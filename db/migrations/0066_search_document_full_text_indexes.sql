-- 0066_search_document_full_text_indexes.sql
-- Native full-text index for Ask/SAGE belief-address activation.
--
-- This intentionally does not create a per-term posting table. The insert-side
-- cost is the GIN expression index maintenance on the compact belief-address
-- sidecar, which is materially cheaper than writing many rows per Model.

DROP INDEX IF EXISTS model_search_documents_active_search_text_fts_idx;

CREATE INDEX IF NOT EXISTS model_belief_addresses_search_text_fts_idx
  ON model_belief_addresses USING gin (to_tsvector('simple', search_text))
  WHERE status = 'active';
