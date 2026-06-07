-- 0064_model_search_documents_trgm.sql
-- Restore an indexed literal-substring path for SAGE lexical activation.
--
-- The reader intentionally uses substring matching so exact user-facing
-- labels, field names, and option text remain retrievable. Without a
-- trigram index, each reader question scans every active Model document
-- and dominates Ask Fyralis latency on dense tenants.

CREATE INDEX IF NOT EXISTS model_search_documents_active_search_text_trgm_idx
  ON model_search_documents USING gin (search_text gin_trgm_ops)
  WHERE status = 'active';
