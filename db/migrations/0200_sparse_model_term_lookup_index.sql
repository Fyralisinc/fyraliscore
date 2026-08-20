-- 0200_sparse_model_term_lookup_index.sql
-- Support scope-first sparse retrieval: given a bounded set of scoped models,
-- check whether each candidate carries focused lexical terms without scanning
-- all sparse postings for the tenant.

CREATE INDEX IF NOT EXISTS model_sparse_terms_active_model_term_idx
  ON model_sparse_terms (tenant_id, model_id, term)
  WHERE status = 'active';
