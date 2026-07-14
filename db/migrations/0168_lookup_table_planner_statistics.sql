-- 0168_lookup_table_planner_statistics.sql
-- Keep retrieval lookup tables planner-stable under high-density tenants.
-- Bulk model creation can leave default statistics underestimating
-- tenant/term/scope selectivity, causing Postgres to choose broad model scans
-- over the intended posting-list indexes.

BEGIN;

ALTER TABLE model_answerability_index
  ALTER COLUMN tenant_id SET STATISTICS 1000,
  ALTER COLUMN status SET STATISTICS 1000,
  ALTER COLUMN primitive SET STATISTICS 1000,
  ALTER COLUMN term SET STATISTICS 1000;

ALTER TABLE model_sparse_terms
  ALTER COLUMN tenant_id SET STATISTICS 1000,
  ALTER COLUMN status SET STATISTICS 1000,
  ALTER COLUMN term SET STATISTICS 1000;

ALTER TABLE model_scope_entities
  ALTER COLUMN tenant_id SET STATISTICS 1000,
  ALTER COLUMN entity_type SET STATISTICS 1000,
  ALTER COLUMN entity_id SET STATISTICS 1000;

CREATE STATISTICS IF NOT EXISTS model_answerability_lookup_stats
  (dependencies, ndistinct, mcv)
  ON tenant_id, status, primitive, term
  FROM model_answerability_index;

CREATE STATISTICS IF NOT EXISTS model_sparse_terms_lookup_stats
  (dependencies, ndistinct, mcv)
  ON tenant_id, status, term
  FROM model_sparse_terms;

CREATE STATISTICS IF NOT EXISTS model_scope_entities_lookup_stats
  (dependencies, ndistinct, mcv)
  ON tenant_id, entity_type, entity_id
  FROM model_scope_entities;

ANALYZE model_answerability_index;
ANALYZE model_sparse_terms;
ANALYZE model_scope_entities;

COMMIT;
