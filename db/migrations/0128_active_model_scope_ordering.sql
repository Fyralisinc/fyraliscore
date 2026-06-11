-- 0128_active_model_scope_ordering.sql
-- Support bounded scoped retrieval by scanning active tenant models in ranking
-- order and checking indexed scope sidecars with EXISTS.

CREATE INDEX IF NOT EXISTS models_active_tenant_activation_created_idx
  ON models (tenant_id, activation DESC, created_at DESC, id)
  WHERE status = 'active';
