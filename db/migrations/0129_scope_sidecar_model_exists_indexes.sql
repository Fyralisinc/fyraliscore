-- 0129_scope_sidecar_model_exists_indexes.sql
-- Cover model-ordered scoped retrieval EXISTS probes.

CREATE INDEX IF NOT EXISTS model_scope_entities_model_entity_lookup_idx
  ON model_scope_entities (tenant_id, model_id, entity_type, entity_id);

CREATE INDEX IF NOT EXISTS model_scope_actors_model_actor_lookup_idx
  ON model_scope_actors (tenant_id, model_id, actor_id);
