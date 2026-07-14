-- 0193_model_events_and_projection_snapshots.sql
--
-- Belief Kernel -> Event Stream -> Rebuildable Projections.
--
-- The Model layer emits neutral, durable belief-change events. Projection
-- workers consume those events and materialize disposable operating views
-- such as constraints, resources, acts, customers, and forecasts.

BEGIN;

CREATE TABLE IF NOT EXISTS model_events (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  event_type TEXT NOT NULL,
  changed_fields TEXT[] NOT NULL DEFAULT '{}',

  -- Duplicated semantic routing fields keep projection dispatch cheap while
  -- semantic_snapshot preserves the full event payload for rebuild/debug.
  proposition_kind TEXT,
  claim_role TEXT,
  domain_tags TEXT[] NOT NULL DEFAULT '{}',
  scope_entities JSONB NOT NULL DEFAULT '[]'::jsonb,
  semantic_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  previous_snapshot JSONB,

  source_event_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT model_events_event_type_valid CHECK (
    event_type IN (
      'model.created',
      'model.updated',
      'model.archived',
      'model.relation_changed'
    )
  )
);

CREATE INDEX IF NOT EXISTS model_events_tenant_created_idx
  ON model_events (tenant_id, created_at, id);

CREATE INDEX IF NOT EXISTS model_events_model_created_idx
  ON model_events (tenant_id, model_id, created_at DESC);

CREATE INDEX IF NOT EXISTS model_events_semantic_idx
  ON model_events (tenant_id, event_type, claim_role);

CREATE INDEX IF NOT EXISTS model_events_domain_tags_idx
  ON model_events USING gin (domain_tags);

CREATE INDEX IF NOT EXISTS model_events_scope_entities_idx
  ON model_events USING gin (scope_entities);

CREATE TABLE IF NOT EXISTS projection_checkpoints (
  tenant_id UUID NOT NULL,
  projection_name TEXT NOT NULL,
  projection_version TEXT NOT NULL,
  last_processed_event_id UUID,
  last_processed_event_created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, projection_name, projection_version)
);

CREATE INDEX IF NOT EXISTS projection_checkpoints_updated_idx
  ON projection_checkpoints (projection_name, projection_version, updated_at DESC);

CREATE TABLE IF NOT EXISTS projection_snapshots (
  tenant_id UUID NOT NULL,
  projection_name TEXT NOT NULL,
  projection_version TEXT NOT NULL,
  subject_key TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  confidence FLOAT NOT NULL DEFAULT 0.0,
  severity TEXT,
  source_model_ids UUID[] NOT NULL DEFAULT '{}',
  source_event_ids UUID[] NOT NULL DEFAULT '{}',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, projection_name, projection_version, subject_key)
);

CREATE INDEX IF NOT EXISTS projection_snapshots_lookup_idx
  ON projection_snapshots (tenant_id, projection_name, subject_key);

CREATE INDEX IF NOT EXISTS projection_snapshots_severity_idx
  ON projection_snapshots (tenant_id, projection_name, severity, updated_at DESC);

CREATE INDEX IF NOT EXISTS projection_snapshots_source_models_idx
  ON projection_snapshots USING gin (source_model_ids);

ALTER TABLE model_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_events;
CREATE POLICY tenant_isolation ON model_events
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

ALTER TABLE projection_checkpoints ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_checkpoints FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON projection_checkpoints;
CREATE POLICY tenant_isolation ON projection_checkpoints
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

ALTER TABLE projection_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE projection_snapshots FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON projection_snapshots;
CREATE POLICY tenant_isolation ON projection_snapshots
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

COMMIT;
