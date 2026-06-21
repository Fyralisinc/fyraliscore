-- 0150_relation_instances.sql
--
-- N-ary relation frames. A relation_instance is the semantic source of
-- truth for multi-model situations; model_edges remain the fast binary
-- projection used by retrieval, graph traversal, and product surfaces.

BEGIN;

CREATE TABLE IF NOT EXISTS relation_instances (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  source_observation_id UUID,
  think_run_id UUID,
  relation_kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'candidate' CHECK (
    status IN (
      'active', 'candidate', 'accepted', 'needs_review',
      'disputed', 'rejected', 'retired'
    )
  ),
  participant_binding_status TEXT NOT NULL DEFAULT 'unbound' CHECK (
    participant_binding_status IN (
      'bound', 'partially_bound', 'unbound', 'ambiguous'
    )
  ),
  write_policy TEXT NOT NULL DEFAULT 'candidate' CHECK (
    write_policy IN (
      'project_edges', 'candidate', 'needs_review', 'no_projection'
    )
  ),
  confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK (
    confidence >= 0.0 AND confidence <= 1.0
  ),
  evidence_event_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  evidence_text TEXT,
  explanation TEXT,
  temporal_bounds JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at TIMESTAMPTZ,
  CHECK (btrim(relation_kind) <> '')
);

CREATE INDEX IF NOT EXISTS relation_instances_tenant_status_idx
  ON relation_instances (tenant_id, status, relation_kind, created_at DESC);

CREATE INDEX IF NOT EXISTS relation_instances_evidence_events_idx
  ON relation_instances USING gin (evidence_event_ids);

CREATE INDEX IF NOT EXISTS relation_instances_evidence_models_idx
  ON relation_instances USING gin (evidence_model_ids);

CREATE TABLE IF NOT EXISTS relation_participants (
  id UUID PRIMARY KEY,
  relation_id UUID NOT NULL REFERENCES relation_instances(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL,
  role TEXT NOT NULL,
  binding_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK (
    binding_confidence >= 0.0 AND binding_confidence <= 1.0
  ),
  cardinality_group TEXT,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (btrim(role) <> ''),
  CONSTRAINT relation_participants_unique
    UNIQUE (relation_id, model_id, role)
);

CREATE INDEX IF NOT EXISTS relation_participants_relation_idx
  ON relation_participants (tenant_id, relation_id, role);

CREATE INDEX IF NOT EXISTS relation_participants_model_idx
  ON relation_participants (tenant_id, model_id, role);

CREATE TABLE IF NOT EXISTS relation_edge_projections (
  id UUID PRIMARY KEY,
  relation_id UUID NOT NULL REFERENCES relation_instances(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL,
  edge_id UUID NOT NULL,
  projection_rule TEXT NOT NULL,
  source_role TEXT NOT NULL,
  target_role TEXT NOT NULL,
  source_model_id UUID NOT NULL,
  target_model_id UUID NOT NULL,
  edge_kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active' CHECK (
    status IN ('active', 'retired', 'failed')
  ),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CHECK (source_model_id <> target_model_id),
  CHECK (btrim(projection_rule) <> ''),
  CHECK (btrim(source_role) <> ''),
  CHECK (btrim(target_role) <> ''),
  CHECK (btrim(edge_kind) <> ''),
  CONSTRAINT relation_edge_projections_unique
    UNIQUE (
      relation_id, projection_rule, source_model_id, target_model_id, edge_kind
    )
);

CREATE INDEX IF NOT EXISTS relation_edge_projections_relation_idx
  ON relation_edge_projections (tenant_id, relation_id, status);

CREATE INDEX IF NOT EXISTS relation_edge_projections_edge_idx
  ON relation_edge_projections (tenant_id, edge_id);

COMMIT;
