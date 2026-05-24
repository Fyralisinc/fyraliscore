-- 0043_memory_graph_upgrade.sql
--
-- Memory graph upgrade: make model_edges rich enough to carry mined,
-- evidence-backed organizational relationships, and add normalized scope
-- sidecars so retrieval can anchor Models without repeatedly spelunking
-- JSONB arrays.

BEGIN;

-- ---------------------------------------------------------------------
-- 1. Edge quality / provenance fields.
-- ---------------------------------------------------------------------
ALTER TABLE model_edges
  ADD COLUMN IF NOT EXISTS confidence FLOAT NOT NULL DEFAULT 1.0,
  ADD COLUMN IF NOT EXISTS evidence_event_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  ADD COLUMN IF NOT EXISTS evidence_model_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  ADD COLUMN IF NOT EXISTS explanation TEXT,
  ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'accepted',
  ADD COLUMN IF NOT EXISTS last_confirmed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS confirmed_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS contested_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS decay_after TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'model_edges_confidence_range'
  ) THEN
    ALTER TABLE model_edges
      ADD CONSTRAINT model_edges_confidence_range
      CHECK (confidence >= 0.0 AND confidence <= 1.0);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'model_edges_counts_nonnegative'
  ) THEN
    ALTER TABLE model_edges
      ADD CONSTRAINT model_edges_counts_nonnegative
      CHECK (confirmed_count >= 0 AND contested_count >= 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'model_edges_review_status_check'
  ) THEN
    ALTER TABLE model_edges
      ADD CONSTRAINT model_edges_review_status_check
      CHECK (review_status IN (
        'accepted',
        'candidate',
        'needs_review',
        'rejected',
        'retired'
      ));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS model_edges_review_idx
  ON model_edges (tenant_id, review_status, edge_kind, created_at DESC);

CREATE INDEX IF NOT EXISTS model_edges_evidence_events_idx
  ON model_edges USING gin (evidence_event_ids);

CREATE INDEX IF NOT EXISTS model_edges_evidence_models_idx
  ON model_edges USING gin (evidence_model_ids);

CREATE INDEX IF NOT EXISTS model_edges_active_decay_idx
  ON model_edges (tenant_id, decay_after, expires_at)
  WHERE status = 'active';

-- ---------------------------------------------------------------------
-- 2. Normalized Model scope sidecars.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS model_scope_entities (
  model_id UUID NOT NULL,
  tenant_id UUID NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id UUID NOT NULL,
  source TEXT NOT NULL DEFAULT 'model_scope',
  confidence FLOAT NOT NULL DEFAULT 1.0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (model_id, entity_type, entity_id),
  CONSTRAINT model_scope_entities_confidence_range
    CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

CREATE INDEX IF NOT EXISTS model_scope_entities_lookup_idx
  ON model_scope_entities (tenant_id, entity_type, entity_id, model_id);

CREATE INDEX IF NOT EXISTS model_scope_entities_model_idx
  ON model_scope_entities (tenant_id, model_id);

CREATE TABLE IF NOT EXISTS model_scope_actors (
  model_id UUID NOT NULL,
  tenant_id UUID NOT NULL,
  actor_id UUID NOT NULL,
  source TEXT NOT NULL DEFAULT 'model_scope',
  confidence FLOAT NOT NULL DEFAULT 1.0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (model_id, actor_id),
  CONSTRAINT model_scope_actors_confidence_range
    CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

CREATE INDEX IF NOT EXISTS model_scope_actors_lookup_idx
  ON model_scope_actors (tenant_id, actor_id, model_id);

CREATE INDEX IF NOT EXISTS model_scope_actors_model_idx
  ON model_scope_actors (tenant_id, model_id);

-- Backfill scope entities that already contain UUID-like ids. Bad legacy
-- JSON stays in models.scope_entities but does not poison the sidecar.
INSERT INTO model_scope_entities (
  model_id, tenant_id, entity_type, entity_id, source, confidence
)
SELECT
  m.id,
  m.tenant_id,
  e.value ->> 'type',
  (e.value ->> 'id')::uuid,
  'backfill',
  1.0
FROM models m
CROSS JOIN LATERAL jsonb_array_elements(m.scope_entities) AS e(value)
WHERE e.value ? 'type'
  AND e.value ? 'id'
  AND (e.value ->> 'id') ~*
    '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
ON CONFLICT (model_id, entity_type, entity_id) DO NOTHING;

INSERT INTO model_scope_actors (
  model_id, tenant_id, actor_id, source, confidence
)
SELECT
  m.id,
  m.tenant_id,
  actor_id,
  'backfill',
  1.0
FROM models m
CROSS JOIN LATERAL unnest(m.scope_actors) AS actor_id
ON CONFLICT (model_id, actor_id) DO NOTHING;

COMMIT;
