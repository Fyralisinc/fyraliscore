-- =====================================================================
-- 0001_foundation.sql — Company OS foundation schema
-- =====================================================================
-- Source of truth: SCHEMA-LOCK.md sections S1-S6, S22.
-- Ordering: dependency-respecting. Actors first (referenced by
-- observations); observations before everything else that references
-- event ids; models before commitments (which reference models).
-- Idempotent: extensions use IF NOT EXISTS; every CREATE TABLE /
-- CREATE INDEX uses IF NOT EXISTS; partition creation is inside a DO
-- block and re-runnable.
-- This file is immutable once committed. Further changes go in
-- 0002_*.sql, 0003_*.sql, ... per BUILD-PLAN §0.6.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- S22.1 — Required extensions
-- ---------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- ---------------------------------------------------------------------
-- S5.1 — actors (referenced by observations.actor_id and many others)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS actors (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  type TEXT NOT NULL,
  display_name TEXT NOT NULL,
  email TEXT,
  status TEXT DEFAULT 'active',
  metadata JSONB,
  specification_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at TIMESTAMPTZ
);

-- S5.2 — actor_identity_mappings
CREATE TABLE IF NOT EXISTS actor_identity_mappings (
  actor_id UUID NOT NULL REFERENCES actors(id),
  source_channel TEXT NOT NULL,
  source_actor_ref TEXT NOT NULL,
  confidence FLOAT DEFAULT 1.0,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (source_channel, source_actor_ref)
);

-- S5.3 — actors indexes
CREATE INDEX IF NOT EXISTS actors_email_idx ON actors (tenant_id, email);
CREATE INDEX IF NOT EXISTS actors_type_idx ON actors (tenant_id, type, status);

-- ---------------------------------------------------------------------
-- S1.1 — observations (partitioned parent)
-- ---------------------------------------------------------------------
-- PostgreSQL requires the partition key to be part of every unique /
-- primary constraint. Spec §1 says `id UUID PRIMARY KEY`; to keep
-- that PK meaningful while partitioning by occurred_at, we make the
-- PK (id, occurred_at). The application-level guarantee of unique id
-- is preserved because id is a UUID v7 and occurred_at is carried in
-- every insert. The UNIQUE (source_channel, external_id) constraint
-- is likewise widened to include occurred_at.
-- Same treatment as S4.2 resource_transactions. Documented in S1.
CREATE TABLE IF NOT EXISTS observations (
  id UUID NOT NULL,
  tenant_id UUID NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  kind TEXT NOT NULL,
  source_channel TEXT NOT NULL,
  source_actor_ref TEXT,
  actor_id UUID REFERENCES actors(id),
  content JSONB NOT NULL,
  content_text TEXT NOT NULL,
  embedding VECTOR(768),
  embedding_pending BOOLEAN DEFAULT FALSE,
  trust_tier TEXT NOT NULL,
  external_id TEXT,
  cause_id UUID,
  sequence_num BIGSERIAL,
  entities_mentioned JSONB DEFAULT '[]'::jsonb,
  PRIMARY KEY (id, occurred_at),
  UNIQUE (source_channel, external_id, occurred_at)
) PARTITION BY RANGE (occurred_at);

-- S1.2 — indexes on observations (declared on the parent; propagate to partitions)
CREATE INDEX IF NOT EXISTS obs_embedding_idx ON observations USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS obs_actor_time_idx ON observations (actor_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS obs_channel_time_idx ON observations (source_channel, occurred_at DESC);
CREATE INDEX IF NOT EXISTS obs_kind_idx ON observations (kind);
CREATE INDEX IF NOT EXISTS obs_cause_idx ON observations (cause_id);
CREATE INDEX IF NOT EXISTS obs_entities_idx ON observations USING gin (entities_mentioned);
CREATE INDEX IF NOT EXISTS obs_tenant_time_idx ON observations (tenant_id, occurred_at DESC);

-- ---------------------------------------------------------------------
-- S2.1 — models (depends on observations)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS models (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  born_from_event_id UUID NOT NULL,

  -- Content
  proposition JSONB NOT NULL,
  -- "natural" is a reserved SQL keyword; quoted identifier preserves
  -- the exact column name mandated by SCHEMA-LOCK.md S2.1.
  "natural" TEXT NOT NULL,
  embedding VECTOR(768) NOT NULL,

  -- Scope
  scope_actors UUID[] DEFAULT '{}',
  scope_entities JSONB DEFAULT '[]'::jsonb,
  scope_temporal JSONB NOT NULL,

  -- Epistemic
  confidence FLOAT NOT NULL CHECK (confidence >= 0.05 AND confidence <= 0.95),
  activation FLOAT NOT NULL DEFAULT 1.0,
  falsifier JSONB,

  -- Signal readings
  signal_readings JSONB DEFAULT '[]'::jsonb,
  reading_contestable BOOLEAN DEFAULT TRUE,

  -- Provenance
  supporting_event_ids UUID[] DEFAULT '{}',
  supporting_model_ids UUID[] DEFAULT '{}',
  evidential_weight FLOAT DEFAULT 0.5,

  -- Lifecycle
  status TEXT NOT NULL DEFAULT 'active',
  archived_at TIMESTAMPTZ,
  archive_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_retrieved_at TIMESTAMPTZ,
  retrieval_count INTEGER DEFAULT 0,

  -- Prediction-specific
  evaluate_at TIMESTAMPTZ,
  resolution_criteria JSONB,
  contributing_models UUID[] DEFAULT '{}',

  -- Access
  visible_to_subjects BOOLEAN DEFAULT TRUE
);
-- Note: FK models.born_from_event_id -> observations(id) is NOT
-- enforced at the DB level because observations is partitioned and
-- PostgreSQL cannot create a foreign key to a partitioned table whose
-- PK includes the partition key column (we would need a FK composite
-- (born_from_event_id, born_from_occurred_at)). Application layer
-- enforces the reference. Same pattern for every *_event_id FK below.

-- S2.2 — indexes on models
CREATE INDEX IF NOT EXISTS models_embedding_idx ON models USING hnsw (embedding vector_cosine_ops) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS models_actors_idx ON models USING gin (scope_actors) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS models_entities_idx ON models USING gin (scope_entities) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS models_evaluate_idx ON models (evaluate_at) WHERE status = 'active' AND evaluate_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS models_retrieved_idx ON models (last_retrieved_at);
CREATE INDEX IF NOT EXISTS models_tenant_status_idx ON models (tenant_id, status);
CREATE INDEX IF NOT EXISTS models_supporting_idx ON models USING gin (supporting_model_ids);
CREATE INDEX IF NOT EXISTS models_activation_idx ON models (activation) WHERE status = 'active';

-- ---------------------------------------------------------------------
-- S3.1 — goals (depends on observations)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS goals (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  state TEXT NOT NULL DEFAULT 'active',
  target_date TIMESTAMPTZ,
  parent_goal_id UUID REFERENCES goals(id),
  altitude TEXT DEFAULT 'operational',
  success_criteria JSONB,
  cached_health TEXT DEFAULT 'healthy',
  cached_health_computed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_state_change_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by_event_id UUID NOT NULL,
  archived_at TIMESTAMPTZ
);

-- S3.2 — indexes on goals
CREATE INDEX IF NOT EXISTS goals_state_idx ON goals (tenant_id, state);
CREATE INDEX IF NOT EXISTS goals_parent_idx ON goals (parent_goal_id);
CREATE INDEX IF NOT EXISTS goals_altitude_idx ON goals (tenant_id, altitude);

-- ---------------------------------------------------------------------
-- S3.3 — commitments (depends on actors, models, observations)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS commitments (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  title TEXT NOT NULL,
  description TEXT,
  state TEXT NOT NULL DEFAULT 'proposed',
  owner_id UUID REFERENCES actors(id),
  due_date TIMESTAMPTZ,
  ambition_level TEXT DEFAULT 'base',
  priority INTEGER DEFAULT 5,
  success_criteria JSONB,
  resolved_by_event_ids UUID[] DEFAULT '{}',
  external_counterparty_ref JSONB,
  estimated_capacity JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_state_change_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  terminal_at TIMESTAMPTZ,
  created_by_event_id UUID NOT NULL,
  last_confidence_basis UUID REFERENCES models(id)
);

-- S3.4 — commitment_contributors (depends on commitments, actors)
CREATE TABLE IF NOT EXISTS commitment_contributors (
  commitment_id UUID NOT NULL REFERENCES commitments(id),
  actor_id UUID NOT NULL REFERENCES actors(id),
  role TEXT,
  PRIMARY KEY (commitment_id, actor_id)
);

-- S3.5 — indexes on commitments / commitment_contributors
CREATE INDEX IF NOT EXISTS commitments_state_idx ON commitments (tenant_id, state);
CREATE INDEX IF NOT EXISTS commitments_owner_idx ON commitments (owner_id);
CREATE INDEX IF NOT EXISTS commitments_due_idx ON commitments (due_date)
  WHERE state NOT IN ('doneverified', 'closed');
CREATE INDEX IF NOT EXISTS commitments_contributors_actor_idx ON commitment_contributors (actor_id);

-- ---------------------------------------------------------------------
-- S3.6 — decisions (depends on observations)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS decisions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  title TEXT NOT NULL,
  decision_text TEXT NOT NULL,
  rationale TEXT,
  state TEXT NOT NULL DEFAULT 'drafted',
  scope JSONB,
  revisit_triggers JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_state_change_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by_event_id UUID NOT NULL,
  archived_at TIMESTAMPTZ
);

-- S3.7 — indexes on decisions
CREATE INDEX IF NOT EXISTS decisions_state_idx ON decisions (tenant_id, state);

-- ---------------------------------------------------------------------
-- S3.8 — contributes_to (Commitment -> Goal)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contributes_to (
  commitment_id UUID NOT NULL REFERENCES commitments(id),
  goal_id UUID NOT NULL REFERENCES goals(id),
  is_critical_path BOOLEAN DEFAULT FALSE,
  PRIMARY KEY (commitment_id, goal_id)
);

-- S3.9 — depends_on (Commitment -> Commitment)
CREATE TABLE IF NOT EXISTS depends_on (
  dependent_commitment_id UUID NOT NULL REFERENCES commitments(id),
  dependency_commitment_id UUID NOT NULL REFERENCES commitments(id),
  PRIMARY KEY (dependent_commitment_id, dependency_commitment_id),
  CHECK (dependent_commitment_id != dependency_commitment_id)
);

-- S3.10 — constrained_by (Commitment -> Decision)
CREATE TABLE IF NOT EXISTS constrained_by (
  commitment_id UUID NOT NULL REFERENCES commitments(id),
  decision_id UUID NOT NULL REFERENCES decisions(id),
  PRIMARY KEY (commitment_id, decision_id)
);

-- S3.11 — edge-table indexes
CREATE INDEX IF NOT EXISTS contributes_goal_idx ON contributes_to (goal_id);
CREATE INDEX IF NOT EXISTS contributes_critical_idx ON contributes_to (goal_id) WHERE is_critical_path = TRUE;
CREATE INDEX IF NOT EXISTS depends_dependency_idx ON depends_on (dependency_commitment_id);
CREATE INDEX IF NOT EXISTS constrained_decision_idx ON constrained_by (decision_id);

-- ---------------------------------------------------------------------
-- S4.1 — resources (depends on observations)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS resources (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  kind TEXT NOT NULL,
  identity TEXT NOT NULL,
  description TEXT,
  current_value JSONB NOT NULL,
  valuation_confidence FLOAT DEFAULT 1.0,
  utilization_state TEXT NOT NULL DEFAULT 'available',
  controllability TEXT NOT NULL DEFAULT 'owned',
  temporal_character TEXT NOT NULL DEFAULT 'permanent',
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_updated_by_event_id UUID,
  archived_at TIMESTAMPTZ
);

-- S4.2 — resource_transactions (partitioned by occurred_at per §22)
CREATE TABLE IF NOT EXISTS resource_transactions (
  id UUID NOT NULL,
  resource_id UUID NOT NULL REFERENCES resources(id),
  tenant_id UUID NOT NULL,
  transaction_type TEXT NOT NULL,
  delta JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  source_event_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at);

-- S4.3 — resource_deployments
CREATE TABLE IF NOT EXISTS resource_deployments (
  resource_id UUID NOT NULL REFERENCES resources(id),
  commitment_id UUID NOT NULL REFERENCES commitments(id),
  deployed_quantity JSONB,
  deployed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  released_at TIMESTAMPTZ,
  PRIMARY KEY (resource_id, commitment_id)
);

-- S4.4 — customer_commitments (Bridge spine, §4 shape; see SCHEMA-QUESTION.md Q2)
CREATE TABLE IF NOT EXISTS customer_commitments (
  customer_resource_id UUID NOT NULL REFERENCES resources(id),
  commitment_id UUID NOT NULL REFERENCES commitments(id),
  served_description TEXT,
  PRIMARY KEY (customer_resource_id, commitment_id)
);

-- S4.5 — indexes on Resources tables
CREATE INDEX IF NOT EXISTS resources_kind_idx ON resources (tenant_id, kind) WHERE archived_at IS NULL;
CREATE INDEX IF NOT EXISTS resources_utilization_idx ON resources (tenant_id, utilization_state);
CREATE INDEX IF NOT EXISTS resource_tx_resource_idx ON resource_transactions (resource_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS resource_deployments_commitment_idx ON resource_deployments (commitment_id)
  WHERE released_at IS NULL;

-- ---------------------------------------------------------------------
-- S6.1 — entity_aliases (depends on actors, observations)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS entity_aliases (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  alias_text TEXT NOT NULL,
  alias_embedding VECTOR(768),
  actor_id UUID REFERENCES actors(id),
  resolved_entity_ref JSONB NOT NULL,
  is_canonical BOOLEAN DEFAULT FALSE,
  entity_metadata JSONB,
  confidence FLOAT NOT NULL DEFAULT 0.8,
  confirmed_count INTEGER DEFAULT 0,
  contested_count INTEGER DEFAULT 0,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_event_id UUID,
  UNIQUE (tenant_id, alias_text, actor_id)
);

-- S6.2 — indexes on entity_aliases
CREATE INDEX IF NOT EXISTS aliases_embedding_idx ON entity_aliases USING hnsw (alias_embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS aliases_text_idx ON entity_aliases (tenant_id, alias_text);
CREATE INDEX IF NOT EXISTS aliases_actor_idx ON entity_aliases (tenant_id, actor_id);
CREATE INDEX IF NOT EXISTS aliases_entity_idx ON entity_aliases USING gin (resolved_entity_ref);
CREATE INDEX IF NOT EXISTS aliases_canonical_idx ON entity_aliases (tenant_id, is_canonical) WHERE is_canonical = TRUE;

-- ---------------------------------------------------------------------
-- Partition creation: current month + next 3 months
-- ---------------------------------------------------------------------
-- Idempotent: uses CREATE TABLE IF NOT EXISTS PARTITION OF. Each
-- partition spans exactly one calendar month. Worker in Wave 4-D
-- extends the window forward.
DO $$
DECLARE
    start_date DATE := DATE_TRUNC('month', CURRENT_DATE)::DATE;
    end_date DATE;
    partition_name TEXT;
    i INT;
BEGIN
    FOR i IN 0..3 LOOP
        end_date := (start_date + INTERVAL '1 month')::DATE;

        partition_name := format('observations_%s', TO_CHAR(start_date, 'YYYY_MM'));
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF observations FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );

        partition_name := format('resource_transactions_%s', TO_CHAR(start_date, 'YYYY_MM'));
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF resource_transactions FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );

        start_date := end_date;
    END LOOP;
END $$;

COMMIT;
