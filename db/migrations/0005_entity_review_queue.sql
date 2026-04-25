-- =====================================================================
-- 0005_entity_review_queue.sql — Entity resolver human-review queue.
-- =====================================================================
-- BUILD-PLAN §3 Prompt 2.B item 2: the entity resolver worker dumps
-- phrases it cannot resolve with high-enough confidence (0.5-0.8) into
-- this queue for human review.
--
-- Not in SCHEMA-LOCK.md S1-S6. Added per explicit Prompt 2.B language
-- ("create the schema yourself in a new migration"). Coordinated with
-- 2-A's 0003 (actor_sessions) and 0004 (think_trigger_queue): this
-- migration claims the next available number (0005).
--
-- Idempotent; immutable once committed.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS entity_review_queue (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  phrase TEXT NOT NULL,
  source_observation_id UUID NOT NULL,
  candidates JSONB NOT NULL,
  -- list of {canonical_ref, confidence, reasoning}
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  resolved_by UUID REFERENCES actors(id),
  chosen_ref JSONB,
  dismissed_reason TEXT
);

-- Operator-facing "open reviews" hot path. Drops resolved rows so the
-- set stays small.
CREATE INDEX IF NOT EXISTS entity_review_queue_open_idx
  ON entity_review_queue (tenant_id, created_at)
  WHERE resolved_at IS NULL;

COMMIT;
