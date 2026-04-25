-- =====================================================================
-- 0002_models_amendments.sql — Post-Wave-0 Q3 resolution
-- =====================================================================
-- Per Rachin's decisions on SCHEMA-QUESTION.md Q3 (logged in BUILD-LOG.md
-- under "Wave 0 amendments Q3/Q5") and SCHEMA-LOCK.md "Post-Wave-0
-- amendments" A1-A4, this migration adds the hot-path columns and
-- sidecar table that later waves depend on.
-- Immutable once committed. Further changes go in 0003_*.sql.
-- =====================================================================

BEGIN;

-- A1 — additions to models.
ALTER TABLE models
  ADD COLUMN IF NOT EXISTS proposition_kind TEXT
    GENERATED ALWAYS AS (proposition->>'kind') STORED;

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS confirmed_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS contested_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS last_confirmed_at TIMESTAMPTZ;

-- confidence_at_assertion is NOT NULL on new rows, but existing rows
-- (none in a fresh DB) need a backfill value. We add with DEFAULT 0.5
-- to cover the zero-row case safely, then drop the default so new
-- INSERTs must supply the value explicitly.
ALTER TABLE models
  ADD COLUMN IF NOT EXISTS confidence_at_assertion FLOAT NOT NULL DEFAULT 0.5;
ALTER TABLE models
  ALTER COLUMN confidence_at_assertion DROP DEFAULT;

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS resolution_outcome BOOLEAN;

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS activation_coefficient FLOAT NOT NULL DEFAULT 1.0;

-- CHECK constraints (guarded — can't use IF NOT EXISTS on constraints).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_resolution_consistency'
  ) THEN
    ALTER TABLE models ADD CONSTRAINT models_resolution_consistency
      CHECK (
        (resolved_at IS NULL AND resolution_outcome IS NULL) OR
        (resolved_at IS NOT NULL AND resolution_outcome IS NOT NULL)
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_confidence_at_assertion_range'
  ) THEN
    ALTER TABLE models ADD CONSTRAINT models_confidence_at_assertion_range
      CHECK (
        confidence_at_assertion >= 0.05 AND confidence_at_assertion <= 0.95
      );
  END IF;
END $$;

-- A2 — proposition_kind hot-path index.
CREATE INDEX IF NOT EXISTS models_proposition_kind_idx
  ON models (tenant_id, proposition_kind)
  WHERE status = 'active';

-- A4 — sidecar table for freeform notes.
CREATE TABLE IF NOT EXISTS model_status_notes (
  id UUID PRIMARY KEY,
  model_id UUID NOT NULL REFERENCES models(id),
  note TEXT NOT NULL,
  authored_by UUID REFERENCES actors(id),
  authored_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  kind TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS model_status_notes_model_idx
  ON model_status_notes (model_id, authored_at DESC);

COMMIT;
