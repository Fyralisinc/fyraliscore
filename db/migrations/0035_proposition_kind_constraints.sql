-- =====================================================================
-- 0035_proposition_kind_constraints.sql — tighten proposition_kind
-- =====================================================================
-- The column was added in 0002 as GENERATED ALWAYS AS (proposition->>'kind')
-- STORED with no CHECK and no NOT NULL declaration. In practice every
-- row has a non-NULL value because Pydantic validates the proposition
-- shape upstream, but the DB had no defense against drift.
--
-- This migration adds:
--   1. A CHECK constraint pinning proposition_kind to the 11 known
--      values from PropositionKind in lib/shared/types.py. IN (...)
--      implicitly rejects NULL, so this enforces non-null + value-set
--      membership in one constraint.
--
-- We can't add NOT NULL via ALTER COLUMN on a GENERATED column, so the
-- CHECK is the canonical mechanism.
--
-- The existing partial index models_proposition_kind_idx (tenant_id,
-- proposition_kind) WHERE status='active' is left in place — it
-- already optimizes the common "active models of kind K for tenant T"
-- query.
-- =====================================================================

BEGIN;

-- Drop any prior version of the constraint so re-runs after editing the
-- value list pick up the new spec. (Idempotent constraint definitions
-- can't be edited in place; drop + re-add is the canonical pattern.)
ALTER TABLE models
  DROP CONSTRAINT IF EXISTS models_proposition_kind_valid;

ALTER TABLE models
  ADD CONSTRAINT models_proposition_kind_valid
  CHECK (
    proposition_kind IS NOT NULL
    AND proposition_kind IN (
      'state',
      'relation',
      'prediction',
      'pattern',
      'pattern_instance',
      'capability_assessment',
      'hypothesis',
      'concern',
      'market_assessment',
      'environmental_trend',
      'recommendation'
    )
  );

COMMIT;
