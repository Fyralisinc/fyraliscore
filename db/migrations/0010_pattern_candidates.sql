-- =====================================================================
-- 0010_pattern_candidates.sql — Precipitation-worker pattern candidates
-- =====================================================================
-- Wave 4-C owns the Precipitation worker (BUILD-PLAN §5 Prompt 4.C,
-- ARCHITECTURE-FINAL.md §19 and §5 "Precipitation worker (nightly)").
-- The worker clusters active hypothesis/concern Models over their
-- embeddings using HDBSCAN. Each dense cluster of ≥3 related Models is
-- materialised here as a `pattern_candidate` row. Think T4 consumes
-- the row via `trigger_kind='T4'`, `trigger_subkind='pattern_review'`
-- and on accept inserts a real Pattern Model then flips `promoted_at`
-- + `promoted_pattern_model_id`. On reject it flips `rejected_at` +
-- `rejection_reason`.
--
-- This migration CLOSES the last open item in SCHEMA-QUESTION.md Q4
-- (think_trigger_queue resolved by 0004; region_locks resolved by
-- 0007; signal_memory_fabric resolved by 0009; pattern_candidates
-- resolved here). See BUILD-LOG entry for Wave 4-C Deviations.
--
-- Design rationale (documented inline so future readers don't have to
-- dig through the SCHEMA-QUESTION history):
--
--   * `proposed_signature JSONB` — the structural shape of the pattern
--     (e.g., {"domain":"distributed_systems","shape":"underestimate"}).
--     Think T4 uses this verbatim as the inserted Model's
--     `proposition.signature`. JSONB so future signatures can carry
--     embedded criteria without another migration.
--
--   * `observed_tendency JSONB` — the empirical summary across the
--     cluster ("6 out of 7 Alice-distsys commitments came in at 2x
--     estimate"). Ships straight into the Pattern's
--     `proposition.observed_tendency`.
--
--   * `constituent_model_ids UUID[]` — the Models that clustered
--     together. After promotion, Think marks each with
--     `supporting_model_ids = supporting_model_ids || <pattern_id>`
--     (the Pattern is supported by every instance). The array is the
--     authoritative link — no separate edge table is needed.
--
--   * `cluster_size INTEGER` + `density FLOAT` — HDBSCAN output we
--     keep for observability. Precipitation's threshold (size ≥ 3,
--     density ≥ 0.5) is enforced at write time; storing both lets
--     operators tune without re-running clustering.
--
--   * Lifecycle: `proposed_at` (always set) → one of {`promoted_at`,
--     `rejected_at`} (mutually exclusive). The partial indexes
--     below keep the "pending review" and "promoted" queries O(log N).
--
--   * `promoted_pattern_model_id UUID REFERENCES models(id)` — the
--     Pattern Model inserted by Think T4. Nullable until promotion.
--     We deliberately DO NOT cascade on delete — if a Model is
--     archived, the candidate row keeps the pointer (the archived
--     Model still exists in the `models` table).
--
-- Immutable once committed. Further changes go in 0012_*.sql.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS pattern_candidates (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  proposed_signature JSONB NOT NULL,
  observed_tendency JSONB NOT NULL,
  constituent_model_ids UUID[] NOT NULL,
  cluster_size INTEGER NOT NULL CHECK (cluster_size >= 3),
  density FLOAT NOT NULL CHECK (density >= 0.0 AND density <= 1.0),
  proposed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  promoted_at TIMESTAMPTZ,
  promoted_pattern_model_id UUID REFERENCES models(id),
  rejected_at TIMESTAMPTZ,
  rejection_reason TEXT,
  CONSTRAINT pattern_candidates_lifecycle_exclusive CHECK (
    promoted_at IS NULL OR rejected_at IS NULL
  ),
  CONSTRAINT pattern_candidates_promotion_consistent CHECK (
    (promoted_at IS NULL AND promoted_pattern_model_id IS NULL) OR
    (promoted_at IS NOT NULL AND promoted_pattern_model_id IS NOT NULL)
  ),
  CONSTRAINT pattern_candidates_rejection_consistent CHECK (
    (rejected_at IS NULL AND rejection_reason IS NULL) OR
    (rejected_at IS NOT NULL AND rejection_reason IS NOT NULL)
  )
);

-- Hot path: "which candidates still need review?"
CREATE INDEX IF NOT EXISTS pattern_candidates_pending_idx
  ON pattern_candidates (tenant_id, proposed_at)
  WHERE promoted_at IS NULL AND rejected_at IS NULL;

-- Reverse lookup: given a Pattern Model, which candidate produced it?
CREATE INDEX IF NOT EXISTS pattern_candidates_promoted_idx
  ON pattern_candidates (promoted_pattern_model_id)
  WHERE promoted_at IS NOT NULL;

COMMIT;
