-- =====================================================================
-- 0009_signal_memory_fabric.sql — Anomaly sub-threshold accumulator
-- =====================================================================
-- Wave 4-B owns the Anomaly processor (BUILD-PLAN §5 Prompt 4.B,
-- ARCHITECTURE-FINAL.md §18 lines 3690-3706). Sub-threshold anomaly
-- candidates (significance < SIGNIFICANCE_THRESHOLD) are recorded here
-- instead of being dropped. A periodic sweep promotes a region to a
-- full anomaly when > 5 rows land in a 7-day window for the same
-- (tenant, region_hash).
--
-- Partially resolves SCHEMA-QUESTION.md Q4 (the `signal_memory_fabric`
-- sub-item; `pattern_candidates` remains open).
--
-- Schema matches ARCHITECTURE-FINAL.md §18 line 3693 verbatim plus:
--   - `fabric_unpromoted` — partial index on the "not yet promoted"
--     set, which is the hot read path for the weekly promote sweep.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS signal_memory_fabric (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  region_hash TEXT NOT NULL,
  signal_ref JSONB NOT NULL,
  significance FLOAT NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  promoted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS fabric_region
  ON signal_memory_fabric (tenant_id, region_hash, recorded_at);

CREATE INDEX IF NOT EXISTS fabric_unpromoted
  ON signal_memory_fabric (tenant_id)
  WHERE promoted_at IS NULL;

COMMIT;
