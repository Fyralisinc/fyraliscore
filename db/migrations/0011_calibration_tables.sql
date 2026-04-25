-- =====================================================================
-- 0011_calibration_tables.sql — Calibration stats + offsets
-- =====================================================================
-- Wave 4-C owns the Calibration updater (BUILD-PLAN §5 Prompt 4.C,
-- ARCHITECTURE-FINAL.md §9 lines 2596-2622). The schema below is
-- copied verbatim from §9 with two additions:
--
--   * `calibration_stats.id UUID PRIMARY KEY` — spec block has it
--     explicitly; we keep UUID v7 so rows are time-sortable.
--   * `calibration_stats_actor_kind_idx` — btree on
--     (tenant_id, actor_id, proposition_kind) is the only query shape
--     `compute.recompute_for_actor_kind` issues. Spec §9 line 2605
--     requests an implicit `INDEX (tenant_id, actor_id,
--     proposition_kind)` inside the CREATE TABLE; Postgres doesn't
--     accept index declarations inside CREATE TABLE, so we materialise
--     it as a separate CREATE INDEX.
--
-- `calibration_offsets` is keyed on
-- `(tenant_id, actor_id, proposition_kind, bucket_low)` per spec line
-- 2621. `bucket_low` is the 0.0-step lower bound of the confidence
-- bucket; `bucket_high` is the exclusive upper. The updater upserts
-- every weekly pass.
--
-- `offset` is a SQL keyword in several contexts but accepted as a
-- column name; we quote it defensively in every statement that
-- touches the column.
--
-- This migration completes the four-subcomponent Wave 4-C surface
-- (Calibration updater + Precipitation worker + Falsifier adequacy
-- re-export + Contestability).
--
-- Immutable once committed. Further changes go in 0012_*.sql.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS calibration_stats (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  actor_id UUID NOT NULL,
  proposition_kind TEXT NOT NULL,
  asserted_confidence FLOAT NOT NULL CHECK (
    asserted_confidence >= 0.0 AND asserted_confidence <= 1.0
  ),
  outcome BOOLEAN,                         -- NULL => inconclusive
  resolved_at TIMESTAMPTZ NOT NULL,
  source_model_id UUID NOT NULL
);

CREATE INDEX IF NOT EXISTS calibration_stats_actor_kind_idx
  ON calibration_stats (tenant_id, actor_id, proposition_kind);

CREATE INDEX IF NOT EXISTS calibration_stats_resolved_idx
  ON calibration_stats (resolved_at DESC);

-- -------------------------------------------------------------------
-- calibration_offsets — computed weekly, read on every Think insert.
-- -------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS calibration_offsets (
  tenant_id UUID NOT NULL,
  actor_id UUID NOT NULL,
  proposition_kind TEXT NOT NULL,
  bucket_low FLOAT NOT NULL CHECK (bucket_low >= 0.0 AND bucket_low <= 1.0),
  bucket_high FLOAT NOT NULL CHECK (bucket_high > 0.0 AND bucket_high <= 1.0),
  "offset" FLOAT NOT NULL CHECK ("offset" >= 0.3 AND "offset" <= 1.5),
  sample_size INTEGER NOT NULL CHECK (sample_size >= 0),
  last_updated TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, actor_id, proposition_kind, bucket_low),
  CONSTRAINT calibration_offsets_bucket_order CHECK (bucket_low < bucket_high)
);

-- Reverse lookup: "what does actor A's calibration look like for
-- proposition kind K right now?" — already covered by the PK prefix
-- (tenant_id, actor_id, proposition_kind); no extra index needed.

COMMIT;
