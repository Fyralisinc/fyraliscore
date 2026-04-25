-- =====================================================================
-- 0006_relationship_maintenance_log.sql — Retrieval background
-- relationship maintenance audit log.
-- =====================================================================
-- BUILD-PLAN §4 Prompt 3.A item 5: the retrieval module owns a nightly
-- background worker that walks the Model supporting_model_ids graph,
-- flags orphans, computes activation percentiles, and SUGGESTS
-- archivals to the Precipitation worker (Wave 4-C). Suggestions are
-- read-only with respect to Models; every decision is written to this
-- log table for auditability and downstream consumption.
--
-- Not in SCHEMA-LOCK.md S1-S6 (operational / runtime). Added per
-- explicit BUILD-PLAN §4 Prompt 3.A "NEW TABLE YOU MUST CREATE" text.
-- Migration number 0006 claimed after 0005 (entity_review_queue).
--
-- Idempotent; immutable once committed.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS relationship_maintenance_log (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  run_id UUID NOT NULL,
  -- groups all entries from one invocation of
  -- background_relationship_maintenance(tenant_id, conn)
  run_started_at TIMESTAMPTZ NOT NULL,
  entry_kind TEXT NOT NULL,
  -- 'orphan_flagged' | 'activation_outlier' |
  -- 'archival_suggested' | 'percentile_snapshot'
  subject_model_id UUID REFERENCES models(id),
  -- NULL for 'percentile_snapshot' (the whole-tenant summary row).
  -- Set for every per-Model row.
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Grouping: fetch every entry for a given run.
CREATE INDEX IF NOT EXISTS relationship_maintenance_log_run_idx
  ON relationship_maintenance_log (run_id);

-- Tenant-scoped history, most recent first.
CREATE INDEX IF NOT EXISTS relationship_maintenance_log_tenant_time_idx
  ON relationship_maintenance_log (tenant_id, created_at DESC);

-- Operator filter: "show me all orphan flags for this tenant".
CREATE INDEX IF NOT EXISTS relationship_maintenance_log_kind_idx
  ON relationship_maintenance_log (entry_kind);

COMMIT;
