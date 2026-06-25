-- =====================================================================
-- 0162_ingestion_failure_operator_workflow.sql
--   Ingestion DLQ replay/quarantine operator workflow.
-- =====================================================================
-- `ingestion_failures` is the durable operator surface for the Kafka
-- ingestion DLQ. The table already supports marking a row as resolved after a
-- replay, but it lacked a way to quarantine rows that should stay out of the
-- active triage queue without deleting evidence.
-- =====================================================================

BEGIN;

ALTER TABLE ingestion_failures
  ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS quarantined_by UUID REFERENCES actors(id),
  ADD COLUMN IF NOT EXISTS quarantine_reason TEXT;

CREATE INDEX IF NOT EXISTS ingestion_failures_open_operator_idx
  ON ingestion_failures (tenant_id, source, last_seen_at DESC)
  WHERE resolved_at IS NULL
    AND quarantined_at IS NULL;

CREATE INDEX IF NOT EXISTS ingestion_failures_quarantine_idx
  ON ingestion_failures (tenant_id, quarantined_at DESC)
  WHERE quarantined_at IS NOT NULL;

COMMIT;
