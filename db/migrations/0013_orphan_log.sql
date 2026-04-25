-- =====================================================================
-- 0013_orphan_log.sql — Wave 4-D Maintenance orphan detection log
-- =====================================================================
-- Spec: ARCHITECTURE-FINAL.md §8 "Background maintenance workers" and
-- BUILD-PLAN §5 Prompt 4.D (maintenance deliverables).
--
-- Wave 4-D's daily `orphan_detection()` job writes rows here when an
-- Observation has no downstream consumer: no Model's
-- `supporting_event_ids` or `born_from_event_id` references it, and no
-- Act's `created_by_event_id` / `last_updated_by_event_id` references
-- it, after a minimum grace period (default: 14 days).
--
-- Critical: this is INVESTIGATION data only. Orphan cleanup (actual
-- deletion) is Phase 5 work. The Wave 4 deliverable MUST NOT delete
-- observations.
--
-- `reason` is a free-text code describing why the row was flagged
-- ('no_downstream_models', 'no_downstream_acts', 'both'). Stored as
-- TEXT rather than an enum so the Phase-5 audit can refine the taxonomy
-- without a migration.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS orphan_log (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  observation_id UUID NOT NULL,
  detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reason TEXT NOT NULL
);

-- Tenant + detected_at DESC so the "recent orphans per tenant" UI view
-- can seek straight to the last N rows.
CREATE INDEX IF NOT EXISTS orphan_log_tenant_idx
  ON orphan_log (tenant_id, detected_at DESC);

-- Secondary: quickly list every detection of a specific observation
-- (dedup within a run is application-level; a given obs can be flagged
-- multiple times across runs before a Phase-5 cleanup kicks in).
CREATE INDEX IF NOT EXISTS orphan_log_obs_idx
  ON orphan_log (observation_id);

COMMIT;
