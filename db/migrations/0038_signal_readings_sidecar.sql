-- =====================================================================
-- 0038_signal_readings_sidecar.sql — typed sidecar for signal_readings JSONB
-- =====================================================================
-- Phase 1c of the architectural overhaul. The `models.signal_readings`
-- column is JSONB-typed `[{kind, observed_at, source_event_id, ...}]`.
-- Storing it as JSONB has been correct under early evolution, but it
-- prevents any per-reading query (count by kind, filter by date,
-- traverse from event back to model) without doing JSONB unrolling.
--
-- This migration creates the typed sidecar table. NO producer code is
-- changed in this migration — `models.signal_readings` remains
-- authoritative. The cutover plan lives in CODEBASE-ARCHITECTURE.md
-- under "Signal readings cutover" and is split into:
--
--   Stage A (this migration): create the sidecar table + indexes.
--   Stage B (separate plan): dual-write — every producer that appends
--     to models.signal_readings also INSERTs into model_signal_readings.
--   Stage C (separate plan): backfill from JSONB into the sidecar.
--   Stage D (separate plan): cutover readers; once green for two
--     weeks, drop the JSONB column.
--
-- Why a sidecar (not a column on models)
-- --------------------------------------
--   * Append-only: most rows have 0-3 readings; a handful have hundreds.
--     A sidecar avoids row width bloat.
--   * Per-reading provenance: source_event_id is itself FK-able, which
--     enables "show me every Model whose contestation came from event X"
--     queries — impossible with the JSONB shape.
--   * RLS-enforceable: signal_readings was opaque to migration 0036's
--     RLS policy because RLS doesn't traverse JSONB. The sidecar gets
--     the same `tenant_isolation` policy as every other tenant-scoped
--     table.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS model_signal_readings (
  id UUID PRIMARY KEY,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  tenant_id UUID NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE,
  -- Reading kind. CHECK pins the closed set so a typo can't drift the
  -- vocabulary; new kinds require a migration.
  reading_kind TEXT NOT NULL CHECK (reading_kind IN (
    'confirm',     -- behavior consistent with the Model's prediction
    'contest',     -- contestation submitted by an actor
    'observe',     -- neutral observation that updates evidential weight
    'falsify'      -- falsifier triggered (predates the lifecycle archive)
  )),
  observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- Which event triggered this reading. Optional because some readings
  -- are derived (from cascade rules or maintenance jobs) without a
  -- single root event.
  source_event_id UUID,
  -- Free-form per-kind detail (actor_id for contest, prediction_window
  -- for observe, etc.). JSONB intentionally — payload shape is per-kind
  -- and not worth normalizing further.
  detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Most-common query: "all readings for this model, newest first."
CREATE INDEX IF NOT EXISTS model_signal_readings_model_idx
  ON model_signal_readings (model_id, observed_at DESC);

-- Per-tenant aggregation: "how many contest readings this week?"
CREATE INDEX IF NOT EXISTS model_signal_readings_tenant_kind_idx
  ON model_signal_readings (tenant_id, reading_kind, observed_at DESC);

-- Reverse traversal: "every model whose contestation came from this event."
CREATE INDEX IF NOT EXISTS model_signal_readings_source_idx
  ON model_signal_readings (source_event_id)
  WHERE source_event_id IS NOT NULL;

-- Apply the same RLS policy as the migration-0036 sweep.
ALTER TABLE model_signal_readings ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_signal_readings FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON model_signal_readings;
CREATE POLICY tenant_isolation ON model_signal_readings
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
