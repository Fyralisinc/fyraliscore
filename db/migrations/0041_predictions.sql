-- =====================================================================
-- 0041_predictions.sql — Forecasts surface backing store
-- =====================================================================
-- Phase 4 of the Forecasts work. Introduces two tables that back the
-- Forecasts page (Active / Resolved / Accuracy tabs) plus the related
-- summary strip, risk-exposure timeseries, and prediction inspector.
--
-- The schema is intentionally thin: a single `predictions` table for
-- the row + a child `prediction_signals` table for the supporting
-- evidence list. We do NOT reuse `models.proposition_kind='prediction'`
-- here — predictions on the Forecasts page are author-facing artifacts
-- (CEO scenarios, surfaced forecasts) with their own lifecycle
-- (active -> resolved with outcome), distinct from the internal Model
-- substrate. Cross-references back into `models` / `commitments`
-- / etc. happen via the loose `target_node_kind` + `target_node_id`
-- columns; we do NOT FK them so that an archived target doesn't cascade
-- delete a historical prediction (the Accuracy tab needs the row to
-- compute calibration).
--
-- Idempotent (CREATE TABLE / INDEX IF NOT EXISTS). RLS + tenant FK
-- follow the pattern set in migrations 0036 / 0037 / 0039.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS predictions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE,
  status TEXT NOT NULL CHECK (status IN ('active', 'resolved', 'superseded')),
  statement TEXT NOT NULL,
  rationale TEXT,
  category TEXT NOT NULL CHECK (category IN (
    'customer_risk', 'capacity', 'delivery', 'strategy',
    'decision', 'pricing', 'partner'
  )),
  -- Loose reference into the wider substrate. Not FK'd: targets may
  -- be archived (commitments closed, models superseded) without the
  -- prediction's accuracy record vanishing.
  target_node_kind TEXT,
  target_node_id UUID,
  target_label TEXT,
  confidence NUMERIC(4, 3) NOT NULL
    CHECK (confidence >= 0 AND confidence <= 1),
  confidence_basis TEXT,
  falsification_condition TEXT,
  -- [{label, delta_label, direction}] — surfaced in the inspector's
  -- "key drivers" block.
  key_drivers JSONB,
  -- Free-form numeric impact bag: {arr_at_risk, customer_count, ...}.
  -- Sum-able fields drive the summary strip + risk-exposure series.
  impact JSONB,
  resolution_at TIMESTAMPTZ NOT NULL,
  resolved_at TIMESTAMPTZ,
  outcome TEXT CHECK (outcome IN ('true', 'false', 'partial') OR outcome IS NULL),
  resolution_timeliness TEXT
    CHECK (resolution_timeliness IN ('early', 'on_time', 'late') OR resolution_timeliness IS NULL),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Hot path: list-by-status for the Active / Resolved tabs.
CREATE INDEX IF NOT EXISTS idx_pred_tenant_status
  ON predictions (tenant_id, status);

-- Hot path: "earliest_resolution" sort + upcoming-resolutions card.
CREATE INDEX IF NOT EXISTS idx_pred_tenant_resolution
  ON predictions (tenant_id, resolution_at);


CREATE TABLE IF NOT EXISTS prediction_signals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  prediction_id UUID NOT NULL
    REFERENCES predictions(id) ON DELETE CASCADE,
  source TEXT NOT NULL,
  title TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  trust_tier TEXT,
  weight NUMERIC(4, 3),
  ordinal INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_pred_signals_pred
  ON prediction_signals (prediction_id, ordinal);


-- ---------------------------------------------------------------------
-- RLS — mirror the 0036 / 0039 pattern. `predictions` is tenant-scoped
-- (has its own tenant_id); `prediction_signals` inherits via parent.
-- ---------------------------------------------------------------------

ALTER TABLE predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE predictions FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON predictions;
CREATE POLICY tenant_isolation ON predictions
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

-- prediction_signals inherits tenant scope via parent FK; no RLS here
-- (junction-style table per the 0036 convention).

COMMIT;
