-- =====================================================================
-- 0040_decision_deltas.sql — Decision Delta primitive (Phase 1)
-- =====================================================================
-- Introduces `decision_deltas` and `decision_delta_evidence` — a
-- first-class representation of the "Proposed Change" UI primitive.
--
-- A Decision Delta is a proposed, evidence-backed change to the
-- company model that the user reviews and accepts/delegates/contests.
-- Where `recommendations` (kind='recommendation' rows on the `models`
-- table) treat the proposed change as a sub-field of the proposition,
-- decision_deltas elevate the *state change itself* (before -> after,
-- falsification condition, consequence preview, evidence chain) to a
-- first-class object — the CEO reviews these directly on the Today
-- page right inspector.
--
-- Bridge: deltas may be promoted FROM a recommendation row via the
-- optional `source_recommendation_id` back-ref. The promotion path
-- lives in services/decision_deltas/promote.py; the recommendation
-- pipeline does not need to change.
--
-- Tenant isolation: follows the migration-0036 / 0037 conventions —
-- explicit tenant_id column with FK to tenants(id) and RLS enabled
-- with the standard `tenant_isolation` policy.
--
-- Idempotent (CREATE TABLE IF NOT EXISTS / CREATE INDEX IF NOT EXISTS).
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- decision_deltas — primary surface
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS decision_deltas (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id UUID NOT NULL,
  -- Review lifecycle. Closed set; see services/decision_deltas/repo.py
  -- for the transition rules.
  status TEXT NOT NULL CHECK (status IN (
    'proposed', 'accepted', 'delegated',
    'contested', 'superseded', 'dismissed'
  )),
  -- UI label / classification chip (see spec §2.1 — "Label" sub-object).
  label TEXT NOT NULL CHECK (label IN (
    'proposed_change', 'needs_review',
    'authority_required', 'recommended_update'
  )),
  -- Plain-language sentence describing what Fyralis found.
  main_assertion TEXT NOT NULL,
  -- {label, value, color_hint} — pre-acceptance state snapshot.
  current_state JSONB,
  -- {label, value, color_hint} — proposed post-acceptance state.
  suggested_update JSONB,
  -- Target node kind + id for cross-reference (open enum, validated at
  -- the application layer).
  target_node_kind TEXT,
  target_node_id UUID,
  -- [0, 1]. NUMERIC(4,3) gives 3 decimal places — same precision used
  -- by services/models for confidence elsewhere.
  confidence NUMERIC(4,3),
  confidence_basis TEXT,
  -- Falsification condition (required for high-confidence claims; the
  -- application layer enforces the conf > 0.7 -> NOT NULL invariant).
  falsification_condition TEXT,
  -- {creates:[...], updates:[...], archives:[...], notifies:[...],
  --  re_evaluates_in:'48h'}
  consequence_preview JSONB,
  -- {arr_at_risk, accounts, signals, stale_days, teams_affected, ...}
  impact JSONB,
  -- Free-form domain chip (customer_risk, capacity, delivery,
  -- strategy, decision, pricing, revenue). Open set.
  category TEXT,
  -- Back-ref to the recommendation row this was promoted from. NULL
  -- for deltas authored directly (e.g. via the bridge promotion path
  -- not being used, or via a seed). Not FK-enforced because the
  -- recommendation lives in `models` and we don't want a stray
  -- recommendation archive to cascade.
  source_recommendation_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  accepted_at TIMESTAMPTZ,
  accepted_by UUID,
  -- Optional effective date (when the suggested update should take
  -- effect; rendered in the Today row "date" column).
  resolution_target_at TIMESTAMPTZ
);

-- Hot path: tenant + status (Today queue filters by status='proposed').
CREATE INDEX IF NOT EXISTS idx_dd_tenant_status
  ON decision_deltas (tenant_id, status);

-- Sort path: most recent first.
CREATE INDEX IF NOT EXISTS idx_dd_tenant_created
  ON decision_deltas (tenant_id, created_at DESC);

-- ---------------------------------------------------------------------
-- decision_delta_evidence — top 3-5 evidence items per delta
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS decision_delta_evidence (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  delta_id UUID NOT NULL REFERENCES decision_deltas(id) ON DELETE CASCADE,
  -- Source: crm, support, email, slack, linear, github, calendar,
  -- finance, product_usage, fyralis_reasoning, ... (open set).
  source TEXT NOT NULL,
  title TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  -- Trust tier mirrors the observation-side taxonomy
  -- (lib/shared/trust.py): authoritative, attested, reputable,
  -- inferential, unvetted.
  trust_tier TEXT,
  excerpt TEXT,
  weight NUMERIC(4,3),
  ordinal INT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_dde_delta_ordinal
  ON decision_delta_evidence (delta_id, ordinal);

-- ---------------------------------------------------------------------
-- Tenant FK + RLS — same pattern as 0036/0037.
-- ---------------------------------------------------------------------
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'tenants'
  ) AND NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'decision_deltas_tenant_fk'
  ) THEN
    ALTER TABLE decision_deltas
      ADD CONSTRAINT decision_deltas_tenant_fk
      FOREIGN KEY (tenant_id) REFERENCES tenants(id)
      DEFERRABLE INITIALLY IMMEDIATE;
  END IF;
END $$;

ALTER TABLE decision_deltas ENABLE ROW LEVEL SECURITY;
ALTER TABLE decision_deltas FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON decision_deltas;
CREATE POLICY tenant_isolation ON decision_deltas
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

-- Evidence inherits tenancy through delta_id; we still enable RLS so a
-- stray cross-tenant read with `app.current_tenant` set can't leak.
-- The policy joins via delta_id back to decision_deltas.
ALTER TABLE decision_delta_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE decision_delta_evidence FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON decision_delta_evidence;
CREATE POLICY tenant_isolation ON decision_delta_evidence
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM decision_deltas d
      WHERE d.id = decision_delta_evidence.delta_id
        AND d.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR EXISTS (
      SELECT 1 FROM decision_deltas d
      WHERE d.id = decision_delta_evidence.delta_id
        AND d.tenant_id = current_setting('app.current_tenant', true)::uuid
    )
  );

-- ---------------------------------------------------------------------
-- updated_at trigger — keep updated_at fresh on every UPDATE.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION decision_deltas_touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS decision_deltas_updated_at_trg ON decision_deltas;
CREATE TRIGGER decision_deltas_updated_at_trg
  BEFORE UPDATE ON decision_deltas
  FOR EACH ROW
  EXECUTE FUNCTION decision_deltas_touch_updated_at();

COMMIT;
