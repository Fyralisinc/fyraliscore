-- =====================================================================
-- 0087_sage_discovery_and_negative_memory.sql
--
-- Phase 10 of the SAGE-inspired self-evolution architecture
-- (fyralis-sage-synthesis-self-evolution.md §§11, 14, Phase 10).
--
-- Introduces two tables that live in the **Discovery Utility Layer**
-- (doc §2), NOT the Canonical Truth Layer:
--
--   * `discovery_shortcuts` — learned retrieval shortcuts. A row says
--     "when this kind of signature appears, this model/region/affordance
--     has historically been useful to inspect." It is NOT a causal
--     edge, NOT a truth claim, NOT evidence. Utility is recorded as a
--     mutable score that decays / strengthens with outcomes.
--
--   * `negative_memory` — rejected hypotheses, noisy paths, failed
--     shortcuts, low-value nodes. Every row carries an `expires_at` so
--     that a learned "this is noise" never becomes a permanent dogma —
--     company reality changes (doc §14, "Requires expiry because
--     company reality changes.").
--
-- Both tables carry a table-level COMMENT making the "not canonical"
-- distinction unmissable to anyone running `\d+` in psql. The column
-- names (`utility_score`, `success_count`, `failure_count`,
-- `rejected_*`, `evidence_snapshot_hash`) are chosen to keep this
-- distinction visible at the call site too.
--
-- Conventions match 0046_inquiry_execution.sql (BEGIN/COMMIT, CREATE
-- TABLE IF NOT EXISTS, explicit CHECK constraints) and 0041_predictions
-- / 0036_rls_permissive_default (per-table RLS policy named
-- `tenant_isolation`, with a permissive branch for code paths that
-- haven't yet bound `app.current_tenant`).
--
-- Note on `to_region_id`: regions are not yet a first-class table in
-- the schema (see doc §6 / §12 — region_sufficient_state arrives in
-- Phase 11). The column is intentionally FK-less so this migration can
-- land before regions exist. When regions become a real table, a
-- follow-up migration can add the FK.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- discovery_shortcuts (doc §11.1)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS discovery_shortcuts (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  -- Shape: {signal_type, entities[], question_primitive}. JSONB so
  -- callers can match by containment (@>) on partial signatures.
  from_signature JSONB NOT NULL,
  -- Targets: at least one must be non-null. A shortcut may point at a
  -- specific Model, a logical region, or a retrieval affordance.
  to_model_id UUID REFERENCES models(id) ON DELETE CASCADE,
  to_region_id UUID,
  to_affordance TEXT,
  -- Learned utility, not truth. Bounded below at 0; the
  -- success/failure update rule (services/reasoning/sage/discovery/shortcuts_repo.py)
  -- clamps and decays in application code so the math stays auditable.
  utility_score FLOAT NOT NULL DEFAULT 0 CHECK (utility_score >= 0),
  success_count INT NOT NULL DEFAULT 0,
  failure_count INT NOT NULL DEFAULT 0,
  last_success_at TIMESTAMPTZ,
  last_failure_at TIMESTAMPTZ,
  -- Optional TTL. Sweeps run via DiscoveryShortcutsRepo.sweep_expired().
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT discovery_shortcuts_has_target CHECK (
    to_model_id IS NOT NULL
    OR to_region_id IS NOT NULL
    OR to_affordance IS NOT NULL
  )
);

COMMENT ON TABLE discovery_shortcuts IS
  'Learned retrieval utility — not canonical truth.';

-- GIN over from_signature for @> containment lookups (the hot path:
-- "find shortcuts whose signature is a subset of the current inquiry
-- signature").
CREATE INDEX IF NOT EXISTS discovery_shortcuts_signature_gin
  ON discovery_shortcuts USING GIN (from_signature);

-- Top-utility ranking inside a tenant (utility DESC).
CREATE INDEX IF NOT EXISTS discovery_shortcuts_tenant_utility_idx
  ON discovery_shortcuts (tenant_id, utility_score DESC);

-- Expiry sweep (find all rows whose expires_at <= now()).
CREATE INDEX IF NOT EXISTS discovery_shortcuts_tenant_expires_idx
  ON discovery_shortcuts (tenant_id, expires_at);


-- ---------------------------------------------------------------------
-- negative_memory (doc §14.1)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS negative_memory (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  memory_type TEXT NOT NULL CHECK (memory_type IN (
    'rejected_hypothesis',
    'noisy_path',
    'failed_shortcut',
    'low_value_node'
  )),
  -- Same shape family as discovery_shortcuts.from_signature so a
  -- single inquiry signature can probe both tables symmetrically.
  signature JSONB NOT NULL,
  rejected_claim TEXT,
  rejected_path JSONB,
  reason TEXT NOT NULL,
  -- Hash of the evidence that justified rejecting this. When the
  -- evidence snapshot changes, the negative memory should be
  -- invalidated (NegativeMemoryRepo.invalidate_by_evidence_change).
  evidence_snapshot_hash TEXT,
  confidence FLOAT CHECK (
    confidence IS NULL OR (confidence >= 0 AND confidence <= 1)
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- NOT NULL: doc §14 mandates every negative memory expires.
  -- "Requires expiry because company reality changes."
  expires_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE negative_memory IS
  'Learned retrieval utility — not canonical truth.';

-- GIN over signature for @> containment lookups.
CREATE INDEX IF NOT EXISTS negative_memory_signature_gin
  ON negative_memory USING GIN (signature);

-- Combined (tenant, memory_type, expires_at) supports both the
-- "find non-expired negatives of kind X for this tenant" probe and
-- the expiry sweep.
CREATE INDEX IF NOT EXISTS negative_memory_tenant_type_expires_idx
  ON negative_memory (tenant_id, memory_type, expires_at);


-- ---------------------------------------------------------------------
-- RLS — mirror 0036 / 0041. Both tables are tenant-scoped via their
-- own tenant_id column.
-- ---------------------------------------------------------------------

ALTER TABLE discovery_shortcuts ENABLE ROW LEVEL SECURITY;
ALTER TABLE discovery_shortcuts FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON discovery_shortcuts;
CREATE POLICY tenant_isolation ON discovery_shortcuts
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE negative_memory ENABLE ROW LEVEL SECURITY;
ALTER TABLE negative_memory FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON negative_memory;
CREATE POLICY tenant_isolation ON negative_memory
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
