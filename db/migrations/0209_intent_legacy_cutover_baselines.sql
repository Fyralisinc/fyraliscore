-- 0209_intent_legacy_cutover_baselines.sql
--
-- Honest compatibility baseline for Acts created before the governed intent
-- protocol. This does not invent historical authority; it marks it unknown and
-- review-required, while allowing new exact commands to version forward.

BEGIN;

CREATE TABLE IF NOT EXISTS intent_legacy_baselines (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  object_kind TEXT NOT NULL CHECK (object_kind IN ('goal', 'decision', 'commitment')),
  aggregate_id UUID NOT NULL,
  baseline_version INTEGER NOT NULL DEFAULT 1 CHECK (baseline_version = 1),
  baseline_payload_digest TEXT NOT NULL CHECK (
    baseline_payload_digest ~ '^[0-9a-f]{64}$'
  ),
  source_table TEXT NOT NULL CHECK (source_table IN ('goals', 'decisions', 'commitments')),
  source_snapshot JSONB NOT NULL CHECK (jsonb_typeof(source_snapshot) = 'object'),
  authority_provenance_status TEXT NOT NULL DEFAULT 'legacy_unknown_review_required'
    CHECK (authority_provenance_status = 'legacy_unknown_review_required'),
  captured_by TEXT NOT NULL DEFAULT 'IntentApplierCompatibilityAdapter',
  captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, object_kind, aggregate_id)
);

ALTER TABLE intent_legacy_baselines ENABLE ROW LEVEL SECURITY;
ALTER TABLE intent_legacy_baselines FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON intent_legacy_baselines;
CREATE POLICY tenant_isolation ON intent_legacy_baselines
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

COMMENT ON TABLE intent_legacy_baselines IS
  'Pre-protocol Act snapshot; authority provenance remains unknown and review-required.';

COMMIT;
