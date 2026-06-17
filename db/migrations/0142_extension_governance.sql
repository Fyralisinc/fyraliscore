-- =====================================================================
-- 0142_extension_governance.sql — audit, kill-switch, provenance (E3.3-3.5, E2.8)
-- =====================================================================
-- The governance trio + provenance foundation for the extension plane:
--   1. extension_audit_log  — what each extension READ and WROTE, per tenant
--      (E3.4). Append-only; queried by ops + the tenant admin console.
--   2. extension_killswitch — global instant disable (E3.5). A row = the
--      extension is hard-off everywhere: no token issued, no read/write/stream,
--      regardless of grants. Per-tenant disable stays grant-revocation (0139).
--   3. model_provenance     — the set of source identities (incl. ext ids) that
--      materially drove each synthesized Model (E2.8 foundation). Lets a Model
--      driven by a third-party extension be surfaced as contestable.
--
-- Host-managed (no RLS); audit/provenance carry tenant_id for scoped queries.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS extension_audit_log (
  id            UUID PRIMARY KEY,
  extension_id  TEXT NOT NULL,
  tenant_id     UUID,
  action        TEXT NOT NULL,            -- read_observations|get_observation|stream_pull|edge_ingest|...
  detail        JSONB NOT NULL DEFAULT '{}'::jsonb,
  item_count    INT NOT NULL DEFAULT 0,
  at            TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS extension_audit_ext_idx ON extension_audit_log (extension_id, at DESC);
CREATE INDEX IF NOT EXISTS extension_audit_tenant_idx ON extension_audit_log (tenant_id, at DESC);

CREATE TABLE IF NOT EXISTS extension_killswitch (
  extension_id  TEXT PRIMARY KEY,
  reason        TEXT,
  disabled_by   TEXT NOT NULL,
  disabled_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS model_provenance (
  model_id        UUID NOT NULL,
  tenant_id       UUID,
  source_identity TEXT NOT NULL,           -- 'extension:<id>' | 'channel:<c>' | 'first_party'
  is_third_party  BOOLEAN NOT NULL DEFAULT FALSE,
  weight          DOUBLE PRECISION NOT NULL DEFAULT 1.0,
  recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (model_id, source_identity)
);
CREATE INDEX IF NOT EXISTS model_provenance_third_party_idx
  ON model_provenance (model_id) WHERE is_third_party;

COMMIT;
