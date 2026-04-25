-- =====================================================================
-- 0021_review1_remediation.sql — ARCHITECTURE-REVIEW-1 remediation.
-- =====================================================================
-- Covers the schema-side follow-ups from AUDIT-REVIEW-1-FIXES.md:
--
--   I1 — `commitments.is_maintenance` column (C10 maintenance flag).
--        Previously encoded in `estimated_capacity->>'maintenance'`;
--        promoted to a first-class column so invariant checks can read
--        a typed boolean and queries can use an index.
--
--   N2 — `anomaly_thresholds` table. Per-tenant rolling P90/P95/P99
--        thresholds for the six anomaly detectors. Populated by the
--        monthly `recalibrate_anomaly_thresholds` job. When a tenant
--        has fewer than 100 observations of a kind, `compute_significance`
--        falls back to the hardcoded cross-tenant defaults.
--
--   C3 — `dedup_keys_seen` table. Publisher-side debounce ledger.
--        Separate from `applied_triggers` so "already published this
--        5-min bucket" does NOT conflict with "already applied this
--        exact trigger". Retention job drops rows > 24h.
--
-- All three changes are additive. No existing rows are migrated; the
-- invariant check keeps honoring the legacy JSONB flag for commitments
-- created before this migration (see services/acts/invariants.py).
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- I1: Commitments.is_maintenance
-- ---------------------------------------------------------------------

ALTER TABLE commitments
  ADD COLUMN IF NOT EXISTS is_maintenance BOOLEAN NOT NULL DEFAULT FALSE;

-- Fast lookup for the orphan-detection nightly job: find commitments
-- that are active AND not maintenance AND have no contributes_to edge.
CREATE INDEX IF NOT EXISTS commitments_active_maintenance_idx
  ON commitments (tenant_id, state, is_maintenance)
  WHERE state IN ('active', 'blocked', 'paused', 'doneunverified');


-- ---------------------------------------------------------------------
-- N2: Per-tenant anomaly thresholds
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS anomaly_thresholds (
  tenant_id     UUID NOT NULL,
  kind          TEXT NOT NULL,       -- 'resource_deployment', 'model_confidence_drop', …
  variable      TEXT NOT NULL,       -- 'utilization', 'drop_magnitude', …
  p90           FLOAT NOT NULL,
  p95           FLOAT NOT NULL,
  p99           FLOAT NOT NULL,
  buffer        FLOAT NOT NULL DEFAULT 0.0,   -- headroom (2σ) added on top of p90
  sample_size   INTEGER NOT NULL CHECK (sample_size >= 0),
  last_updated  TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, kind, variable),
  CONSTRAINT anomaly_thresholds_order CHECK (p90 <= p95 AND p95 <= p99)
);


-- ---------------------------------------------------------------------
-- C3: Publisher-side debounce ledger
-- ---------------------------------------------------------------------
--
-- dedup_keys_seen is distinct from applied_triggers (0008). applied_triggers
-- is keyed by trigger_id (UUID v4 per publication); dedup_keys_seen is
-- keyed by a stable hash of (tenant, trigger_kind, subkind, region_hash,
-- 5-min bucket) so the anomaly processor (or any future rate-limited
-- publisher) can decide whether a new trigger should be suppressed.
--
-- Never join these two tables. Idempotency and debounce are different
-- concerns; conflating them silently drops legitimate updates (the bug
-- ARCHITECTURE-REVIEW-1 §C3 describes).

CREATE TABLE IF NOT EXISTS dedup_keys_seen (
  dedup_key      TEXT PRIMARY KEY,
  tenant_id      UUID NOT NULL,
  trigger_kind   TEXT NOT NULL,
  subkind        TEXT,
  region_hash    TEXT,
  first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  hit_count      INTEGER NOT NULL DEFAULT 1 CHECK (hit_count >= 1)
);

-- Retention job (runs hourly) scans this index.
CREATE INDEX IF NOT EXISTS dedup_keys_seen_first_seen_idx
  ON dedup_keys_seen (first_seen_at);

CREATE INDEX IF NOT EXISTS dedup_keys_seen_tenant_idx
  ON dedup_keys_seen (tenant_id);

COMMIT;
