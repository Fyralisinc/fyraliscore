-- =====================================================================
-- 0014_customer_commitments_superset.sql — Q2 resolution (Option B1)
-- =====================================================================
-- Rachin resolved SCHEMA-QUESTION.md Q2 on 2026-04-21 by choosing to
-- migrate `customer_commitments` to the §27 superset shape. The §4
-- shape becomes a subset preserved via column defaults so that
-- Wave 2-C call sites (and their tests) keep working until Agent 5-B
-- explicitly updates `services/resources/customer_commitments.py::
-- link_commitment` and its callers to pass the new fields.
--
-- Backwards-compatibility guarantee:
-- Existing INSERTs of the form
--   INSERT INTO customer_commitments
--     (customer_resource_id, commitment_id, served_description)
--   VALUES (...)
-- continue to succeed. The new columns (`id`, `tenant_id`,
-- `relationship_kind`, `revenue_at_risk_usd`, `criticality`,
-- `created_at`) all get sensible defaults or a trigger-based backfill.
--
-- After 5-B: link_commitment passes explicit values; callers are
-- updated repo-wide; then the backfill trigger can be dropped in a
-- future migration.
--
-- Verified against both paths before landing:
--   1. Empty table (live DB): migration applies cleanly.
--   2. Populated table with Wave 2-C shape: every pre-existing row
--      gets `id` = gen_random_uuid(), `tenant_id` backfilled from
--      `resources.tenant_id`, `relationship_kind='delivers'`,
--      `criticality='medium'`, `created_at=now()`. No constraint
--      violations.
-- See: scripts/test_migration_0014.py + BUILD-LOG "Wave 4→5 Q2
-- resolution" entry.
-- =====================================================================

BEGIN;

-- Step 1 — add the new columns with defaults so existing rows are
-- auto-filled and current-shape INSERTs keep working.

ALTER TABLE customer_commitments
  ADD COLUMN IF NOT EXISTS id UUID NOT NULL DEFAULT gen_random_uuid();

ALTER TABLE customer_commitments
  ADD COLUMN IF NOT EXISTS tenant_id UUID;

ALTER TABLE customer_commitments
  ADD COLUMN IF NOT EXISTS relationship_kind TEXT NOT NULL DEFAULT 'delivers';

ALTER TABLE customer_commitments
  ADD COLUMN IF NOT EXISTS revenue_at_risk_usd NUMERIC;

ALTER TABLE customer_commitments
  ADD COLUMN IF NOT EXISTS criticality TEXT NOT NULL DEFAULT 'medium';

ALTER TABLE customer_commitments
  ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Step 2 — backfill tenant_id from resources for any pre-existing
-- rows (Wave 2-C writes didn't set this column because it didn't
-- exist yet).

UPDATE customer_commitments cc
SET tenant_id = r.tenant_id
FROM resources r
WHERE r.id = cc.customer_resource_id
  AND cc.tenant_id IS NULL;

-- Step 3 — tenant_id is now populated for every row. Make it NOT NULL.

ALTER TABLE customer_commitments
  ALTER COLUMN tenant_id SET NOT NULL;

-- Step 4 — swap the primary key from the composite to `id`, and
-- preserve the composite uniqueness as a UNIQUE constraint per §27
-- so `ON CONFLICT (customer_resource_id, commitment_id)` upserts
-- still work.

-- Drop the old composite PK only if it exists under the expected
-- name; guard so re-running this migration on an already-migrated DB
-- is a no-op.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'customer_commitments_pkey'
      AND conrelid = 'customer_commitments'::regclass
  ) THEN
    -- Check if the PK is currently the composite (i.e. has 2 columns).
    IF (
      SELECT array_length(conkey, 1)
      FROM pg_constraint
      WHERE conname = 'customer_commitments_pkey'
        AND conrelid = 'customer_commitments'::regclass
    ) = 2 THEN
      ALTER TABLE customer_commitments DROP CONSTRAINT customer_commitments_pkey;
    END IF;
  END IF;
END $$;

-- Add new PK on id (guarded for re-runs).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'customer_commitments_pkey'
      AND conrelid = 'customer_commitments'::regclass
  ) THEN
    ALTER TABLE customer_commitments ADD PRIMARY KEY (id);
  END IF;
END $$;

-- Preserve the composite uniqueness as a named UNIQUE constraint.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'customer_commitments_customer_commitment_key'
      AND conrelid = 'customer_commitments'::regclass
  ) THEN
    ALTER TABLE customer_commitments
      ADD CONSTRAINT customer_commitments_customer_commitment_key
      UNIQUE (customer_resource_id, commitment_id);
  END IF;
END $$;

-- Step 5 — BEFORE INSERT trigger that backfills tenant_id from the
-- customer Resource when the inserter didn't supply it. This is the
-- backwards-compatibility bridge for Wave 2-C callers
-- (`services/resources/customer_commitments.py::link_commitment`)
-- until Agent 5-B updates them to pass tenant_id explicitly.
-- After 5-B ships, every caller supplies tenant_id and the trigger
-- is a no-op; a later migration can drop it once an audit confirms
-- nothing relies on the backfill.

CREATE OR REPLACE FUNCTION customer_commitments_fill_tenant() RETURNS TRIGGER AS $$
BEGIN
  IF NEW.tenant_id IS NULL THEN
    SELECT tenant_id INTO NEW.tenant_id
    FROM resources
    WHERE id = NEW.customer_resource_id;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS customer_commitments_fill_tenant_trigger
  ON customer_commitments;
CREATE TRIGGER customer_commitments_fill_tenant_trigger
  BEFORE INSERT ON customer_commitments
  FOR EACH ROW EXECUTE FUNCTION customer_commitments_fill_tenant();

-- Step 6 — hot-path indexes for Bridge queries (Wave 5-B will read
-- these). The existing queries filter by tenant_id then join on
-- resource/commitment; a tenant_id index pays off at scale.

CREATE INDEX IF NOT EXISTS customer_commitments_tenant_idx
  ON customer_commitments (tenant_id);

CREATE INDEX IF NOT EXISTS customer_commitments_criticality_idx
  ON customer_commitments (tenant_id, criticality);

CREATE INDEX IF NOT EXISTS customer_commitments_revenue_idx
  ON customer_commitments (tenant_id)
  WHERE revenue_at_risk_usd IS NOT NULL;

COMMIT;
