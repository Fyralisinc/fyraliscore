-- 0126_observations_tenant_scoped_dedup.sql
-- Ensure external source deduplication cannot cross tenant boundaries.

BEGIN;

ALTER TABLE observations
  DROP CONSTRAINT IF EXISTS observations_source_channel_external_id_occurred_at_key;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'observations'::regclass
      AND conname = 'observations_tenant_source_external_occurred_key'
  ) THEN
    ALTER TABLE observations
      ADD CONSTRAINT observations_tenant_source_external_occurred_key
      UNIQUE (tenant_id, source_channel, external_id, occurred_at);
  END IF;
END
$$;

COMMIT;
