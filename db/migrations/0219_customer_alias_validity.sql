-- 0219_customer_alias_validity.sql
--
-- Customer names are time-bounded claims about one stable resource identity.
-- Keep historical aliases so delayed signals can resolve against event time and
-- so a retired name can later be reused without rewriting old observations or
-- model scopes.

ALTER TABLE entity_aliases
  ADD COLUMN IF NOT EXISTS valid_from TIMESTAMPTZ;

UPDATE entity_aliases
SET valid_from = first_seen_at
WHERE valid_from IS NULL;

ALTER TABLE entity_aliases
  ALTER COLUMN valid_from SET DEFAULT now();

ALTER TABLE entity_aliases
  ALTER COLUMN valid_from SET NOT NULL;

ALTER TABLE entity_aliases
  ADD COLUMN IF NOT EXISTS valid_until TIMESTAMPTZ;

ALTER TABLE entity_aliases
  ADD COLUMN IF NOT EXISTS validity_event_id UUID;

ALTER TABLE entity_aliases
  ADD COLUMN IF NOT EXISTS validity_reason TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'entity_aliases_valid_interval_check'
      AND conrelid = 'entity_aliases'::regclass
  ) THEN
    ALTER TABLE entity_aliases
      ADD CONSTRAINT entity_aliases_valid_interval_check
      CHECK (valid_until IS NULL OR valid_until >= valid_from);
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS aliases_normalized_validity_idx
  ON entity_aliases (
    tenant_id,
    (regexp_replace(lower(alias_text), '\s+', ' ', 'g')),
    valid_from,
    valid_until
  );

CREATE INDEX IF NOT EXISTS aliases_current_entity_idx
  ON entity_aliases USING gin (resolved_entity_ref)
  WHERE valid_until IS NULL;
