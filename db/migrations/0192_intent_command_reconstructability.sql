-- Complete authority and command capture for consequential reconstruction.
-- Rows written before this amendment remain explicitly legacy-missing rather
-- than receiving fabricated fingerprints.

BEGIN;

ALTER TABLE intent_command_results
  ADD COLUMN IF NOT EXISTS command JSONB,
  ADD COLUMN IF NOT EXISTS processing_authority_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS consumption_authority_fingerprint TEXT,
  ADD COLUMN IF NOT EXISTS authority_capture_status TEXT NOT NULL
    DEFAULT 'legacy_missing';

ALTER TABLE intent_command_results
  DROP CONSTRAINT IF EXISTS intent_command_results_authority_capture_valid;

ALTER TABLE intent_command_results
  ADD CONSTRAINT intent_command_results_authority_capture_valid CHECK (
    (
      authority_capture_status = 'complete'
      AND jsonb_typeof(command) = 'object'
      AND processing_authority_fingerprint ~ '^[0-9a-f]{64}$'
      AND consumption_authority_fingerprint ~ '^[0-9a-f]{64}$'
    )
    OR (
      authority_capture_status = 'legacy_missing'
      AND command IS NULL
      AND processing_authority_fingerprint IS NULL
      AND consumption_authority_fingerprint IS NULL
    )
  );

ALTER TABLE intent_command_results
  ALTER COLUMN authority_capture_status DROP DEFAULT;

COMMIT;
