-- =====================================================================
-- 0172_ask_authority_snapshots.sql — durable Ask authority context
-- =====================================================================

BEGIN;

ALTER TABLE ask_answers
  ADD COLUMN IF NOT EXISTS authority_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE INDEX IF NOT EXISTS ask_answers_authority_fingerprint_idx
  ON ask_answers ((authority_snapshot->>'fingerprint'))
  WHERE authority_snapshot ? 'fingerprint';

CREATE INDEX IF NOT EXISTS ask_scopes_access_fingerprint_idx
  ON ask_scopes ((access_snapshot->>'fingerprint'))
  WHERE access_snapshot ? 'fingerprint';

COMMIT;
