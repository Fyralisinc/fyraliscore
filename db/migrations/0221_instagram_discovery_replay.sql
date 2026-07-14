-- Ensure a first-time customer conversation can request one bounded Graph
-- discovery run without creating an unbounded replay trigger for every DM.

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS onboarding_triggers_instagram_discovery_open_idx
  ON onboarding_triggers (tenant_id, source)
  WHERE source = 'instagram'
    AND trigger_kind = 'manual_replay'
    AND consumed_at IS NULL;

COMMIT;
