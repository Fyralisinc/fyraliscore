-- 0101_aws.sql
--   IN-AWS — AWS as an ingestion source (CloudTrail management events +
--   CloudWatch alarm-state changes).
--
-- AWS is a cloud-infrastructure signal source exposing the CloudTrail
-- LookupEvents API, authenticated with IAM credentials and SIGNED per request
-- with SigV4 (botocore). The install stores the target account_id + region + a
-- credential descriptor (credential_kind ∈ {assume_role, static_keys}); the
-- actual role ARN / key material is held in encrypted_secrets and referenced by
-- secret_ref. AWS needs only an account/region-scoped install — CloudTrail
-- management events and alarm-state changes are account/region-wide, so there is
-- NO per-resource shard table (unlike Jira's jira_projects / Mercury's
-- mercury_accounts): one shard per installation streams the account/region's
-- events over a TIME WINDOW. The install table therefore mirrors the SHAPE of
-- grafana_installations (the time-window-backfill archetype) but keyed on
-- (tenant, account, region) with no child table:
--
--   aws_installations — one row per (tenant, account_id, region)
--
-- DUAL EDGE (time-window backfill + POLL live, NOT a webhook):
--   - BACKFILL/POLL (pull): CloudTrail:LookupEvents (IAM SigV4) over a time
--     window bounded below by a 90-day floor (CloudTrail's management-event
--     retention). events_cursor_ms is the warm-start high-water (max event
--     eventTime in epoch ms) so a re-onboarding runs incrementally.
--   - LIVE (push): a SQS / EventBridge POLL loop drains CloudTrail events and
--     dispatches each through live_poll.handle_polled_event (ingress_kind=poll).
--     The trust boundary is the IAM-authenticated poll of the customer's own
--     queue (as with Telegram's MTProto connection / Gmail's Pub/Sub) — there is
--     NO webhook HMAC and NO provider_installations row; the poll edge resolves
--     the tenant/install directly from aws_installations by (account_id, region).
--
-- Plus the source-registry CHECK widening every new ingestion source needs: the
-- M6 substrate pins allowed `source` values with an inline CHECK on FOUR tables.
-- Here we add 'aws' AND carry the full canonical 22-source set forward verbatim:
--   - source_onboarding_runs  (migration 0055)
--   - onboarding_shards        (migration 0045)
--   - ingestion_failures       (migration 0046)
--   - onboarding_triggers      (migration 0047)
-- Missing any one breaks a different stage: triggers (onboarding emits one),
-- runs + shards (backfill planning), failures (DLQ writer). The CHECK lists
-- below carry ALL prior sources forward (a strict superset) so applying this
-- migration last cannot drop an existing source from the allowed set.
--
-- §II compliance:
--   - Append-only: CREATE TABLE IF NOT EXISTS is additive; the CHECK widening
--     admits a strict superset of the prior allowed set, so no existing row can
--     violate it — non-destructive, no staged plan required.
--   - Idempotent: table guarded by IF NOT EXISTS; each constraint dropped
--     IF EXISTS and re-added with the same Postgres-assigned inline name
--     (`<table>_source_check`). Re-running is a no-op.
--   - Tenant isolation (§III): the new table ENABLEs + FORCEs RLS with a
--     tenant_isolation policy keyed on app.current_tenant, mirroring the
--     grafana_installations / jira_installations policy template.

BEGIN;

-- ---------------------------------------------------------------------
-- aws_installations — one row per (tenant, account_id, region)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS aws_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- The 12-digit AWS account id this install targets.
  account_id TEXT NOT NULL,
  -- The AWS region (e.g. us-east-1). Part of the external_id namespace.
  region TEXT NOT NULL,
  -- How the IAM credentials are obtained: 'assume_role' (cross-account role ARN,
  -- recommended) or 'static_keys' (long-lived access key pair). The actual
  -- material lives in encrypted_secrets, referenced by secret_ref.
  credential_kind TEXT NOT NULL DEFAULT 'assume_role',
  -- Opaque pointer into encrypted_secrets for the credential material (role ARN
  -- or access-key pair). Mirrors grafana_installations.secret_ref.
  secret_ref TEXT,
  -- Lower-bound window (days) for the time-window backfill walk. Defaults to 90
  -- (CloudTrail's LookupEvents management-event retention).
  backfill_window_days INT DEFAULT 90,
  -- Warm-start high-water for the events backfill: the max event `eventTime`
  -- (epoch milliseconds) ingested for this install. NULL until the first full
  -- sync. Analogous to grafana_installations.annotations_cursor_ms; v1 keeps the
  -- live cursor in workflow_states (the N1 primitive) and this column is the
  -- planner-visible warm-start hint (reconciler-maintained in a later pass).
  events_cursor_ms BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, account_id, region)
);

CREATE INDEX IF NOT EXISTS aws_installations_tenant_idx
  ON aws_installations (tenant_id);

-- The live poll edge resolves the tenant/install by (account_id, region); index
-- that lookup path.
CREATE INDEX IF NOT EXISTS aws_installations_account_region_idx
  ON aws_installations (account_id, region);

-- ---------------------------------------------------------------------
-- RLS — mirror the grafana_installations / jira_installations template.
-- ---------------------------------------------------------------------
ALTER TABLE aws_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE aws_installations FORCE  ROW LEVEL SECURITY;

DROP POLICY IF EXISTS aws_installations_tenant_isolation ON aws_installations;
CREATE POLICY aws_installations_tenant_isolation ON aws_installations
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

-- ---------------------------------------------------------------------
-- Source-registry CHECK widening — admit 'aws' on all four M6 substrate tables.
-- Carries the FULL canonical 22-source set forward (a strict superset) so
-- applying this migration last does not drop any of them.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta'));

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta'));

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta'));

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana', 'telegram', 'brex', 'ramp', 'gusto', 'deel', 'fireflies', 'signal', 'aws', 'miro', 'figma', 'carta'));

COMMIT;
