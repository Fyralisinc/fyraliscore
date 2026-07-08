-- 0081_grafana.sql
--   IN-GRAFANA — Grafana as an ingestion source (alerts + annotations).
--
-- NUMBERING (was 0080 on the grafana-source branch): renumbered to 0081 because
-- cannonical landed 0080_onboarding_runs_behind_schedule.sql first. That migration
-- only `ADD COLUMN onboarding_runs.behind_schedule_emitted_at` — it does NOT touch
-- the source-CHECK constraints — so there is no ordering hazard with the CHECK
-- widening below; 0081 simply applies after it. (Same renumber pattern as the
-- IN-16/IN-17 0061→0062 collision.)
--
-- Grafana is an observability/alerting platform exposing an HTTP API
-- authenticated with a SERVICE-ACCOUNT TOKEN presented as a Bearer credential
-- (API keys were deprecated in 2025). The token is held in encrypted_secrets and
-- referenced by secret_ref; the base_url (the Grafana instance root, e.g.
-- https://acme.grafana.net or https://grafana.internal:3000) + org_id live on the
-- install row. Grafana needs only an instance-scoped install — annotations and
-- alert state are org-wide, so there is NO per-resource shard table (unlike Jira's
-- jira_projects / Mercury's mercury_accounts): one shard per installation streams
-- the org's annotations. The install table therefore mirrors the SHAPE of
-- jira_installations / mercury_installations but with no child table:
--
--   grafana_installations — one row per (tenant, base_url)
--
-- DUAL EDGE (mirrors Jira/Mercury):
--   - BACKFILL/POLL (pull): GET /api/annotations (service-account Bearer token);
--     annotations include Grafana's auto-created alert-state-change annotations,
--     so the annotation stream carries historical alert transitions. secret_ref
--     points at the SA token.
--   - LIVE (push): a Grafana Alerting webhook contact point POSTs the
--     Alertmanager-superset alert JSON, signed HMAC-SHA256 in the
--     `X-Grafana-Alerting-Signature` header (Grafana 12.0+). The webhook edge
--     resolves the tenant from the payload `externalURL` host (registered in
--     provider_installations, provider='grafana') and verifies the HMAC against
--     webhook_secret_ref. Backfill uses grafana_installations; live uses
--     provider_installations — seeded together, independent.
--
-- Plus the source-registry CHECK widening every new ingestion source needs:
-- the M6 substrate pins allowed `source` values with an inline CHECK on FOUR
-- tables (last widened by 0075 to add 'quickbooks'; here we add 'grafana'):
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
--     jira_installations / mercury_installations policy template (0073 / 0074).

BEGIN;

-- ---------------------------------------------------------------------
-- grafana_installations — one row per (tenant, base_url)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS grafana_installations (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  -- Grafana instance root URL, e.g. https://acme.grafana.net (no trailing slash).
  base_url TEXT NOT NULL,
  -- Grafana org id the service-account token is scoped to. Defaults to '1'
  -- (Grafana's default org). Stored as TEXT for forward-compat with Cloud
  -- stack/org identifiers.
  org_id TEXT NOT NULL DEFAULT '1',
  -- Opaque pointer into encrypted_secrets for the service-account token (the
  -- Bearer credential). Mirrors jira_installations.secret_ref.
  secret_ref TEXT,
  -- Opaque pointer into encrypted_secrets for the webhook HMAC shared secret
  -- (the X-Grafana-Alerting-Signature key, Grafana 12.0+). Mirrors
  -- jira_installations.webhook_secret_ref.
  webhook_secret_ref TEXT,
  -- Warm-start high-water for the annotations backfill: the max annotation
  -- `time` (epoch milliseconds) ingested for this install. NULL until the first
  -- full sync. Analogous to jira_projects.updated_cursor; v1 keeps the live
  -- cursor in workflow_states (the N1 primitive) and this column is the
  -- planner-visible warm-start hint (reconciler-maintained in a later pass).
  annotations_cursor_ms BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  disabled_at TIMESTAMPTZ,
  UNIQUE (tenant_id, base_url)
);

CREATE INDEX IF NOT EXISTS grafana_installations_tenant_idx
  ON grafana_installations (tenant_id);

-- ---------------------------------------------------------------------
-- RLS — mirror the jira_installations / mercury_installations template.
-- ---------------------------------------------------------------------
ALTER TABLE grafana_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE grafana_installations FORCE  ROW LEVEL SECURITY;

DROP POLICY IF EXISTS grafana_installations_tenant_isolation ON grafana_installations;
CREATE POLICY grafana_installations_tenant_isolation ON grafana_installations
  USING (tenant_id = current_setting('app.current_tenant', true)::uuid)
  WITH CHECK (tenant_id = current_setting('app.current_tenant', true)::uuid);

-- ---------------------------------------------------------------------
-- Source-registry CHECK widening — admit 'grafana' on all four M6 substrate
-- tables. Carries every prior source forward (strict superset of 0075's list)
-- so applying this migration last does not drop any of them.
-- ---------------------------------------------------------------------
ALTER TABLE source_onboarding_runs
    DROP CONSTRAINT IF EXISTS source_onboarding_runs_source_check;
ALTER TABLE source_onboarding_runs
    ADD CONSTRAINT source_onboarding_runs_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana')) NOT VALID;

ALTER TABLE onboarding_shards
    DROP CONSTRAINT IF EXISTS onboarding_shards_source_check;
ALTER TABLE onboarding_shards
    ADD CONSTRAINT onboarding_shards_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana')) NOT VALID;

ALTER TABLE ingestion_failures
    DROP CONSTRAINT IF EXISTS ingestion_failures_source_check;
ALTER TABLE ingestion_failures
    ADD CONSTRAINT ingestion_failures_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana')) NOT VALID;

ALTER TABLE onboarding_triggers
    DROP CONSTRAINT IF EXISTS onboarding_triggers_source_check;
ALTER TABLE onboarding_triggers
    ADD CONSTRAINT onboarding_triggers_source_check
    CHECK (source IN ('slack', 'github', 'discord', 'gmail', 'notion', 'google_calendar', 'google_drive', 'jira', 'mercury', 'quickbooks', 'grafana')) NOT VALID;

COMMIT;
