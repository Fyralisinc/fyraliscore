-- Phase 3 durable connector control-plane state. Existing source-specific
-- installation tables remain extension storage; this header is the binding
-- and lifecycle authority used by the connector runtime.

BEGIN;

CREATE TABLE IF NOT EXISTS source_connector_installations (
  id                       UUID PRIMARY KEY,
  tenant_id                UUID NOT NULL REFERENCES tenants(id),
  connector_id             TEXT NOT NULL CHECK (connector_id <> ''),
  external_installation_id TEXT NOT NULL CHECK (external_installation_id <> ''),
  desired_state            TEXT NOT NULL DEFAULT 'Ready'
                               CHECK (desired_state IN ('Ready', 'Paused', 'Maintenance', 'Removed')),
  observed_phase           TEXT NOT NULL DEFAULT 'Draft'
                               CHECK (observed_phase IN (
                                 'Draft', 'Authorizing', 'Validating', 'Initializing',
                                 'Ready', 'Degraded', 'Paused', 'Maintenance', 'Failed',
                                 'Uninstalling', 'Removed'
                               )),
  generation               BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
  observed_generation      BIGINT NOT NULL DEFAULT 0 CHECK (observed_generation >= 0),
  bound_connector_version  TEXT,
  enabled_capabilities     TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  conditions               JSONB NOT NULL DEFAULT '[]'::jsonb
                               CHECK (jsonb_typeof(conditions) = 'array'),
  provenance               JSONB NOT NULL DEFAULT '{}'::jsonb
                               CHECK (jsonb_typeof(provenance) = 'object'),
  next_reconcile_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
  removed_at               TIMESTAMPTZ,
  UNIQUE (connector_id, external_installation_id)
);

CREATE INDEX IF NOT EXISTS source_connector_installations_due_idx
  ON source_connector_installations (next_reconcile_at, connector_id)
  WHERE observed_phase <> 'Removed';

CREATE INDEX IF NOT EXISTS source_connector_installations_tenant_idx
  ON source_connector_installations (tenant_id, connector_id);

CREATE TABLE IF NOT EXISTS source_connector_authority_grants (
  installation_id        UUID PRIMARY KEY
                             REFERENCES source_connector_installations(id) ON DELETE CASCADE,
  tenant_id              UUID NOT NULL REFERENCES tenants(id),
  connector_id           TEXT NOT NULL CHECK (connector_id <> ''),
  authority_generation   BIGINT NOT NULL DEFAULT 1 CHECK (authority_generation > 0),
  credential_owner       TEXT NOT NULL CHECK (credential_owner <> ''),
  granted_secret_slots   TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  granted_scopes         TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  granted_outbound_hosts TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  maximum_trust_tier     TEXT NOT NULL DEFAULT 'untrusted',
  provenance             JSONB NOT NULL DEFAULT '{}'::jsonb
                             CHECK (jsonb_typeof(provenance) = 'object'),
  granted_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  revoked_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS source_connector_authority_tenant_idx
  ON source_connector_authority_grants (tenant_id, connector_id);

CREATE TABLE IF NOT EXISTS source_connector_credentials (
  installation_id UUID NOT NULL
                      REFERENCES source_connector_installations(id) ON DELETE CASCADE,
  tenant_id       UUID NOT NULL REFERENCES tenants(id),
  slot            TEXT NOT NULL CHECK (slot <> ''),
  secret_ref      TEXT NOT NULL CHECK (secret_ref <> ''),
  state           TEXT NOT NULL DEFAULT 'pending'
                      CHECK (state IN ('pending', 'current', 'retired', 'rejected')),
  generation      BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
  owner           TEXT NOT NULL,
  provenance      JSONB NOT NULL DEFAULT '{}'::jsonb
                      CHECK (jsonb_typeof(provenance) = 'object'),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  verified_at     TIMESTAMPTZ,
  retired_at      TIMESTAMPTZ,
  PRIMARY KEY (installation_id, slot, secret_ref)
);

CREATE UNIQUE INDEX IF NOT EXISTS source_connector_one_current_credential_idx
  ON source_connector_credentials (installation_id, slot)
  WHERE state = 'current';

CREATE TABLE IF NOT EXISTS source_connector_installation_data (
  installation_id UUID NOT NULL
                      REFERENCES source_connector_installations(id) ON DELETE CASCADE,
  tenant_id       UUID NOT NULL REFERENCES tenants(id),
  namespace       TEXT NOT NULL CHECK (namespace <> ''),
  generation      BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
  values          JSONB NOT NULL DEFAULT '{}'::jsonb
                      CHECK (jsonb_typeof(values) = 'object'),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (installation_id, namespace)
);

CREATE TABLE IF NOT EXISTS source_connector_callbacks (
  endpoint_id      UUID PRIMARY KEY,
  installation_id UUID NOT NULL
                       REFERENCES source_connector_installations(id) ON DELETE CASCADE,
  tenant_id       UUID NOT NULL REFERENCES tenants(id),
  purpose         TEXT NOT NULL CHECK (purpose <> ''),
  nonce_secret_ref TEXT NOT NULL CHECK (nonce_secret_ref <> ''),
  status          TEXT NOT NULL DEFAULT 'active'
                      CHECK (status IN ('active', 'disabled', 'retired')),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (installation_id, purpose, status)
);

CREATE TABLE IF NOT EXISTS source_connector_artifacts (
  connector_id            TEXT NOT NULL,
  connector_version       TEXT NOT NULL,
  artifact_sha256          TEXT NOT NULL CHECK (artifact_sha256 ~ '^[0-9a-f]{64}$'),
  manifest_sha256          TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
  conformance_fingerprint TEXT NOT NULL CHECK (conformance_fingerprint ~ '^[0-9a-f]{64}$'),
  signer_key_id           TEXT NOT NULL,
  builder_id              TEXT NOT NULL,
  source_revision         TEXT NOT NULL,
  built_at                TIMESTAMPTZ NOT NULL,
  signature               TEXT NOT NULL CHECK (signature <> ''),
  deployment_status       TEXT NOT NULL DEFAULT 'disabled'
                              CHECK (deployment_status IN ('disabled', 'enabled', 'quarantined', 'retired')),
  quarantine_reason       TEXT,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (connector_id, connector_version),
  CHECK ((deployment_status = 'quarantined') = (quarantine_reason IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS source_connector_routing_revisions (
  revision            BIGINT PRIMARY KEY CHECK (revision > 0),
  policy              JSONB NOT NULL CHECK (jsonb_typeof(policy) = 'object'),
  status              TEXT NOT NULL DEFAULT 'draft'
                          CHECK (status IN ('draft', 'staged', 'active', 'rolled_back', 'rejected')),
  cohort              JSONB NOT NULL DEFAULT '{}'::jsonb
                          CHECK (jsonb_typeof(cohort) = 'object'),
  rollback_thresholds JSONB NOT NULL DEFAULT '{}'::jsonb
                          CHECK (jsonb_typeof(rollback_thresholds) = 'object'),
  created_by          TEXT NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  activated_at        TIMESTAMPTZ,
  superseded_by       BIGINT REFERENCES source_connector_routing_revisions(revision)
);

CREATE UNIQUE INDEX IF NOT EXISTS source_connector_one_active_routing_idx
  ON source_connector_routing_revisions ((status)) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS source_connector_rollout_audit (
  id               UUID PRIMARY KEY,
  revision         BIGINT NOT NULL REFERENCES source_connector_routing_revisions(revision),
  action           TEXT NOT NULL,
  actor            TEXT NOT NULL,
  reason           TEXT NOT NULL,
  metrics_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb
                       CHECK (jsonb_typeof(metrics_snapshot) = 'object'),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source_connector_rollout_metric_windows (
  revision              BIGINT NOT NULL
                            REFERENCES source_connector_routing_revisions(revision),
  window_started_at     TIMESTAMPTZ NOT NULL,
  executions            BIGINT NOT NULL DEFAULT 0,
  failures              BIGINT NOT NULL DEFAULT 0,
  parity_samples        BIGINT NOT NULL DEFAULT 0,
  parity_mismatches     BIGINT NOT NULL DEFAULT 0,
  connector_p95_ms      DOUBLE PRECISION NOT NULL DEFAULT 0,
  legacy_p95_ms         DOUBLE PRECISION NOT NULL DEFAULT 0,
  lifecycle_failures    BIGINT NOT NULL DEFAULT 0,
  connector_dlq_rate    DOUBLE PRECISION NOT NULL DEFAULT 0,
  baseline_dlq_rate     DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY (revision, window_started_at)
);

-- Seed the approved Phase 2 pilots from their existing installation rows. The
-- runtime can therefore require durable grants immediately after this
-- migration without fabricating authority in process memory.
INSERT INTO source_connector_installations (
  id, tenant_id, connector_id, external_installation_id,
  desired_state, observed_phase, observed_generation,
  bound_connector_version, provenance
)
SELECT id, tenant_id, 'fyralis/' || provider, installation_id,
       CASE WHEN enabled THEN 'Ready' ELSE 'Paused' END,
       CASE WHEN enabled THEN 'Ready' ELSE 'Paused' END,
       1, '1.0.0',
       jsonb_build_object('storage', 'provider_installations', 'migrated_by', '0187')
  FROM provider_installations
 WHERE provider IN ('slack', 'notion')
ON CONFLICT (id) DO NOTHING;

INSERT INTO source_connector_authority_grants (
  installation_id, tenant_id, connector_id, credential_owner,
  granted_secret_slots, granted_scopes, granted_outbound_hosts,
  maximum_trust_tier, provenance
)
SELECT id, tenant_id, 'fyralis/' || provider, 'provider_installations',
       CASE provider
         WHEN 'slack' THEN ARRAY['oauth_access_token', 'webhook_signing_secret']::text[]
         ELSE ARRAY['oauth_access_token']::text[]
       END,
       CASE provider
         WHEN 'slack' THEN ARRAY[
           'channels:read', 'channels:history', 'groups:read',
           'groups:history', 'users:read', 'team:read'
         ]::text[]
         ELSE ARRAY[]::text[]
       END,
       CASE provider
         WHEN 'slack' THEN ARRAY['slack.com']::text[]
         ELSE ARRAY['api.notion.com']::text[]
       END,
       'attested_agent',
       jsonb_build_object(
         'credential_ref_present', secret_ref IS NOT NULL,
         'migrated_by', '0187'
       )
  FROM provider_installations
 WHERE provider IN ('slack', 'notion')
   AND secret_ref IS NOT NULL
ON CONFLICT (installation_id) DO NOTHING;

INSERT INTO source_connector_credentials (
  installation_id, tenant_id, slot, secret_ref, state, owner, provenance
)
SELECT id, tenant_id,
       CASE provider
         WHEN 'slack' THEN 'webhook_signing_secret'
         ELSE 'oauth_access_token'
       END,
       secret_ref, 'current', 'provider_installations',
       jsonb_build_object('migrated_by', '0187')
  FROM provider_installations
 WHERE provider IN ('slack', 'notion')
   AND secret_ref IS NOT NULL
ON CONFLICT DO NOTHING;

-- WhatsApp is a complete live-webhook connector. Its deliberately deferred
-- backfill placeholders are not declared capabilities and remain non-runnable.
INSERT INTO source_connector_installations (
  id, tenant_id, connector_id, external_installation_id,
  desired_state, observed_phase, observed_generation,
  bound_connector_version, provenance
)
SELECT id, tenant_id, 'fyralis/whatsapp', phone_number_id,
       CASE WHEN enabled THEN 'Ready' ELSE 'Paused' END,
       CASE WHEN enabled THEN 'Ready' ELSE 'Paused' END,
       1, '1.0.0',
       jsonb_build_object('storage', 'whatsapp_installations', 'migrated_by', '0187')
  FROM whatsapp_installations
ON CONFLICT (id) DO NOTHING;

INSERT INTO source_connector_authority_grants (
  installation_id, tenant_id, connector_id, credential_owner,
  granted_secret_slots, granted_scopes, granted_outbound_hosts,
  maximum_trust_tier, provenance
)
SELECT id, tenant_id, 'fyralis/whatsapp', 'whatsapp_installations',
       ARRAY['app_secret']::text[], ARRAY[]::text[], ARRAY[]::text[],
       'attested_agent',
       jsonb_build_object('migrated_by', '0187')
  FROM whatsapp_installations
 WHERE app_secret_ref IS NOT NULL
ON CONFLICT (installation_id) DO NOTHING;

INSERT INTO source_connector_credentials (
  installation_id, tenant_id, slot, secret_ref, state, owner, provenance
)
SELECT id, tenant_id, 'app_secret', app_secret_ref, 'current',
       'whatsapp_installations', jsonb_build_object('migrated_by', '0187')
  FROM whatsapp_installations
 WHERE app_secret_ref IS NOT NULL
ON CONFLICT DO NOTHING;

INSERT INTO source_connector_credentials (
  installation_id, tenant_id, slot, secret_ref, state, owner, provenance
)
SELECT install.id, install.tenant_id, 'oauth_access_token', secret.id::text,
       'current', 'encrypted_secrets',
       jsonb_build_object('migrated_by', '0187', 'legacy_label', secret.label)
  FROM provider_installations AS install
  JOIN encrypted_secrets AS secret
    ON secret.tenant_id = install.tenant_id
   AND secret.label = 'slack_bot_token:' || install.installation_id
 WHERE install.provider = 'slack'
ON CONFLICT DO NOTHING;

ALTER TABLE source_connector_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_connector_installations FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON source_connector_installations;
CREATE POLICY tenant_isolation ON source_connector_installations
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE source_connector_authority_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_connector_authority_grants FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON source_connector_authority_grants;
CREATE POLICY tenant_isolation ON source_connector_authority_grants
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE source_connector_credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_connector_credentials FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON source_connector_credentials;
CREATE POLICY tenant_isolation ON source_connector_credentials
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE source_connector_installation_data ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_connector_installation_data FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON source_connector_installation_data;
CREATE POLICY tenant_isolation ON source_connector_installation_data
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

ALTER TABLE source_connector_callbacks ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_connector_callbacks FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON source_connector_callbacks;
CREATE POLICY tenant_isolation ON source_connector_callbacks
  USING (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  )
  WITH CHECK (
    current_setting('app.current_tenant', true) IS NULL
    OR current_setting('app.current_tenant', true) = ''
    OR tenant_id = current_setting('app.current_tenant', true)::uuid
  );

COMMIT;
