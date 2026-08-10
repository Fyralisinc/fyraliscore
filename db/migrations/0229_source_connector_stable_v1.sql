-- Stable-v1 fleet upgrade: import source-specific installation authority,
-- declare state compatibility, and persist operational/retirement evidence.

BEGIN;

ALTER TABLE source_connector_installations
  ADD COLUMN IF NOT EXISTS state_schema_version INTEGER NOT NULL DEFAULT 1
    CHECK (state_schema_version > 0),
  ADD COLUMN IF NOT EXISTS accepted_state_schema_versions INTEGER[]
    NOT NULL DEFAULT ARRAY[1]::INTEGER[],
  ADD COLUMN IF NOT EXISTS last_replay_certified_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS source_connector_resilience_evidence (
  connector_id      TEXT NOT NULL,
  connector_version TEXT NOT NULL,
  scenario          TEXT NOT NULL CHECK (scenario IN (
    'provider_throttle', 'provider_outage', 'lease_loss', 'cancellation',
    'secret_rotation', 'credential_revocation', 'poison_payload',
    'multi_region_failover', 'disaster_recovery_replay'
  )),
  region             TEXT NOT NULL CHECK (region <> ''),
  passed             BOOLEAN NOT NULL,
  observed_at        TIMESTAMPTZ NOT NULL,
  evidence_ref       TEXT NOT NULL CHECK (evidence_ref <> ''),
  recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (connector_id, connector_version, scenario, region)
);

CREATE TABLE IF NOT EXISTS source_connector_retirement_evidence (
  connector_id       TEXT NOT NULL,
  connector_version  TEXT NOT NULL,
  legacy_surface     TEXT NOT NULL,
  last_legacy_use_at TIMESTAMPTZ,
  parity_accepted_at TIMESTAMPTZ NOT NULL,
  rollback_owner     TEXT NOT NULL CHECK (rollback_owner <> ''),
  evidence_ref       TEXT NOT NULL CHECK (evidence_ref <> ''),
  recorded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (connector_id, connector_version, legacy_surface)
);

-- Import the source-specific installation identities and secret references.
-- Dynamic SQL keeps this migration declarative while still tolerating an
-- installation table with zero rows. All identifiers below are checked-in.
DO $$
DECLARE
  cfg RECORD;
  secret_cfg RECORD;
  credential_predicate TEXT;
  slot_expression TEXT;
BEGIN
  FOR cfg IN
    SELECT * FROM (VALUES
      ('gmail_installations', 'gmail', 'workspace_domain',
       '{}'::jsonb, ARRAY['gmail.googleapis.com']::text[]),
      ('google_calendar_installations', 'google_calendar', 'workspace_domain',
       '{}'::jsonb, ARRAY['www.googleapis.com']::text[]),
      ('google_drive_installations', 'google_drive', 'workspace_domain',
       '{}'::jsonb, ARRAY['www.googleapis.com']::text[]),
      ('jira_installations', 'jira', 'cloud_id',
       '{"oauth_access_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.atlassian.com']::text[]),
      ('mercury_installations', 'mercury', 'organization_id',
       '{"api_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.mercury.com']::text[]),
      ('quickbooks_installations', 'quickbooks', 'realm_id',
       '{"oauth_access_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['quickbooks.api.intuit.com']::text[]),
      ('grafana_installations', 'grafana', 'org_id',
       '{"service_account_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['grafana.com']::text[]),
      ('telegram_installations', 'telegram', 'account_label',
       '{"bot_token":"backfill_session_secret_ref"}'::jsonb,
       ARRAY['api.telegram.org']::text[]),
      ('brex_installations', 'brex', 'organization_id',
       '{"access_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['platform.brexapis.com']::text[]),
      ('ramp_installations', 'ramp', 'business_id',
       '{"oauth_access_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.ramp.com']::text[]),
      ('gusto_installations', 'gusto', 'company_uuid',
       '{"oauth_access_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.gusto.com']::text[]),
      ('deel_installations', 'deel', 'organization_id',
       '{"api_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.letsdeel.com']::text[]),
      ('fireflies_installations', 'fireflies', 'workspace_id',
       '{"api_key":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.fireflies.ai']::text[]),
      ('signal_installations', 'signal', 'account_label',
       '{"linked_device_token":"backfill_session_secret_ref"}'::jsonb,
       ARRAY['chat.signal.org']::text[]),
      ('aws_installations', 'aws', 'account_id',
       '{"session_token":"secret_ref"}'::jsonb,
       ARRAY['cloudtrail.amazonaws.com']::text[]),
      ('miro_installations', 'miro', 'org_id',
       '{"oauth_access_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.miro.com']::text[]),
      ('figma_installations', 'figma', 'team_id',
       '{"access_token":"secret_ref","webhook_passcode":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.figma.com']::text[]),
      ('carta_installations', 'carta', 'firm_id',
       '{"oauth_access_token":"secret_ref"}'::jsonb,
       ARRAY['api.carta.com']::text[]),
      ('hibob_installations', 'hibob', 'company_id',
       '{"service_token":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.hibob.com']::text[]),
      ('ashby_installations', 'ashby', 'org_id',
       '{"api_key":"secret_ref","webhook_signing_secret":"webhook_secret_ref"}'::jsonb,
       ARRAY['api.ashbyhq.com']::text[]),
      ('linkedin_installations', 'linkedin', 'organization_urn',
       '{"oauth_access_token":"secret_ref"}'::jsonb,
       ARRAY['api.linkedin.com']::text[])
    ) AS valueset(table_name, source, external_column, credential_columns, hosts)
  LOOP
    SELECT
      COALESCE(
        string_agg(format('%I IS NOT NULL', value), ' OR '),
        'FALSE'
      ),
      COALESCE(
        string_agg(
          format('CASE WHEN %I IS NOT NULL THEN %L END', value, key),
          ', '
        ),
        'NULL::text'
      )
      INTO credential_predicate, slot_expression
      FROM jsonb_each_text(cfg.credential_columns);
    EXECUTE format(
      'INSERT INTO source_connector_installations (
         id, tenant_id, connector_id, external_installation_id,
         desired_state, observed_phase, observed_generation,
         bound_connector_version, provenance
       ) SELECT id, tenant_id, %L,
                COALESCE(NULLIF(%I::text, ''''), id::text),
                CASE
                  WHEN disabled_at IS NOT NULL THEN ''Paused''
                  WHEN (%s) THEN ''Ready''
                  ELSE ''Maintenance''
                END,
                CASE
                  WHEN disabled_at IS NOT NULL THEN ''Paused''
                  WHEN (%s) THEN ''Ready''
                  ELSE ''Maintenance''
                END,
                1, ''1.0.0'',
                jsonb_build_object(
                  ''storage'', %L,
                  ''migrated_by'', ''0189'',
                  ''requires_reauthorization'', NOT (%s)
                )
           FROM %I ON CONFLICT DO NOTHING',
      'fyralis/' || cfg.source,
      cfg.external_column,
      credential_predicate,
      credential_predicate,
      cfg.table_name,
      credential_predicate,
      cfg.table_name
    );
    EXECUTE format(
      'INSERT INTO source_connector_authority_grants (
         installation_id, tenant_id, connector_id, credential_owner,
         granted_slot_names, granted_scopes, granted_outbound_hosts,
         maximum_trust_tier, provenance
       ) SELECT id, tenant_id, %L, %L,
                ARRAY_REMOVE(ARRAY[%s]::text[], NULL),
                ARRAY[]::text[], $1,
                ''attested_agent'', jsonb_build_object(''migrated_by'', ''0189'')
           FROM %I ON CONFLICT (installation_id) DO NOTHING',
      'fyralis/' || cfg.source,
      cfg.table_name,
      slot_expression,
      cfg.table_name
    ) USING cfg.hosts;
    FOR secret_cfg IN SELECT key AS slot, value #>> '{}' AS column_name
      FROM jsonb_each(cfg.credential_columns)
    LOOP
      EXECUTE format(
        'INSERT INTO source_connector_credentials (
           installation_id, tenant_id, slot, secret_ref, state, owner,
           provenance, verified_at
         ) SELECT id, tenant_id, %L, %I::text, ''current'', %L,
                  jsonb_build_object(''migrated_by'', ''0189''), now()
             FROM %I WHERE %I IS NOT NULL ON CONFLICT DO NOTHING',
        secret_cfg.slot,
        secret_cfg.column_name,
        cfg.table_name,
        cfg.table_name,
        secret_cfg.column_name
      );
    END LOOP;
  END LOOP;

  INSERT INTO source_connector_installations (
    id, tenant_id, connector_id, external_installation_id,
    desired_state, observed_phase, observed_generation,
    bound_connector_version, provenance
  )
  SELECT install.id, install.tenant_id, 'fyralis/' || install.provider,
         install.installation_id,
         CASE
           WHEN NOT install.enabled THEN 'Paused'
           WHEN install.provider = 'discord'
             AND install.secret_ref IS NOT NULL
             AND bot.id IS NOT NULL THEN 'Ready'
           ELSE 'Maintenance'
         END,
         CASE
           WHEN NOT install.enabled THEN 'Paused'
           WHEN install.provider = 'discord'
             AND install.secret_ref IS NOT NULL
             AND bot.id IS NOT NULL THEN 'Ready'
           ELSE 'Maintenance'
         END,
         1, '1.0.0',
         jsonb_build_object(
           'storage', 'provider_installations',
           'migrated_by', '0189',
           'requires_reauthorization',
             install.provider = 'github'
             OR install.secret_ref IS NULL
             OR bot.id IS NULL
         )
    FROM provider_installations AS install
    LEFT JOIN LATERAL (
      SELECT secret.id
        FROM encrypted_secrets AS secret
       WHERE secret.tenant_id = install.tenant_id
         AND secret.label = 'discord_bot_token:' || install.installation_id
       ORDER BY secret.created_at DESC, secret.id
       LIMIT 1
    ) AS bot ON install.provider = 'discord'
   WHERE install.provider IN ('github', 'discord')
  ON CONFLICT DO NOTHING;

  INSERT INTO source_connector_authority_grants (
    installation_id, tenant_id, connector_id, credential_owner,
    granted_slot_names, granted_scopes, granted_outbound_hosts,
    maximum_trust_tier, provenance
  )
  SELECT install.id, install.tenant_id, 'fyralis/' || install.provider,
         'provider_installations',
         CASE install.provider
           WHEN 'discord' THEN ARRAY_REMOVE(ARRAY[
             CASE WHEN bot.id IS NOT NULL THEN 'bot_token' END,
             CASE WHEN install.secret_ref IS NOT NULL
                  THEN 'webhook_public_key' END
           ]::text[], NULL)
           ELSE ARRAY[]::text[]
         END,
         ARRAY[]::text[],
         CASE install.provider
           WHEN 'github' THEN ARRAY['api.github.com']::text[]
           ELSE ARRAY['discord.com']::text[]
         END,
         'attested_agent',
         jsonb_build_object('migrated_by', '0189')
    FROM provider_installations AS install
    LEFT JOIN LATERAL (
      SELECT secret.id
        FROM encrypted_secrets AS secret
       WHERE secret.tenant_id = install.tenant_id
         AND secret.label = 'discord_bot_token:' || install.installation_id
       ORDER BY secret.created_at DESC, secret.id
       LIMIT 1
    ) AS bot ON install.provider = 'discord'
   WHERE install.provider IN ('github', 'discord')
  ON CONFLICT (installation_id) DO NOTHING;

  INSERT INTO source_connector_credentials (
    installation_id, tenant_id, slot, secret_ref, state, owner, provenance
  )
  SELECT install.id, install.tenant_id, 'webhook_public_key',
         install.secret_ref, 'current', 'provider_installations',
         jsonb_build_object('migrated_by', '0189')
    FROM provider_installations AS install
   WHERE install.provider = 'discord'
     AND install.secret_ref IS NOT NULL
  ON CONFLICT DO NOTHING;

  INSERT INTO source_connector_credentials (
    installation_id, tenant_id, slot, secret_ref, state, owner, provenance
  )
  SELECT install.id, install.tenant_id, 'bot_token', secret.id::text,
         'current', 'encrypted_secrets',
         jsonb_build_object(
           'migrated_by', '0189', 'legacy_label', secret.label
         )
    FROM provider_installations AS install
    JOIN LATERAL (
      SELECT candidate.id, candidate.label
        FROM encrypted_secrets AS candidate
       WHERE candidate.tenant_id = install.tenant_id
         AND candidate.label =
             'discord_bot_token:' || install.installation_id
       ORDER BY candidate.created_at DESC, candidate.id
       LIMIT 1
    ) AS secret ON TRUE
   WHERE install.provider = 'discord'
  ON CONFLICT DO NOTHING;
END $$;

COMMIT;
