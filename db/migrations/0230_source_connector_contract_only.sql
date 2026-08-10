-- Final source-ingestion cutover: one contract control plane and one runtime
-- owner. Historical provider tables may still exist for audit/export, but no
-- source execution decision or worker reads them after this migration.

BEGIN;

-- Operator lifecycle actions now target the common connector installation
-- resource. The retired Kafka-path re-enable action has no runtime owner.
ALTER TABLE operator_action_log
  DROP CONSTRAINT IF EXISTS operator_action_log_action_check;

ALTER TABLE operator_action_log
  ADD CONSTRAINT operator_action_log_action_check CHECK (
    action IN (
      'dead_letter.list',
      'dead_letter.retry',
      'dead_letter.quarantine',
      'role.list',
      'role.grant',
      'role.revoke',
      'source_installation.status',
      'source_installation.pause',
      'source_installation.resume',
      'source_installation.maintenance',
      'source_installation.uninstall',
      'source_installation.secret.rotate',
      'queue_depth.inspect',
      'support_bundle.export',
      'customer_data_export.record',
      'today.brand.update',
      'map.projection.refresh'
    )
  );

-- The onboarding outbox now names its common control-plane foreign key.
ALTER TABLE onboarding_triggers
  ADD COLUMN IF NOT EXISTS connector_installation_id UUID
    REFERENCES source_connector_installations(id) ON DELETE CASCADE;

UPDATE onboarding_triggers AS trigger
   SET connector_installation_id = COALESCE(
         trigger.installation_row_id,
         trigger.gmail_installation_id
       )
 WHERE trigger.connector_installation_id IS NULL
   AND COALESCE(trigger.installation_row_id, trigger.gmail_installation_id) IS NOT NULL
   AND EXISTS (
     SELECT 1 FROM source_connector_installations AS install
      WHERE install.id = COALESCE(
        trigger.installation_row_id,
        trigger.gmail_installation_id
      )
   );

CREATE UNIQUE INDEX IF NOT EXISTS
  onboarding_triggers_unique_per_connector_install_idx
  ON onboarding_triggers (tenant_id, source, connector_installation_id)
  WHERE connector_installation_id IS NOT NULL;

DROP INDEX IF EXISTS onboarding_triggers_unique_per_provider_install_idx;
DROP INDEX IF EXISTS onboarding_triggers_unique_per_gmail_install_idx;
ALTER TABLE onboarding_triggers
  DROP COLUMN IF EXISTS installation_row_id,
  DROP COLUMN IF EXISTS gmail_installation_id;

-- Routing revisions can only propagate a connector artifact revision.
UPDATE source_connector_routing_revisions
   SET policy = jsonb_build_object(
         'revision', revision,
         'global', 'connector'
       );

ALTER TABLE source_connector_routing_revisions
  DROP CONSTRAINT IF EXISTS source_connector_routing_revisions_contract_only_check;
ALTER TABLE source_connector_routing_revisions
  ADD CONSTRAINT source_connector_routing_revisions_contract_only_check CHECK (
    policy->>'global' = 'connector'
    AND NOT policy ?| ARRAY[
      'connectors', 'capabilities', 'tenants', 'tenant_connectors',
      'connector_capabilities', 'tenant_capabilities'
    ]
  );

DELETE FROM source_connector_rollout_events WHERE event_type = 'parity';
UPDATE source_connector_rollout_events
   SET implementation = 'connector'
 WHERE implementation IS DISTINCT FROM 'connector';

ALTER TABLE source_connector_rollout_events
  DROP CONSTRAINT IF EXISTS source_connector_rollout_events_event_type_check,
  DROP CONSTRAINT IF EXISTS source_connector_rollout_events_implementation_check;
ALTER TABLE source_connector_rollout_events
  ALTER COLUMN implementation SET DEFAULT 'connector',
  ALTER COLUMN implementation SET NOT NULL;
ALTER TABLE source_connector_rollout_events
  ADD CONSTRAINT source_connector_rollout_events_event_type_check
    CHECK (event_type IN ('execution', 'duration', 'lifecycle', 'dlq')),
  ADD CONSTRAINT source_connector_rollout_events_implementation_check
    CHECK (implementation = 'connector');

ALTER TABLE source_connector_rollout_metric_windows
  DROP COLUMN IF EXISTS parity_samples,
  DROP COLUMN IF EXISTS parity_mismatches,
  DROP COLUMN IF EXISTS legacy_p95_ms,
  DROP COLUMN IF EXISTS baseline_dlq_rate;

DROP TABLE IF EXISTS source_connector_retirement_evidence;

-- Complete grants whose runtime transport differs from the imported shape.
UPDATE source_connector_authority_grants
   SET granted_outbound_hosts = ARRAY['discord.com', 'gateway.discord.gg'],
       updated_at = now()
 WHERE connector_id = 'fyralis/discord';

UPDATE source_connector_authority_grants
   SET granted_outbound_hosts = ARRAY['cloudtrail.*.amazonaws.com'],
       updated_at = now()
 WHERE connector_id = 'fyralis/aws';

UPDATE source_connector_authority_grants
   SET granted_outbound_hosts = ARRAY[
         'quickbooks.api.intuit.com', 'oauth.platform.intuit.com'
       ],
       granted_scopes = ARRAY['com.intuit.quickbooks.accounting'],
       updated_at = now()
 WHERE connector_id = 'fyralis/quickbooks';

-- Preserve provider-specific resource selection as contract-owned install data.
INSERT INTO source_connector_installation_data (
  installation_id, tenant_id, namespace, generation, values
)
SELECT install.id, install.tenant_id, 'configuration', 1,
       jsonb_build_object(
         'external_installation_id', install.workspace_domain,
         'selected_resources', COALESCE(
           (SELECT jsonb_agg(watch.email_address ORDER BY watch.email_address)
              FROM gmail_mailbox_watches AS watch
             WHERE watch.gmail_installation_id = install.id
               AND watch.state IN ('pending', 'active')),
           '[]'::jsonb
         ),
         'service_account_email', install.service_account_email,
         'scope', install.scope,
         'inclusion_spec', install.inclusion_spec
       )
  FROM gmail_installations AS install
ON CONFLICT (installation_id, namespace) DO UPDATE
  SET values = EXCLUDED.values,
      generation = source_connector_installation_data.generation + 1,
      updated_at = now();

INSERT INTO source_connector_installation_data (
  installation_id, tenant_id, namespace, generation, values
)
SELECT install.id, install.tenant_id, 'google_watch', 1,
       jsonb_build_object(
         'topic_name', topic.topic_name,
         'subscription_name', topic.subscription_name,
         'email_addresses', COALESCE(
           (SELECT jsonb_agg(watch.email_address ORDER BY watch.email_address)
              FROM gmail_mailbox_watches AS watch
             WHERE watch.gmail_installation_id = install.id
               AND watch.state IN ('pending', 'active')),
           '[]'::jsonb
         )
       )
  FROM gmail_installations AS install
  LEFT JOIN LATERAL (
    SELECT value.topic_name, value.subscription_name
      FROM gmail_pubsub_topics AS value
     WHERE value.gmail_installation_id = install.id
       AND value.teardown_at IS NULL
     ORDER BY value.created_at DESC
     LIMIT 1
  ) AS topic ON TRUE
ON CONFLICT (installation_id, namespace) DO UPDATE
  SET values = EXCLUDED.values,
      generation = source_connector_installation_data.generation + 1,
      updated_at = now();

INSERT INTO source_connector_installation_data (
  installation_id, tenant_id, namespace, generation, values
)
SELECT install.id, install.tenant_id, 'configuration', 1,
       jsonb_build_object(
         'external_installation_id', install.workspace_domain,
         'selected_resources', COALESCE(
           (SELECT jsonb_agg(calendar.calendar_id ORDER BY calendar.calendar_id)
              FROM google_calendar_calendars AS calendar
             WHERE calendar.google_calendar_installation_id = install.id
               AND calendar.state IN ('pending', 'active')),
           '[]'::jsonb
         ),
         'service_account_email', install.service_account_email,
         'scope', install.scope,
         'inclusion_spec', install.inclusion_spec
       )
  FROM google_calendar_installations AS install
ON CONFLICT (installation_id, namespace) DO UPDATE
  SET values = EXCLUDED.values,
      generation = source_connector_installation_data.generation + 1,
      updated_at = now();

INSERT INTO source_connector_installation_data (
  installation_id, tenant_id, namespace, generation, values
)
SELECT install.id, install.tenant_id, 'configuration', 1,
       jsonb_build_object(
         'external_installation_id', install.workspace_domain,
         'selected_resources', COALESCE(
           (SELECT jsonb_agg(target.drive_id ORDER BY target.drive_id)
              FROM google_drive_targets AS target
             WHERE target.google_drive_installation_id = install.id
               AND target.state IN ('pending', 'active')),
           '[]'::jsonb
         ),
         'service_account_email', install.service_account_email,
         'scope', install.scope,
         'inclusion_spec', install.inclusion_spec,
         'include_shared_drives', install.include_shared_drives
       )
  FROM google_drive_installations AS install
ON CONFLICT (installation_id, namespace) DO UPDATE
  SET values = EXCLUDED.values,
      generation = source_connector_installation_data.generation + 1,
      updated_at = now();

INSERT INTO source_connector_installation_data (
  installation_id, tenant_id, namespace, generation, values
)
SELECT install.id, install.tenant_id, 'aws', 1,
       jsonb_build_object(
         'region', install.region,
         'regions', jsonb_build_array(install.region),
         'backfill_window_days', install.backfill_window_days
       )
  FROM aws_installations AS install
ON CONFLICT (installation_id, namespace) DO UPDATE
  SET values = EXCLUDED.values,
      generation = source_connector_installation_data.generation + 1,
      updated_at = now();

-- Imported MTProto/libsignal/AWS opaque credentials are not compatible with
-- the new declared slots. Fail closed and require configuration through
-- POST /integrations/{source}/configure rather than guessing secret contents.
UPDATE source_connector_installations
   SET desired_state = 'Maintenance',
       observed_phase = 'Maintenance',
       provenance = provenance || jsonb_build_object(
         'contract_only_cutover', '0230',
         'requires_reconfiguration', true
       ),
       updated_at = now()
 WHERE connector_id IN ('fyralis/telegram', 'fyralis/signal', 'fyralis/aws')
   AND provenance->>'migrated_by' = '0189';

COMMIT;
