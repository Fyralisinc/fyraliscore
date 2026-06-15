-- 0134_rls_empty_current_tenant_permissive_default.sql
--
-- Some pooled connections can observe app.current_tenant as an empty string
-- after a prior SET LOCAL transaction ends. Policies that cast
-- current_setting(..., true)::uuid directly then fail before they can apply
-- the intended permissive-default branch for pre-tenant lookup paths.
--
-- Keep the established RLS contract:
--   * no tenant bound: allow cross-tenant resolver/setup reads
--   * tenant bound: isolate to that tenant
--   * writes with a tenant bound must match that tenant

BEGIN;

DROP POLICY IF EXISTS tenant_isolation ON provider_installations;
CREATE POLICY tenant_isolation ON provider_installations
    USING (
        NULLIF(current_setting('app.current_tenant', true), '') IS NULL
        OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
    )
    WITH CHECK (
        NULLIF(current_setting('app.current_tenant', true), '') IS NULL
        OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
    );

DO $$
DECLARE
  t TEXT;
  tenant_tables TEXT[] := ARRAY[
    'actors', 'observations', 'models', 'goals', 'commitments',
    'decisions', 'resources', 'resource_transactions', 'entity_aliases',
    'actor_sessions', 'think_trigger_queue', 'entity_review_queue',
    'relationship_maintenance_log',
    'model_reeval_queue', 'think_region_lock_log',
    'applied_triggers', 'think_runs', 'model_reeval_dead_letter',
    'think_anomalies_raw',
    'signal_memory_fabric', 'pattern_candidates',
    'calibration_stats', 'calibration_offsets',
    'realtime_replay_cursors', 'orphan_log',
    'actor_roles', 'shared_channels', 'access_override_log',
    'pending_post_commit_actions',
    'think_run_costs', 'view_ceo_cache', 'view_render_costs',
    'anomaly_thresholds', 'dedup_keys_seen',
    'demo_sessions',
    'card_conversations',
    'model_watchers',
    'reconciliation_events', 'audit_events', 'model_edges',
    'topo_dirty_queue', 'model_neighborhoods',
    'model_neighborhood_membership', 'topology_events'
  ];
BEGIN
  FOREACH t IN ARRAY tenant_tables LOOP
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_schema = 'public' AND table_name = t
    ) THEN
      CONTINUE;
    END IF;

    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I '
      'USING ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ') '
      'WITH CHECK ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ')',
      t
    );
  END LOOP;
END $$;

DO $$
DECLARE t TEXT;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'gmail_installations',
    'gmail_install_audit',
    'gmail_pubsub_topics',
    'gmail_mailbox_watches',
    'gmail_mailbox_optouts',
    'gmail_threads_canonical',
    'gmail_thread_members',
    'gmail_read_audit'
  ]
  LOOP
    EXECUTE format(
      'DROP POLICY IF EXISTS %I_tenant_isolation ON %I',
      t, t
    );
    EXECUTE format(
      'CREATE POLICY %I_tenant_isolation ON %I '
      'USING ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ') '
      'WITH CHECK ('
      '  NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL'
      '  OR tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ')',
      t, t
    );
  END LOOP;
END $$;

COMMIT;
