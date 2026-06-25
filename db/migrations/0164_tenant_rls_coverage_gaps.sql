-- =====================================================================
-- 0164_tenant_rls_coverage_gaps.sql
-- =====================================================================
-- Enable and force row-level security on tenant-scoped tables that still
-- carried tenant_id without an RLS policy. This preserves the current
-- compatibility policy shape for unbound app.current_tenant; the strict
-- no-unbound policy migration remains a separate rollout after all production
-- repositories/workers are tenant-bound.
-- =====================================================================

BEGIN;

DO $$
DECLARE
  t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'card_exchanges',
    'circuit_breaker_state',
    'extension_audit_log',
    'extension_egress',
    'inquiry_evidence_items',
    'inquiry_question_runs',
    'inquiry_sessions',
    'model_answerability_index',
    'model_belief_addresses',
    'model_composition_members',
    'model_pair_evidence',
    'model_provenance',
    'model_scope_actors',
    'model_scope_entities',
    'model_search_documents',
    'model_sparse_terms',
    'onboarding_triggers',
    'recommendation_feedback_stats',
    'relation_claims',
    'relation_edge_projections',
    'relation_evidence',
    'relation_instances',
    'relation_participants',
    'relationship_candidates',
    'tenant_flags',
    'think_run_artifacts',
    'whatsapp_installations',
    'workflow_states'
  ]
  LOOP
    IF to_regclass(format('public.%I', t)) IS NOT NULL THEN
      EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
      EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
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
    END IF;
  END LOOP;
END $$;

COMMIT;
