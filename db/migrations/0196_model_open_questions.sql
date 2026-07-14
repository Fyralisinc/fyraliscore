-- 0196_model_open_questions.sql
--
-- First-class unresolved uncertainty for the Model layer.
--
-- Open questions are Model-layer facets: they attach to a belief, describe what
-- evidence would materially improve that belief, and can drive post-commit T4
-- search without adding more columns or proposition kinds to `models`.

BEGIN;

CREATE TABLE IF NOT EXISTS model_open_questions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL,
  model_id UUID NOT NULL REFERENCES models(id) ON DELETE CASCADE,
  question TEXT NOT NULL,
  question_type TEXT NOT NULL,
  rationale TEXT,
  priority DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK (
    priority >= 0.0 AND priority <= 1.0
  ),
  status TEXT NOT NULL DEFAULT 'open' CHECK (
    status IN (
      'open',
      'resolved',
      'stale',
      'superseded',
      'duplicate',
      'archived'
    )
  ),
  expected_resolution_signal JSONB NOT NULL DEFAULT '{}'::jsonb,
  search_signature JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_event_id UUID,
  source_model_ids UUID[] NOT NULL DEFAULT '{}',
  dedupe_key TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_searched_at TIMESTAMPTZ,
  next_search_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ,
  resolution_model_id UUID REFERENCES models(id) ON DELETE SET NULL,
  resolution_note TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS model_open_questions_open_dedupe_idx
  ON model_open_questions (tenant_id, model_id, question_type, dedupe_key)
  WHERE status = 'open';

CREATE INDEX IF NOT EXISTS model_open_questions_due_idx
  ON model_open_questions (tenant_id, status, next_search_at, priority DESC)
  WHERE status = 'open';

CREATE INDEX IF NOT EXISTS model_open_questions_model_status_idx
  ON model_open_questions (tenant_id, model_id, status);

CREATE INDEX IF NOT EXISTS model_open_questions_source_models_idx
  ON model_open_questions USING gin (source_model_ids);

CREATE INDEX IF NOT EXISTS model_open_questions_expected_signal_idx
  ON model_open_questions USING gin (expected_resolution_signal);

CREATE INDEX IF NOT EXISTS model_open_questions_search_signature_idx
  ON model_open_questions USING gin (search_signature);

ALTER TABLE model_open_questions ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_open_questions FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON model_open_questions;
CREATE POLICY tenant_isolation ON model_open_questions
USING (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
)
WITH CHECK (
  tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
);

DO $$
DECLARE
  existing_check TEXT;
BEGIN
  IF to_regclass('model_events') IS NOT NULL THEN
    SELECT conname
    INTO existing_check
    FROM pg_constraint
    WHERE conrelid = 'model_events'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%event_type%'
    LIMIT 1;

    IF existing_check IS NOT NULL THEN
      EXECUTE format('ALTER TABLE model_events DROP CONSTRAINT %I', existing_check);
    END IF;

    ALTER TABLE model_events
      ADD CONSTRAINT model_events_event_type_valid CHECK (
        event_type IN (
          'model.created',
          'model.updated',
          'model.archived',
          'model.relation_changed',
          'model.open_question_changed'
        )
      );
  END IF;
END $$;

DO $$
DECLARE
  existing_check TEXT;
BEGIN
  IF to_regclass('pending_post_commit_actions') IS NOT NULL THEN
    SELECT conname
    INTO existing_check
    FROM pg_constraint
    WHERE conrelid = 'pending_post_commit_actions'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) LIKE '%action_kind%'
    LIMIT 1;

    IF existing_check IS NOT NULL THEN
      EXECUTE format(
        'ALTER TABLE pending_post_commit_actions DROP CONSTRAINT %I',
        existing_check
      );
    END IF;

    ALTER TABLE pending_post_commit_actions
      ADD CONSTRAINT pending_post_commit_actions_action_kind_check
      CHECK (
        action_kind IN (
          'publish_anomalies',
          'schedule_predictions',
          'broadcast_realtime',
          'invalidate_metrics',
          'materialize_projections',
          'discover_model_edges',
          'search_open_questions'
        )
      );
  END IF;
END $$;

COMMIT;
