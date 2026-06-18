-- 0152_clarification_requests.sql
--
-- Durable user-facing clarification queue.
--
-- Subsystems use this when they have enough evidence to ask a bounded
-- question but not enough evidence to safely canonicalize, merge, classify, or
-- discard on their own. Examples: ambiguous actor identity, customer/vendor
-- classification, entity resolution, ontology gaps, and representation-budget
-- gaps.

BEGIN;

CREATE TABLE IF NOT EXISTS clarification_requests (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  kind TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open' CHECK (
    status IN ('open', 'answered', 'dismissed', 'expired', 'superseded')
  ),
  priority TEXT NOT NULL DEFAULT 'normal' CHECK (
    priority IN ('low', 'normal', 'high', 'critical')
  ),
  question TEXT NOT NULL,
  explanation TEXT NOT NULL DEFAULT '',
  object_kind TEXT NOT NULL,
  object_id UUID,
  object_key TEXT,
  source_observation_id UUID,
  model_id UUID REFERENCES models(id) ON DELETE SET NULL,
  options JSONB NOT NULL DEFAULT '[]'::jsonb,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  answer JSONB,
  answered_by UUID REFERENCES actors(id) ON DELETE SET NULL,
  answered_at TIMESTAMPTZ,
  dismissed_reason TEXT,
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS clarification_requests_open_object_idx
  ON clarification_requests (
    tenant_id,
    kind,
    object_kind,
    COALESCE(object_key, object_id::text)
  )
  WHERE status = 'open'
    AND (object_key IS NOT NULL OR object_id IS NOT NULL);

CREATE INDEX IF NOT EXISTS clarification_requests_open_idx
  ON clarification_requests (
    tenant_id,
    status,
    priority,
    created_at DESC
  )
  WHERE status = 'open';

CREATE INDEX IF NOT EXISTS clarification_requests_observation_idx
  ON clarification_requests (tenant_id, source_observation_id)
  WHERE source_observation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS clarification_requests_payload_gin
  ON clarification_requests USING gin (payload);

ALTER TABLE clarification_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE clarification_requests FORCE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation ON clarification_requests;
CREATE POLICY tenant_isolation ON clarification_requests
  USING (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  )
  WITH CHECK (
    NULLIF(current_setting('app.current_tenant', true), '') IS NULL
    OR tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid
  );

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'company_os') THEN
    GRANT SELECT, INSERT, UPDATE, DELETE
      ON TABLE clarification_requests TO company_os;
  END IF;
END
$$;

COMMIT;
