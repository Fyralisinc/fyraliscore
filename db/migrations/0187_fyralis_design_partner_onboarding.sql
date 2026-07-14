-- =====================================================================
-- 0187_fyralis_design_partner_onboarding.sql
--   Hosted-portal onboarding intent registry for Design Partner BYOC
-- =====================================================================
-- These tables own the commercial/setup metadata that exists before the
-- customer-cloud data plane is deployed. They deliberately store identifiers,
-- company/setup-owner metadata, cloud shape, and lifecycle status only.
--
-- They must never store source credentials, cloud credentials, raw logs,
-- prompts, embeddings, source payloads, customer records, private URLs,
-- evidence bodies, or provider tokens.
-- =====================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS fyralis_customers (
  customer_id TEXT PRIMARY KEY CHECK (
    customer_id ~ '^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'
  ),
  company_name TEXT NOT NULL CHECK (length(trim(company_name)) BETWEEN 2 AND 200),
  selected_plan_code TEXT NOT NULL CHECK (
    selected_plan_code IN ('design_partner_byoc_pilot')
  ),
  status TEXT NOT NULL DEFAULT 'pilot' CHECK (
    status IN ('prospect', 'pilot', 'active', 'paused', 'cancelled')
  ),
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_onboarding_metadata_only' CHECK (
    stored_scope = 'sanitized_onboarding_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fyralis_customers_status_idx
  ON fyralis_customers (status, created_at DESC);

CREATE TABLE IF NOT EXISTS fyralis_onboarding_intents (
  intent_id TEXT PRIMARY KEY CHECK (intent_id ~ '^ofi_[0-9a-f]{32}$'),
  plan_code TEXT NOT NULL CHECK (
    plan_code IN ('design_partner_byoc_pilot', 'enterprise_byoc')
  ),
  procurement_channel TEXT NOT NULL CHECK (
    procurement_channel IN ('design_partner', 'sales', 'direct', 'aws_marketplace', 'private_offer')
  ),
  entrypoint TEXT NOT NULL DEFAULT 'get_fyralis' CHECK (
    entrypoint ~ '^[A-Za-z0-9_.:-]{1,100}$'
  ),
  status TEXT NOT NULL DEFAULT 'draft' CHECK (
    status IN (
      'draft',
      'intake_submitted',
      'workspace_created',
      'commercial_review',
      'cancelled'
    )
  ),
  customer_id TEXT REFERENCES fyralis_customers(customer_id),
  tenant_id UUID REFERENCES tenants(id),
  deployment_id TEXT CHECK (
    deployment_id IS NULL
    OR deployment_id ~ '^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'
  ),
  company_name TEXT CHECK (
    company_name IS NULL OR length(trim(company_name)) BETWEEN 2 AND 200
  ),
  setup_owner_email TEXT CHECK (
    setup_owner_email IS NULL
    OR setup_owner_email ~* '^[^@\s]+@[^@\s]+\.[^@\s]+$'
  ),
  target_cloud TEXT CHECK (
    target_cloud IS NULL OR target_cloud IN ('aws')
  ),
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_onboarding_metadata_only' CHECK (
    stored_scope = 'sanitized_onboarding_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fyralis_onboarding_intents_status_idx
  ON fyralis_onboarding_intents (status, created_at DESC);

CREATE INDEX IF NOT EXISTS fyralis_onboarding_intents_customer_idx
  ON fyralis_onboarding_intents (customer_id, created_at DESC)
  WHERE customer_id IS NOT NULL;

ALTER TABLE fyralis_onboarding_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE fyralis_onboarding_intents FORCE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS fyralis_byoc_deployments (
  deployment_id TEXT PRIMARY KEY CHECK (
    deployment_id ~ '^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'
  ),
  customer_id TEXT NOT NULL REFERENCES fyralis_customers(customer_id),
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  intent_id TEXT NOT NULL UNIQUE REFERENCES fyralis_onboarding_intents(intent_id),
  cloud_provider TEXT NOT NULL CHECK (cloud_provider IN ('aws')),
  region TEXT,
  environment TEXT NOT NULL DEFAULT 'pilot' CHECK (
    environment IN ('pilot', 'staging', 'production')
  ),
  runtime TEXT NOT NULL DEFAULT 'kubernetes' CHECK (runtime IN ('kubernetes')),
  status TEXT NOT NULL DEFAULT 'planned' CHECK (
    status IN (
      'planned',
      'package_ready',
      'preflight_passed',
      'deployed',
      'healthy',
      'needs_attention',
      'cancelled'
    )
  ),
  artifact_revision TEXT CHECK (
    artifact_revision IS NULL OR artifact_revision ~ '^[A-Za-z0-9_.:-]{1,100}$'
  ),
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_onboarding_metadata_only' CHECK (
    stored_scope = 'sanitized_onboarding_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fyralis_byoc_deployments_customer_idx
  ON fyralis_byoc_deployments (customer_id, created_at DESC);

CREATE INDEX IF NOT EXISTS fyralis_byoc_deployments_tenant_idx
  ON fyralis_byoc_deployments (tenant_id, status);

ALTER TABLE fyralis_byoc_deployments ENABLE ROW LEVEL SECURITY;
ALTER TABLE fyralis_byoc_deployments FORCE ROW LEVEL SECURITY;

CREATE TABLE IF NOT EXISTS fyralis_onboarding_events (
  event_id UUID PRIMARY KEY,
  intent_id TEXT NOT NULL REFERENCES fyralis_onboarding_intents(intent_id),
  customer_id TEXT CHECK (
    customer_id IS NULL OR customer_id ~ '^cus_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'
  ),
  deployment_id TEXT CHECK (
    deployment_id IS NULL
    OR deployment_id ~ '^dep_[A-Za-z0-9][A-Za-z0-9_-]{5,79}$'
  ),
  event_type TEXT NOT NULL CHECK (
    event_type IN (
      'plan_selected',
      'design_partner_intake_submitted',
      'workspace_created',
      'status_changed'
    )
  ),
  actor TEXT NOT NULL DEFAULT 'system' CHECK (length(trim(actor)) BETWEEN 1 AND 200),
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  stored_scope TEXT NOT NULL DEFAULT 'sanitized_onboarding_metadata_only' CHECK (
    stored_scope = 'sanitized_onboarding_metadata_only'
  ),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS fyralis_onboarding_events_intent_idx
  ON fyralis_onboarding_events (intent_id, created_at DESC);

CREATE INDEX IF NOT EXISTS fyralis_onboarding_events_customer_idx
  ON fyralis_onboarding_events (customer_id, created_at DESC)
  WHERE customer_id IS NOT NULL;

COMMIT;
