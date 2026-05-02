-- =====================================================================
-- 0023_demo_infrastructure.sql — Demo tenant + session + cost-cap tables
-- =====================================================================
-- Adds the substrate for the VC-pitch demo flow per DEMO-BUILD-PLAN
-- Session 1. Three new tables — tenants (the canonical registry that
-- every existing tenant_id implicitly references), demo_configs (the
-- per-company demo settings: model routing, cost cap, determinism),
-- and demo_sessions (one row per VC pitch session).
--
-- Idempotent — every CREATE / ALTER guards with IF NOT EXISTS so the
-- migration is safe to re-run after partial application.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- tenants — canonical registry. Existing rows reference tenant_id as
-- a free-floating UUID; this table makes the demo flag a queryable
-- column without rewriting every consumer. Non-demo tenants are
-- represented by absence (look-up returns NULL → assume non-demo).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenants (
  id UUID PRIMARY KEY,
  name TEXT NOT NULL DEFAULT 'unnamed',
  is_demo BOOLEAN NOT NULL DEFAULT FALSE,
  demo_config_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  archived_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS tenants_is_demo_idx ON tenants (is_demo)
  WHERE is_demo = TRUE;

-- ---------------------------------------------------------------------
-- demo_configs — per-company demo settings. One row per preloaded
-- company (Truss, Northwind, Meridian).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demo_configs (
  id UUID PRIMARY KEY,
  company_id TEXT NOT NULL UNIQUE,         -- 'truss' | 'northwind' | 'meridian'
  name TEXT NOT NULL,
  description TEXT NOT NULL,
  tagline TEXT NOT NULL DEFAULT '',
  snapshot_uri TEXT NOT NULL,              -- where to find the SQL snapshot
  model_routing JSONB NOT NULL DEFAULT '{}'::jsonb,
                                           -- e.g. {"think":"haiku","render":"haiku"}
  cost_cap_usd_per_session NUMERIC(10, 4) NOT NULL DEFAULT 5.0000,
  notifications_suppressed BOOLEAN NOT NULL DEFAULT TRUE,
  determinism_seed INTEGER,                -- if set, Think uses temp=0 + seed
  reset_on_session_end BOOLEAN NOT NULL DEFAULT TRUE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- FK back to tenants.demo_config_id once both tables exist.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'tenants_demo_config_id_fkey'
  ) THEN
    ALTER TABLE tenants
      ADD CONSTRAINT tenants_demo_config_id_fkey
      FOREIGN KEY (demo_config_id) REFERENCES demo_configs(id);
  END IF;
END $$;

-- ---------------------------------------------------------------------
-- demo_sessions — one row per VC pitch. Bound to the cloned tenant.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demo_sessions (
  id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id),
  demo_config_id UUID NOT NULL REFERENCES demo_configs(id),
  ceo_actor_id UUID,                       -- the actor whose token is issued
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_active_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at TIMESTAMPTZ,
  end_reason TEXT,                         -- 'user_ended' | 'inactivity' | 'cost_cap'
  total_cost_usd NUMERIC(10, 6) NOT NULL DEFAULT 0,
  signals_injected INTEGER NOT NULL DEFAULT 0,
  actions_taken INTEGER NOT NULL DEFAULT 0,
  cost_cap_breached_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS demo_sessions_tenant_idx
  ON demo_sessions (tenant_id);
CREATE INDEX IF NOT EXISTS demo_sessions_active_idx
  ON demo_sessions (started_at DESC)
  WHERE ended_at IS NULL;

-- ---------------------------------------------------------------------
-- demo_session_costs — append-only ledger of LLM calls within a session.
-- Lets the cost-cap query be a single SELECT SUM rather than racing
-- updates on demo_sessions.total_cost_usd.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS demo_session_costs (
  id UUID PRIMARY KEY,
  demo_session_id UUID NOT NULL REFERENCES demo_sessions(id),
  call_kind TEXT NOT NULL,                 -- 'think' | 'render' | 'entity_resolver' | ...
  model_name TEXT NOT NULL,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  cost_usd NUMERIC(10, 6) NOT NULL DEFAULT 0,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS demo_session_costs_session_idx
  ON demo_session_costs (demo_session_id, occurred_at DESC);

-- ---------------------------------------------------------------------
-- Seed the three preloaded company configs. UPSERT so re-running the
-- migration won't error and operators can update the snapshot URI by
-- editing the migration and re-running.
-- ---------------------------------------------------------------------
INSERT INTO demo_configs (
  id, company_id, name, description, tagline, snapshot_uri,
  model_routing, cost_cap_usd_per_session, determinism_seed
) VALUES
  (
    '00000000-0000-7d23-8000-000000000001'::uuid,
    'truss',
    'Truss',
    '40-person AI-native developer infrastructure company. Just closed a $12M Series A. The founder is operating at full cognitive load — too many customers to track in her head, too many parallel workstreams, too many decisions whose rationale is fading. Despite the small headcount, the company ships at the throughput of what would have been a 100-person team three years ago. Company OS is the substrate that makes scale legible before the chaos arrives.',
    'Series A, founder at full cognitive load',
    'demo/snapshots/truss-v1.sql.zst',
    '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
    5.00,
    42
  ),
  (
    '00000000-0000-7d23-8000-000000000002'::uuid,
    'northwind',
    'Northwind Software',
    'Series B SaaS, 180 employees, $14M ARR, growing 80% YoY. Building a modern HR platform for mid-market companies. The CEO is past the founder-cognitive-overload stage — the company has structure now — but coordination across functions is starting to consume executive attention. Most things are working; the action list helps you stay ahead of the small fires before they become big ones.',
    'Series B, healthy growth, normal Tuesday',
    'demo/snapshots/northwind-v1.sql.zst',
    '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
    5.00,
    43
  ),
  (
    '00000000-0000-7d23-8000-000000000003'::uuid,
    'meridian',
    'Meridian Industrial',
    'Series C enterprise software, 1100 employees, $85M ARR, growing 45% YoY. Building supply chain optimization software for industrial manufacturers. A major customer ($4.2M ARR) is escalating about a missed feature commitment. The action list surfaces what is at risk and what to do about it. This is the product on a real day at a real company.',
    'Series C, $4.2M ARR customer escalating',
    'demo/snapshots/meridian-v1.sql.zst',
    '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
    7.50,
    44
  )
ON CONFLICT (company_id) DO UPDATE
  SET name = EXCLUDED.name,
      description = EXCLUDED.description,
      tagline = EXCLUDED.tagline,
      snapshot_uri = EXCLUDED.snapshot_uri,
      model_routing = EXCLUDED.model_routing,
      cost_cap_usd_per_session = EXCLUDED.cost_cap_usd_per_session,
      determinism_seed = EXCLUDED.determinism_seed;

COMMIT;
