-- =====================================================================
-- 0028_pelago_demo_config.sql — Register the Pelago demo company
-- =====================================================================
-- Pelago is the fourth demo company alongside Truss / Northwind /
-- Meridian. It is the first whose corpus + Models are sourced from
-- the LSOB simulator + curated synthesis (corpora/pelago/), rather
-- than hand-authored.
--
-- Idempotent — ON CONFLICT DO UPDATE keeps re-runs safe.
-- =====================================================================

BEGIN;

INSERT INTO demo_configs (
  id, company_id, name, description, tagline, snapshot_uri,
  model_routing, cost_cap_usd_per_session, determinism_seed
) VALUES (
  '00000000-0000-7d23-8000-000000000004'::uuid,
  'pelago',
  'Pelago',
  'Series A B2B SaaS revenue-intelligence platform. 35 people, $5.8M ARR, 28 customers (8 design partners + 12 mid-market + 8 prospects). Just closed a $14M Series A. The CEO/founder Diana Mercer is third-time founder, ex-VP Sales; CTO Sanjay Iyer owns the data + forecast stack. The company is 9 months in: an anchor design partner has churned, the VP Eng departed mid-year, and the org has just reorganized around integration surfaces. The action list surfaces what the founder still has to decide on a Tuesday morning.',
  'Series A, multi-shock year, founder running on signals',
  'demo/snapshots/pelago-v1.sql.zst',
  '{"think":"haiku","render":"haiku","entity_resolver":"haiku"}'::jsonb,
  5.00,
  42
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
