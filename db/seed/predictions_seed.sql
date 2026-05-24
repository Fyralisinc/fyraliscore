-- =====================================================================
-- predictions_seed.sql — demo data for the Forecasts page
-- =====================================================================
-- Seeds 9 predictions (6 active + 3 resolved) and 24 supporting signals
-- against the dogfood tenant `00000000-0000-0000-0000-000000000001`.
-- Idempotent via fixed UUIDs + ON CONFLICT DO NOTHING.
--
-- Two predictions target the "Beacon" account so the Forecasts page
-- ties to the Decision Delta examples surfaced on the Today page.
--
-- Resolution dates are anchored relative to NOW() so the Active tab
-- always shows fresh-looking rows regardless of when the seed runs.
-- Resolved predictions are anchored 14-60 days in the past so the
-- Accuracy tab has enough bin samples to render non-empty.
-- =====================================================================

BEGIN;

-- Ensure the demo tenant exists. This is the only place the seed
-- depends on outside infrastructure.
INSERT INTO tenants (id, name, created_at)
VALUES ('00000000-0000-0000-0000-000000000001', 'fyralis_dogfood', now())
ON CONFLICT (id) DO NOTHING;


-- ---------------------------------------------------------------------
-- Active predictions
-- ---------------------------------------------------------------------

INSERT INTO predictions (
  id, tenant_id, status, statement, rationale, category,
  target_node_kind, target_node_id, target_label,
  confidence, confidence_basis, falsification_condition,
  key_drivers, impact, resolution_at, created_at, updated_at
) VALUES
  (
    '00000000-0000-0000-0000-000000000101',
    '00000000-0000-0000-0000-000000000001',
    'active',
    'Beacon renewal at risk',
    'Anchor-customer engagement has dropped and renewal sits two days out with no decision-maker reply.',
    'customer_risk',
    'resource', NULL, 'Beacon',
    0.78,
    'Trailing 30-day signal coverage with 4 authoritative sources.',
    'Beacon executive confirms renewal in writing before May 18.',
    '[{"label":"Open sync errors","delta_label":"+42%","direction":"up"},
      {"label":"Champion replies","delta_label":"-3","direction":"down"},
      {"label":"Last QBR","delta_label":"39 days ago","direction":"flat"}]'::jsonb,
    '{"arr_at_risk": 1200000, "customer_count": 1}'::jsonb,
    now() + interval '2 days',
    now() - interval '6 days',
    now() - interval '1 day'
  ),
  (
    '00000000-0000-0000-0000-000000000102',
    '00000000-0000-0000-0000-000000000001',
    'active',
    'Engineering capacity will exceed 90%',
    'Sustained utilization likely to hit saturation as integration commitments stack into the same sprint.',
    'capacity',
    'actor', NULL, 'Engineering',
    0.72,
    'Velocity trend over last 6 sprints + 3 new commitments accepted this week.',
    'Two commitments are explicitly deferred or re-staffed before May 22.',
    '[{"label":"Active commitments","delta_label":"+3","direction":"up"},
      {"label":"On-call load","delta_label":"4/6 weeks","direction":"up"}]'::jsonb,
    '{"capacity_pct": 92, "team_size": 9}'::jsonb,
    now() + interval '6 days',
    now() - interval '4 days',
    now() - interval '1 day'
  ),
  (
    '00000000-0000-0000-0000-000000000103',
    '00000000-0000-0000-0000-000000000001',
    'active',
    'Q3 delivery commitments at risk',
    'Three Q3 commitments depend on the same connector surface; one is already paused.',
    'delivery',
    'goal', NULL, 'Q3 deliverables',
    0.65,
    'Cross-commitment dependency analysis from model substrate.',
    'Paused commitment resumes by end of May with confirmed staffing.',
    '[{"label":"Blocked dependencies","delta_label":"3","direction":"flat"},
      {"label":"Slip risk","delta_label":"medium","direction":"up"}]'::jsonb,
    '{"arr_at_risk": 480000, "commitment_count": 3}'::jsonb,
    now() + interval '19 days',
    now() - interval '10 days',
    now() - interval '2 days'
  ),
  (
    '00000000-0000-0000-0000-000000000104',
    '00000000-0000-0000-0000-000000000001',
    'active',
    'ICP score will decline below 65',
    'Recent pipeline composition is drifting toward fit segments we historically lose.',
    'strategy',
    'model', NULL, 'ICP scoring model',
    0.58,
    'Pipeline-fit moving average over 8 weeks.',
    'Two new design-partner-grade accounts close before May 30.',
    '[{"label":"Pipeline ICP avg","delta_label":"-7","direction":"down"},
      {"label":"Won-deal ICP","delta_label":"71","direction":"flat"}]'::jsonb,
    '{"icp_score_projected": 62}'::jsonb,
    now() + interval '25 days',
    now() - interval '12 days',
    now() - interval '3 days'
  ),
  (
    '00000000-0000-0000-0000-000000000105',
    '00000000-0000-0000-0000-000000000001',
    'active',
    'Pricing decision will continue to block roadmap',
    'No owner has been named on the v2 pricing decision for nine days; downstream PRDs are paused.',
    'pricing',
    'decision', NULL, 'Pricing v2',
    0.55,
    'Open-decision age + downstream commitment freeze ratio.',
    'Owner assigned and a target decision date is set before May 28.',
    '[{"label":"Age open","delta_label":"9d","direction":"up"},
      {"label":"Paused PRDs","delta_label":"2","direction":"flat"}]'::jsonb,
    '{"blocked_commitment_count": 2}'::jsonb,
    now() + interval '28 days',
    now() - interval '9 days',
    now() - interval '1 day'
  ),
  (
    '00000000-0000-0000-0000-000000000106',
    '00000000-0000-0000-0000-000000000001',
    'active',
    'Design partner health will remain weak',
    'Beacon and one other design partner continue to under-engage despite outreach.',
    'partner',
    'resource', NULL, 'Beacon',
    0.46,
    'Partner-engagement composite over rolling 21 days.',
    'Two design partners attend the May product council and provide written feedback.',
    '[{"label":"Engagement composite","delta_label":"-12","direction":"down"},
      {"label":"Open feedback","delta_label":"0","direction":"flat"}]'::jsonb,
    '{"partner_count": 2}'::jsonb,
    now() + interval '34 days',
    now() - interval '15 days',
    now() - interval '5 days'
  )
ON CONFLICT (id) DO NOTHING;


-- ---------------------------------------------------------------------
-- Resolved predictions (for Accuracy tab + recent_resolutions list)
-- ---------------------------------------------------------------------

INSERT INTO predictions (
  id, tenant_id, status, statement, rationale, category,
  target_node_kind, target_node_id, target_label,
  confidence, confidence_basis, falsification_condition,
  key_drivers, impact, resolution_at, resolved_at,
  outcome, resolution_timeliness, created_at, updated_at
) VALUES
  (
    '00000000-0000-0000-0000-000000000201',
    '00000000-0000-0000-0000-000000000001',
    'resolved',
    'Salesforce sync stability will degrade to "Critical"',
    'Sync error rate trending up across two consecutive weeks.',
    'capacity',
    'model', NULL, 'Salesforce connector',
    0.74,
    'Connector reliability moving average.',
    'Sync error rate stays below 5% for 7 consecutive days.',
    '[{"label":"Error rate 7d","delta_label":"+38%","direction":"up"}]'::jsonb,
    '{"sla_impact": "watch_to_critical"}'::jsonb,
    now() - interval '14 days', now() - interval '14 days',
    'true', 'on_time',
    now() - interval '35 days', now() - interval '14 days'
  ),
  (
    '00000000-0000-0000-0000-000000000202',
    '00000000-0000-0000-0000-000000000001',
    'resolved',
    'Q2 hiring target met for senior backend role',
    'Pipeline of two finalists with closing interviews scheduled.',
    'capacity',
    'goal', NULL, 'Senior backend hire',
    0.68,
    'Recruiter-stage pipeline progression.',
    'No offer extended by April 30.',
    '[{"label":"Active finalists","delta_label":"2","direction":"flat"}]'::jsonb,
    '{"role_count": 1}'::jsonb,
    now() - interval '30 days', now() - interval '28 days',
    'true', 'early',
    now() - interval '60 days', now() - interval '28 days'
  ),
  (
    '00000000-0000-0000-0000-000000000203',
    '00000000-0000-0000-0000-000000000001',
    'resolved',
    'Mid-market expansion deal will close in March',
    'Champion alignment + verbal commit logged.',
    'customer_risk',
    'resource', NULL, 'Acme Robotics',
    0.62,
    'Champion confidence + procurement-stage signals.',
    'Deal slips past March 31 without signed contract.',
    '[{"label":"Procurement stage","delta_label":"legal_review","direction":"flat"}]'::jsonb,
    '{"arr_at_risk": 240000, "customer_count": 1}'::jsonb,
    now() - interval '45 days', now() - interval '42 days',
    'false', 'late',
    now() - interval '75 days', now() - interval '42 days'
  )
ON CONFLICT (id) DO NOTHING;


-- ---------------------------------------------------------------------
-- Supporting signals — 2-4 per prediction
-- ---------------------------------------------------------------------

INSERT INTO prediction_signals (
  id, prediction_id, source, title, ts, trust_tier, weight, ordinal
) VALUES
  -- Beacon renewal at risk
  ('00000000-0000-0000-0000-000000001101',
   '00000000-0000-0000-0000-000000000101',
   'salesforce', 'Beacon - last meeting 12 days ago',
   now() - interval '12 days', 'authoritative', 0.85, 0),
  ('00000000-0000-0000-0000-000000001102',
   '00000000-0000-0000-0000-000000000101',
   'slack', 'Beacon thread: no reply for 6 days',
   now() - interval '6 days', 'observed', 0.55, 1),
  ('00000000-0000-0000-0000-000000001103',
   '00000000-0000-0000-0000-000000000101',
   'product_telemetry', 'Beacon weekly active drop -38%',
   now() - interval '4 days', 'authoritative', 0.78, 2),
  ('00000000-0000-0000-0000-000000001104',
   '00000000-0000-0000-0000-000000000101',
   'github', 'Beacon-related integration issue reopened',
   now() - interval '2 days', 'observed', 0.5, 3),

  -- Engineering capacity will exceed 90%
  ('00000000-0000-0000-0000-000000001201',
   '00000000-0000-0000-0000-000000000102',
   'sprint_planning', 'Q2-S5 commitments accepted: +3',
   now() - interval '3 days', 'authoritative', 0.8, 0),
  ('00000000-0000-0000-0000-000000001202',
   '00000000-0000-0000-0000-000000000102',
   'oncall_rotation', 'Maya at 4/6 weeks on-call',
   now() - interval '5 days', 'authoritative', 0.6, 1),
  ('00000000-0000-0000-0000-000000001203',
   '00000000-0000-0000-0000-000000000102',
   'velocity', '6-sprint trailing velocity flat -2%',
   now() - interval '1 days', 'observed', 0.55, 2),

  -- Q3 delivery commitments at risk
  ('00000000-0000-0000-0000-000000001301',
   '00000000-0000-0000-0000-000000000103',
   'commitments', 'Salesforce sync overhaul paused',
   now() - interval '8 days', 'authoritative', 0.85, 0),
  ('00000000-0000-0000-0000-000000001302',
   '00000000-0000-0000-0000-000000000103',
   'commitments', 'Pricing tier migration depends on sync overhaul',
   now() - interval '8 days', 'authoritative', 0.7, 1),
  ('00000000-0000-0000-0000-000000001303',
   '00000000-0000-0000-0000-000000000103',
   'slack', 'Tom raised slip risk in standup',
   now() - interval '3 days', 'observed', 0.45, 2),

  -- ICP score will decline below 65
  ('00000000-0000-0000-0000-000000001401',
   '00000000-0000-0000-0000-000000000104',
   'icp_model', 'Pipeline ICP avg dropped to 64',
   now() - interval '5 days', 'authoritative', 0.7, 0),
  ('00000000-0000-0000-0000-000000001402',
   '00000000-0000-0000-0000-000000000104',
   'pipeline', '3 new accounts opened below 60 ICP',
   now() - interval '6 days', 'observed', 0.55, 1),
  ('00000000-0000-0000-0000-000000001403',
   '00000000-0000-0000-0000-000000000104',
   'won_deals', 'Closed-won ICP stable at 71',
   now() - interval '12 days', 'authoritative', 0.5, 2),

  -- Pricing decision will continue to block roadmap
  ('00000000-0000-0000-0000-000000001501',
   '00000000-0000-0000-0000-000000000105',
   'decisions', 'Pricing v2 decision open 9 days, no owner',
   now() - interval '9 days', 'authoritative', 0.8, 0),
  ('00000000-0000-0000-0000-000000001502',
   '00000000-0000-0000-0000-000000000105',
   'prd_tracker', 'Tiering PRD paused awaiting pricing',
   now() - interval '7 days', 'observed', 0.6, 1),
  ('00000000-0000-0000-0000-000000001503',
   '00000000-0000-0000-0000-000000000105',
   'slack', 'Owen flagged blocker in #strategy',
   now() - interval '3 days', 'observed', 0.4, 2),

  -- Design partner health will remain weak
  ('00000000-0000-0000-0000-000000001601',
   '00000000-0000-0000-0000-000000000106',
   'partner_engagement', 'Engagement composite -12 over 21 days',
   now() - interval '10 days', 'authoritative', 0.75, 0),
  ('00000000-0000-0000-0000-000000001602',
   '00000000-0000-0000-0000-000000000106',
   'feedback_log', 'Zero written feedback in last 30 days',
   now() - interval '5 days', 'observed', 0.55, 1),

  -- Resolved: Salesforce sync stability
  ('00000000-0000-0000-0000-000000002101',
   '00000000-0000-0000-0000-000000000201',
   'sync_log', 'Error rate doubled week-over-week',
   now() - interval '21 days', 'authoritative', 0.85, 0),
  ('00000000-0000-0000-0000-000000002102',
   '00000000-0000-0000-0000-000000000201',
   'oncall_rotation', 'Two pages during last 7 days',
   now() - interval '17 days', 'observed', 0.5, 1),

  -- Resolved: Q2 hiring target met
  ('00000000-0000-0000-0000-000000002201',
   '00000000-0000-0000-0000-000000000202',
   'recruiter', '2 finalists in closing interviews',
   now() - interval '40 days', 'authoritative', 0.75, 0),
  ('00000000-0000-0000-0000-000000002202',
   '00000000-0000-0000-0000-000000000202',
   'hiring_pipeline', 'Offer accepted before April 30',
   now() - interval '28 days', 'authoritative', 0.9, 1),

  -- Resolved: Mid-market expansion deal
  ('00000000-0000-0000-0000-000000002301',
   '00000000-0000-0000-0000-000000000203',
   'salesforce', 'Stage moved to legal_review',
   now() - interval '60 days', 'authoritative', 0.7, 0),
  ('00000000-0000-0000-0000-000000002302',
   '00000000-0000-0000-0000-000000000203',
   'champion_log', 'Verbal commit recorded',
   now() - interval '70 days', 'observed', 0.55, 1)
ON CONFLICT (id) DO NOTHING;

COMMIT;
