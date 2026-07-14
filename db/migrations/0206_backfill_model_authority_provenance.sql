-- =====================================================================
-- 0206_backfill_model_authority_provenance.sql — existing Model sources
-- =====================================================================

BEGIN;

INSERT INTO object_provenance_edges (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind, metadata
)
SELECT
  tenant_id,
  'model',
  id,
  'observation',
  born_from_event_id,
  'model_born_from_event',
  jsonb_build_object('source_column', 'born_from_event_id')
FROM models
WHERE born_from_event_id IS NOT NULL
ON CONFLICT (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind
)
DO UPDATE SET metadata = EXCLUDED.metadata;

INSERT INTO object_provenance_edges (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind, metadata
)
SELECT
  m.tenant_id,
  'model',
  m.id,
  'observation',
  source_id,
  'model_supporting_event',
  jsonb_build_object('source_column', 'supporting_event_ids')
FROM models m
CROSS JOIN LATERAL unnest(m.supporting_event_ids) AS source_id
ON CONFLICT (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind
)
DO UPDATE SET metadata = EXCLUDED.metadata;

INSERT INTO object_provenance_edges (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind, metadata
)
SELECT
  m.tenant_id,
  'model',
  m.id,
  'model',
  source_id,
  'model_supporting_model',
  jsonb_build_object('source_column', 'supporting_model_ids')
FROM models m
CROSS JOIN LATERAL unnest(m.supporting_model_ids) AS source_id
ON CONFLICT (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind
)
DO UPDATE SET metadata = EXCLUDED.metadata;

INSERT INTO object_provenance_edges (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind, metadata
)
SELECT
  m.tenant_id,
  'model',
  m.id,
  'model',
  source_id,
  'model_contributing_model',
  jsonb_build_object('source_column', 'contributing_models')
FROM models m
CROSS JOIN LATERAL unnest(m.contributing_models) AS source_id
ON CONFLICT (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind
)
DO UPDATE SET metadata = EXCLUDED.metadata;

COMMIT;
