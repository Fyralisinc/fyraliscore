-- =====================================================================
-- 0174_backfill_access_labels.sql — source labels and derived labels
-- =====================================================================

BEGIN;

INSERT INTO object_access_labels (
  tenant_id, object_kind, object_id, label, source, metadata
)
SELECT
  tenant_id,
  'resource',
  id,
  'classification:internal',
  'resource_kind',
  jsonb_build_object('resource_kind', kind)
FROM resources
ON CONFLICT (tenant_id, object_kind, object_id, label, source)
DO UPDATE SET metadata = EXCLUDED.metadata;

INSERT INTO object_access_labels (
  tenant_id, object_kind, object_id, label, source, metadata
)
SELECT
  tenant_id,
  'resource',
  id,
  'resource_kind:' || kind,
  'resource_kind',
  jsonb_build_object('resource_kind', kind)
FROM resources
ON CONFLICT (tenant_id, object_kind, object_id, label, source)
DO UPDATE SET metadata = EXCLUDED.metadata;

INSERT INTO object_access_labels (
  tenant_id, object_kind, object_id, label, source, metadata
)
SELECT
  tenant_id,
  'resource',
  id,
  CASE kind
    WHEN 'financial' THEN 'domain:financial'
    WHEN 'ip' THEN 'domain:ip'
    WHEN 'regulatory' THEN 'domain:regulatory'
    WHEN 'infrastructure' THEN 'domain:infrastructure'
    WHEN 'capacity' THEN 'domain:capacity'
    WHEN 'relational' THEN 'domain:customer'
  END,
  'resource_kind',
  jsonb_build_object('resource_kind', kind)
FROM resources
WHERE kind IN (
  'financial', 'ip', 'regulatory', 'infrastructure', 'capacity', 'relational'
)
ON CONFLICT (tenant_id, object_kind, object_id, label, source)
DO UPDATE SET metadata = EXCLUDED.metadata;

INSERT INTO object_access_labels (
  tenant_id, object_kind, object_id, label, source, metadata
)
SELECT
  tenant_id,
  'observation',
  id,
  'classification:internal',
  'source_channel',
  jsonb_build_object('source_channel', source_channel)
FROM observations
ON CONFLICT (tenant_id, object_kind, object_id, label, source)
DO UPDATE SET metadata = EXCLUDED.metadata;

WITH channel_labels AS (
  SELECT
    tenant_id,
    id,
    source_channel,
    lower(split_part(source_channel, ':', 1)) AS family
  FROM observations
),
mapped AS (
  SELECT tenant_id, id, source_channel, 'domain:financial' AS label
  FROM channel_labels
  WHERE family IN (
    'finance', 'ramp', 'mercury', 'brex', 'quickbooks', 'stripe', 'bank', 'ledger'
  )
  UNION ALL
  SELECT tenant_id, id, source_channel, 'channel:finance' AS label
  FROM channel_labels
  WHERE family IN (
    'finance', 'ramp', 'mercury', 'brex', 'quickbooks', 'stripe', 'bank', 'ledger'
  )
  UNION ALL
  SELECT tenant_id, id, source_channel, 'domain:legal' AS label
  FROM channel_labels
  WHERE family IN ('legal', 'docusign', 'ironclad')
  UNION ALL
  SELECT tenant_id, id, source_channel, 'channel:legal' AS label
  FROM channel_labels
  WHERE family IN ('legal', 'docusign', 'ironclad')
  UNION ALL
  SELECT tenant_id, id, source_channel, 'domain:infrastructure' AS label
  FROM channel_labels
  WHERE family IN ('aws', 'grafana', 'sentry', 'pagerduty', 'incident', 'security')
  UNION ALL
  SELECT tenant_id, id, source_channel, 'channel:incident' AS label
  FROM channel_labels
  WHERE family IN ('aws', 'grafana', 'sentry', 'pagerduty', 'incident', 'security')
)
INSERT INTO object_access_labels (
  tenant_id, object_kind, object_id, label, source, metadata
)
SELECT
  tenant_id,
  'observation',
  id,
  label,
  'source_channel',
  jsonb_build_object('source_channel', source_channel)
FROM mapped
ON CONFLICT (tenant_id, object_kind, object_id, label, source)
DO UPDATE SET metadata = EXCLUDED.metadata;

INSERT INTO object_provenance_edges (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind, metadata
)
SELECT
  tenant_id,
  'observation',
  id,
  'observation',
  cause_id,
  'observation_cause',
  jsonb_build_object('source_column', 'cause_id')
FROM observations
WHERE cause_id IS NOT NULL
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
  tenant_id,
  'observation',
  id,
  content->>'entity_kind',
  (content->>'entity_id')::uuid,
  'state_change_entity',
  jsonb_build_object('source', 'state_change')
FROM observations
WHERE kind = 'state_change'
  AND content ? 'entity_kind'
  AND content ? 'entity_id'
  AND content->>'entity_kind' IN (
    'observation', 'commitment', 'goal', 'decision', 'resource', 'model'
  )
  AND content->>'entity_id' ~* '^[0-9a-f-]{36}$'
ON CONFLICT (
  tenant_id, derived_kind, derived_id,
  source_kind, source_id, derivation_kind
)
DO UPDATE SET metadata = EXCLUDED.metadata;

INSERT INTO object_access_labels (
  tenant_id, object_kind, object_id, label, source, metadata
)
SELECT
  edge.tenant_id,
  edge.derived_kind,
  edge.derived_id,
  source_label.label,
  'provenance_backfill',
  jsonb_build_object(
    'source_kind', edge.source_kind,
    'source_id', edge.source_id::text,
    'source_label_source', source_label.source
  )
FROM object_provenance_edges edge
JOIN object_access_labels source_label
  ON source_label.tenant_id = edge.tenant_id
 AND source_label.object_kind = edge.source_kind
 AND source_label.object_id = edge.source_id
ON CONFLICT (tenant_id, object_kind, object_id, label, source)
DO UPDATE SET metadata = EXCLUDED.metadata;

COMMIT;
