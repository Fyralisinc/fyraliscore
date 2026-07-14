-- Backfill authority provenance and labels for persisted Ask Fyralis evidence.

WITH evidence_base AS (
    SELECT
        s.tenant_id,
        e.id AS evidence_id,
        lower(trim(e.source_kind)) AS source_kind,
        e.source_ref,
        COALESCE(e.raw_payload, '{}'::jsonb) AS raw_payload
    FROM ask_evidence_items e
    JOIN ask_retrieval_runs r ON r.id = e.retrieval_run_id
    JOIN ask_sessions s ON s.id = r.session_id
),
source_refs AS (
    SELECT
        tenant_id,
        evidence_id,
        CASE
            WHEN source_kind IN (
                'model', 'fyralis_model', 'synthesis_model', 'omitted_model'
            ) THEN 'model'
            WHEN source_kind IN ('observation', 'event', 'signal') THEN 'observation'
            WHEN source_kind IN ('resource', 'commitment', 'goal', 'decision') THEN source_kind
            ELSE NULL
        END AS source_kind,
        source_ref AS source_id
    FROM evidence_base
    WHERE source_ref IS NOT NULL

    UNION ALL

    SELECT
        b.tenant_id,
        b.evidence_id,
        'observation' AS source_kind,
        src.value::uuid AS source_id
    FROM evidence_base b
    CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE
            WHEN jsonb_typeof(b.raw_payload -> 'source_observation_ids') = 'array'
            THEN b.raw_payload -> 'source_observation_ids'
            ELSE '[]'::jsonb
        END
    ) AS src(value)
    WHERE b.source_kind = 'composed_chain'
      AND src.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    UNION ALL

    SELECT
        b.tenant_id,
        b.evidence_id,
        keys.source_kind,
        (b.raw_payload ->> keys.payload_key)::uuid AS source_id
    FROM evidence_base b
    CROSS JOIN LATERAL (
        VALUES
            ('source_model_id', 'model'),
            ('model_id', 'model'),
            ('fyralis_model_id', 'model'),
            ('source_observation_id', 'observation'),
            ('observation_id', 'observation')
    ) AS keys(payload_key, source_kind)
    WHERE b.raw_payload ? keys.payload_key
      AND (b.raw_payload ->> keys.payload_key)
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
),
deduped_refs AS (
    SELECT DISTINCT tenant_id, evidence_id, source_kind, source_id
    FROM source_refs
    WHERE source_kind IS NOT NULL
      AND source_id IS NOT NULL
)
INSERT INTO object_provenance_edges (
    tenant_id,
    derived_kind,
    derived_id,
    source_kind,
    source_id,
    derivation_kind,
    metadata
)
SELECT
    tenant_id,
    'evidence',
    evidence_id,
    source_kind,
    source_id,
    'ask_evidence_source',
    jsonb_build_object('artifact', 'ask_fyralis_evidence')
FROM deduped_refs
ON CONFLICT (
    tenant_id, derived_kind, derived_id, source_kind, source_id, derivation_kind
)
DO UPDATE SET metadata = EXCLUDED.metadata;

WITH evidence_base AS (
    SELECT
        s.tenant_id,
        e.id AS evidence_id,
        lower(trim(e.source_kind)) AS source_kind,
        e.source_ref,
        COALESCE(e.raw_payload, '{}'::jsonb) AS raw_payload
    FROM ask_evidence_items e
    JOIN ask_retrieval_runs r ON r.id = e.retrieval_run_id
    JOIN ask_sessions s ON s.id = r.session_id
),
source_refs AS (
    SELECT
        tenant_id,
        evidence_id,
        CASE
            WHEN source_kind IN (
                'model', 'fyralis_model', 'synthesis_model', 'omitted_model'
            ) THEN 'model'
            WHEN source_kind IN ('observation', 'event', 'signal') THEN 'observation'
            WHEN source_kind IN ('resource', 'commitment', 'goal', 'decision') THEN source_kind
            ELSE NULL
        END AS source_kind,
        source_ref AS source_id
    FROM evidence_base
    WHERE source_ref IS NOT NULL

    UNION ALL

    SELECT
        b.tenant_id,
        b.evidence_id,
        'observation' AS source_kind,
        src.value::uuid AS source_id
    FROM evidence_base b
    CROSS JOIN LATERAL jsonb_array_elements_text(
        CASE
            WHEN jsonb_typeof(b.raw_payload -> 'source_observation_ids') = 'array'
            THEN b.raw_payload -> 'source_observation_ids'
            ELSE '[]'::jsonb
        END
    ) AS src(value)
    WHERE b.source_kind = 'composed_chain'
      AND src.value ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

    UNION ALL

    SELECT
        b.tenant_id,
        b.evidence_id,
        keys.source_kind,
        (b.raw_payload ->> keys.payload_key)::uuid AS source_id
    FROM evidence_base b
    CROSS JOIN LATERAL (
        VALUES
            ('source_model_id', 'model'),
            ('model_id', 'model'),
            ('fyralis_model_id', 'model'),
            ('source_observation_id', 'observation'),
            ('observation_id', 'observation')
    ) AS keys(payload_key, source_kind)
    WHERE b.raw_payload ? keys.payload_key
      AND (b.raw_payload ->> keys.payload_key)
        ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
),
deduped_refs AS (
    SELECT DISTINCT tenant_id, evidence_id, source_kind, source_id
    FROM source_refs
    WHERE source_kind IS NOT NULL
      AND source_id IS NOT NULL
)
INSERT INTO object_access_labels (
    tenant_id,
    object_kind,
    object_id,
    label,
    source,
    metadata
)
SELECT
    refs.tenant_id,
    'evidence',
    refs.evidence_id,
    labels.label,
    'ask_evidence_source',
    jsonb_build_object(
        'source_kind', labels.object_kind,
        'source_id', labels.object_id::text,
        'source_label_source', labels.source
    )
FROM deduped_refs refs
JOIN object_access_labels labels
  ON labels.tenant_id = refs.tenant_id
 AND labels.object_kind = refs.source_kind
 AND labels.object_id = refs.source_id
ON CONFLICT (tenant_id, object_kind, object_id, label, source)
DO UPDATE SET metadata = EXCLUDED.metadata;
