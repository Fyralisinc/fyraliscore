-- 0113_compact_model_search_documents.sql
-- Keep rich Model propositions, but compact the text sidecar used by SAGE
-- literal search. Operational facets can carry evidence snippets and
-- attributes; dumping the full proposition JSON into search_text makes every
-- question-only lexical scan do too much string work.

CREATE OR REPLACE FUNCTION compact_model_scope_entities_text(
  scope_entities jsonb
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  WITH raw_entities AS (
    SELECT entity, ordinality
    FROM jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(scope_entities) = 'array' THEN scope_entities
        ELSE '[]'::jsonb
      END
    ) WITH ORDINALITY AS item(entity, ordinality)
  ),
  compact_entities AS (
    SELECT
      ordinality,
      left(
        regexp_replace(
          concat_ws(
            ' ',
            nullif(entity->>'type', ''),
            nullif(entity->>'id', '')
          ),
          '\s+',
          ' ',
          'g'
        ),
        CASE
          WHEN entity->>'type' IN ('workflow', 'action', 'form_control', 'sort_field') THEN 140
          ELSE 96
        END
      ) AS token
    FROM raw_entities
    WHERE nullif(entity->>'id', '') IS NOT NULL
      AND coalesce(entity->>'type', '') NOT IN (
        'route_from',
        'route_to',
        'phase_from',
        'phase_to',
        'ui_label_added',
        'ui_label_removed'
      )
      AND coalesce(entity->>'type', '') NOT LIKE 'route%'
    ORDER BY ordinality
    LIMIT 48
  )
  SELECT lower(coalesce(string_agg(token, ' ' ORDER BY ordinality), ''))
  FROM compact_entities
  WHERE token <> '';
$$;

CREATE OR REPLACE FUNCTION model_search_document_text(
  natural_text text,
  proposition jsonb,
  scope_entities jsonb
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT lower(concat_ws(
    ' ',
    coalesce(natural_text, ''),
    coalesce(proposition->>'subject', ''),
    coalesce(proposition->>'summary', ''),
    coalesce(proposition->>'assertion', ''),
    coalesce(proposition->>'claim', ''),
    coalesce(proposition->>'event', ''),
    coalesce(proposition->>'object', ''),
    coalesce(proposition->>'benchmark_source', ''),
    coalesce((proposition->'domain_tags')::text, ''),
    coalesce((proposition->'operational_roles')::text, ''),
    coalesce(jsonb_path_query_array(
      coalesce(proposition, '{}'::jsonb),
      '$.operational_facets[*].subrole'
    )::text, ''),
    coalesce(jsonb_path_query_array(
      coalesce(proposition, '{}'::jsonb),
      '$.operational_facets[*].subject'
    )::text, ''),
    coalesce(jsonb_path_query_array(
      coalesce(proposition, '{}'::jsonb),
      '$.operational_facets[*].property'
    )::text, ''),
    coalesce(jsonb_path_query_array(
      coalesce(proposition, '{}'::jsonb),
      '$.operational_facets[*].value'
    )::text, ''),
    coalesce(jsonb_path_query_array(
      coalesce(proposition, '{}'::jsonb),
      '$.operational_facets[*].state'
    )::text, ''),
    coalesce(compact_model_scope_entities_text(scope_entities), '')
  ));
$$;

CREATE OR REPLACE FUNCTION refresh_model_search_document()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_search_documents WHERE model_id = OLD.id;
    RETURN OLD;
  END IF;

  INSERT INTO model_search_documents (
    model_id,
    tenant_id,
    status,
    search_text,
    updated_at
  ) VALUES (
    NEW.id,
    NEW.tenant_id,
    NEW.status,
    model_search_document_text(
      NEW."natural",
      NEW.proposition,
      NEW.scope_entities
    ),
    now()
  )
  ON CONFLICT (model_id) DO UPDATE SET
    tenant_id = EXCLUDED.tenant_id,
    status = EXCLUDED.status,
    search_text = EXCLUDED.search_text,
    updated_at = EXCLUDED.updated_at
  WHERE model_search_documents.tenant_id IS DISTINCT FROM EXCLUDED.tenant_id
     OR model_search_documents.status IS DISTINCT FROM EXCLUDED.status
     OR model_search_documents.search_text IS DISTINCT FROM EXCLUDED.search_text;

  RETURN NEW;
END;
$$;

UPDATE model_search_documents msd
SET search_text = model_search_document_text(
      m."natural",
      m.proposition,
      m.scope_entities
    ),
    updated_at = now()
FROM models m
WHERE m.id = msd.model_id
  AND msd.search_text IS DISTINCT FROM model_search_document_text(
        m."natural",
        m.proposition,
        m.scope_entities
      );
