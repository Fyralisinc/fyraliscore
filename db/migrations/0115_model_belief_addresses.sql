-- 0115_model_belief_addresses.sql
-- Materialize the Model belief address as a queryable sidecar.
--
-- The Model proposition remains the canonical belief payload. This table is
-- a read-side code: enough normalized address bits for retrieval, compaction,
-- diagnostics, and future SAGE policy learning to ask "which belief obligation
-- does this Model satisfy?" without reparsing prose or broad JSON text.

CREATE TABLE IF NOT EXISTS model_belief_addresses (
  model_id uuid PRIMARY KEY REFERENCES models(id) ON DELETE CASCADE,
  tenant_id uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  status text NOT NULL,
  claim_role text,
  abstraction_level text,
  time_mode text,
  modality text,
  polarity text,
  subject text NOT NULL DEFAULT '',
  predicate text NOT NULL DEFAULT '',
  object text NOT NULL DEFAULT '',
  qualifier text NOT NULL DEFAULT '',
  fingerprint text NOT NULL,
  obligation_keys text[] NOT NULL DEFAULT ARRAY[]::text[],
  answerable_primitives text[] NOT NULL DEFAULT ARRAY[]::text[],
  search_text text NOT NULL DEFAULT '',
  updated_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE model_belief_addresses
  ADD COLUMN IF NOT EXISTS search_text text NOT NULL DEFAULT '';

CREATE OR REPLACE FUNCTION model_belief_address_token(value text, max_len integer DEFAULT 160)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT left(
    lower(regexp_replace(trim(coalesce(value, '')), '\s+', ' ', 'g')),
    greatest(1, max_len)
  );
$$;

CREATE OR REPLACE FUNCTION model_belief_address_text_array(address jsonb, field_name text)
RETURNS text[]
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT coalesce(array_agg(value ORDER BY value), ARRAY[]::text[])
  FROM (
    SELECT DISTINCT model_belief_address_token(elem.value, 160) AS value
    FROM jsonb_array_elements_text(
      CASE
        WHEN jsonb_typeof(address -> field_name) = 'array' THEN address -> field_name
        ELSE '[]'::jsonb
      END
    ) AS elem(value)
    WHERE nullif(model_belief_address_token(elem.value, 160), '') IS NOT NULL
  ) cleaned;
$$;

CREATE OR REPLACE FUNCTION model_belief_address_search_text(
  subject text,
  predicate text,
  object_text text,
  qualifier text,
  obligation_keys text[],
  answerable_primitives text[]
)
RETURNS text
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT lower(concat_ws(
    ' ',
    coalesce(subject, ''),
    coalesce(predicate, ''),
    coalesce(object_text, ''),
    coalesce(qualifier, ''),
    coalesce(array_to_string(obligation_keys, ' '), ''),
    coalesce(array_to_string(answerable_primitives, ' '), '')
  ));
$$;

CREATE OR REPLACE FUNCTION model_belief_address_default_primitives(
  claim_role text,
  polarity text,
  subject text,
  predicate text,
  object_text text,
  qualifier text
)
RETURNS text[]
LANGUAGE sql
IMMUTABLE
AS $$
  WITH blob AS (
    SELECT concat_ws(
      ' ',
      coalesce(claim_role, ''),
      coalesce(polarity, ''),
      coalesce(subject, ''),
      coalesce(predicate, ''),
      coalesce(object_text, ''),
      coalesce(qualifier, '')
    ) AS text_blob
  ),
  primitive_rows AS (
    SELECT 'DEPENDENCY'::text AS primitive
    WHERE claim_role IN ('relation', 'situation', 'capability')
    UNION
    SELECT 'RECURRENCE'
    WHERE claim_role = 'pattern'
    UNION
    SELECT 'COUNTEREVIDENCE'
    WHERE claim_role IN ('concern', 'hypothesis', 'prediction', 'situation')
       OR polarity = 'mixed'
    UNION
    SELECT 'CONSTRAINT'
    FROM blob
    WHERE claim_role = 'concern'
       OR text_blob ~ '(risk|block|constraint|scarce|quota)'
    UNION
    SELECT 'OWNERSHIP'
    FROM blob
    WHERE text_blob ~ '(owner|owned|owns|assigned|responsible)'
    UNION
    SELECT 'GOAL_IMPACT'
    FROM blob
    WHERE text_blob ~ '(customer|revenue|resource|goal|arr|renewal)'
    UNION
    SELECT 'COMMITMENT'
    WHERE claim_role = 'recommendation'
  )
  SELECT CASE
    WHEN count(*) = 0 THEN ARRAY['DEPENDENCY']::text[]
    ELSE array_agg(primitive ORDER BY primitive)
  END
  FROM primitive_rows;
$$;

CREATE OR REPLACE FUNCTION refresh_model_belief_address()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  prop jsonb;
  address jsonb;
  claim text;
  level text;
  tmode text;
  modal text;
  pol text;
  subj text;
  pred text;
  obj text;
  qual text;
  fp text;
  keys text[];
  primitives text[];
BEGIN
  IF TG_OP = 'DELETE' THEN
    DELETE FROM model_belief_addresses WHERE model_id = OLD.id;
    RETURN OLD;
  END IF;

  prop := coalesce(NEW.proposition, '{}'::jsonb);
  address := CASE
    WHEN jsonb_typeof(prop -> 'belief_address') = 'object' THEN prop -> 'belief_address'
    WHEN jsonb_typeof(prop -> 'semantic_address') = 'object' THEN prop -> 'semantic_address'
    ELSE '{}'::jsonb
  END;

  claim := nullif(model_belief_address_token(coalesce(address->>'claim_role', NEW.claim_role), 80), '');
  level := nullif(model_belief_address_token(coalesce(address->>'abstraction_level', NEW.abstraction_level), 80), '');
  tmode := nullif(model_belief_address_token(coalesce(address->>'time_mode', NEW.time_mode), 80), '');
  modal := nullif(model_belief_address_token(coalesce(address->>'modality', NEW.modality), 80), '');
  pol := nullif(model_belief_address_token(coalesce(address->>'polarity', NEW.polarity), 80), '');
  subj := model_belief_address_token(coalesce(
    address->>'subject',
    prop->>'subject',
    prop->>'about',
    prop->>'subject_external',
    prop->>'capability_id',
    prop->>'situation',
    ''
  ), 240);
  pred := model_belief_address_token(coalesce(
    address->>'predicate',
    prop->>'relation',
    CASE
      WHEN claim = 'prediction' THEN 'expected'
      WHEN claim = 'situation' THEN 'shared_mechanism'
      WHEN claim = 'concern' THEN 'risk'
      WHEN claim = 'pattern' THEN 'pattern'
      WHEN claim = 'recommendation' THEN 'proposed_change'
      ELSE 'asserts'
    END,
    ''
  ), 160);
  obj := model_belief_address_token(coalesce(
    address->>'object',
    prop->>'object',
    prop->>'value',
    prop->>'assertion',
    prop->>'claim',
    prop->>'assessment',
    prop->>'expected',
    prop->>'desired_state',
    prop->>'goal',
    prop->>'policy',
    prop->>'nature',
    prop->>'summary',
    prop->>'event',
    prop->>'observed_tendency',
    ''
  ), 240);
  qual := model_belief_address_token(coalesce(
    address->>'qualifier',
    prop->>'qualifier',
    prop->>'status',
    prop->>'relationship_summary',
    prop->>'shared_mechanism',
    prop->>'open_falsifier',
    ''
  ), 240);
  fp := nullif(model_belief_address_token(address->>'fingerprint', 80), '');
  IF fp IS NULL THEN
    fp := left(md5(concat_ws('|', claim, level, tmode, modal, pol, subj, pred, obj, qual)), 24);
  END IF;

  keys := model_belief_address_text_array(address, 'obligation_keys');
  IF coalesce(array_length(keys, 1), 0) = 0 THEN
    keys := array_remove(ARRAY[
      CASE WHEN claim IS NOT NULL THEN 'role:' || claim END,
      CASE WHEN level IS NOT NULL THEN 'level:' || level END,
      CASE WHEN tmode IS NOT NULL THEN 'time:' || tmode END,
      CASE WHEN pol IS NOT NULL THEN 'polarity:' || pol END,
      CASE WHEN pred <> '' THEN 'predicate:' || pred END,
      CASE WHEN subj <> '' THEN 'subject:' || subj END,
      CASE WHEN subj <> '' AND pred <> '' THEN 'subject_predicate:' || subj || '|' || pred END,
      CASE WHEN subj <> '' AND pred <> '' AND obj <> '' THEN 'spo:' || subj || '|' || pred || '|' || left(obj, 96) END,
      CASE WHEN qual <> '' THEN 'qualifier:' || left(qual, 96) END
    ], NULL);
  END IF;

  primitives := model_belief_address_text_array(address, 'answerable_primitives');
  IF coalesce(array_length(primitives, 1), 0) = 0 THEN
    primitives := model_belief_address_default_primitives(
      claim, pol, subj, pred, obj, qual
    );
  ELSE
    SELECT array_agg(upper(value) ORDER BY upper(value))
    INTO primitives
    FROM unnest(primitives) AS raw(value);
  END IF;

  INSERT INTO model_belief_addresses (
    model_id,
    tenant_id,
    status,
    claim_role,
    abstraction_level,
    time_mode,
    modality,
    polarity,
    subject,
    predicate,
    object,
    qualifier,
    fingerprint,
    obligation_keys,
    answerable_primitives,
    search_text,
    updated_at
  ) VALUES (
    NEW.id,
    NEW.tenant_id,
    NEW.status,
    claim,
    level,
    tmode,
    modal,
    pol,
    subj,
    pred,
    obj,
    qual,
    fp,
    keys,
    primitives,
    model_belief_address_search_text(subj, pred, obj, qual, keys, primitives),
    now()
  )
  ON CONFLICT (model_id) DO UPDATE SET
    tenant_id = EXCLUDED.tenant_id,
    status = EXCLUDED.status,
    claim_role = EXCLUDED.claim_role,
    abstraction_level = EXCLUDED.abstraction_level,
    time_mode = EXCLUDED.time_mode,
    modality = EXCLUDED.modality,
    polarity = EXCLUDED.polarity,
    subject = EXCLUDED.subject,
    predicate = EXCLUDED.predicate,
    object = EXCLUDED.object,
    qualifier = EXCLUDED.qualifier,
    fingerprint = EXCLUDED.fingerprint,
    obligation_keys = EXCLUDED.obligation_keys,
    answerable_primitives = EXCLUDED.answerable_primitives,
    search_text = EXCLUDED.search_text,
    updated_at = EXCLUDED.updated_at
  WHERE model_belief_addresses.tenant_id IS DISTINCT FROM EXCLUDED.tenant_id
     OR model_belief_addresses.status IS DISTINCT FROM EXCLUDED.status
     OR model_belief_addresses.claim_role IS DISTINCT FROM EXCLUDED.claim_role
     OR model_belief_addresses.abstraction_level IS DISTINCT FROM EXCLUDED.abstraction_level
     OR model_belief_addresses.time_mode IS DISTINCT FROM EXCLUDED.time_mode
     OR model_belief_addresses.modality IS DISTINCT FROM EXCLUDED.modality
     OR model_belief_addresses.polarity IS DISTINCT FROM EXCLUDED.polarity
     OR model_belief_addresses.subject IS DISTINCT FROM EXCLUDED.subject
     OR model_belief_addresses.predicate IS DISTINCT FROM EXCLUDED.predicate
     OR model_belief_addresses.object IS DISTINCT FROM EXCLUDED.object
     OR model_belief_addresses.qualifier IS DISTINCT FROM EXCLUDED.qualifier
     OR model_belief_addresses.fingerprint IS DISTINCT FROM EXCLUDED.fingerprint
     OR model_belief_addresses.obligation_keys IS DISTINCT FROM EXCLUDED.obligation_keys
     OR model_belief_addresses.answerable_primitives IS DISTINCT FROM EXCLUDED.answerable_primitives
     OR model_belief_addresses.search_text IS DISTINCT FROM EXCLUDED.search_text;

  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS models_refresh_belief_address ON models;
CREATE TRIGGER models_refresh_belief_address
AFTER INSERT OR UPDATE OF tenant_id, status, proposition, claim_role, abstraction_level, time_mode, modality, polarity
OR DELETE ON models
FOR EACH ROW EXECUTE FUNCTION refresh_model_belief_address();

INSERT INTO model_belief_addresses (
  model_id,
  tenant_id,
  status,
  claim_role,
  abstraction_level,
  time_mode,
  modality,
  polarity,
  subject,
  predicate,
  object,
  qualifier,
  fingerprint,
  obligation_keys,
  answerable_primitives,
  search_text,
  updated_at
)
SELECT
  m.id,
  m.tenant_id,
  m.status,
  base.claim_role,
  base.abstraction_level,
  base.time_mode,
  base.modality,
  base.polarity,
  base.subject,
  base.predicate,
  base.object,
  base.qualifier,
  mba.fingerprint,
  mba.obligation_keys,
  mba.answerable_primitives,
  model_belief_address_search_text(
    base.subject,
    base.predicate,
    base.object,
    base.qualifier,
    mba.obligation_keys,
    mba.answerable_primitives
  ),
  now()
FROM models m
CROSS JOIN LATERAL (
  SELECT
    coalesce(m.proposition->'belief_address', m.proposition->'semantic_address', '{}'::jsonb) AS address
) raw
CROSS JOIN LATERAL (
  SELECT
    nullif(model_belief_address_token(coalesce(raw.address->>'claim_role', m.claim_role), 80), '') AS claim_role,
    nullif(model_belief_address_token(coalesce(raw.address->>'abstraction_level', m.abstraction_level), 80), '') AS abstraction_level,
    nullif(model_belief_address_token(coalesce(raw.address->>'time_mode', m.time_mode), 80), '') AS time_mode,
    nullif(model_belief_address_token(coalesce(raw.address->>'modality', m.modality), 80), '') AS modality,
    nullif(model_belief_address_token(coalesce(raw.address->>'polarity', m.polarity), 80), '') AS polarity,
    model_belief_address_token(coalesce(raw.address->>'subject', m.proposition->>'subject', m.proposition->>'about', m.proposition->>'subject_external', m.proposition->>'capability_id', m.proposition->>'situation', ''), 240) AS subject,
    model_belief_address_token(coalesce(raw.address->>'predicate', m.proposition->>'relation', 'asserts'), 160) AS predicate,
    model_belief_address_token(coalesce(raw.address->>'object', m.proposition->>'object', m.proposition->>'value', m.proposition->>'assertion', m.proposition->>'claim', m.proposition->>'assessment', m.proposition->>'expected', m.proposition->>'summary', ''), 240) AS object,
    model_belief_address_token(coalesce(raw.address->>'qualifier', m.proposition->>'qualifier', m.proposition->>'status', m.proposition->>'relationship_summary', m.proposition->>'shared_mechanism', m.proposition->>'open_falsifier', ''), 240) AS qualifier,
    model_belief_address_text_array(raw.address, 'obligation_keys') AS raw_keys,
    model_belief_address_text_array(raw.address, 'answerable_primitives') AS raw_primitives,
    nullif(model_belief_address_token(raw.address->>'fingerprint', 80), '') AS raw_fingerprint
) base
CROSS JOIN LATERAL (
  SELECT
    coalesce(
      base.raw_fingerprint,
      left(md5(concat_ws('|', base.claim_role, base.abstraction_level, base.time_mode, base.modality, base.polarity, base.subject, base.predicate, base.object, base.qualifier)), 24)
    ) AS fingerprint,
    CASE
      WHEN coalesce(array_length(base.raw_keys, 1), 0) > 0 THEN base.raw_keys
      ELSE array_remove(ARRAY[
        CASE WHEN base.claim_role IS NOT NULL THEN 'role:' || base.claim_role END,
        CASE WHEN base.abstraction_level IS NOT NULL THEN 'level:' || base.abstraction_level END,
        CASE WHEN base.time_mode IS NOT NULL THEN 'time:' || base.time_mode END,
        CASE WHEN base.polarity IS NOT NULL THEN 'polarity:' || base.polarity END,
        CASE WHEN base.predicate <> '' THEN 'predicate:' || base.predicate END,
        CASE WHEN base.subject <> '' THEN 'subject:' || base.subject END,
        CASE WHEN base.subject <> '' AND base.predicate <> '' THEN 'subject_predicate:' || base.subject || '|' || base.predicate END,
        CASE WHEN base.subject <> '' AND base.predicate <> '' AND base.object <> '' THEN 'spo:' || base.subject || '|' || base.predicate || '|' || left(base.object, 96) END,
        CASE WHEN base.qualifier <> '' THEN 'qualifier:' || left(base.qualifier, 96) END
      ], NULL)
    END AS obligation_keys,
    CASE
      WHEN coalesce(array_length(base.raw_primitives, 1), 0) > 0
        THEN ARRAY(SELECT upper(value) FROM unnest(base.raw_primitives) AS raw_primitive(value) ORDER BY upper(value))
      ELSE model_belief_address_default_primitives(base.claim_role, base.polarity, base.subject, base.predicate, base.object, base.qualifier)
    END AS answerable_primitives
) mba
ON CONFLICT (model_id) DO UPDATE SET
  tenant_id = EXCLUDED.tenant_id,
  status = EXCLUDED.status,
  claim_role = EXCLUDED.claim_role,
  abstraction_level = EXCLUDED.abstraction_level,
  time_mode = EXCLUDED.time_mode,
  modality = EXCLUDED.modality,
  polarity = EXCLUDED.polarity,
  subject = EXCLUDED.subject,
  predicate = EXCLUDED.predicate,
  object = EXCLUDED.object,
  qualifier = EXCLUDED.qualifier,
  fingerprint = EXCLUDED.fingerprint,
  obligation_keys = EXCLUDED.obligation_keys,
  answerable_primitives = EXCLUDED.answerable_primitives,
  search_text = EXCLUDED.search_text,
  updated_at = now();

CREATE INDEX IF NOT EXISTS model_belief_addresses_active_tenant_role_idx
  ON model_belief_addresses (tenant_id, claim_role, abstraction_level, time_mode)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS model_belief_addresses_active_fingerprint_idx
  ON model_belief_addresses (tenant_id, fingerprint)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS model_belief_addresses_obligation_keys_idx
  ON model_belief_addresses USING gin (obligation_keys)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS model_belief_addresses_answerable_primitives_idx
  ON model_belief_addresses USING gin (answerable_primitives)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS model_belief_addresses_search_text_trgm_idx
  ON model_belief_addresses USING gin (search_text gin_trgm_ops)
  WHERE status = 'active';
