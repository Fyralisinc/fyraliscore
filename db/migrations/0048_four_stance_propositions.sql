-- =====================================================================
-- 0048_four_stance_propositions.sql
--
-- Collapse proposition_kind from the old semantic taxonomy into four
-- epistemic stances:
--   observation | belief | prediction | norm
--
-- Subject semantics move into proposition memory-grammar fields
-- (`claim_role`, `abstraction_level`, `time_mode`, `modality`,
-- `polarity`, `domain_tags`) and legacy rows retain `legacy_kind` as a
-- backfill hint.
-- =====================================================================

BEGIN;

DROP INDEX IF EXISTS recommendations_active_idx;
DROP INDEX IF EXISTS models_memory_grammar_idx;
DROP INDEX IF EXISTS models_proposition_kind_idx;

ALTER TABLE models DROP CONSTRAINT IF EXISTS models_proposition_kind_valid;
ALTER TABLE models DROP CONSTRAINT IF EXISTS models_situation_compositional_fields;
ALTER TABLE models DROP CONSTRAINT IF EXISTS models_claim_role_valid;
ALTER TABLE models DROP CONSTRAINT IF EXISTS models_abstraction_level_valid;
ALTER TABLE models DROP CONSTRAINT IF EXISTS models_time_mode_valid;
ALTER TABLE models DROP CONSTRAINT IF EXISTS models_modality_valid;
ALTER TABLE models DROP CONSTRAINT IF EXISTS models_polarity_valid;

-- Rewrite existing twelve-kind rows into canonical stance rows. The old
-- kind is preserved as proposition.legacy_kind so generated grammar and
-- audits can still recover the prior semantic bucket.
UPDATE models
SET proposition =
  jsonb_set(
    CASE
      WHEN proposition ? 'legacy_kind' THEN proposition
      ELSE jsonb_set(proposition, '{legacy_kind}', to_jsonb(proposition->>'kind'), true)
    END,
    '{kind}',
    to_jsonb(
      CASE proposition->>'kind'
        WHEN 'recommendation' THEN 'norm'
        WHEN 'prediction' THEN 'prediction'
        WHEN 'observation' THEN 'observation'
        WHEN 'belief' THEN 'belief'
        WHEN 'norm' THEN 'norm'
        ELSE 'belief'
      END
    ),
    true
  )
WHERE proposition->>'kind' NOT IN ('observation', 'belief', 'prediction', 'norm')
   OR NOT (proposition ? 'legacy_kind');

-- Upgrade generated columns only when this migration is being applied to
-- a database created before the four-stance expressions existed. The
-- local test runner reapplies migrations, so these guards avoid repeated
-- DROP/ADD churn on generated columns.
DO $$
DECLARE
  expr TEXT;
BEGIN
  SELECT pg_get_expr(d.adbin, d.adrelid)
  INTO expr
  FROM pg_attrdef d
  JOIN pg_attribute a
    ON a.attrelid = d.adrelid AND a.attnum = d.adnum
  WHERE d.adrelid = 'models'::regclass
    AND a.attname = 'proposition_kind';

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_four_stance_generated_columns_applied'
  ) AND (expr IS NULL OR expr NOT LIKE '%recommendation%norm%') THEN
    ALTER TABLE models DROP COLUMN IF EXISTS proposition_kind;
    ALTER TABLE models
      ADD COLUMN proposition_kind TEXT
        GENERATED ALWAYS AS (
          CASE proposition->>'kind'
            WHEN 'observation' THEN 'observation'
            WHEN 'belief' THEN 'belief'
            WHEN 'prediction' THEN 'prediction'
            WHEN 'norm' THEN 'norm'
            WHEN 'recommendation' THEN 'norm'
            WHEN 'state' THEN 'belief'
            WHEN 'relation' THEN 'belief'
            WHEN 'pattern' THEN 'belief'
            WHEN 'pattern_instance' THEN 'belief'
            WHEN 'capability_assessment' THEN 'belief'
            WHEN 'hypothesis' THEN 'belief'
            WHEN 'concern' THEN 'belief'
            WHEN 'market_assessment' THEN 'belief'
            WHEN 'environmental_trend' THEN 'belief'
            WHEN 'situation' THEN 'belief'
            ELSE proposition->>'kind'
          END
        ) STORED;
  END IF;
END $$;

DO $$
DECLARE
  expr TEXT;
BEGIN
  SELECT pg_get_expr(d.adbin, d.adrelid)
  INTO expr
  FROM pg_attrdef d
  JOIN pg_attribute a
    ON a.attrelid = d.adrelid AND a.attnum = d.adnum
  WHERE d.adrelid = 'models'::regclass
    AND a.attname = 'claim_role';

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_four_stance_generated_columns_applied'
  ) AND (expr IS NULL OR expr NOT LIKE '%claim_role%') THEN
    ALTER TABLE models DROP COLUMN IF EXISTS claim_role;
    ALTER TABLE models DROP COLUMN IF EXISTS abstraction_level;
    ALTER TABLE models DROP COLUMN IF EXISTS time_mode;
    ALTER TABLE models DROP COLUMN IF EXISTS modality;
    ALTER TABLE models DROP COLUMN IF EXISTS polarity;

    ALTER TABLE models
      ADD COLUMN claim_role TEXT
        GENERATED ALWAYS AS (
          COALESCE(
            proposition->>'claim_role',
            CASE COALESCE(proposition->>'legacy_kind', proposition->>'kind')
              WHEN 'relation' THEN 'relation'
              WHEN 'prediction' THEN 'prediction'
              WHEN 'pattern' THEN 'pattern'
              WHEN 'pattern_instance' THEN 'pattern'
              WHEN 'capability_assessment' THEN 'capability'
              WHEN 'hypothesis' THEN 'hypothesis'
              WHEN 'concern' THEN 'concern'
              WHEN 'environmental_trend' THEN 'pattern'
              WHEN 'situation' THEN 'situation'
              WHEN 'recommendation' THEN 'recommendation'
              WHEN 'norm' THEN 'recommendation'
              ELSE 'fact'
            END
          )
        ) STORED;

    ALTER TABLE models
      ADD COLUMN abstraction_level TEXT
        GENERATED ALWAYS AS (
          COALESCE(
            proposition->>'abstraction_level',
            CASE COALESCE(proposition->>'legacy_kind', proposition->>'kind')
              WHEN 'relation' THEN 'relationship'
              WHEN 'pattern' THEN 'pattern'
              WHEN 'environmental_trend' THEN 'pattern'
              WHEN 'situation' THEN 'composite'
              ELSE 'atomic'
            END
          )
        ) STORED;

    ALTER TABLE models
      ADD COLUMN time_mode TEXT
        GENERATED ALWAYS AS (
          COALESCE(
            proposition->>'time_mode',
            CASE COALESCE(proposition->>'legacy_kind', proposition->>'kind')
              WHEN 'prediction' THEN 'future'
              WHEN 'recommendation' THEN 'future'
              WHEN 'norm' THEN 'future'
              WHEN 'pattern' THEN 'recurring'
              WHEN 'environmental_trend' THEN 'recurring'
              WHEN 'pattern_instance' THEN 'past'
              WHEN 'hypothesis' THEN 'unspecified'
              WHEN 'observation' THEN 'past'
              ELSE 'current'
            END
          )
        ) STORED;

    ALTER TABLE models
      ADD COLUMN modality TEXT
        GENERATED ALWAYS AS (
          COALESCE(
            proposition->>'modality',
            CASE COALESCE(proposition->>'legacy_kind', proposition->>'kind')
              WHEN 'state' THEN 'observed'
              WHEN 'pattern_instance' THEN 'observed'
              WHEN 'observation' THEN 'observed'
              WHEN 'prediction' THEN 'expected'
              WHEN 'recommendation' THEN 'normative'
              WHEN 'norm' THEN 'normative'
              ELSE 'inferred'
            END
          )
        ) STORED;

    ALTER TABLE models
      ADD COLUMN polarity TEXT
        GENERATED ALWAYS AS (
          COALESCE(
            proposition->>'polarity',
            CASE COALESCE(proposition->>'legacy_kind', proposition->>'kind')
              WHEN 'concern' THEN 'negative'
              WHEN 'situation' THEN 'mixed'
              WHEN 'recommendation' THEN 'mixed'
              WHEN 'norm' THEN 'mixed'
              ELSE 'neutral'
            END
          )
        ) STORED;
  END IF;
END $$;

DO $$
DECLARE
  expr TEXT;
BEGIN
  SELECT pg_get_expr(d.adbin, d.adrelid)
  INTO expr
  FROM pg_attrdef d
  JOIN pg_attribute a
    ON a.attrelid = d.adrelid AND a.attnum = d.adnum
  WHERE d.adrelid = 'models'::regclass
    AND a.attname = 'target_actor_id';

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_four_stance_generated_columns_applied'
  ) AND (expr IS NULL OR expr NOT LIKE '%norm%') THEN
    ALTER TABLE models DROP COLUMN IF EXISTS target_actor_id;
    ALTER TABLE models
      ADD COLUMN target_actor_id UUID
        GENERATED ALWAYS AS (
          CASE
            WHEN (
              proposition->>'claim_role' = 'recommendation'
              OR proposition->>'legacy_kind' = 'recommendation'
              OR proposition->>'kind' = 'recommendation'
              OR (
                proposition->>'kind' = 'norm'
                AND proposition->>'claim_role' = 'recommendation'
              )
            )
             AND proposition->>'target_actor_id' IS NOT NULL
            THEN (proposition->>'target_actor_id')::uuid
            ELSE NULL
          END
        ) STORED;
  END IF;
END $$;

ALTER TABLE models
  ADD CONSTRAINT models_proposition_kind_valid
  CHECK (
    proposition_kind IS NOT NULL
    AND proposition_kind IN ('observation', 'belief', 'prediction', 'norm')
  );

ALTER TABLE models ADD CONSTRAINT models_claim_role_valid
  CHECK (claim_role IN (
    'fact', 'concern', 'hypothesis', 'prediction', 'pattern',
    'situation', 'capability', 'relation', 'recommendation'
  ));

ALTER TABLE models ADD CONSTRAINT models_abstraction_level_valid
  CHECK (abstraction_level IN ('atomic', 'relationship', 'composite', 'pattern'));

ALTER TABLE models ADD CONSTRAINT models_time_mode_valid
  CHECK (time_mode IN ('past', 'current', 'future', 'recurring', 'unspecified'));

ALTER TABLE models ADD CONSTRAINT models_modality_valid
  CHECK (modality IN ('observed', 'inferred', 'expected', 'normative'));

ALTER TABLE models ADD CONSTRAINT models_polarity_valid
  CHECK (polarity IN ('positive', 'negative', 'mixed', 'neutral'));

ALTER TABLE models
  ADD CONSTRAINT models_situation_compositional_fields
  CHECK (
    claim_role <> 'situation'
    OR (
      proposition ? 'pressure_type'
      AND proposition ? 'shared_mechanism'
      AND proposition->>'pressure_type' IN (
        'capacity', 'trust', 'revenue', 'compliance',
        'decision', 'execution', 'market', 'resource'
      )
      AND length(coalesce(proposition->>'shared_mechanism', '')) > 0
    )
  )
  NOT VALID;

CREATE INDEX IF NOT EXISTS models_memory_grammar_idx
  ON models (tenant_id, claim_role, abstraction_level, time_mode)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS models_proposition_kind_idx
  ON models (tenant_id, proposition_kind)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS recommendations_active_idx
  ON models (tenant_id, target_actor_id, created_at DESC)
  WHERE claim_role = 'recommendation' AND status = 'active';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_four_stance_generated_columns_applied'
  ) THEN
    ALTER TABLE models ADD CONSTRAINT models_four_stance_generated_columns_applied
      CHECK (true) NOT VALID;
  END IF;
END $$;

COMMIT;
