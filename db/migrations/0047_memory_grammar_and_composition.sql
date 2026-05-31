-- =====================================================================
-- 0047_memory_grammar_and_composition.sql
--
-- Makes the synthesis-layer organization explicit.
--
-- proposition_kind remains the JSON discriminator and compatibility
-- filter. The new generated grammar columns describe the structural role
-- a Model plays in memory, independent of product views. Situations also
-- gain a normalized composition-members sidecar so membership can be
-- queried, audited, and lifecycled without spelunking JSONB.
-- =====================================================================

BEGIN;

-- ---------------------------------------------------------------------
-- 1. Structural memory grammar columns.
-- ---------------------------------------------------------------------

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS claim_role TEXT
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
  ADD COLUMN IF NOT EXISTS abstraction_level TEXT
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
  ADD COLUMN IF NOT EXISTS time_mode TEXT
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
  ADD COLUMN IF NOT EXISTS modality TEXT
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
  ADD COLUMN IF NOT EXISTS polarity TEXT
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

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS domain_tags TEXT[] NOT NULL DEFAULT '{}'::text[];

ALTER TABLE models
  ADD COLUMN IF NOT EXISTS memory_grammar_version TEXT NOT NULL DEFAULT 'v1';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_claim_role_valid'
  ) THEN
    ALTER TABLE models ADD CONSTRAINT models_claim_role_valid
      CHECK (claim_role IN (
        'fact',
        'concern',
        'hypothesis',
        'prediction',
        'pattern',
        'situation',
        'capability',
        'relation',
        'recommendation'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_abstraction_level_valid'
  ) THEN
    ALTER TABLE models ADD CONSTRAINT models_abstraction_level_valid
      CHECK (abstraction_level IN (
        'atomic',
        'relationship',
        'composite',
        'pattern'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_time_mode_valid'
  ) THEN
    ALTER TABLE models ADD CONSTRAINT models_time_mode_valid
      CHECK (time_mode IN (
        'past',
        'current',
        'future',
        'recurring',
        'unspecified'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_modality_valid'
  ) THEN
    ALTER TABLE models ADD CONSTRAINT models_modality_valid
      CHECK (modality IN (
        'observed',
        'inferred',
        'expected',
        'normative'
      ));
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'models_polarity_valid'
  ) THEN
    ALTER TABLE models ADD CONSTRAINT models_polarity_valid
      CHECK (polarity IN (
        'positive',
        'negative',
        'mixed',
        'neutral'
      ));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS models_memory_grammar_idx
  ON models (tenant_id, claim_role, abstraction_level, time_mode)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS models_domain_tags_idx
  ON models USING gin (domain_tags)
  WHERE status = 'active';

-- ---------------------------------------------------------------------
-- 2. Normalized composite membership sidecar for situation Models.
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS model_composition_members (
  composite_model_id UUID NOT NULL,
  tenant_id UUID NOT NULL,
  member_model_id UUID NOT NULL,
  member_role TEXT NOT NULL DEFAULT 'member',
  contribution TEXT,
  confidence FLOAT NOT NULL DEFAULT 1.0,
  evidence_event_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
  source TEXT NOT NULL DEFAULT 'model_proposition',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (composite_model_id, member_model_id),
  CHECK (composite_model_id <> member_model_id),
  CHECK (confidence >= 0.0 AND confidence <= 1.0)
);

CREATE INDEX IF NOT EXISTS model_composition_members_composite_idx
  ON model_composition_members (tenant_id, composite_model_id);

CREATE INDEX IF NOT EXISTS model_composition_members_member_idx
  ON model_composition_members (tenant_id, member_model_id);

CREATE INDEX IF NOT EXISTS model_composition_members_evidence_idx
  ON model_composition_members USING gin (evidence_event_ids);

COMMIT;
