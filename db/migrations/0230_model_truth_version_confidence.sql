-- First-class immutable confidence for canonical Model truth.

BEGIN;

ALTER TABLE model_truth_versions
  ADD COLUMN IF NOT EXISTS confidence DOUBLE PRECISION;
ALTER TABLE model_truth_versions
  ADD COLUMN IF NOT EXISTS semantic_digest_version SMALLINT NOT NULL DEFAULT 1;
ALTER TABLE model_truth_versions ADD COLUMN IF NOT EXISTS falsifier JSONB;
ALTER TABLE model_truth_versions ADD COLUMN IF NOT EXISTS evidential_weight DOUBLE PRECISION NOT NULL DEFAULT 0.5;
ALTER TABLE model_truth_versions ADD COLUMN IF NOT EXISTS supporting_model_ids UUID[] NOT NULL DEFAULT '{}';
ALTER TABLE model_truth_versions ADD COLUMN IF NOT EXISTS visible_to_subjects BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE model_truth_versions ADD COLUMN IF NOT EXISTS resolution_outcome BOOLEAN;
ALTER TABLE model_truth_versions ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;
ALTER TABLE model_truth_versions ADD COLUMN IF NOT EXISTS temporal_scope JSONB NOT NULL DEFAULT '{}';

ALTER TABLE model_truth_versions DISABLE TRIGGER model_truth_versions_immutable;
ALTER TABLE model_truth_versions DISABLE TRIGGER model_truth_versions_command_authority;

UPDATE model_truth_versions version
SET confidence = COALESCE(model.confidence, 0.5),
    falsifier = model.falsifier,
    evidential_weight = model.evidential_weight,
    supporting_model_ids = model.supporting_model_ids,
    visible_to_subjects = model.visible_to_subjects,
    resolution_outcome = model.resolution_outcome,
    resolved_at = model.resolved_at
    , temporal_scope = model.scope_temporal
FROM models model
WHERE version.tenant_id = model.tenant_id
  AND version.model_id = model.id
  AND version.semantic_digest_version = 1;

UPDATE model_truth_versions SET confidence = 0.5 WHERE confidence IS NULL;

ALTER TABLE model_truth_versions ENABLE TRIGGER model_truth_versions_command_authority;
ALTER TABLE model_truth_versions ENABLE TRIGGER model_truth_versions_immutable;

ALTER TABLE model_truth_versions ALTER COLUMN confidence SET DEFAULT 0.5;
ALTER TABLE model_truth_versions ALTER COLUMN confidence SET NOT NULL;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'model_truth_versions_confidence_check'
      AND conrelid = 'model_truth_versions'::regclass
  ) THEN
    ALTER TABLE model_truth_versions
      ADD CONSTRAINT model_truth_versions_confidence_check
      CHECK (confidence >= 0.05 AND confidence <= 0.95);
  END IF;
END $$;

DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'model_truth_versions_semantic_digest_version_check'
      AND conrelid = 'model_truth_versions'::regclass
  ) THEN
    ALTER TABLE model_truth_versions
      ADD CONSTRAINT model_truth_versions_semantic_digest_version_check
      CHECK (semantic_digest_version IN (1, 2));
  END IF;
END $$;

CREATE OR REPLACE VIEW accepted_current_models AS
SELECT
  v.model_id AS id, v.tenant_id, v.proposition, v.natural_text,
  v.created_at, h.version_id AS truth_version_id, h.version AS truth_version,
  h.semantic_digest AS truth_semantic_digest, h.lifecycle AS truth_lifecycle,
  h.advanced_at AS truth_advanced_at, v.confidence
FROM model_truth_heads h
JOIN model_truth_versions v
  ON v.tenant_id = h.tenant_id AND v.version_id = h.version_id
JOIN truth_admission_decisions d
  ON d.tenant_id = v.tenant_id AND d.decision_id = v.admission_decision_id
 AND d.disposition = 'accepted'
WHERE h.lifecycle = 'active'
  AND EXISTS (
    SELECT 1 FROM model_truth_evidence_references evidence
    WHERE evidence.tenant_id = v.tenant_id
      AND evidence.model_version_id = v.version_id
  )
  AND NOT EXISTS (
    SELECT 1
    FROM model_truth_evidence_references evidence
    LEFT JOIN model_truth_heads evidence_head
      ON evidence_head.tenant_id = evidence.tenant_id
     AND evidence_head.version_id::text = evidence.evidence_id
     AND evidence_head.lifecycle = 'active'
    WHERE evidence.tenant_id = v.tenant_id
      AND evidence.model_version_id = v.version_id
      AND evidence.evidence_kind = 'model_version'
      AND evidence_head.version_id IS NULL
  );

CREATE OR REPLACE FUNCTION guard_accepted_model_legacy_payload()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  capability text := NULLIF(current_setting('app.truth_kernel_command', true), '');
BEGIN
  IF EXISTS (
    SELECT 1 FROM model_truth_heads head
    WHERE head.tenant_id = OLD.tenant_id AND head.model_id = OLD.id
  ) THEN
    IF TG_OP = 'DELETE' THEN
      RAISE EXCEPTION 'accepted Model compatibility payload is immutable';
    END IF;
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.proposition IS DISTINCT FROM OLD.proposition
       OR NEW."natural" IS DISTINCT FROM OLD."natural"
       OR NEW.scope_actors IS DISTINCT FROM OLD.scope_actors
       OR NEW.scope_entities IS DISTINCT FROM OLD.scope_entities
       OR NEW.scope_temporal IS DISTINCT FROM OLD.scope_temporal
       OR NEW.confidence IS DISTINCT FROM OLD.confidence
       OR NEW.falsifier IS DISTINCT FROM OLD.falsifier
       OR NEW.supporting_event_ids IS DISTINCT FROM OLD.supporting_event_ids
       OR NEW.supporting_model_ids IS DISTINCT FROM OLD.supporting_model_ids
       OR NEW.evidential_weight IS DISTINCT FROM OLD.evidential_weight
       OR NEW.visible_to_subjects IS DISTINCT FROM OLD.visible_to_subjects
       OR NEW.resolution_outcome IS DISTINCT FROM OLD.resolution_outcome
       OR NEW.resolved_at IS DISTINCT FROM OLD.resolved_at
       OR NEW.status IS DISTINCT FROM OLD.status
       OR NEW.archived_at IS DISTINCT FROM OLD.archived_at
       OR NEW.archive_reason IS DISTINCT FROM OLD.archive_reason THEN
      IF capability IS NULL OR capability !~
         '^model:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' THEN
        RAISE EXCEPTION 'accepted Model semantics require a truth-kernel command';
      END IF;
    END IF;
  END IF;
  RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END $$;

COMMIT;
