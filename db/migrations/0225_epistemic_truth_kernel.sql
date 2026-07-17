-- 0225_epistemic_truth_kernel.sql
--
-- Immutable admission, version, evidence, scope, and relation substrate for
-- the P2 epistemic truth kernel.  Existing models/relation_instances remain
-- available as compatibility payload rows; accepted-current views are the
-- admission-gated read boundary.

BEGIN;

CREATE TABLE IF NOT EXISTS truth_candidates (
  candidate_id UUID NOT NULL,
  candidate_version INTEGER NOT NULL CHECK (candidate_version >= 1),
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN (
    'atomic_claim', 'synthesis', 'batch_envelope',
    'control_language', 'processing_wrapper'
  )),
  review_state TEXT NOT NULL CHECK (review_state IN ('proposed', 'in_review')),
  natural_text TEXT NOT NULL CHECK (btrim(natural_text) <> ''),
  proposition JSONB NOT NULL CHECK (
    jsonb_typeof(proposition) = 'object' AND proposition <> '{}'::jsonb
  ),
  candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, candidate_id, candidate_version),
  UNIQUE (tenant_id, candidate_id, candidate_version, candidate_digest)
);

CREATE TABLE IF NOT EXISTS truth_admission_decisions (
  decision_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  candidate_id UUID NOT NULL,
  candidate_version INTEGER NOT NULL CHECK (candidate_version >= 1),
  candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
  disposition TEXT NOT NULL CHECK (
    disposition IN ('accepted', 'rejected', 'needs_review')
  ),
  reason_codes TEXT[] NOT NULL CHECK (cardinality(reason_codes) > 0),
  decided_by TEXT NOT NULL CHECK (btrim(decided_by) <> ''),
  decided_at TIMESTAMPTZ NOT NULL,
  admitted_model_id UUID,
  admitted_version_id UUID,
  CHECK (
    (disposition = 'accepted' AND admitted_model_id IS NOT NULL AND admitted_version_id IS NOT NULL)
    OR
    (disposition <> 'accepted' AND admitted_model_id IS NULL AND admitted_version_id IS NULL)
  ),
  UNIQUE (tenant_id, decision_id),
  UNIQUE (tenant_id, candidate_id, candidate_version),
  FOREIGN KEY (tenant_id, candidate_id, candidate_version, candidate_digest)
    REFERENCES truth_candidates (
      tenant_id, candidate_id, candidate_version, candidate_digest
    ) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS truth_command_receipts (
  command_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  idempotency_key TEXT NOT NULL CHECK (btrim(idempotency_key) <> ''),
  command_kind TEXT NOT NULL CHECK (command_kind IN (
    'admit_model', 'transition_model', 'admit_relation', 'transition_relation'
  )),
  request_digest TEXT NOT NULL CHECK (request_digest ~ '^[0-9a-f]{64}$'),
  outcome TEXT NOT NULL CHECK (outcome IN ('applied', 'rejected', 'conflict')),
  result_model_version_id UUID,
  result_relation_version_id UUID,
  rejection_code TEXT,
  recorded_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, idempotency_key),
  UNIQUE (tenant_id, command_id),
  CHECK (outcome = 'applied' OR rejection_code IS NOT NULL),
  CHECK (result_model_version_id IS NULL OR result_relation_version_id IS NULL)
);

CREATE TABLE IF NOT EXISTS model_truth_versions (
  version_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_id UUID NOT NULL,
  version INTEGER NOT NULL CHECK (version >= 1),
  admission_decision_id UUID NOT NULL,
  source_candidate_id UUID NOT NULL,
  source_candidate_version INTEGER NOT NULL CHECK (source_candidate_version >= 1),
  natural_text TEXT NOT NULL CHECK (btrim(natural_text) <> ''),
  proposition JSONB NOT NULL CHECK (
    jsonb_typeof(proposition) = 'object' AND proposition <> '{}'::jsonb
  ),
  lifecycle TEXT NOT NULL CHECK (
    lifecycle IN ('active', 'disputed', 'falsified', 'superseded', 'archived')
  ),
  semantic_digest TEXT NOT NULL CHECK (semantic_digest ~ '^[0-9a-f]{64}$'),
  supersedes_version_id UUID,
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, version_id),
  UNIQUE (tenant_id, model_id, version),
  UNIQUE (tenant_id, model_id, version_id),
  UNIQUE (tenant_id, model_id, version_id, version, semantic_digest, lifecycle),
  FOREIGN KEY (tenant_id, admission_decision_id)
    REFERENCES truth_admission_decisions (tenant_id, decision_id)
    DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, source_candidate_id, source_candidate_version)
    REFERENCES truth_candidates (tenant_id, candidate_id, candidate_version)
    DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, supersedes_version_id)
    REFERENCES model_truth_versions (tenant_id, version_id)
    DEFERRABLE INITIALLY DEFERRED,
  CHECK (version > 1 OR supersedes_version_id IS NULL)
);

ALTER TABLE truth_admission_decisions
  DROP CONSTRAINT IF EXISTS truth_admission_decisions_admitted_version_fk;
ALTER TABLE truth_admission_decisions
  ADD CONSTRAINT truth_admission_decisions_admitted_version_fk
  FOREIGN KEY (tenant_id, admitted_model_id, admitted_version_id)
  REFERENCES model_truth_versions (tenant_id, model_id, version_id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE truth_command_receipts
  DROP CONSTRAINT IF EXISTS truth_command_receipts_model_version_fk;
ALTER TABLE truth_command_receipts
  ADD CONSTRAINT truth_command_receipts_model_version_fk
  FOREIGN KEY (tenant_id, result_model_version_id)
  REFERENCES model_truth_versions (tenant_id, version_id)
  DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS model_truth_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_id UUID NOT NULL,
  version_id UUID NOT NULL,
  version INTEGER NOT NULL CHECK (version >= 1),
  semantic_digest TEXT NOT NULL CHECK (semantic_digest ~ '^[0-9a-f]{64}$'),
  lifecycle TEXT NOT NULL CHECK (
    lifecycle IN ('active', 'disputed', 'falsified', 'superseded', 'archived')
  ),
  advanced_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, model_id),
  UNIQUE (tenant_id, version_id),
  FOREIGN KEY (tenant_id, model_id, version_id, version, semantic_digest, lifecycle)
    REFERENCES model_truth_versions (
      tenant_id, model_id, version_id, version, semantic_digest, lifecycle
    ) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS model_truth_evidence_references (
  reference_id UUID NOT NULL,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_version_id UUID NOT NULL,
  evidence_kind TEXT NOT NULL CHECK (
    evidence_kind IN ('observation', 'model_version', 'registered')
  ),
  evidence_id TEXT NOT NULL CHECK (btrim(evidence_id) <> ''),
  evidence_version INTEGER NOT NULL CHECK (evidence_version >= 1),
  evidence_digest TEXT NOT NULL CHECK (evidence_digest ~ '^[0-9a-f]{64}$'),
  evidence_role TEXT NOT NULL CHECK (
    evidence_role IN ('support', 'counterevidence', 'context', 'derivation', 'authority')
  ),
  source_system TEXT NOT NULL CHECK (btrim(source_system) <> ''),
  source_object_id TEXT NOT NULL CHECK (btrim(source_object_id) <> ''),
  source_revision TEXT NOT NULL CHECK (btrim(source_revision) <> ''),
  field_path TEXT,
  span_start INTEGER CHECK (span_start >= 0),
  span_end INTEGER CHECK (span_end >= 0),
  time_range_start TIMESTAMPTZ,
  time_range_end TIMESTAMPTZ,
  authority_ref TEXT NOT NULL CHECK (btrim(authority_ref) <> ''),
  policy_version TEXT NOT NULL CHECK (btrim(policy_version) <> ''),
  authority_epoch INTEGER NOT NULL CHECK (authority_epoch >= 1),
  authority_decided_at TIMESTAMPTZ NOT NULL,
  authority_expires_at TIMESTAMPTZ,
  occurred_at TIMESTAMPTZ NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL,
  cutoff_at TIMESTAMPTZ NOT NULL,
  reference_digest TEXT NOT NULL CHECK (reference_digest ~ '^[0-9a-f]{64}$'),
  PRIMARY KEY (tenant_id, model_version_id, reference_id),
  UNIQUE (tenant_id, model_version_id, evidence_kind, evidence_id, evidence_version, evidence_role),
  FOREIGN KEY (tenant_id, model_version_id)
    REFERENCES model_truth_versions (tenant_id, version_id) ON DELETE RESTRICT,
  CHECK ((span_start IS NULL) = (span_end IS NULL)),
  CHECK (span_start IS NULL OR span_end > span_start),
  CHECK ((time_range_start IS NULL) = (time_range_end IS NULL)),
  CHECK (time_range_start IS NULL OR time_range_end > time_range_start),
  CHECK (occurred_at <= recorded_at AND recorded_at <= cutoff_at),
  CHECK (authority_decided_at <= cutoff_at),
  CHECK (authority_expires_at IS NULL OR authority_expires_at > cutoff_at)
);

-- Citation identity is version-bound: an unchanged citation may legitimately
-- be carried into a new immutable Model version.
ALTER TABLE model_truth_evidence_references
  DROP CONSTRAINT IF EXISTS model_truth_evidence_references_pkey;
ALTER TABLE model_truth_evidence_references
  ADD CONSTRAINT model_truth_evidence_references_pkey
  PRIMARY KEY (tenant_id, model_version_id, reference_id);

CREATE TABLE IF NOT EXISTS model_truth_scope_bindings (
  binding_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_version_id UUID NOT NULL,
  subject_id UUID NOT NULL,
  subject_kind TEXT NOT NULL CHECK (subject_kind IN (
    'person', 'team', 'organization', 'project', 'product', 'customer',
    'issue', 'work_item', 'location', 'other'
  )),
  scope_role TEXT NOT NULL CHECK (scope_role IN (
    'actor', 'subject', 'object', 'owner', 'beneficiary', 'affected', 'location'
  )),
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, model_version_id, subject_id, scope_role),
  UNIQUE (tenant_id, model_version_id, binding_id),
  FOREIGN KEY (tenant_id, model_version_id)
    REFERENCES model_truth_versions (tenant_id, version_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS model_truth_scope_evidence (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_version_id UUID NOT NULL,
  binding_id UUID NOT NULL,
  evidence_reference_id UUID NOT NULL,
  PRIMARY KEY (tenant_id, model_version_id, binding_id, evidence_reference_id),
  FOREIGN KEY (tenant_id, model_version_id, binding_id)
    REFERENCES model_truth_scope_bindings (tenant_id, model_version_id, binding_id)
    ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, model_version_id, evidence_reference_id)
    REFERENCES model_truth_evidence_references (tenant_id, model_version_id, reference_id)
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS relation_truth_admission_decisions (
  decision_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  candidate_relation_id UUID NOT NULL REFERENCES relation_instances(id),
  candidate_digest TEXT NOT NULL CHECK (candidate_digest ~ '^[0-9a-f]{64}$'),
  disposition TEXT NOT NULL CHECK (
    disposition IN ('accepted', 'rejected', 'needs_review')
  ),
  reason_codes TEXT[] NOT NULL CHECK (cardinality(reason_codes) > 0),
  decided_by TEXT NOT NULL CHECK (btrim(decided_by) <> ''),
  decided_at TIMESTAMPTZ NOT NULL,
  admitted_relation_version_id UUID,
  CHECK (
    (disposition = 'accepted' AND admitted_relation_version_id IS NOT NULL)
    OR (disposition <> 'accepted' AND admitted_relation_version_id IS NULL)
  ),
  UNIQUE (tenant_id, decision_id),
  UNIQUE (tenant_id, candidate_relation_id, candidate_digest)
);

CREATE TABLE IF NOT EXISTS relation_truth_versions (
  relation_version_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  relation_id UUID NOT NULL REFERENCES relation_instances(id),
  version INTEGER NOT NULL CHECK (version >= 1),
  admission_decision_id UUID NOT NULL,
  relation_kind TEXT NOT NULL CHECK (relation_kind IN (
    'causal_influence', 'dependency_constraint', 'enablement', 'predictive_indicator'
  )),
  lifecycle TEXT NOT NULL CHECK (
    lifecycle IN ('active', 'disputed', 'retired')
  ),
  rationale TEXT NOT NULL CHECK (btrim(rationale) <> ''),
  temporal_bounds JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (
    jsonb_typeof(temporal_bounds) = 'object'
  ),
  semantic_digest TEXT NOT NULL CHECK (semantic_digest ~ '^[0-9a-f]{64}$'),
  supersedes_relation_version_id UUID,
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, relation_version_id),
  UNIQUE (tenant_id, relation_id, version),
  UNIQUE (tenant_id, relation_id, relation_version_id),
  UNIQUE (tenant_id, relation_id, relation_version_id, version, semantic_digest, lifecycle),
  FOREIGN KEY (tenant_id, admission_decision_id)
    REFERENCES relation_truth_admission_decisions (tenant_id, decision_id)
    DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, supersedes_relation_version_id)
    REFERENCES relation_truth_versions (tenant_id, relation_version_id)
    DEFERRABLE INITIALLY DEFERRED,
  CHECK (version > 1 OR supersedes_relation_version_id IS NULL)
);

ALTER TABLE relation_truth_admission_decisions
  DROP CONSTRAINT IF EXISTS relation_truth_admission_decisions_version_fk;
ALTER TABLE relation_truth_admission_decisions
  ADD CONSTRAINT relation_truth_admission_decisions_version_fk
  FOREIGN KEY (tenant_id, candidate_relation_id, admitted_relation_version_id)
  REFERENCES relation_truth_versions (tenant_id, relation_id, relation_version_id)
  DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE truth_command_receipts
  DROP CONSTRAINT IF EXISTS truth_command_receipts_relation_version_fk;
ALTER TABLE truth_command_receipts
  ADD CONSTRAINT truth_command_receipts_relation_version_fk
  FOREIGN KEY (tenant_id, result_relation_version_id)
  REFERENCES relation_truth_versions (tenant_id, relation_version_id)
  DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS relation_truth_heads (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  relation_id UUID NOT NULL REFERENCES relation_instances(id),
  relation_version_id UUID NOT NULL,
  version INTEGER NOT NULL CHECK (version >= 1),
  semantic_digest TEXT NOT NULL CHECK (semantic_digest ~ '^[0-9a-f]{64}$'),
  lifecycle TEXT NOT NULL CHECK (lifecycle IN ('active', 'disputed', 'retired')),
  advanced_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (tenant_id, relation_id),
  UNIQUE (tenant_id, relation_version_id),
  FOREIGN KEY (tenant_id, relation_id, relation_version_id, version, semantic_digest, lifecycle)
    REFERENCES relation_truth_versions (
      tenant_id, relation_id, relation_version_id, version, semantic_digest, lifecycle
    ) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS model_truth_lifecycle_events (
  lifecycle_event_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_id UUID NOT NULL,
  command_id UUID NOT NULL,
  from_version_id UUID NOT NULL,
  to_version_id UUID NOT NULL,
  transition TEXT NOT NULL CHECK (
    transition IN ('confirm', 'contest', 'falsify', 'supersede', 'archive')
  ),
  reason_codes TEXT[] NOT NULL CHECK (cardinality(reason_codes) > 0),
  occurred_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, command_id),
  UNIQUE (tenant_id, model_id, from_version_id, to_version_id),
  FOREIGN KEY (tenant_id, command_id)
    REFERENCES truth_command_receipts (tenant_id, command_id)
    DEFERRABLE INITIALLY DEFERRED,
  FOREIGN KEY (tenant_id, from_version_id)
    REFERENCES model_truth_versions (tenant_id, version_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, to_version_id)
    REFERENCES model_truth_versions (tenant_id, version_id) ON DELETE RESTRICT,
  CHECK (from_version_id <> to_version_id)
);

CREATE TABLE IF NOT EXISTS relation_truth_participants (
  participant_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  relation_version_id UUID NOT NULL,
  model_id UUID NOT NULL,
  model_version_id UUID NOT NULL,
  role TEXT NOT NULL CHECK (btrim(role) <> ''),
  ordinal INTEGER NOT NULL DEFAULT 0 CHECK (ordinal >= 0),
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, relation_version_id, role, ordinal),
  UNIQUE (tenant_id, relation_version_id, model_version_id, role),
  FOREIGN KEY (tenant_id, relation_version_id)
    REFERENCES relation_truth_versions (tenant_id, relation_version_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, model_version_id)
    REFERENCES model_truth_versions (tenant_id, version_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, model_id, model_version_id)
    REFERENCES model_truth_versions (tenant_id, model_id, version_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS relation_truth_evidence (
  relation_evidence_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  relation_version_id UUID NOT NULL,
  evidence_reference_id UUID NOT NULL,
  model_version_id UUID NOT NULL,
  polarity SMALLINT NOT NULL CHECK (polarity IN (-1, 1)),
  weight DOUBLE PRECISION NOT NULL CHECK (weight >= 0.0 AND weight <= 1.0),
  evidence_digest TEXT NOT NULL CHECK (evidence_digest ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL,
  UNIQUE (tenant_id, relation_version_id, evidence_reference_id),
  FOREIGN KEY (tenant_id, relation_version_id)
    REFERENCES relation_truth_versions (tenant_id, relation_version_id) ON DELETE RESTRICT,
  FOREIGN KEY (tenant_id, model_version_id, evidence_reference_id)
    REFERENCES model_truth_evidence_references (tenant_id, model_version_id, reference_id)
    ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS truth_repair_obligations (
  obligation_id UUID PRIMARY KEY,
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  invalidated_model_version_id UUID NOT NULL,
  affected_kind TEXT NOT NULL CHECK (
    affected_kind IN ('model_version', 'relation_version', 'projection')
  ),
  affected_id UUID NOT NULL,
  cause_code TEXT NOT NULL CHECK (btrim(cause_code) <> ''),
  status TEXT NOT NULL DEFAULT 'pending' CHECK (
    status IN ('pending', 'in_progress', 'resolved', 'waived')
  ),
  created_at TIMESTAMPTZ NOT NULL,
  resolved_at TIMESTAMPTZ,
  resolution JSONB,
  UNIQUE (
    tenant_id, invalidated_model_version_id, affected_kind, affected_id, cause_code
  ),
  FOREIGN KEY (tenant_id, invalidated_model_version_id)
    REFERENCES model_truth_versions (tenant_id, version_id) ON DELETE RESTRICT,
  CHECK ((status IN ('resolved', 'waived')) = (resolved_at IS NOT NULL)),
  CHECK (resolution IS NULL OR jsonb_typeof(resolution) = 'object')
);

CREATE TABLE IF NOT EXISTS model_activity_sidecar (
  tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
  model_id UUID NOT NULL,
  retrieval_count BIGINT NOT NULL DEFAULT 0 CHECK (retrieval_count >= 0),
  activation DOUBLE PRECISION NOT NULL DEFAULT 1.0 CHECK (activation >= 0.0),
  first_retrieved_at TIMESTAMPTZ,
  last_retrieved_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, model_id),
  CHECK (
    first_retrieved_at IS NULL
    OR last_retrieved_at IS NULL
    OR first_retrieved_at <= last_retrieved_at
  )
);

CREATE OR REPLACE VIEW accepted_current_models AS
SELECT
  v.model_id AS id,
  v.tenant_id,
  v.proposition,
  v.natural_text,
  v.created_at,
  h.version_id AS truth_version_id,
  h.version AS truth_version,
  h.semantic_digest AS truth_semantic_digest,
  h.lifecycle AS truth_lifecycle,
  h.advanced_at AS truth_advanced_at
FROM model_truth_heads h
JOIN model_truth_versions v
  ON v.tenant_id = h.tenant_id
 AND v.version_id = h.version_id
JOIN truth_admission_decisions d
  ON d.tenant_id = v.tenant_id
 AND d.decision_id = v.admission_decision_id
 AND d.disposition = 'accepted'
WHERE h.lifecycle = 'active'
  AND EXISTS (
    SELECT 1
    FROM model_truth_evidence_references evidence
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

CREATE OR REPLACE VIEW accepted_current_relations AS
SELECT
  r.*,
  h.relation_version_id AS truth_relation_version_id,
  h.version AS truth_version,
  h.semantic_digest AS truth_semantic_digest,
  h.lifecycle AS truth_lifecycle,
  h.advanced_at AS truth_advanced_at,
  v.relation_kind AS truth_relation_kind,
  v.rationale AS truth_rationale
FROM relation_truth_heads h
JOIN relation_truth_versions v
  ON v.tenant_id = h.tenant_id
 AND v.relation_version_id = h.relation_version_id
JOIN relation_truth_admission_decisions d
  ON d.tenant_id = v.tenant_id
 AND d.decision_id = v.admission_decision_id
 AND d.disposition = 'accepted'
JOIN relation_instances r
  ON r.tenant_id = h.tenant_id
 AND r.id = h.relation_id
WHERE h.lifecycle = 'active'
  AND EXISTS (
    SELECT 1
    FROM relation_truth_evidence evidence
    WHERE evidence.tenant_id = v.tenant_id
      AND evidence.relation_version_id = v.relation_version_id
      AND evidence.polarity = 1
  )
  AND 2 <= (
    SELECT count(*)
    FROM relation_truth_participants participant
    WHERE participant.tenant_id = v.tenant_id
      AND participant.relation_version_id = v.relation_version_id
  )
  AND NOT EXISTS (
    SELECT 1
    FROM relation_truth_participants participant
    LEFT JOIN model_truth_heads model_head
      ON model_head.tenant_id = participant.tenant_id
     AND model_head.version_id = participant.model_version_id
     AND model_head.lifecycle = 'active'
    WHERE participant.tenant_id = v.tenant_id
      AND participant.relation_version_id = v.relation_version_id
      AND model_head.version_id IS NULL
  )
  AND NOT EXISTS (
    SELECT 1
    FROM relation_truth_evidence evidence
    LEFT JOIN model_truth_heads evidence_head
      ON evidence_head.tenant_id = evidence.tenant_id
     AND evidence_head.version_id = evidence.model_version_id
     AND evidence_head.lifecycle = 'active'
    WHERE evidence.tenant_id = v.tenant_id
      AND evidence.relation_version_id = v.relation_version_id
      AND evidence_head.version_id IS NULL
  );

CREATE INDEX IF NOT EXISTS truth_candidates_review_idx
  ON truth_candidates (tenant_id, review_state, created_at DESC);
CREATE INDEX IF NOT EXISTS model_truth_versions_model_idx
  ON model_truth_versions (tenant_id, model_id, version DESC);
CREATE INDEX IF NOT EXISTS model_truth_evidence_source_idx
  ON model_truth_evidence_references (tenant_id, evidence_kind, evidence_id, evidence_version);
CREATE INDEX IF NOT EXISTS model_truth_scope_subject_idx
  ON model_truth_scope_bindings (tenant_id, subject_kind, subject_id, scope_role);
CREATE INDEX IF NOT EXISTS relation_truth_versions_relation_idx
  ON relation_truth_versions (tenant_id, relation_id, version DESC);
CREATE INDEX IF NOT EXISTS relation_truth_participants_model_idx
  ON relation_truth_participants (tenant_id, model_version_id, role);
CREATE INDEX IF NOT EXISTS truth_repair_obligations_pending_idx
  ON truth_repair_obligations (tenant_id, created_at)
  WHERE status IN ('pending', 'in_progress');

-- Only heads, repair workflow state, and retrieval activity are mutable.
DO $$
DECLARE
  immutable_table TEXT;
BEGIN
  FOREACH immutable_table IN ARRAY ARRAY[
    'truth_candidates',
    'truth_admission_decisions',
    'truth_command_receipts',
    'model_truth_versions',
    'model_truth_lifecycle_events',
    'model_truth_evidence_references',
    'model_truth_scope_bindings',
    'model_truth_scope_evidence',
    'relation_truth_admission_decisions',
    'relation_truth_versions',
    'relation_truth_participants',
    'relation_truth_evidence'
  ]
  LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I_immutable ON %I', immutable_table, immutable_table);
    EXECUTE format(
      'CREATE TRIGGER %I_immutable BEFORE UPDATE OR DELETE ON %I '
      'FOR EACH ROW EXECUTE FUNCTION reject_consequential_immutable_mutation()',
      immutable_table, immutable_table
    );
  END LOOP;
END $$;

CREATE OR REPLACE FUNCTION enforce_model_truth_head_transition()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.lifecycle IN ('falsified', 'superseded', 'archived') THEN
    RAISE EXCEPTION 'terminal Model truth head cannot advance';
  END IF;
  IF NEW.version <= OLD.version OR NEW.version_id = OLD.version_id THEN
    RAISE EXCEPTION 'Model truth head must advance to a new higher version';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_scope_subject_kind_coherence()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM model_truth_scope_bindings existing
    WHERE existing.tenant_id = NEW.tenant_id
      AND existing.model_version_id = NEW.model_version_id
      AND existing.subject_id = NEW.subject_id
      AND existing.subject_kind <> NEW.subject_kind
  ) THEN
    RAISE EXCEPTION 'one claim scope subject cannot have conflicting entity types';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS model_truth_scope_subject_kind_guard
  ON model_truth_scope_bindings;
CREATE TRIGGER model_truth_scope_subject_kind_guard
  BEFORE INSERT ON model_truth_scope_bindings
  FOR EACH ROW EXECUTE FUNCTION enforce_scope_subject_kind_coherence();

DROP TRIGGER IF EXISTS model_truth_heads_transition_guard ON model_truth_heads;
CREATE TRIGGER model_truth_heads_transition_guard
  BEFORE UPDATE ON model_truth_heads
  FOR EACH ROW EXECUTE FUNCTION enforce_model_truth_head_transition();

CREATE OR REPLACE FUNCTION enforce_relation_truth_head_transition()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.lifecycle = 'retired' THEN
    RAISE EXCEPTION 'retired relation truth head cannot advance';
  END IF;
  IF NEW.version <= OLD.version OR NEW.relation_version_id = OLD.relation_version_id THEN
    RAISE EXCEPTION 'relation truth head must advance to a new higher version';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS relation_truth_heads_transition_guard ON relation_truth_heads;
CREATE TRIGGER relation_truth_heads_transition_guard
  BEFORE UPDATE ON relation_truth_heads
  FOR EACH ROW EXECUTE FUNCTION enforce_relation_truth_head_transition();

DO $$
DECLARE
  tenant_table TEXT;
BEGIN
  FOREACH tenant_table IN ARRAY ARRAY[
    'truth_candidates', 'truth_admission_decisions', 'truth_command_receipts',
    'model_truth_versions', 'model_truth_lifecycle_events',
    'model_truth_heads', 'model_truth_evidence_references',
    'model_truth_scope_bindings', 'model_truth_scope_evidence',
    'relation_truth_admission_decisions', 'relation_truth_versions',
    'relation_truth_heads', 'relation_truth_participants',
    'relation_truth_evidence', 'truth_repair_obligations',
    'model_activity_sidecar'
  ]
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', tenant_table);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', tenant_table);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', tenant_table);
    EXECUTE format(
      'CREATE POLICY tenant_isolation ON %I USING ('
      'NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL OR '
      'tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid'
      ') WITH CHECK ('
      'NULLIF(current_setting(''app.current_tenant'', true), '''') IS NULL OR '
      'tenant_id = NULLIF(current_setting(''app.current_tenant'', true), '''')::uuid)',
      tenant_table
    );
  END LOOP;
END $$;

COMMENT ON VIEW accepted_current_models IS
  'Canonical Model read boundary: admitted exact current versions with active lifecycle only.';
COMMENT ON VIEW accepted_current_relations IS
  'Canonical business-relation read boundary: admitted exact current relation versions only.';
COMMENT ON TABLE model_activity_sidecar IS
  'Mutable retrieval activity isolated from immutable Model semantics and truth heads.';

COMMIT;
