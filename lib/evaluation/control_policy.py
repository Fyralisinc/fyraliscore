"""Continuous evaluation of governed adaptive-policy and learned-state control."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from statistics import median
from typing import Any, Mapping, Sequence
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.kernel import canonical_sha256
from lib.contracts.runtime import (
    BootstrapPolicy,
    ControlPolicyCandidate,
    ControlPolicyState,
    ControlPolicyVersion,
    ExperimentAssignment,
    ExperimentAssignmentArm,
    ExperimentEffectDirection,
    ExperimentPlan,
    LearnedArtifactManifest,
    LearnedArtifactStateTransitionCommand,
    LearnedArtifactStatus,
    LearningUpdate,
    PolicyEligibilityMeasurement,
    PolicyPromotionDecision,
    PolicyPromotionDisposition,
    PolicyRegistryRegistrationCommand,
    PolicyStateTransitionCommand,
)
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceTier,
    FateDenominatorRecord,
    IncidentObservation,
    IncidentStatus,
    InvariantRunEvidence,
    MetricObservation,
)


class _ControlEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ControlPolicyEvaluationScope(_ControlEvaluationModel):
    tenant_id: UUID
    start: datetime
    end: datetime
    run_id: str = Field(min_length=1)

    @field_validator("start", "end")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def interval_is_forward(self):
        if self.end <= self.start:
            raise ValueError("control-policy evaluation end must follow start")
        return self


class ControlPolicyEvaluationState(_ControlEvaluationModel):
    scope: ControlPolicyEvaluationScope
    bootstrap_count: int = Field(ge=0)
    valid_bootstrap_count: int = Field(ge=0)
    bootstrap_contract_validity_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    cold_start_fate_counts: dict[str, int]
    artifact_count: int = Field(ge=0)
    valid_artifact_manifest_count: int = Field(ge=0)
    artifact_manifest_validity_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    governed_artifact_activation_count: int = Field(ge=0)
    artifact_activation_count: int = Field(ge=0)
    artifact_activation_governance_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    experiment_plan_count: int = Field(ge=0)
    valid_preregistered_plan_count: int = Field(ge=0)
    experiment_preregistration_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    experiment_assignment_count: int = Field(ge=0)
    valid_preexposure_assignment_count: int = Field(ge=0)
    assignment_integrity_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    candidate_count: int = Field(ge=0)
    bootstrap_covered_candidate_count: int = Field(ge=0)
    candidate_bootstrap_coverage_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    policy_state_counts: dict[str, int]
    policy_version_count: int = Field(ge=0)
    legal_policy_version_count: int = Field(ge=0)
    policy_lifecycle_conformance_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    measurement_count: int = Field(ge=0)
    independently_reconstructable_measurement_count: int = Field(ge=0)
    measurement_reconstructability_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    eligibility_correct_measurement_count: int = Field(ge=0)
    eligibility_correctness_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    qualifying_measurement_count: int = Field(ge=0)
    promotion_decision_count: int = Field(ge=0)
    independently_authorized_promotion_count: int = Field(ge=0)
    promotion_authority_separation_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    active_policy_count: int = Field(ge=0)
    governed_active_policy_count: int = Field(ge=0)
    governed_activation_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    family_head_count: int = Field(ge=0)
    valid_family_head_count: int = Field(ge=0)
    family_head_integrity_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    frozen_fallback_count: int = Field(ge=0)
    valid_frozen_fallback_count: int = Field(ge=0)
    frozen_fallback_integrity_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    learning_update_count: int = Field(ge=0)
    proposal_only_learning_update_count: int = Field(ge=0)
    proposal_only_learning_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    corrected_learning_update_count: int = Field(ge=0)
    reward_retracted_update_count: int = Field(ge=0)
    reward_retraction_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    command_count: int = Field(ge=0)
    reconstructable_command_count: int = Field(ge=0)
    command_reconstructability_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    command_event_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    command_outbox_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    immutable_table_count: int = Field(ge=0)
    guarded_immutable_table_count: int = Field(ge=0)
    immutable_storage_guard_rate: float = Field(ge=0.0, le=1.0)
    median_seconds_to_eligibility: float | None = Field(default=None, ge=0.0)
    median_seconds_to_activation: float | None = Field(default=None, ge=0.0)
    mean_qualified_effect: float | None = None
    incident_counts: dict[str, int]
    incident_refs: dict[str, tuple[str, ...]]
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


async def evaluate_control_policy_state(
    conn: asyncpg.Connection,
    *,
    scope: ControlPolicyEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> ControlPolicyEvaluationState:
    bootstraps = await conn.fetch(
        """
        SELECT * FROM policy_bootstrap_policies
        WHERE tenant_id = $1 AND registered_at >= $2 AND registered_at < $3
        ORDER BY registered_at, registry_object_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    artifacts = await conn.fetch(
        """
        SELECT m.*, h.current_status, h.current_status_version,
               h.promotion_decision_ref
        FROM learned_artifact_manifests m
        JOIN learned_artifact_heads h
          ON h.tenant_id = m.tenant_id
         AND h.artifact_id = m.artifact_id
         AND h.artifact_version = m.artifact_version
        WHERE m.tenant_id = $1 AND m.registered_at >= $2 AND m.registered_at < $3
        ORDER BY m.registered_at, m.registry_object_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    artifact_versions = await conn.fetch(
        """
        SELECT * FROM learned_artifact_status_versions
        WHERE tenant_id = $1 AND transitioned_at >= $2 AND transitioned_at < $3
        ORDER BY artifact_id, artifact_version, status_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    plans = await conn.fetch(
        """
        SELECT * FROM policy_experiment_plans
        WHERE tenant_id = $1 AND preregistered_at >= $2 AND preregistered_at < $3
        ORDER BY preregistered_at, plan_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    assignments = await conn.fetch(
        """
        SELECT * FROM policy_experiment_assignments
        WHERE tenant_id = $1 AND assigned_at >= $2 AND assigned_at < $3
        ORDER BY assigned_at, assignment_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    candidates = await conn.fetch(
        """
        SELECT c.*, h.current_version, h.current_state,
               h.current_version_digest
        FROM control_policy_candidates c
        JOIN control_policy_heads h
          ON h.tenant_id = c.tenant_id AND h.policy_id = c.policy_id
        WHERE c.tenant_id = $1 AND c.created_at >= $2 AND c.created_at < $3
        ORDER BY c.created_at, c.policy_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    versions = await conn.fetch(
        """
        SELECT * FROM control_policy_versions
        WHERE tenant_id = $1 AND effective_at >= $2 AND effective_at < $3
        ORDER BY policy_id, aggregate_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    measurements = await conn.fetch(
        """
        SELECT * FROM policy_eligibility_measurements
        WHERE tenant_id = $1 AND measured_at >= $2 AND measured_at < $3
        ORDER BY measured_at, measurement_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    decisions = await conn.fetch(
        """
        SELECT * FROM policy_promotion_decisions
        WHERE tenant_id = $1 AND decided_at >= $2 AND decided_at < $3
        ORDER BY decided_at, decision_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    family_heads = await conn.fetch(
        """SELECT * FROM policy_family_heads WHERE tenant_id = $1""",
        scope.tenant_id,
    )
    family_versions = await conn.fetch(
        """
        SELECT * FROM policy_family_versions
        WHERE tenant_id = $1 AND effective_at >= $2 AND effective_at < $3
        ORDER BY policy_family, family_version
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    updates = await conn.fetch(
        """
        SELECT * FROM policy_learning_updates
        WHERE tenant_id = $1 AND created_at >= $2 AND created_at < $3
        ORDER BY created_at, update_id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    commands = await conn.fetch(
        """
        SELECT r.*,
               (SELECT count(*) FROM agency_canonical_events e
                 WHERE e.command_result_id = r.id) AS event_count,
               (SELECT count(*) FROM agency_canonical_events e
                 JOIN agency_outbox_records o ON o.event_id = e.id
                 WHERE e.command_result_id = r.id) AS outbox_count
        FROM agency_command_results r
        WHERE r.tenant_id = $1 AND r.writer_id = 'PolicyRegistryApplier'
          AND r.created_at >= $2 AND r.created_at < $3
        ORDER BY r.created_at, r.id
        """,
        scope.tenant_id,
        scope.start,
        scope.end,
    )
    outcome_evidence = await conn.fetch(
        """
        SELECT s.id AS settlement_id, s.episode_id, s.disposition,
               p.metric_definition AS prediction_metric,
               o.metric_definition AS outcome_metric,
               o.independent_of_execution_claim, o.valid_time,
               o.outcome->'observed_value' AS observed_value
        FROM consequential_settlements s
        JOIN consequential_predictions p ON p.id = s.prediction_id
        LEFT JOIN consequential_outcomes o ON o.id = s.outcome_id
        WHERE s.tenant_id = $1
        """,
        scope.tenant_id,
    )
    attribution_evidence = await conn.fetch(
        """
        SELECT id, settlement_id, subject_ref, withheld_credit
        FROM consequential_attributions WHERE tenant_id = $1
        """,
        scope.tenant_id,
    )
    guarded_tables = await conn.fetch(
        """
        SELECT DISTINCT c.relname AS table_name
        FROM pg_trigger t
        JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND NOT t.tgisinternal
          AND t.tgname LIKE 'reject_%_mutation'
        """
    )
    return analyze_control_policy_rows(
        scope=scope,
        bootstraps=bootstraps,
        artifacts=artifacts,
        artifact_versions=artifact_versions,
        plans=plans,
        assignments=assignments,
        candidates=candidates,
        versions=versions,
        measurements=measurements,
        decisions=decisions,
        family_heads=family_heads,
        family_versions=family_versions,
        updates=updates,
        commands=commands,
        outcome_evidence=outcome_evidence,
        attribution_evidence=attribution_evidence,
        guarded_tables={row["table_name"] for row in guarded_tables},
        artifact_refs=artifact_refs,
    )


def analyze_control_policy_rows(
    *,
    scope: ControlPolicyEvaluationScope,
    bootstraps: Sequence[Mapping[str, Any]],
    artifacts: Sequence[Mapping[str, Any]],
    artifact_versions: Sequence[Mapping[str, Any]],
    plans: Sequence[Mapping[str, Any]],
    assignments: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    versions: Sequence[Mapping[str, Any]],
    measurements: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    family_heads: Sequence[Mapping[str, Any]],
    family_versions: Sequence[Mapping[str, Any]],
    updates: Sequence[Mapping[str, Any]],
    commands: Sequence[Mapping[str, Any]],
    outcome_evidence: Sequence[Mapping[str, Any]],
    attribution_evidence: Sequence[Mapping[str, Any]],
    guarded_tables: set[str],
    artifact_refs: tuple[str, ...],
) -> ControlPolicyEvaluationState:
    incidents: Counter[str] = Counter()
    incident_refs: dict[str, list[str]] = defaultdict(list)

    def fail(name: str, ref: str) -> None:
        incidents[name] += 1
        incident_refs[name].append(ref)

    bootstrap_by_ref: dict[str, BootstrapPolicy] = {}
    valid_bootstraps = 0
    for row in bootstraps:
        ref = str(row["policy_ref"])
        try:
            policy = BootstrapPolicy.model_validate(_json(row["policy"]))
            valid = (
                policy.bootstrap_policy_ref == ref
                and policy.bootstrap_policy_digest == row["policy_digest"]
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            policy = None
        if valid and policy is not None:
            valid_bootstraps += 1
            bootstrap_by_ref[ref] = policy
        else:
            fail("invalid_bootstrap_contract", ref)

    artifact_by_ref: dict[str, LearnedArtifactManifest] = {}
    valid_artifacts = 0
    for row in artifacts:
        ref = str(row["manifest_ref"])
        try:
            manifest = LearnedArtifactManifest.model_validate(_json(row["manifest"]))
            valid = (
                manifest.manifest_ref == ref
                and manifest.manifest_digest == row["manifest_digest"]
                and manifest.status is LearnedArtifactStatus.SHADOW
                and scope.tenant_id in manifest.permitted_tenant_ids
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            manifest = None
        if valid and manifest is not None:
            valid_artifacts += 1
            artifact_by_ref[ref] = manifest
        else:
            fail("invalid_learned_artifact_manifest", ref)

    plan_by_id: dict[UUID, ExperimentPlan] = {}
    valid_plans = 0
    for row in plans:
        ref = f"experiment-plan:{row['plan_id']}"
        try:
            plan = ExperimentPlan.model_validate(_json(row["plan"]))
            valid = (
                plan.plan_id == row["plan_id"]
                and plan.plan_digest == row["plan_digest"]
                and plan.preregistered_at <= plan.exposure_window_start
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            plan = None
        if valid and plan is not None:
            valid_plans += 1
            plan_by_id[plan.plan_id] = plan
        else:
            fail("late_or_invalid_experiment_plan", ref)

    assignment_by_id: dict[UUID, ExperimentAssignment] = {}
    assignment_by_episode: dict[UUID, ExperimentAssignment] = {}
    valid_assignments = 0
    for row in assignments:
        ref = f"experiment-assignment:{row['assignment_id']}"
        try:
            assignment = ExperimentAssignment.model_validate(_json(row["assignment"]))
            plan = plan_by_id[assignment.plan_id]
            episode_id = _required_ref_uuid(assignment.subject_ref, "episode")
            valid = (
                assignment.assignment_digest == row["assignment_digest"]
                and assignment.plan_digest == plan.plan_digest
                and assignment.assigned_at <= assignment.first_exposure_at
                and plan.exposure_window_start
                <= assignment.first_exposure_at
                < plan.exposure_window_end
                and not assignment.invalidated
                and episode_id not in assignment_by_episode
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            assignment = None
            episode_id = None
        if valid and assignment is not None and episode_id is not None:
            valid_assignments += 1
            assignment_by_id[assignment.assignment_id] = assignment
            assignment_by_episode[episode_id] = assignment
        else:
            fail("invalid_or_postexposure_assignment", ref)

    candidate_by_id: dict[UUID, ControlPolicyCandidate] = {}
    bootstrap_covered = 0
    for row in candidates:
        ref = f"control-policy:{row['policy_id']}"
        try:
            candidate = ControlPolicyCandidate.model_validate(_json(row["candidate"]))
            bootstrap = bootstrap_by_ref[candidate.bootstrap_policy_ref]
            manifest = artifact_by_ref[candidate.learned_artifact_ref]
            covered = (
                candidate.candidate_digest == row["candidate_digest"]
                and candidate.policy_family == bootstrap.adaptive_family
                and candidate.purpose in manifest.permitted_purposes
                and candidate.tenant_id in manifest.permitted_tenant_ids
            )
        except (TypeError, ValueError, KeyError):
            covered = False
            candidate = None
        if covered and candidate is not None:
            bootstrap_covered += 1
            candidate_by_id[candidate.policy_id] = candidate
        else:
            fail("candidate_without_governed_bootstrap_or_artifact", ref)

    outcome_by_settlement = {row["settlement_id"]: row for row in outcome_evidence}
    attribution_by_id = {row["id"]: row for row in attribution_evidence}
    measurement_by_id: dict[UUID, PolicyEligibilityMeasurement] = {}
    reconstructable_measurements = 0
    eligibility_correct = 0
    qualifying = 0
    for row in measurements:
        ref = f"policy-eligibility:{row['measurement_id']}"
        try:
            measurement = PolicyEligibilityMeasurement.model_validate(
                _json(row["measurement"])
            )
            candidate = candidate_by_id[measurement.policy_id]
            bootstrap = bootstrap_by_ref[measurement.bootstrap_policy_ref]
            settlement_ids = _unique_ref_ids(measurement.settlement_refs, "settlement")
            attribution_ids = _unique_ref_ids(
                measurement.attribution_refs, "attribution"
            )
            settlement_rows = [outcome_by_settlement[item] for item in settlement_ids]
            attribution_rows = [attribution_by_id[item] for item in attribution_ids]
            evidence_valid = (
                measurement.measurement_digest == row["measurement_digest"]
                and measurement.candidate_digest == candidate.candidate_digest
                and measurement.required_independent_evidence_count
                == bootstrap.minimum_independent_evidence
                and measurement.primary_metric_id == bootstrap.promotion_metric_id
                and measurement.minimum_effect == bootstrap.minimum_effect
                and measurement.maximum_harm_rate == bootstrap.maximum_harm_rate
                and measurement.independent_evidence_count == len(settlement_rows)
                and {item["settlement_id"] for item in attribution_rows}
                == set(settlement_ids)
                and all(
                    item["disposition"] == "settled"
                    and item["prediction_metric"] == measurement.primary_metric_id
                    and item["outcome_metric"] == measurement.primary_metric_id
                    and item["independent_of_execution_claim"]
                    for item in settlement_rows
                )
                and all(not item["withheld_credit"] for item in attribution_rows)
            )
            computed_effect = _reconstruct_effect(
                measurement=measurement,
                candidate=candidate,
                plan_by_id=plan_by_id,
                assignment_by_id=assignment_by_id,
                assignment_by_episode=assignment_by_episode,
                settlement_rows=settlement_rows,
                attribution_rows=attribution_rows,
            )
            if computed_effect is not None:
                evidence_valid = evidence_valid and abs(
                    measurement.observed_effect - computed_effect
                ) <= 1e-9
            correct = evidence_valid and bool(row["eligible"]) == measurement.eligible
        except (TypeError, ValueError, KeyError):
            evidence_valid = False
            correct = False
            measurement = None
        reconstructable_measurements += int(evidence_valid)
        eligibility_correct += int(correct)
        if correct and measurement is not None:
            measurement_by_id[measurement.measurement_id] = measurement
            qualifying += int(measurement.eligible)
        else:
            fail("unreconstructable_or_incorrect_policy_eligibility", ref)

    command_by_result = {row["id"]: row for row in commands}
    decision_by_id: dict[UUID, PolicyPromotionDecision] = {}
    independent_promotions = 0
    for row in decisions:
        ref = f"policy-promotion:{row['decision_id']}"
        try:
            decision = PolicyPromotionDecision.model_validate(_json(row["decision"]))
            candidate = candidate_by_id[decision.policy_id]
            measurement = measurement_by_id[decision.eligibility_measurement_id]
            command_row = command_by_result[row["command_result_id"]]
            command = PolicyStateTransitionCommand.model_validate(
                _json(command_row["command"])
            )
            valid = (
                decision.decision_digest == row["decision_digest"]
                and decision.candidate_digest == candidate.candidate_digest
                and decision.eligibility_measurement_digest
                == measurement.measurement_digest
                and measurement.eligible
                and decision.disposition is PolicyPromotionDisposition.AUTHORIZED
                and decision.authority.is_live(decision.decided_at)
                and decision.authority.operation == "authorize_policy_promotion"
                and decision.authority.purpose == candidate.purpose
                and decision.authority.object_types.permits("control_policy")
                and decision.authority.object_ids.permits(str(candidate.policy_id))
                and decision.governance_principal_ref
                != command.context.processing_authority.principal_or_service_id
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            decision = None
        independent_promotions += int(valid)
        if valid and decision is not None:
            decision_by_id[decision.decision_id] = decision
        else:
            fail("unearned_or_self_authorized_policy_promotion", ref)

    versions_by_policy: dict[UUID, list[ControlPolicyVersion]] = defaultdict(list)
    legal_versions = 0
    eligibility_delays: list[float] = []
    activation_delays: list[float] = []
    active_policy_ids: set[UUID] = set()
    governed_active_ids: set[UUID] = set()
    state_counts: Counter[str] = Counter()
    for row in versions:
        ref = f"control-policy:{row['policy_id']}:v{row['aggregate_version']}"
        try:
            version = ControlPolicyVersion.model_validate(_json(row["policy_version"]))
            valid = (
                version.version_digest == row["version_digest"]
                and version.policy_id == row["policy_id"]
                and version.aggregate_version == row["aggregate_version"]
            )
        except (TypeError, ValueError, KeyError):
            valid = False
            version = None
        legal_versions += int(valid)
        if valid and version is not None:
            versions_by_policy[version.policy_id].append(version)
            state_counts[version.state.value] += 1
        else:
            fail("invalid_control_policy_version", ref)
    for policy_id, policy_versions in versions_by_policy.items():
        candidate = candidate_by_id.get(policy_id)
        if candidate is None:
            continue
        lifecycle_ok = _policy_lifecycle_valid(policy_versions)
        if not lifecycle_ok:
            for version in policy_versions:
                fail(
                    "illegal_control_policy_lifecycle",
                    f"control-policy:{policy_id}:v{version.aggregate_version}",
                )
            legal_versions -= len(policy_versions)
            continue
        for version in policy_versions:
            if version.state is ControlPolicyState.ELIGIBLE:
                eligibility_delays.append(
                    (version.effective_at - candidate.created_at).total_seconds()
                )
            if version.state is ControlPolicyState.ACTIVE:
                active_policy_ids.add(policy_id)
                activation_delays.append(
                    (version.effective_at - candidate.created_at).total_seconds()
                )
                measurement_id = _optional_ref_uuid(
                    version.eligibility_measurement_ref, "policy-eligibility"
                )
                decision_id = _optional_ref_uuid(
                    version.promotion_decision_ref, "policy-promotion"
                )
                states_before = {
                    item.state
                    for item in policy_versions
                    if item.aggregate_version < version.aggregate_version
                }
                if (
                    measurement_id in measurement_by_id
                    and measurement_by_id[measurement_id].eligible
                    and decision_id in decision_by_id
                    and ControlPolicyState.CANARY in states_before
                ):
                    governed_active_ids.add(policy_id)
                else:
                    fail(
                        "active_policy_without_measurement_authority_or_canary",
                        f"control-policy:{policy_id}:v{version.aggregate_version}",
                    )

    active_artifact_versions = [
        row for row in artifact_versions if row["to_status"] == "active"
    ]
    governed_artifact_activations = 0
    for row in active_artifact_versions:
        ref = f"learned-artifact:{row['artifact_id']}:{row['artifact_version']}"
        try:
            decision_id = _required_ref_uuid(
                row["promotion_decision_ref"], "policy-promotion"
            )
            decision = decision_by_id[decision_id]
            manifest_ref = ref
            candidate = candidate_by_id[decision.policy_id]
            valid = candidate.learned_artifact_ref == manifest_ref
        except (TypeError, ValueError, KeyError):
            valid = False
        governed_artifact_activations += int(valid)
        if not valid:
            fail("ungoverned_learned_artifact_activation", ref)

    family_versions_by_name: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in family_versions:
        family_versions_by_name[str(row["policy_family"])].append(row)
    valid_family_heads = 0
    frozen_fallbacks = 0
    valid_fallbacks = 0
    for row in family_heads:
        family = str(row["policy_family"])
        ref = f"policy-family:{family}"
        history = family_versions_by_name.get(family, [])
        sequential = [int(item["family_version"]) for item in history]
        valid = bool(history) and sequential == list(range(1, len(history) + 1))
        if valid:
            latest = history[-1]
            valid = (
                int(row["family_version"]) == int(latest["family_version"])
                and row["active_policy_id"] == latest["active_policy_id"]
                and row["active_policy_aggregate_version"]
                == latest["active_policy_aggregate_version"]
                and row["active_candidate_digest"]
                == latest["active_candidate_digest"]
            )
            if row["active_policy_id"] is not None:
                policy_id = row["active_policy_id"]
                current = next(
                    (
                        candidate_row
                        for candidate_row in candidates
                        if candidate_row["policy_id"] == policy_id
                    ),
                    None,
                )
                valid = valid and bool(
                    current
                    and current["current_state"] == "active"
                    and current["current_version"]
                    == row["active_policy_aggregate_version"]
                )
            else:
                frozen_fallbacks += 1
                fallback_valid = bool(latest["fallback_control_ref"])
                valid_fallbacks += int(fallback_valid)
                valid = valid and fallback_valid
        valid_family_heads += int(valid)
        if not valid:
            fail("invalid_control_policy_family_head", ref)

    proposal_only_updates = 0
    corrected_updates = 0
    retracted_updates = 0
    policy_version_command_ids = {row["command_result_id"] for row in versions}
    for row in updates:
        ref = f"learning-update:{row['update_id']}"
        try:
            update = LearningUpdate.model_validate(_json(row["learning_update"]))
            proposal_only = (
                update.update_digest == row["update_digest"]
                and row["command_result_id"] not in policy_version_command_ids
            )
            corrected = update.correction_epoch > 0
            retracted = update.reward_retracted
        except (TypeError, ValueError, KeyError):
            proposal_only = False
            corrected = False
            retracted = False
        proposal_only_updates += int(proposal_only)
        corrected_updates += int(corrected)
        retracted_updates += int(corrected and retracted)
        if not proposal_only:
            fail("learning_update_mutated_policy_directly", ref)
        if corrected and not retracted:
            fail("corrected_reward_not_retracted", ref)

    reconstructable_commands = 0
    event_covered = 0
    outbox_covered = 0
    for row in commands:
        ref = f"command-result:{row['id']}"
        reconstructable = _command_reconstructable(row)
        reconstructable_commands += int(reconstructable)
        event_covered += int(int(row.get("event_count") or 0) == 1)
        outbox_covered += int(int(row.get("outbox_count") or 0) == 1)
        if not reconstructable:
            fail("unreconstructable_policy_command", ref)
        if int(row.get("event_count") or 0) != 1:
            fail("policy_command_without_one_event", ref)
        if int(row.get("outbox_count") or 0) != 1:
            fail("policy_command_without_one_outbox", ref)

    immutable_tables = {
        "policy_bootstrap_policies",
        "policy_experiment_plans",
        "policy_experiment_assignments",
        "learned_artifact_manifests",
        "learned_artifact_status_versions",
        "control_policy_candidates",
        "policy_eligibility_measurements",
        "policy_promotion_decisions",
        "control_policy_versions",
        "policy_family_versions",
        "policy_learning_updates",
    }
    guarded = len(immutable_tables & guarded_tables)
    for table in sorted(immutable_tables - guarded_tables):
        fail("unguarded_immutable_policy_table", f"table:{table}")

    current_state_by_bootstrap: dict[str, list[str]] = defaultdict(list)
    for row in candidates:
        current_state_by_bootstrap[str(row["bootstrap_policy_ref"])].append(
            str(row["current_state"])
        )
    cold_start_fates: Counter[str] = Counter()
    for ref in bootstrap_by_ref:
        states = current_state_by_bootstrap.get(ref, [])
        if not states:
            cold_start_fates["bootstrap_active"] += 1
        elif "active" in states:
            cold_start_fates["promoted"] += 1
        elif "frozen" in states:
            cold_start_fates["frozen_fallback"] += 1
        elif "rolled_back" in states:
            cold_start_fates["rolled_back"] += 1
        elif "shadow" in states or "eligible" in states or "authorized" in states:
            cold_start_fates["shadowed"] += 1
        elif "rejected" in states or "superseded" in states:
            cold_start_fates["expired_or_rejected"] += 1
        else:
            cold_start_fates["candidate"] += 1

    qualified_effects = [
        item.observed_effect for item in measurement_by_id.values() if item.eligible
    ]
    return ControlPolicyEvaluationState(
        scope=scope,
        bootstrap_count=len(bootstraps),
        valid_bootstrap_count=valid_bootstraps,
        bootstrap_contract_validity_rate=_rate(valid_bootstraps, len(bootstraps)),
        cold_start_fate_counts=dict(sorted(cold_start_fates.items())),
        artifact_count=len(artifacts),
        valid_artifact_manifest_count=valid_artifacts,
        artifact_manifest_validity_rate=_rate(valid_artifacts, len(artifacts)),
        governed_artifact_activation_count=governed_artifact_activations,
        artifact_activation_count=len(active_artifact_versions),
        artifact_activation_governance_rate=_rate(
            governed_artifact_activations, len(active_artifact_versions)
        ),
        experiment_plan_count=len(plans),
        valid_preregistered_plan_count=valid_plans,
        experiment_preregistration_rate=_rate(valid_plans, len(plans)),
        experiment_assignment_count=len(assignments),
        valid_preexposure_assignment_count=valid_assignments,
        assignment_integrity_rate=_rate(valid_assignments, len(assignments)),
        candidate_count=len(candidates),
        bootstrap_covered_candidate_count=bootstrap_covered,
        candidate_bootstrap_coverage_rate=_rate(bootstrap_covered, len(candidates)),
        policy_state_counts=dict(sorted(state_counts.items())),
        policy_version_count=len(versions),
        legal_policy_version_count=max(0, legal_versions),
        policy_lifecycle_conformance_rate=_rate(max(0, legal_versions), len(versions)),
        measurement_count=len(measurements),
        independently_reconstructable_measurement_count=reconstructable_measurements,
        measurement_reconstructability_rate=_rate(
            reconstructable_measurements, len(measurements)
        ),
        eligibility_correct_measurement_count=eligibility_correct,
        eligibility_correctness_rate=_rate(eligibility_correct, len(measurements)),
        qualifying_measurement_count=qualifying,
        promotion_decision_count=len(decisions),
        independently_authorized_promotion_count=independent_promotions,
        promotion_authority_separation_rate=_rate(
            independent_promotions, len(decisions)
        ),
        active_policy_count=len(active_policy_ids),
        governed_active_policy_count=len(governed_active_ids),
        governed_activation_rate=_rate(
            len(governed_active_ids), len(active_policy_ids)
        ),
        family_head_count=len(family_heads),
        valid_family_head_count=valid_family_heads,
        family_head_integrity_rate=_rate(valid_family_heads, len(family_heads)),
        frozen_fallback_count=frozen_fallbacks,
        valid_frozen_fallback_count=valid_fallbacks,
        frozen_fallback_integrity_rate=_rate(valid_fallbacks, frozen_fallbacks),
        learning_update_count=len(updates),
        proposal_only_learning_update_count=proposal_only_updates,
        proposal_only_learning_rate=_rate(proposal_only_updates, len(updates)),
        corrected_learning_update_count=corrected_updates,
        reward_retracted_update_count=retracted_updates,
        reward_retraction_rate=_rate(retracted_updates, corrected_updates),
        command_count=len(commands),
        reconstructable_command_count=reconstructable_commands,
        command_reconstructability_rate=_rate(reconstructable_commands, len(commands)),
        command_event_coverage=_rate(event_covered, len(commands)),
        command_outbox_coverage=_rate(outbox_covered, len(commands)),
        immutable_table_count=len(immutable_tables),
        guarded_immutable_table_count=guarded,
        immutable_storage_guard_rate=guarded / len(immutable_tables),
        median_seconds_to_eligibility=(
            median(eligibility_delays) if eligibility_delays else None
        ),
        median_seconds_to_activation=(
            median(activation_delays) if activation_delays else None
        ),
        mean_qualified_effect=(
            sum(qualified_effects) / len(qualified_effects)
            if qualified_effects
            else None
        ),
        incident_counts=dict(sorted(incidents.items())),
        incident_refs={
            name: tuple(sorted(refs)) for name, refs in sorted(incident_refs.items())
        },
        uncertainty=(
            "This is E3 component evidence for governance mechanics, not E4 cold-start worlds or E5 policy-value causality.",
            "Observed effect intervals and harm predicates are registered artifacts; this evaluator reconstructs assigned point effects but does not independently recompute statistical intervals.",
            "The component proves no direct learner activation; live retrieval, inquiry, scheduling and routing consumers are not yet cut over to this registry.",
            "Correction closure beyond explicit LearningUpdate reward retraction still requires RepairLedger propagation through settlement, attribution, policy and dependent intent.",
            "Tenant and purpose manifests plus RLS are checked structurally; cross-tenant twin, membership-inference, deletion and unlearning probes remain required.",
            "One component experiment cannot establish generalization, old-domain retention, regret reduction or safe long-horizon adaptation.",
        ),
        artifact_refs=artifact_refs,
    )


def build_control_policy_invariant_evidence(
    state: ControlPolicyEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    by_id = {item.invariant_id: item for item in registry.invariants}
    definitions = {
        "INV-34": (
            "inv.loop_bootstrap",
            min(state.valid_bootstrap_count, state.bootstrap_covered_candidate_count),
            max(state.bootstrap_count, state.candidate_count),
            {
                "invalid_bootstrap_contract",
                "candidate_without_governed_bootstrap_or_artifact",
                "unearned_or_self_authorized_policy_promotion",
                "active_policy_without_measurement_authority_or_canary",
            },
        ),
        "INV-38": (
            "inv.model_state_authority",
            min(
                state.valid_artifact_manifest_count,
                state.governed_artifact_activation_count
                if state.governed_artifact_activation_count
                else state.valid_artifact_manifest_count,
            ),
            state.artifact_count,
            {
                "invalid_learned_artifact_manifest",
                "ungoverned_learned_artifact_activation",
            },
        ),
    }
    evidence: list[InvariantRunEvidence] = []
    for invariant_id, (metric_id, numerator, denominator_value, names) in definitions.items():
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        violations = sum(state.incident_counts.get(name, 0) for name in names)
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:control-policy",
            denominator_version="governed-control-policy-denominator-v1",
            population_definition_version="policy-registry-object-lifecycle-v1",
            query_or_manifest_hash=canonical_sha256(
                {
                    "scope": state.scope.model_dump(mode="json"),
                    "invariant": invariant_id,
                }
            ),
            source_or_oracle_population=denominator_value,
            production_accepted=denominator_value,
            eligible=denominator_value,
            attempted_or_committed=denominator_value,
            terminal_fates={"covered": min(numerator, denominator_value)},
            nonterminal_fates={"uncovered": max(0, denominator_value - numerator)},
            report_cutoff=state.scope.end.isoformat(),
            population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
            population_partition_value="governed_control_policy",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{name}",
                incident_class=name,
                status=IncidentStatus.CONFIRMED,
                severity=5 if "unearned" in name or "ungoverned" in name else 4,
                summary=f"Observed {state.incident_counts[name]} scoped {name} incidents.",
                artifact_refs=state.artifact_refs,
            )
            for name in sorted(names)
            if state.incident_counts.get(name, 0)
        )
        evidence.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=denominator_value,
                observed_trace_facts=frozenset(
                    {
                        "loop_kind_and_version",
                        "bootstrap_policy",
                        "shadow_assignments",
                        "promotion_criteria_and_decision",
                        "training_artifact_ids",
                        "tenant_purpose_and_source_labels",
                        "model_assignment_and_version",
                    }
                ),
                executed_scenario_ids=frozenset(invariant.proof.suite_and_scenario_ids)
                & executed_scenario_ids,
                metric_observations=(
                    MetricObservation(
                        metric_id=metric_id,
                        metric_version="governed-control-policy-runtime-v1",
                        raw_numerator=float(numerator),
                        raw_denominator=float(denominator_value),
                        point_estimate=(
                            numerator / denominator_value if denominator_value else None
                        ),
                        violation_count=violations,
                        severity_mass=float(violations),
                        artifact_refs=state.artifact_refs,
                    ),
                ),
                incidents=incidents,
                achieved_evidence_tier=EvidenceTier.E3,
                denominator=denominator,
                uncertainty=state.uncertainty,
                blind_spots=state.uncertainty,
                artifact_refs=state.artifact_refs,
            )
        )
    return tuple(evidence)


def render_control_policy_markdown(state: ControlPolicyEvaluationState) -> str:
    metrics = (
        ("Bootstrap contracts", state.valid_bootstrap_count, state.bootstrap_count, state.bootstrap_contract_validity_rate),
        ("Artifact manifests", state.valid_artifact_manifest_count, state.artifact_count, state.artifact_manifest_validity_rate),
        (
            "Governed artifact activation",
            state.governed_artifact_activation_count,
            state.artifact_activation_count,
            state.artifact_activation_governance_rate,
        ),
        ("Experiment preregistration", state.valid_preregistered_plan_count, state.experiment_plan_count, state.experiment_preregistration_rate),
        ("Pre-exposure assignment", state.valid_preexposure_assignment_count, state.experiment_assignment_count, state.assignment_integrity_rate),
        ("Candidate bootstrap coverage", state.bootstrap_covered_candidate_count, state.candidate_count, state.candidate_bootstrap_coverage_rate),
        ("Policy lifecycle", state.legal_policy_version_count, state.policy_version_count, state.policy_lifecycle_conformance_rate),
        ("Measurement reconstruction", state.independently_reconstructable_measurement_count, state.measurement_count, state.measurement_reconstructability_rate),
        ("Eligibility correctness", state.eligibility_correct_measurement_count, state.measurement_count, state.eligibility_correctness_rate),
        ("Independent promotion authority", state.independently_authorized_promotion_count, state.promotion_decision_count, state.promotion_authority_separation_rate),
        ("Governed active policy", state.governed_active_policy_count, state.active_policy_count, state.governed_activation_rate),
        ("Family-head integrity", state.valid_family_head_count, state.family_head_count, state.family_head_integrity_rate),
        ("Frozen fallback integrity", state.valid_frozen_fallback_count, state.frozen_fallback_count, state.frozen_fallback_integrity_rate),
        ("Proposal-only learning", state.proposal_only_learning_update_count, state.learning_update_count, state.proposal_only_learning_rate),
        ("Reward retraction", state.reward_retracted_update_count, state.corrected_learning_update_count, state.reward_retraction_rate),
        ("Command reconstruction", state.reconstructable_command_count, state.command_count, state.command_reconstructability_rate),
    )
    lines = [
        f"# Governed control-policy evaluation: {state.scope.run_id}",
        "",
        f"- Tenant: `{state.scope.tenant_id}`",
        f"- Scope: `{state.scope.start.isoformat()}` to `{state.scope.end.isoformat()}`",
        "",
        "## State vector",
        "",
        *(
            f"- {label}: **{numerator}/{denominator} ({_format_rate(rate)})**"
            for label, numerator, denominator, rate in metrics
        ),
        f"- Append-only guards: **{state.guarded_immutable_table_count}/{state.immutable_table_count} ({state.immutable_storage_guard_rate:.1%})**",
        f"- Median time to eligibility: **{_format_seconds(state.median_seconds_to_eligibility)}**",
        f"- Median time to activation: **{_format_seconds(state.median_seconds_to_activation)}**",
        f"- Mean qualifying effect: **{state.mean_qualified_effect if state.mean_qualified_effect is not None else 'unknown'}**",
        "",
        "## Current and cold-start fates",
        "",
        *(f"- current `{name}` versions: {count}" for name, count in state.policy_state_counts.items()),
        *(f"- cold-start `{name}`: {count}" for name, count in state.cold_start_fate_counts.items()),
        "",
        "## Incidents",
        "",
        *(
            (
                f"- {name}: {count} ({', '.join(state.incident_refs.get(name, ()))})"
                for name, count in state.incident_counts.items()
            )
            if state.incident_counts
            else ("- none observed in this scope",)
        ),
        "",
        "## Proof limits",
        "",
        *(f"- {item}" for item in state.uncertainty),
        "",
    ]
    return "\n".join(lines)


_LEGAL_POLICY_TRANSITIONS = {
    ControlPolicyState.CANDIDATE: {
        ControlPolicyState.SHADOW,
        ControlPolicyState.FROZEN,
        ControlPolicyState.REJECTED,
    },
    ControlPolicyState.SHADOW: {
        ControlPolicyState.ELIGIBLE,
        ControlPolicyState.FROZEN,
        ControlPolicyState.REJECTED,
    },
    ControlPolicyState.ELIGIBLE: {
        ControlPolicyState.AUTHORIZED,
        ControlPolicyState.FROZEN,
        ControlPolicyState.REJECTED,
    },
    ControlPolicyState.AUTHORIZED: {
        ControlPolicyState.CANARY,
        ControlPolicyState.FROZEN,
        ControlPolicyState.REJECTED,
    },
    ControlPolicyState.CANARY: {
        ControlPolicyState.ACTIVE,
        ControlPolicyState.FROZEN,
        ControlPolicyState.ROLLED_BACK,
    },
    ControlPolicyState.ACTIVE: {
        ControlPolicyState.FROZEN,
        ControlPolicyState.ROLLED_FORWARD,
        ControlPolicyState.ROLLED_BACK,
        ControlPolicyState.SUPERSEDED,
    },
    ControlPolicyState.FROZEN: {
        ControlPolicyState.ROLLED_FORWARD,
        ControlPolicyState.ROLLED_BACK,
        ControlPolicyState.REJECTED,
        ControlPolicyState.SUPERSEDED,
    },
    ControlPolicyState.REJECTED: set(),
    ControlPolicyState.ROLLED_FORWARD: set(),
    ControlPolicyState.ROLLED_BACK: set(),
    ControlPolicyState.SUPERSEDED: set(),
}


def _policy_lifecycle_valid(versions: Sequence[ControlPolicyVersion]) -> bool:
    ordered = sorted(versions, key=lambda item: item.aggregate_version)
    if not ordered or ordered[0].aggregate_version != 1:
        return False
    if ordered[0].state is not ControlPolicyState.CANDIDATE:
        return False
    for previous, current in zip(ordered, ordered[1:], strict=False):
        if current.aggregate_version != previous.aggregate_version + 1:
            return False
        if current.state not in _LEGAL_POLICY_TRANSITIONS[previous.state]:
            return False
        if current.candidate_digest != previous.candidate_digest:
            return False
    return True


def _reconstruct_effect(
    *,
    measurement: PolicyEligibilityMeasurement,
    candidate: ControlPolicyCandidate,
    plan_by_id: Mapping[UUID, ExperimentPlan],
    assignment_by_id: Mapping[UUID, ExperimentAssignment],
    assignment_by_episode: Mapping[UUID, ExperimentAssignment],
    settlement_rows: Sequence[Mapping[str, Any]],
    attribution_rows: Sequence[Mapping[str, Any]],
) -> float | None:
    plan_ids = _unique_ref_ids(measurement.experiment_plan_refs, "experiment-plan")
    assignment_ids = _unique_ref_ids(
        measurement.experiment_assignment_refs, "experiment-assignment"
    )
    if not plan_ids and not assignment_ids:
        if any(row["subject_ref"] != candidate.candidate_ref for row in attribution_rows):
            raise ValueError("nonexperiment attribution subject mismatch")
        return None
    if not plan_ids or not assignment_ids:
        raise ValueError("plan and assignment evidence must appear together")
    plans = [plan_by_id[item] for item in plan_ids]
    assignments = [assignment_by_id[item] for item in assignment_ids]
    if any(
        plan.primary_metric_id != measurement.primary_metric_id
        or plan.treatment_policy_ref != candidate.candidate_ref
        or plan.control_policy_ref != candidate.frozen_control_ref
        for plan in plans
    ):
        raise ValueError("experiment plan mismatch")
    cited_assignments = {item.assignment_id for item in assignments}
    attribution_by_settlement = {
        row["settlement_id"]: row for row in attribution_rows
    }
    values = {
        ExperimentAssignmentArm.CONTROL: [],
        ExperimentAssignmentArm.TREATMENT: [],
    }
    for settlement in settlement_rows:
        assignment = assignment_by_episode[settlement["episode_id"]]
        if assignment.assignment_id not in cited_assignments:
            raise ValueError("settlement has uncited assignment")
        if assignment.first_exposure_at > settlement["valid_time"]:
            raise ValueError("assignment follows outcome")
        expected_subject = (
            candidate.candidate_ref
            if assignment.arm is ExperimentAssignmentArm.TREATMENT
            else candidate.frozen_control_ref
        )
        if attribution_by_settlement[settlement["settlement_id"]][
            "subject_ref"
        ] != expected_subject:
            raise ValueError("attribution disagrees with arm")
        values[assignment.arm].append(_numeric_json(settlement["observed_value"]))
    if not all(values.values()):
        raise ValueError("comparison has no control or treatment")
    control_mean = sum(values[ExperimentAssignmentArm.CONTROL]) / len(
        values[ExperimentAssignmentArm.CONTROL]
    )
    treatment_mean = sum(values[ExperimentAssignmentArm.TREATMENT]) / len(
        values[ExperimentAssignmentArm.TREATMENT]
    )
    directions = {plan.effect_direction for plan in plans}
    if len(directions) != 1:
        raise ValueError("effect directions disagree")
    direction = next(iter(directions))
    return (
        treatment_mean - control_mean
        if direction is ExperimentEffectDirection.HIGHER_IS_BETTER
        else control_mean - treatment_mean
    )


def _command_reconstructable(row: Mapping[str, Any]) -> bool:
    try:
        kind = str(row["command_kind"])
        if kind.startswith("register_"):
            command = PolicyRegistryRegistrationCommand.model_validate(
                _json(row["command"])
            )
        elif kind == "transition_control_policy":
            command = PolicyStateTransitionCommand.model_validate(_json(row["command"]))
        elif kind == "transition_learned_artifact":
            command = LearnedArtifactStateTransitionCommand.model_validate(
                _json(row["command"])
            )
        else:
            return False
        return (
            command.request_digest == row["request_digest"]
            and command.context.processing_authority.fingerprint
            == row["processing_authority_fingerprint"]
            and command.context.writer_scope_epoch.scope_id == row["writer_scope_id"]
            and command.context.writer_scope_epoch.epoch == row["writer_epoch"]
        )
    except (TypeError, ValueError, KeyError):
        return False


def _unique_ref_ids(values: Sequence[str], prefix: str) -> list[UUID]:
    parsed = [_required_ref_uuid(value, prefix) for value in values]
    if len(parsed) != len(set(parsed)):
        raise ValueError("duplicate object references")
    return parsed


def _required_ref_uuid(value: str, prefix: str) -> UUID:
    if not value.startswith(f"{prefix}:"):
        raise ValueError(f"invalid {prefix} reference")
    return UUID(value.rsplit(":", 1)[-1])


def _optional_ref_uuid(value: str | None, prefix: str) -> UUID | None:
    return _required_ref_uuid(value, prefix) if value else None


def _numeric_json(value: Any) -> float:
    if isinstance(value, str):
        value = json.loads(value)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("metric is not numeric")
    return float(value)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _rate(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _format_rate(value: float | None) -> str:
    return f"{value:.1%}" if value is not None else "unknown/not exposed"


def _format_seconds(value: float | None) -> str:
    return f"{value:.0f}s" if value is not None else "unknown/not exposed"


__all__ = [
    "ControlPolicyEvaluationScope",
    "ControlPolicyEvaluationState",
    "analyze_control_policy_rows",
    "build_control_policy_invariant_evidence",
    "evaluate_control_policy_state",
    "render_control_policy_markdown",
]
