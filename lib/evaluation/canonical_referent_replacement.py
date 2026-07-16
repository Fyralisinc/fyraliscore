"""Sealed continuous proof contract for canonical resource replacement."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.checklist_ratio import ChecklistRatio
from lib.evaluation.company_learning_experiment import CanonicalEntityRef


class _ReplacementModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class ReplacementProofCell(_ReplacementModel):
    """One required measurement, observed or explicitly unsupported."""

    status: Literal["observed", "unsupported"]
    satisfied: bool | None = None
    unsupported_reason: str | None = None
    artifact_refs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def evidence_is_explicit(self) -> Self:
        if self.status == "observed":
            if (
                self.satisfied is None
                or self.unsupported_reason is not None
                or not self.artifact_refs
            ):
                raise ValueError(
                    "observed replacement measurements require a result and evidence"
                )
        elif (
            self.satisfied is not None
            or not self.unsupported_reason
            or self.artifact_refs
        ):
            raise ValueError(
                "unsupported replacement measurements require one reason and "
                "cannot carry fabricated evidence"
            )
        return self


class CanonicalResourceReplacementObservation(_ReplacementModel):
    """Raw evidence for the sealed end-to-end replacement obligations."""

    schema_version: Literal["canonical-resource-replacement-observation-v1"] = (
        "canonical-resource-replacement-observation-v1"
    )
    case_id: Literal["canonical-system-resource-replacement-v1"] = (
        "canonical-system-resource-replacement-v1"
    )
    predecessor: CanonicalEntityRef
    successor: CanonicalEntityRef
    effective_at: datetime
    transaction_at: datetime
    delayed_event_occurred_at: datetime
    replacement_reason: str = Field(min_length=1)
    transition_applied: ReplacementProofCell
    operation_replay_idempotent: ReplacementProofCell
    operation_conflict_rejected: ReplacementProofCell
    stale_head_rejected: ReplacementProofCell
    tenant_isolated: ReplacementProofCell
    predecessor_retired: ReplacementProofCell
    successor_active: ReplacementProofCell
    alias_current_successor_safe: ReplacementProofCell
    alias_asof_predecessor_safe: ReplacementProofCell
    exact_source_binding_boundary_safe: ReplacementProofCell
    delayed_event_attachment_fail_closed: ReplacementProofCell
    old_attachment_immutable: ReplacementProofCell
    source_observation_immutable: ReplacementProofCell
    model_scope_immutable: ReplacementProofCell
    projection_invalidated: ReplacementProofCell
    projection_single_refresh: ReplacementProofCell
    lineage_reason_correct: ReplacementProofCell
    lineage_time_boundary_safe: ReplacementProofCell
    hard_dependency_rejected: ReplacementProofCell
    transaction_atomic: ReplacementProofCell
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @field_validator(
        "effective_at",
        "transaction_at",
        "delayed_event_occurred_at",
    )
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def replacement_scope_is_exact(self) -> Self:
        if self.predecessor.type != "resource" or self.successor.type != "resource":
            raise ValueError(
                "canonical system-resource proof requires resource referents"
            )
        if self.predecessor == self.successor:
            raise ValueError("replacement predecessor and successor must differ")
        if self.delayed_event_occurred_at >= self.effective_at:
            raise ValueError(
                "delayed-event proof requires an event before replacement effect"
            )
        return self

    @property
    def measurements(self) -> dict[str, ReplacementProofCell]:
        return {name: getattr(self, name) for name in _MEASUREMENT_NAMES}


_TRANSITION_MEASUREMENTS = (
    "transition_applied",
    "operation_replay_idempotent",
    "operation_conflict_rejected",
    "stale_head_rejected",
    "tenant_isolated",
)
_LIFECYCLE_MEASUREMENTS = (
    "predecessor_retired",
    "successor_active",
    "alias_current_successor_safe",
    "alias_asof_predecessor_safe",
)
_SOURCE_BOUNDARY_MEASUREMENTS = (
    "exact_source_binding_boundary_safe",
    "delayed_event_attachment_fail_closed",
)
_IMMUTABILITY_MEASUREMENTS = (
    "old_attachment_immutable",
    "source_observation_immutable",
    "model_scope_immutable",
)
_PROJECTION_MEASUREMENTS = (
    "projection_invalidated",
    "projection_single_refresh",
)
_LINEAGE_MEASUREMENTS = (
    "lineage_reason_correct",
    "lineage_time_boundary_safe",
)
_DEPENDENCY_ATOMICITY_MEASUREMENTS = (
    "hard_dependency_rejected",
    "transaction_atomic",
)
_SAFETY_MEASUREMENTS = (
    "operation_conflict_rejected",
    "stale_head_rejected",
    "tenant_isolated",
    "alias_current_successor_safe",
    "alias_asof_predecessor_safe",
    "exact_source_binding_boundary_safe",
    "delayed_event_attachment_fail_closed",
    "lineage_time_boundary_safe",
    "hard_dependency_rejected",
    "transaction_atomic",
)
_MEASUREMENT_NAMES = (
    *_TRANSITION_MEASUREMENTS,
    *_LIFECYCLE_MEASUREMENTS,
    *_SOURCE_BOUNDARY_MEASUREMENTS,
    *_IMMUTABILITY_MEASUREMENTS,
    *_PROJECTION_MEASUREMENTS,
    *_LINEAGE_MEASUREMENTS,
    *_DEPENDENCY_ATOMICITY_MEASUREMENTS,
)


class CanonicalResourceReplacementReport(_ReplacementModel):
    schema_version: Literal["canonical-resource-replacement-report-v1"] = (
        "canonical-resource-replacement-report-v1"
    )
    status: Literal["observed", "observed_with_gaps", "contradicted"]
    expected_measurement_count: int = Field(ge=0)
    observed_measurement_count: int = Field(ge=0)
    unsupported_measurement_count: int = Field(ge=0)
    violating_measurement_count: int = Field(ge=0)
    safety_violation_count: int = Field(ge=0)
    immutability_violation_count: int = Field(ge=0)
    runtime_support_rate: ChecklistRatio
    overall_satisfaction_rate: ChecklistRatio | None
    transition_control_rate: ChecklistRatio | None
    lifecycle_alias_safety_rate: ChecklistRatio | None
    source_boundary_rate: ChecklistRatio | None
    immutability_rate: ChecklistRatio | None
    projection_coherence_rate: ChecklistRatio | None
    lineage_retrieval_rate: ChecklistRatio | None
    dependency_atomicity_rate: ChecklistRatio | None
    measurement_rates: dict[str, ChecklistRatio | None]
    unsupported_reason_counts: dict[str, int]
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def full_scope_complete(self) -> bool:
        return bool(
            self.status == "observed"
            and self.observed_measurement_count == self.expected_measurement_count
            and self.unsupported_measurement_count == 0
            and self.violating_measurement_count == 0
            and self.overall_satisfaction_rate is not None
            and self.overall_satisfaction_rate.point_estimate == 1.0
        )


class CanonicalReplacementDatabaseEvidence(_ReplacementModel):
    """Digest-bound raw database evidence behind every replacement cell."""

    schema_version: Literal["canonical-replacement-database-evidence-v1"] = (
        "canonical-replacement-database-evidence-v1"
    )
    query_manifest: dict[str, str] = Field(min_length=1)
    snapshots: dict[str, Any] = Field(min_length=1)
    measurement_evidence: dict[str, tuple[str, ...]]

    @model_validator(mode="after")
    def every_measurement_maps_to_raw_evidence(self) -> Self:
        if set(self.measurement_evidence) != set(_MEASUREMENT_NAMES):
            raise ValueError(
                "database evidence must map every sealed replacement measurement"
            )
        if any(
            not query_name.strip() or not statement.strip()
            for query_name, statement in self.query_manifest.items()
        ):
            raise ValueError("database query manifest entries must be non-empty")
        snapshot_names = set(self.snapshots)
        for measurement, references in self.measurement_evidence.items():
            if not references:
                raise ValueError(
                    f"replacement measurement {measurement} lacks raw evidence"
                )
            unknown = set(references) - snapshot_names
            if unknown:
                raise ValueError(
                    f"replacement measurement {measurement} references unknown "
                    f"database evidence: {sorted(unknown)}"
                )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class CanonicalResourceReplacementEvidence(_ReplacementModel):
    schema_version: Literal["canonical-resource-replacement-evidence-v1"] = (
        "canonical-resource-replacement-evidence-v1"
    )
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    observation: CanonicalResourceReplacementObservation
    database_evidence: CanonicalReplacementDatabaseEvidence
    report: CanonicalResourceReplacementReport
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def report_matches_raw_observation(self) -> Self:
        recomputed = evaluate_canonical_resource_replacement(self.observation)
        if recomputed != self.report:
            raise ValueError(
                "canonical replacement report does not match raw observation"
            )
        if set(self.database_evidence.measurement_evidence) != set(
            self.observation.measurements
        ):
            raise ValueError(
                "canonical replacement database evidence changed sealed scope"
            )
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def artifact_payload(self) -> dict[str, Any]:
        return {
            **self.model_dump(mode="json"),
            "evidence_digest": self.digest,
        }


def evaluate_canonical_resource_replacement(
    observation: CanonicalResourceReplacementObservation,
) -> CanonicalResourceReplacementReport:
    """Evaluate every sealed replacement obligation without compensation."""

    measurements = observation.measurements
    observed = {
        name: cell for name, cell in measurements.items() if cell.status == "observed"
    }
    unsupported = {
        name: cell
        for name, cell in measurements.items()
        if cell.status == "unsupported"
    }
    violating = {
        name: cell for name, cell in observed.items() if cell.satisfied is False
    }
    safety_violations = set(violating).intersection(_SAFETY_MEASUREMENTS)
    immutability_violations = set(violating).intersection(_IMMUTABILITY_MEASUREMENTS)
    status: Literal["observed", "observed_with_gaps", "contradicted"] = (
        "contradicted"
        if violating
        else "observed_with_gaps"
        if unsupported
        else "observed"
    )
    return CanonicalResourceReplacementReport(
        status=status,
        expected_measurement_count=len(_MEASUREMENT_NAMES),
        observed_measurement_count=len(observed),
        unsupported_measurement_count=len(unsupported),
        violating_measurement_count=len(violating),
        safety_violation_count=len(safety_violations),
        immutability_violation_count=len(immutability_violations),
        runtime_support_rate=ChecklistRatio.from_flags(
            [cell.status == "observed" for cell in measurements.values()]
        ),
        overall_satisfaction_rate=_rate(observed),
        transition_control_rate=_category_rate(
            observed,
            _TRANSITION_MEASUREMENTS,
        ),
        lifecycle_alias_safety_rate=_category_rate(
            observed,
            _LIFECYCLE_MEASUREMENTS,
        ),
        source_boundary_rate=_category_rate(
            observed,
            _SOURCE_BOUNDARY_MEASUREMENTS,
        ),
        immutability_rate=_category_rate(
            observed,
            _IMMUTABILITY_MEASUREMENTS,
        ),
        projection_coherence_rate=_category_rate(
            observed,
            _PROJECTION_MEASUREMENTS,
        ),
        lineage_retrieval_rate=_category_rate(
            observed,
            _LINEAGE_MEASUREMENTS,
        ),
        dependency_atomicity_rate=_category_rate(
            observed,
            _DEPENDENCY_ATOMICITY_MEASUREMENTS,
        ),
        measurement_rates={
            name: (
                ChecklistRatio.from_flags([bool(cell.satisfied)])
                if cell.status == "observed"
                else None
            )
            for name, cell in measurements.items()
        },
        unsupported_reason_counts=dict(
            sorted(
                Counter(
                    str(cell.unsupported_reason) for cell in unsupported.values()
                ).items()
            )
        ),
        observation_digest=canonical_sha256(observation.model_dump(mode="json")),
    )


def validate_canonical_resource_replacement_artifact(
    payload: dict[str, Any],
) -> CanonicalResourceReplacementEvidence:
    supplied = str(payload.get("evidence_digest") or "")
    evidence = CanonicalResourceReplacementEvidence.model_validate(
        {key: value for key, value in payload.items() if key != "evidence_digest"}
    )
    if supplied != evidence.digest:
        raise ValueError("canonical replacement evidence digest mismatch")
    return evidence


def _rate(
    observed: dict[str, ReplacementProofCell],
) -> ChecklistRatio | None:
    values = [bool(cell.satisfied) for cell in observed.values()]
    return ChecklistRatio.from_flags(values) if values else None


def _category_rate(
    observed: dict[str, ReplacementProofCell],
    names: tuple[str, ...],
) -> ChecklistRatio | None:
    values = [bool(observed[name].satisfied) for name in names if name in observed]
    return ChecklistRatio.from_flags(values) if values else None


__all__ = [
    "CanonicalResourceReplacementEvidence",
    "CanonicalReplacementDatabaseEvidence",
    "CanonicalResourceReplacementObservation",
    "CanonicalResourceReplacementReport",
    "ReplacementProofCell",
    "evaluate_canonical_resource_replacement",
    "validate_canonical_resource_replacement_artifact",
]
