"""Sealed continuous proof contract for source-identity binding lifecycle."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_population import (
    IntervalEstimate,
    _wilson_estimate,
)


class _LifecycleModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class BindingLifecycleProofCell(_LifecycleModel):
    """One mandatory lifecycle result or an explicit unsupported gap."""

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
                    "observed binding lifecycle measurements require a result "
                    "and evidence"
                )
        elif (
            self.satisfied is not None
            or not self.unsupported_reason
            or self.artifact_refs
        ):
            raise ValueError(
                "unsupported binding lifecycle measurements require one "
                "reason and cannot carry fabricated evidence"
            )
        return self


class SourceIdentityBindingLifecycleObservation(_LifecycleModel):
    """Raw evidence for the exact source-binding lifecycle obligations."""

    schema_version: Literal[
        "source-identity-binding-lifecycle-observation-v1"
    ] = "source-identity-binding-lifecycle-observation-v1"
    case_id: Literal["source-identity-binding-lifecycle-v1"] = (
        "source-identity-binding-lifecycle-v1"
    )
    tenant_id: UUID
    binding_lineage_id: UUID
    source_system: str = Field(min_length=1)
    source_native_identifier: str = Field(min_length=1)
    source_surface: str = Field(min_length=1)
    original_binding_version: int = Field(ge=1)
    closure_binding_version: int = Field(ge=2)
    successor_binding_version: int = Field(ge=2)
    original_valid_from: datetime
    transition_effective_at: datetime
    transaction_at: datetime
    as_of_valid_at: datetime
    as_of_known_at: datetime
    source_observation_ref: str = Field(min_length=1)
    current_resolution_correct: BindingLifecycleProofCell
    asof_resolution_correct: BindingLifecycleProofCell
    exact_attachment_preserved: BindingLifecycleProofCell
    closure_correct: BindingLifecycleProofCell
    revocation_correct: BindingLifecycleProofCell
    supersession_correct: BindingLifecycleProofCell
    overlap_prevented: BindingLifecycleProofCell
    stale_version_rejected: BindingLifecycleProofCell
    replay_idempotent: BindingLifecycleProofCell
    foreign_tenant_isolated: BindingLifecycleProofCell
    source_immutable: BindingLifecycleProofCell
    transaction_atomic: BindingLifecycleProofCell
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @field_validator(
        "original_valid_from",
        "transition_effective_at",
        "transaction_at",
        "as_of_valid_at",
        "as_of_known_at",
    )
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def sealed_lifecycle_scope(self) -> Self:
        if self.closure_binding_version != self.original_binding_version + 1:
            raise ValueError(
                "closure binding version must immediately follow original"
            )
        if self.successor_binding_version != self.closure_binding_version + 1:
            raise ValueError(
                "successor binding version must immediately follow closure"
            )
        if self.transition_effective_at < self.original_valid_from:
            raise ValueError(
                "binding transition cannot predate the original binding"
            )
        return self

    @property
    def measurements(self) -> dict[str, BindingLifecycleProofCell]:
        return {
            name: getattr(self, name)
            for name in _MEASUREMENT_NAMES
        }


_RESOLUTION_MEASUREMENTS = (
    "current_resolution_correct",
    "asof_resolution_correct",
)
_ATTACHMENT_MEASUREMENTS = ("exact_attachment_preserved",)
_TRANSITION_MEASUREMENTS = (
    "closure_correct",
    "revocation_correct",
    "supersession_correct",
)
_CONTROL_MEASUREMENTS = (
    "overlap_prevented",
    "stale_version_rejected",
    "replay_idempotent",
)
_SAFETY_MEASUREMENTS = (
    "current_resolution_correct",
    "asof_resolution_correct",
    "exact_attachment_preserved",
    "closure_correct",
    "revocation_correct",
    "supersession_correct",
    "overlap_prevented",
    "stale_version_rejected",
    "replay_idempotent",
    "foreign_tenant_isolated",
    "source_immutable",
    "transaction_atomic",
)
_INTEGRITY_MEASUREMENTS = (
    "foreign_tenant_isolated",
    "source_immutable",
    "transaction_atomic",
)
_MEASUREMENT_NAMES = (
    *_RESOLUTION_MEASUREMENTS,
    *_ATTACHMENT_MEASUREMENTS,
    *_TRANSITION_MEASUREMENTS,
    *_CONTROL_MEASUREMENTS,
    *_INTEGRITY_MEASUREMENTS,
)


class SourceIdentityBindingLifecycleReport(_LifecycleModel):
    schema_version: Literal[
        "source-identity-binding-lifecycle-report-v1"
    ] = "source-identity-binding-lifecycle-report-v1"
    status: Literal["observed", "observed_with_gaps", "contradicted"]
    expected_measurement_count: int = Field(ge=0)
    observed_measurement_count: int = Field(ge=0)
    unsupported_measurement_count: int = Field(ge=0)
    violating_measurement_count: int = Field(ge=0)
    safety_violation_count: int = Field(ge=0)
    immutability_violation_count: int = Field(ge=0)
    runtime_support_rate: IntervalEstimate
    overall_satisfaction_rate: IntervalEstimate | None
    resolution_temporal_rate: IntervalEstimate | None
    exact_attachment_rate: IntervalEstimate | None
    lifecycle_transition_rate: IntervalEstimate | None
    overlap_stale_replay_rate: IntervalEstimate | None
    isolation_immutability_atomicity_rate: IntervalEstimate | None
    measurement_rates: dict[str, IntervalEstimate | None]
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


class SourceIdentityBindingLifecycleEvidence(_LifecycleModel):
    schema_version: Literal[
        "source-identity-binding-lifecycle-evidence-v1"
    ] = "source-identity-binding-lifecycle-evidence-v1"
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    observation: SourceIdentityBindingLifecycleObservation
    report: SourceIdentityBindingLifecycleReport
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def report_matches_raw_observation(self) -> Self:
        recomputed = evaluate_source_identity_binding_lifecycle(
            self.observation
        )
        if recomputed != self.report:
            raise ValueError(
                "source identity lifecycle report does not match raw observation"
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


def evaluate_source_identity_binding_lifecycle(
    observation: SourceIdentityBindingLifecycleObservation,
) -> SourceIdentityBindingLifecycleReport:
    """Evaluate all sealed lifecycle obligations without compensation."""

    measurements = observation.measurements
    observed = {
        name: cell
        for name, cell in measurements.items()
        if cell.status == "observed"
    }
    unsupported = {
        name: cell
        for name, cell in measurements.items()
        if cell.status == "unsupported"
    }
    violating = {
        name: cell
        for name, cell in observed.items()
        if cell.satisfied is False
    }
    safety_violations = set(violating).intersection(_SAFETY_MEASUREMENTS)
    immutability_violations = int("source_immutable" in violating)
    status: Literal["observed", "observed_with_gaps", "contradicted"] = (
        "contradicted"
        if safety_violations or violating
        else "observed_with_gaps"
        if unsupported
        else "observed"
    )
    return SourceIdentityBindingLifecycleReport(
        status=status,
        expected_measurement_count=len(_MEASUREMENT_NAMES),
        observed_measurement_count=len(observed),
        unsupported_measurement_count=len(unsupported),
        violating_measurement_count=len(violating),
        safety_violation_count=len(safety_violations),
        immutability_violation_count=immutability_violations,
        runtime_support_rate=_wilson_estimate(
            [
                float(cell.status == "observed")
                for cell in measurements.values()
            ]
        ),
        overall_satisfaction_rate=_rate(observed),
        resolution_temporal_rate=_category_rate(
            observed,
            _RESOLUTION_MEASUREMENTS,
        ),
        exact_attachment_rate=_category_rate(
            observed,
            _ATTACHMENT_MEASUREMENTS,
        ),
        lifecycle_transition_rate=_category_rate(
            observed,
            _TRANSITION_MEASUREMENTS,
        ),
        overlap_stale_replay_rate=_category_rate(
            observed,
            _CONTROL_MEASUREMENTS,
        ),
        isolation_immutability_atomicity_rate=_category_rate(
            observed,
            _INTEGRITY_MEASUREMENTS,
        ),
        measurement_rates={
            name: (
                _wilson_estimate([float(bool(cell.satisfied))])
                if cell.status == "observed"
                else None
            )
            for name, cell in measurements.items()
        },
        unsupported_reason_counts=dict(
            sorted(
                Counter(
                    str(cell.unsupported_reason)
                    for cell in unsupported.values()
                ).items()
            )
        ),
        observation_digest=canonical_sha256(
            observation.model_dump(mode="json")
        ),
    )


def validate_source_identity_binding_lifecycle_artifact(
    payload: dict[str, Any],
) -> SourceIdentityBindingLifecycleEvidence:
    supplied = str(payload.get("evidence_digest") or "")
    evidence = SourceIdentityBindingLifecycleEvidence.model_validate(
        {key: value for key, value in payload.items() if key != "evidence_digest"}
    )
    if supplied != evidence.digest:
        raise ValueError("source identity lifecycle evidence digest mismatch")
    return evidence


def _rate(
    observed: dict[str, BindingLifecycleProofCell],
) -> IntervalEstimate | None:
    values = [float(bool(cell.satisfied)) for cell in observed.values()]
    return _wilson_estimate(values) if values else None


def _category_rate(
    observed: dict[str, BindingLifecycleProofCell],
    names: tuple[str, ...],
) -> IntervalEstimate | None:
    values = [
        float(bool(observed[name].satisfied))
        for name in names
        if name in observed
    ]
    return _wilson_estimate(values) if values else None


__all__ = [
    "BindingLifecycleProofCell",
    "SourceIdentityBindingLifecycleEvidence",
    "SourceIdentityBindingLifecycleObservation",
    "SourceIdentityBindingLifecycleReport",
    "evaluate_source_identity_binding_lifecycle",
    "validate_source_identity_binding_lifecycle_artifact",
]
