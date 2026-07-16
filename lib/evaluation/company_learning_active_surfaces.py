"""Continuous proof contracts for newly active company-learning surfaces."""

from __future__ import annotations

from collections import Counter
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning_population import (
    IntervalEstimate,
    _wilson_estimate,
)


class _SurfaceModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class StructuredIdentityClaimContract(_SurfaceModel):
    source_system: str = Field(min_length=1)
    source_native_identifier: str = Field(min_length=1)
    source_surface: str = Field(min_length=1)
    claim_authority_ref: str = Field(min_length=1)


_LINEAR_CLAIMS = (
    StructuredIdentityClaimContract(
        source_system="linear",
        source_native_identifier="linear:project:project-1",
        source_surface="Billing Reliability",
        claim_authority_ref=(
            "linear-handler:structured-project-name-field-v1"
        ),
    ),
    StructuredIdentityClaimContract(
        source_system="linear",
        source_native_identifier="linear:team:team-1",
        source_surface="ENG",
        claim_authority_ref="linear-handler:structured-team-key-field-v1",
    ),
    StructuredIdentityClaimContract(
        source_system="linear",
        source_native_identifier="linear:team:team-1",
        source_surface="Engineering",
        claim_authority_ref="linear-handler:structured-team-name-field-v1",
    ),
)
_DRIVE_CLAIM = (
    StructuredIdentityClaimContract(
        source_system="google_drive",
        source_native_identifier=(
            "google_drive:file:drive-file-active-surface"
        ),
        source_surface="Revenue Planning",
        claim_authority_ref="google-drive-handler:structured-file-fields-v1",
    ),
)
SEALED_ACTIVE_SURFACE_CLAIMS: dict[
    str,
    tuple[StructuredIdentityClaimContract, ...],
] = {
    "jira_project": (
        StructuredIdentityClaimContract(
            source_system="jira",
            source_native_identifier=(
                "jira:acme.atlassian.net:project:10000"
            ),
            source_surface="ENG",
            claim_authority_ref=(
                "jira-handler:structured-project-field-v1"
            ),
        ),
    ),
    "linear_issue_bundle": _LINEAR_CLAIMS,
    "google_drive_file": _DRIVE_CLAIM,
    "google_drive_comment": _DRIVE_CLAIM,
    "google_drive_revision": _DRIVE_CLAIM,
    "gmail_thread": (
        StructuredIdentityClaimContract(
            source_system="gmail",
            source_native_identifier=(
                "gmail:00000000-0000-0000-0000-000000000002:"
                "thread:gmail-thread-active-surface"
            ),
            source_surface="Executive Planning",
            claim_authority_ref=(
                "gmail-handler:structured-thread-subject-fields-v1"
            ),
        ),
    ),
}


class StructuredIdentitySurfaceObservation(_SurfaceModel):
    case_id: Literal[
        "jira_project",
        "linear_issue_bundle",
        "google_drive_file",
        "google_drive_comment",
        "google_drive_revision",
        "gmail_thread",
    ]
    execution_status: Literal["observed", "unsupported"] = "observed"
    unsupported_reason: str | None = None
    expected_claims: tuple[StructuredIdentityClaimContract, ...] | None = None
    observed_claims: tuple[StructuredIdentityClaimContract, ...] | None = None
    claim_emitted: bool | None = None
    claim_preserved: bool | None = None
    preexisting_binding_attached: bool | None = None
    handler_created_authority: bool | None = None
    ingest_created_authority: bool | None = None
    forged_text_resolved: bool | None = None
    missing_binding_authoritative: bool | None = None
    cross_source_leak: bool | None = None
    cross_tenant_leak: bool | None = None
    source_observation_immutable: bool | None = None
    artifact_refs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def complete_execution(self) -> Self:
        measurements = (
            self.expected_claims,
            self.observed_claims,
            self.claim_emitted,
            self.claim_preserved,
            self.preexisting_binding_attached,
            self.handler_created_authority,
            self.ingest_created_authority,
            self.forged_text_resolved,
            self.missing_binding_authoritative,
            self.cross_source_leak,
            self.cross_tenant_leak,
            self.source_observation_immutable,
        )
        if self.execution_status == "unsupported":
            if (
                not self.unsupported_reason
                or any(value is not None for value in measurements)
                or self.artifact_refs
            ):
                raise ValueError(
                    "unsupported identity surfaces require one reason and no evidence"
                )
        elif (
            self.unsupported_reason
            or any(value is None for value in measurements)
            or not self.artifact_refs
        ):
            raise ValueError("observed identity surfaces require complete evidence")
        elif not self.expected_claims:
            raise ValueError(
                "observed identity surfaces require an explicit source contract"
            )
        return self

    @property
    def safe(self) -> bool:
        return bool(
            self.claim_emitted
            and self.observed_claims == self.expected_claims
            and self.claim_preserved
            and self.preexisting_binding_attached
            and not self.handler_created_authority
            and not self.ingest_created_authority
            and not self.forged_text_resolved
            and not self.missing_binding_authoritative
            and not self.cross_source_leak
            and not self.cross_tenant_leak
            and self.source_observation_immutable
        )


class SourceSalienceObservation(_SurfaceModel):
    case_id: Literal[
        "settled_useful",
        "corrected",
        "pending",
        "foreign_tenant",
        "profile_load",
    ]
    execution_status: Literal["observed", "unsupported"] = "observed"
    unsupported_reason: str | None = None
    baseline_salience: float | None = None
    learned_salience: float | None = None
    credit_observed: bool | None = None
    foreign_tenant_learned: bool | None = None
    canonical_truth_immutable: bool | None = None
    grounding_truth_immutable: bool | None = None
    artifact_refs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def complete_execution(self) -> Self:
        measurements = (
            self.baseline_salience,
            self.learned_salience,
            self.credit_observed,
            self.foreign_tenant_learned,
            self.canonical_truth_immutable,
            self.grounding_truth_immutable,
        )
        if self.execution_status == "unsupported":
            if (
                not self.unsupported_reason
                or any(value is not None for value in measurements)
                or self.artifact_refs
            ):
                raise ValueError(
                    "unsupported salience cases require one reason and no evidence"
                )
        elif (
            self.unsupported_reason
            or any(value is None for value in measurements)
            or not self.artifact_refs
        ):
            raise ValueError("observed salience cases require complete evidence")
        return self

    @property
    def direction_safe(self) -> bool:
        if self.execution_status != "observed":
            return False
        assert self.baseline_salience is not None
        assert self.learned_salience is not None
        if self.case_id == "settled_useful":
            return bool(
                self.credit_observed and self.learned_salience > self.baseline_salience
            )
        if self.case_id == "corrected":
            return bool(
                not self.credit_observed
                and self.learned_salience <= self.baseline_salience
            )
        if self.case_id == "pending":
            return bool(
                not self.credit_observed
                and self.learned_salience == self.baseline_salience
            )
        if self.case_id == "foreign_tenant":
            return bool(
                not self.foreign_tenant_learned
                and self.learned_salience == self.baseline_salience
            )
        return self.learned_salience == self.baseline_salience

    @property
    def immutable(self) -> bool:
        return bool(self.canonical_truth_immutable and self.grounding_truth_immutable)


class StructuredIdentitySurfaceReport(_SurfaceModel):
    schema_version: Literal["company-learning-structured-identity-report-v1"] = (
        "company-learning-structured-identity-report-v1"
    )
    status: Literal["observed", "contradicted"]
    case_count: int = Field(ge=0)
    observed_case_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    violating_case_count: int = Field(ge=0)
    unsupported_reason_counts: dict[str, int]
    runtime_support_rate: IntervalEstimate
    claim_emission_rate: IntervalEstimate
    claim_preservation_rate: IntervalEstimate
    governed_attachment_rate: IntervalEstimate
    handler_non_authority_rate: IntervalEstimate
    ingest_non_authority_rate: IntervalEstimate
    forged_text_rejection_rate: IntervalEstimate
    missing_binding_non_authority_rate: IntervalEstimate
    cross_source_isolation_rate: IntervalEstimate
    cross_tenant_isolation_rate: IntervalEstimate
    source_immutability_rate: IntervalEstimate
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class SourceSalienceSurfaceReport(_SurfaceModel):
    schema_version: Literal["company-learning-source-salience-report-v1"] = (
        "company-learning-source-salience-report-v1"
    )
    status: Literal["observed", "contradicted"]
    case_count: int = Field(ge=0)
    observed_case_count: int = Field(ge=0)
    unsupported_case_count: int = Field(ge=0)
    violating_case_count: int = Field(ge=0)
    unsupported_reason_counts: dict[str, int]
    runtime_support_rate: IntervalEstimate
    useful_salience_increase_rate: IntervalEstimate
    corrected_nonincrease_rate: IntervalEstimate
    pending_zero_credit_rate: IntervalEstimate
    foreign_tenant_isolation_rate: IntervalEstimate
    canonical_truth_immutability_rate: IntervalEstimate
    grounding_truth_immutability_rate: IntervalEstimate
    salience_direction_rate: IntervalEstimate
    observation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ActiveLearningSurfacesReport(_SurfaceModel):
    schema_version: Literal["company-learning-active-surfaces-report-v1"] = (
        "company-learning-active-surfaces-report-v1"
    )
    status: Literal["observed", "contradicted"]
    structured_identity: StructuredIdentitySurfaceReport
    source_salience: SourceSalienceSurfaceReport

    @model_validator(mode="after")
    def noncompensatory_status(self) -> Self:
        expected = (
            "observed"
            if (
                self.structured_identity.status == "observed"
                and self.source_salience.status == "observed"
            )
            else "contradicted"
        )
        if self.status != expected:
            raise ValueError("active surface status must be noncompensatory")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ActiveLearningSurfacesEvidence(_SurfaceModel):
    schema_version: Literal["company-learning-active-surfaces-evidence-v1"] = (
        "company-learning-active-surfaces-evidence-v1"
    )
    run_id: str = Field(min_length=1)
    system_version: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    identity_observations: tuple[StructuredIdentitySurfaceObservation, ...]
    salience_observations: tuple[SourceSalienceObservation, ...]
    report: ActiveLearningSurfacesReport
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def report_matches_raw_observations(self) -> Self:
        recomputed = evaluate_active_learning_surfaces(
            identity_observations=self.identity_observations,
            salience_observations=self.salience_observations,
        )
        if recomputed != self.report:
            raise ValueError("active surface report does not match raw observations")
        return self

    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    def artifact_payload(self) -> dict[str, Any]:
        return {
            **self.model_dump(mode="json"),
            "evidence_digest": self.digest,
        }


def validate_active_learning_surfaces_artifact(
    payload: dict[str, Any],
) -> ActiveLearningSurfacesEvidence:
    supplied = str(payload.get("evidence_digest") or "")
    evidence = ActiveLearningSurfacesEvidence.model_validate(
        {key: value for key, value in payload.items() if key != "evidence_digest"}
    )
    if supplied != evidence.digest:
        raise ValueError("active learning surfaces evidence digest mismatch")
    return evidence


def evaluate_active_learning_surfaces(
    *,
    identity_observations: tuple[StructuredIdentitySurfaceObservation, ...],
    salience_observations: tuple[SourceSalienceObservation, ...],
) -> ActiveLearningSurfacesReport:
    identity = _evaluate_identity(identity_observations)
    salience = _evaluate_salience(salience_observations)
    return ActiveLearningSurfacesReport(
        status=(
            "observed"
            if identity.status == "observed" and salience.status == "observed"
            else "contradicted"
        ),
        structured_identity=identity,
        source_salience=salience,
    )


def _evaluate_identity(
    observations: tuple[StructuredIdentitySurfaceObservation, ...],
) -> StructuredIdentitySurfaceReport:
    _require_exact_ids(
        observations,
        {
            "jira_project",
            "linear_issue_bundle",
            "google_drive_file",
            "google_drive_comment",
            "google_drive_revision",
            "gmail_thread",
        },
        "identity",
    )
    observed = tuple(row for row in observations if row.execution_status == "observed")
    if not observed:
        raise ValueError("identity surface has no observed cases")
    violating = sum(not row.safe for row in observed)
    return StructuredIdentitySurfaceReport(
        status=(
            "observed" if len(observed) == 6 and violating == 0 else "contradicted"
        ),
        case_count=6,
        observed_case_count=len(observed),
        unsupported_case_count=6 - len(observed),
        violating_case_count=violating,
        unsupported_reason_counts=_unsupported_reasons(observations),
        runtime_support_rate=_wilson_estimate(
            [float(row.execution_status == "observed") for row in observations]
        ),
        claim_emission_rate=_estimate(observed, "claim_emitted"),
        claim_preservation_rate=_estimate(observed, "claim_preserved"),
        governed_attachment_rate=_estimate(observed, "preexisting_binding_attached"),
        handler_non_authority_rate=_inverse_estimate(
            observed, "handler_created_authority"
        ),
        ingest_non_authority_rate=_inverse_estimate(
            observed, "ingest_created_authority"
        ),
        forged_text_rejection_rate=_inverse_estimate(observed, "forged_text_resolved"),
        missing_binding_non_authority_rate=_inverse_estimate(
            observed, "missing_binding_authoritative"
        ),
        cross_source_isolation_rate=_inverse_estimate(observed, "cross_source_leak"),
        cross_tenant_isolation_rate=_inverse_estimate(observed, "cross_tenant_leak"),
        source_immutability_rate=_estimate(observed, "source_observation_immutable"),
        observation_digest=canonical_sha256(
            [row.model_dump(mode="json") for row in observations]
        ),
    )


def _evaluate_salience(
    observations: tuple[SourceSalienceObservation, ...],
) -> SourceSalienceSurfaceReport:
    expected = {
        "settled_useful",
        "corrected",
        "pending",
        "foreign_tenant",
        "profile_load",
    }
    _require_exact_ids(observations, expected, "salience")
    observed = tuple(row for row in observations if row.execution_status == "observed")
    if not observed:
        raise ValueError("source salience has no observed cases")
    by_id = {row.case_id: row for row in observed}
    violating = sum(not row.direction_safe or not row.immutable for row in observed)
    return SourceSalienceSurfaceReport(
        status=(
            "observed"
            if len(observed) == len(expected) and violating == 0
            else "contradicted"
        ),
        case_count=len(expected),
        observed_case_count=len(observed),
        unsupported_case_count=len(expected) - len(observed),
        violating_case_count=violating,
        unsupported_reason_counts=_unsupported_reasons(observations),
        runtime_support_rate=_wilson_estimate(
            [float(row.execution_status == "observed") for row in observations]
        ),
        useful_salience_increase_rate=_wilson_estimate(
            [float(by_id.get("settled_useful", _unsupported_salience()).direction_safe)]
        ),
        corrected_nonincrease_rate=_wilson_estimate(
            [float(by_id.get("corrected", _unsupported_salience()).direction_safe)]
        ),
        pending_zero_credit_rate=_wilson_estimate(
            [float(by_id.get("pending", _unsupported_salience()).direction_safe)]
        ),
        foreign_tenant_isolation_rate=_wilson_estimate(
            [float(by_id.get("foreign_tenant", _unsupported_salience()).direction_safe)]
        ),
        canonical_truth_immutability_rate=_estimate(
            observed, "canonical_truth_immutable"
        ),
        grounding_truth_immutability_rate=_estimate(
            observed, "grounding_truth_immutable"
        ),
        salience_direction_rate=_wilson_estimate(
            [float(row.direction_safe) for row in observed]
        ),
        observation_digest=canonical_sha256(
            [row.model_dump(mode="json") for row in observations]
        ),
    )


def _require_exact_ids(
    observations: tuple[_SurfaceModel, ...],
    expected: set[str],
    label: str,
) -> None:
    ids = [str(getattr(row, "case_id")) for row in observations]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{label} observations must be unique")
    if set(ids) != expected:
        raise ValueError(f"{label} observations must exactly cover sealed cases")


def _estimate(rows: tuple[_SurfaceModel, ...], field: str) -> IntervalEstimate:
    return _wilson_estimate([float(bool(getattr(row, field))) for row in rows])


def _inverse_estimate(
    rows: tuple[_SurfaceModel, ...],
    field: str,
) -> IntervalEstimate:
    return _wilson_estimate([float(not bool(getattr(row, field))) for row in rows])


def _unsupported_reasons(rows: tuple[_SurfaceModel, ...]) -> dict[str, int]:
    return dict(
        sorted(
            Counter(
                str(getattr(row, "unsupported_reason"))
                for row in rows
                if getattr(row, "execution_status") == "unsupported"
            ).items()
        )
    )


def _unsupported_salience() -> SourceSalienceObservation:
    return SourceSalienceObservation(
        case_id="profile_load",
        execution_status="unsupported",
        unsupported_reason="missing required observed salience case",
    )


__all__ = [
    "ActiveLearningSurfacesReport",
    "ActiveLearningSurfacesEvidence",
    "SourceSalienceObservation",
    "SourceSalienceSurfaceReport",
    "SEALED_ACTIVE_SURFACE_CLAIMS",
    "StructuredIdentityClaimContract",
    "StructuredIdentitySurfaceObservation",
    "StructuredIdentitySurfaceReport",
    "evaluate_active_learning_surfaces",
    "validate_active_learning_surfaces_artifact",
]
