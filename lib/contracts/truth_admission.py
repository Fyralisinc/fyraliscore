"""Pure immutable contracts for candidate admission and Model truth heads."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .kernel import canonical_sha256
from .truth_evidence import (
    ClaimScopeBinding,
    TruthEvidenceReference,
    validate_claim_local_scope,
)


class _TruthAdmissionContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class TruthCandidateKind(StrEnum):
    ATOMIC_CLAIM = "atomic_claim"
    SYNTHESIS = "synthesis"
    BATCH_ENVELOPE = "batch_envelope"
    CONTROL_LANGUAGE = "control_language"
    PROCESSING_WRAPPER = "processing_wrapper"

    @property
    def canonically_admissible(self) -> bool:
        return self in {self.ATOMIC_CLAIM, self.SYNTHESIS}


class CandidateReviewState(StrEnum):
    PROPOSED = "proposed"
    IN_REVIEW = "in_review"


class AdmissionDisposition(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    NEEDS_REVIEW = "needs_review"


class ModelTruthLifecycle(StrEnum):
    ACTIVE = "active"
    DISPUTED = "disputed"
    FALSIFIED = "falsified"
    SUPERSEDED = "superseded"
    ARCHIVED = "archived"

    @property
    def terminal(self) -> bool:
        return self in {self.FALSIFIED, self.SUPERSEDED, self.ARCHIVED}


class ModelTruthTransition(StrEnum):
    CONFIRM = "confirm"
    CONTEST = "contest"
    FALSIFY = "falsify"
    SUPERSEDE = "supersede"
    ARCHIVE = "archive"

    @property
    def resulting_lifecycle(self) -> ModelTruthLifecycle:
        return {
            self.CONFIRM: ModelTruthLifecycle.ACTIVE,
            self.CONTEST: ModelTruthLifecycle.DISPUTED,
            self.FALSIFY: ModelTruthLifecycle.FALSIFIED,
            self.SUPERSEDE: ModelTruthLifecycle.SUPERSEDED,
            self.ARCHIVE: ModelTruthLifecycle.ARCHIVED,
        }[self]


class TruthCandidate(_TruthAdmissionContract):
    candidate_id: UUID
    candidate_version: int = Field(default=1, ge=1)
    tenant_id: UUID
    kind: TruthCandidateKind
    review_state: CandidateReviewState
    natural: str = Field(min_length=1)
    proposition: dict[str, Any]
    confidence: float = Field(default=0.5, ge=0.05, le=0.95)
    falsifier: dict[str, Any] | None = None
    evidential_weight: float = Field(default=0.5, ge=0.0, le=1.0)
    supporting_model_ids: tuple[UUID, ...] = ()
    visible_to_subjects: bool = True
    resolution_outcome: bool | None = None
    resolved_at: datetime | None = None
    temporal_scope: dict[str, Any] = Field(default_factory=dict)
    proposed_evidence: tuple[TruthEvidenceReference, ...] = Field(min_length=1)
    proposed_scope: tuple[ClaimScopeBinding, ...] = ()
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="created_at")

    @model_validator(mode="after")
    def candidate_is_claim_local(self) -> Self:
        if not self.proposition:
            raise ValueError("truth candidate proposition cannot be empty")
        validate_claim_local_scope(
            evidence=self.proposed_evidence,
            scope=self.proposed_scope,
            tenant_id=self.tenant_id,
        )
        return self

    @property
    def candidate_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class AdmissionDecision(_TruthAdmissionContract):
    decision_id: UUID
    tenant_id: UUID
    candidate_id: UUID
    candidate_version: int = Field(ge=1)
    candidate_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    disposition: AdmissionDisposition
    reason_codes: tuple[str, ...] = Field(min_length=1)
    decided_by: str = Field(min_length=1)
    decided_at: datetime
    admitted_model_id: UUID | None = None
    admitted_version_id: UUID | None = None

    @field_validator("decided_at")
    @classmethod
    def decided_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="decided_at")

    @model_validator(mode="after")
    def canonical_target_exists_only_when_accepted(self) -> Self:
        has_target = (
            self.admitted_model_id is not None
            or self.admitted_version_id is not None
        )
        if self.disposition is AdmissionDisposition.ACCEPTED:
            if self.admitted_model_id is None or self.admitted_version_id is None:
                raise ValueError("accepted admission must identify its Model version")
        elif has_target:
            raise ValueError("nonaccepted admission cannot identify canonical truth")
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("admission reason codes must be unique")
        return self


class ModelVersion(_TruthAdmissionContract):
    """One immutable semantic version; activity metadata is intentionally absent."""

    version_id: UUID
    model_id: UUID
    version: int = Field(ge=1)
    tenant_id: UUID
    admission_decision_id: UUID
    source_candidate_id: UUID
    source_candidate_version: int = Field(ge=1)
    natural: str = Field(min_length=1)
    proposition: dict[str, Any]
    confidence: float = Field(default=0.5, ge=0.05, le=0.95)
    semantic_digest_version: int = Field(default=1, ge=1, le=2)
    falsifier: dict[str, Any] | None = None
    evidential_weight: float = Field(default=0.5, ge=0.0, le=1.0)
    supporting_model_ids: tuple[UUID, ...] = ()
    visible_to_subjects: bool = True
    resolution_outcome: bool | None = None
    resolved_at: datetime | None = None
    temporal_scope: dict[str, Any] = Field(default_factory=dict)
    evidence: tuple[TruthEvidenceReference, ...] = Field(min_length=1)
    scope: tuple[ClaimScopeBinding, ...] = ()
    lifecycle: ModelTruthLifecycle = ModelTruthLifecycle.ACTIVE
    created_at: datetime
    semantic_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("created_at")
    @classmethod
    def created_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="created_at")

    @staticmethod
    def compute_semantic_digest(
        *,
        proposition: dict[str, Any],
        natural: str,
        evidence: tuple[TruthEvidenceReference, ...],
        scope: tuple[ClaimScopeBinding, ...],
        confidence: float | None = None,
        falsifier: dict[str, Any] | None = None,
        evidential_weight: float | None = None,
        supporting_model_ids: tuple[UUID, ...] | None = None,
        visible_to_subjects: bool | None = None,
        resolution_outcome: bool | None = None,
        resolved_at: datetime | None = None,
        temporal_scope: dict[str, Any] | None = None,
    ) -> str:
        payload: dict[str, Any] = {
                "proposition": proposition,
                "natural": natural,
                "evidence": [item.model_dump(mode="json") for item in evidence],
                "scope": [item.model_dump(mode="json") for item in scope],
        }
        if confidence is not None:
            payload["confidence"] = confidence
            payload["falsifier"] = falsifier
            payload["evidential_weight"] = evidential_weight
            payload["supporting_model_ids"] = [str(x) for x in (supporting_model_ids or ())]
            payload["visible_to_subjects"] = visible_to_subjects
            payload["resolution_outcome"] = resolution_outcome
            payload["resolved_at"] = resolved_at.isoformat() if resolved_at else None
            payload["temporal_scope"] = temporal_scope or {}
        return canonical_sha256(payload)

    @model_validator(mode="after")
    def version_is_coherent_and_claim_local(self) -> Self:
        if not self.proposition:
            raise ValueError("Model version proposition cannot be empty")
        validate_claim_local_scope(
            evidence=self.evidence, scope=self.scope, tenant_id=self.tenant_id
        )
        expected = self.compute_semantic_digest(
            proposition=self.proposition,
            natural=self.natural,
            evidence=self.evidence,
            scope=self.scope,
            confidence=(self.confidence if self.semantic_digest_version >= 2 else None),
            falsifier=self.falsifier,
            evidential_weight=self.evidential_weight,
            supporting_model_ids=self.supporting_model_ids,
            visible_to_subjects=self.visible_to_subjects,
            resolution_outcome=self.resolution_outcome,
            resolved_at=self.resolved_at,
            temporal_scope=self.temporal_scope,
        )
        if self.semantic_digest != expected:
            raise ValueError("Model semantic digest does not match its representation")
        return self


class ModelHead(_TruthAdmissionContract):
    tenant_id: UUID
    model_id: UUID
    version_id: UUID
    version: int = Field(ge=1)
    semantic_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    lifecycle: ModelTruthLifecycle
    advanced_at: datetime

    @field_validator("advanced_at")
    @classmethod
    def advanced_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="advanced_at")


class AdmitModelCommand(_TruthAdmissionContract):
    command_id: UUID
    idempotency_key: str = Field(min_length=1)
    tenant_id: UUID
    candidate: TruthCandidate
    decision: AdmissionDecision
    version: ModelVersion
    issued_at: datetime

    @field_validator("issued_at")
    @classmethod
    def issued_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="issued_at")

    @model_validator(mode="after")
    def bundle_is_one_accepted_transition(self) -> Self:
        if self.decision.disposition is not AdmissionDisposition.ACCEPTED:
            raise ValueError("admission command requires an accepted decision")
        if not self.candidate.kind.canonically_admissible:
            raise ValueError("wrapper and control candidates cannot become truth")
        if any(
            tenant != self.tenant_id
            for tenant in (
                self.candidate.tenant_id,
                self.decision.tenant_id,
                self.version.tenant_id,
            )
        ):
            raise ValueError("admission bundle crosses tenant boundaries")
        if (
            self.decision.candidate_id != self.candidate.candidate_id
            or self.decision.candidate_version != self.candidate.candidate_version
            or self.decision.candidate_digest != self.candidate.candidate_digest
        ):
            raise ValueError("admission decision does not bind the exact candidate")
        if (
            self.version.source_candidate_id != self.candidate.candidate_id
            or self.version.source_candidate_version != self.candidate.candidate_version
            or self.version.admission_decision_id != self.decision.decision_id
            or self.version.model_id != self.decision.admitted_model_id
            or self.version.version_id != self.decision.admitted_version_id
        ):
            raise ValueError("Model version does not bind the admission decision")
        if (
            self.version.natural != self.candidate.natural
            or self.version.proposition != self.candidate.proposition
            or self.version.confidence != self.candidate.confidence
            or self.version.falsifier != self.candidate.falsifier
            or self.version.evidential_weight != self.candidate.evidential_weight
            or self.version.supporting_model_ids != self.candidate.supporting_model_ids
            or self.version.visible_to_subjects != self.candidate.visible_to_subjects
            or self.version.resolution_outcome != self.candidate.resolution_outcome
            or self.version.resolved_at != self.candidate.resolved_at
            or self.version.temporal_scope != self.candidate.temporal_scope
            or self.version.evidence != self.candidate.proposed_evidence
            or self.version.scope != self.candidate.proposed_scope
        ):
            raise ValueError("admitted Model semantics must equal the exact candidate")
        if self.version.version != 1:
            raise ValueError("initial admission must create Model version 1")
        if self.version.lifecycle is not ModelTruthLifecycle.ACTIVE:
            raise ValueError("newly admitted Model must start active")
        if self.candidate.created_at > self.decision.decided_at:
            raise ValueError("admission decision cannot precede its candidate")
        if self.decision.decided_at > self.version.created_at:
            raise ValueError("Model version cannot precede its admission decision")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ModelHeadExpectation(_TruthAdmissionContract):
    tenant_id: UUID
    model_id: UUID
    expected_version_id: UUID
    expected_version: int = Field(ge=1)
    expected_semantic_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    expected_lifecycle: ModelTruthLifecycle


class AdvanceModelHeadCommand(_TruthAdmissionContract):
    command_id: UUID
    idempotency_key: str = Field(min_length=1)
    tenant_id: UUID
    expectation: ModelHeadExpectation
    next_version: ModelVersion
    transition: ModelTruthTransition
    reason_codes: tuple[str, ...] = Field(min_length=1)
    issued_at: datetime

    @field_validator("issued_at")
    @classmethod
    def issued_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="issued_at")

    @model_validator(mode="after")
    def transition_is_tenant_scoped_and_monotone(self) -> Self:
        if (
            self.expectation.tenant_id != self.tenant_id
            or self.next_version.tenant_id != self.tenant_id
        ):
            raise ValueError("head command crosses tenant boundaries")
        if self.expectation.model_id != self.next_version.model_id:
            raise ValueError("head command targets different Models")
        if self.next_version.version != self.expectation.expected_version + 1:
            raise ValueError("head command must advance exactly one version")
        if self.expectation.expected_lifecycle.terminal:
            raise ValueError("terminal Model head cannot be advanced or resurrected")
        if self.next_version.lifecycle is not self.transition.resulting_lifecycle:
            raise ValueError("Model transition does not match the next lifecycle")
        if len(set(self.reason_codes)) != len(self.reason_codes):
            raise ValueError("Model transition reason codes must be unique")
        return self

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


__all__ = [
    "AdmissionDecision",
    "AdmissionDisposition",
    "AdmitModelCommand",
    "AdvanceModelHeadCommand",
    "CandidateReviewState",
    "ModelHead",
    "ModelHeadExpectation",
    "ModelTruthLifecycle",
    "ModelTruthTransition",
    "ModelVersion",
    "TruthCandidate",
    "TruthCandidateKind",
]
