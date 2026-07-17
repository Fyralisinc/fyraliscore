"""Immutable contracts for admitted business relations.

Relation candidates deliberately accept an open vocabulary.  Admission is the
point at which the small, governed vocabulary and its role/direction contract
become mandatory; unknown semantics therefore remain representable as
pre-truth candidates without being coerced into canonical truth.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.kernel import canonical_sha256


class _RelationContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class RelationKind(StrEnum):
    CAUSAL_INFLUENCE = "causal_influence"
    DEPENDENCY_CONSTRAINT = "dependency_constraint"
    ENABLEMENT = "enablement"
    PREDICTIVE_INDICATOR = "predictive_indicator"


ROLE_SCHEMA: dict[RelationKind, tuple[str, str]] = {
    RelationKind.CAUSAL_INFLUENCE: ("cause", "effect"),
    RelationKind.DEPENDENCY_CONSTRAINT: ("dependent", "prerequisite"),
    RelationKind.ENABLEMENT: ("enabler", "enabled"),
    RelationKind.PREDICTIVE_INDICATOR: ("indicator", "outcome"),
}


class RelationLifecycle(StrEnum):
    ACTIVE = "active"
    DISPUTED = "disputed"
    RETIRED = "retired"


class RelationDisposition(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    NEEDS_REVIEW = "needs_review"


class RelationParticipant(_RelationContract):
    model_id: UUID
    model_version_id: UUID
    role: str = Field(min_length=1)
    ordinal: int = Field(default=0, ge=0)


class RelationEvidence(_RelationContract):
    """A unique, signed reference to evidence owned by one ModelVersion."""

    evidence_reference_id: UUID
    model_version_id: UUID
    evidence_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    polarity: int = Field(ge=-1, le=1)
    weight: float = Field(ge=0.0, le=1.0)

    @field_validator("polarity")
    @classmethod
    def polarity_is_signed(cls, value: int) -> int:
        if value not in {-1, 1}:
            raise ValueError("relation evidence polarity must be -1 or 1")
        return value


class DirectionAssertion(_RelationContract):
    """Machine-checkable meaning of the natural rationale.

    This prevents admission from guessing direction or polarity from prose.
    The assertion must exactly agree with the proposed relation kind and its
    ordered endpoints, and positive polarity prevents self-negating rationale.
    """

    kind: RelationKind
    source_model_version_id: UUID
    target_model_version_id: UUID
    polarity: int = Field(ge=-1, le=1)

    @field_validator("polarity")
    @classmethod
    def polarity_is_signed(cls, value: int) -> int:
        if value not in {-1, 1}:
            raise ValueError("rationale polarity must be -1 or 1")
        return value


class RelationCandidate(_RelationContract):
    candidate_relation_id: UUID
    tenant_id: UUID
    proposed_kind: str = Field(min_length=1)
    participants: tuple[RelationParticipant, ...] = Field(min_length=2)
    rationale: str = Field(min_length=1)
    assertion: DirectionAssertion | None = None
    evidence: tuple[RelationEvidence, ...] = Field(min_length=1)
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def immutable_members_are_unique(self) -> Self:
        participant_keys = [(p.role, p.ordinal) for p in self.participants]
        if len(set(participant_keys)) != len(participant_keys):
            raise ValueError("relation participant role/ordinal must be unique")
        evidence_ids = [item.evidence_reference_id for item in self.evidence]
        if len(set(evidence_ids)) != len(evidence_ids):
            raise ValueError("relation evidence references must be unique")
        return self

    @property
    def candidate_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))

    @property
    def known_kind(self) -> RelationKind | None:
        try:
            return RelationKind(self.proposed_kind)
        except ValueError:
            return None


class RelationVersion(_RelationContract):
    relation_version_id: UUID
    relation_id: UUID
    tenant_id: UUID
    version: int = Field(ge=1)
    admission_decision_id: UUID
    kind: RelationKind
    lifecycle: RelationLifecycle = RelationLifecycle.ACTIVE
    participants: tuple[RelationParticipant, ...]
    rationale: str = Field(min_length=1)
    assertion: DirectionAssertion
    evidence: tuple[RelationEvidence, ...]
    supersedes_relation_version_id: UUID | None = None
    created_at: datetime
    semantic_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("created_at")
    @classmethod
    def created_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def semantics_are_consistent(self) -> Self:
        if len({(p.role, p.ordinal) for p in self.participants}) != len(self.participants):
            raise ValueError("relation participant role/ordinal must be unique")
        if len({item.evidence_reference_id for item in self.evidence}) != len(self.evidence):
            raise ValueError("relation evidence references must be unique")
        validate_admissible_relation(
            kind=self.kind,
            participants=self.participants,
            assertion=self.assertion,
            evidence=self.evidence,
        )
        expected = self.compute_semantic_digest(
            kind=self.kind,
            participants=self.participants,
            rationale=self.rationale,
            assertion=self.assertion,
            evidence=self.evidence,
        )
        if self.semantic_digest != expected:
            raise ValueError("relation semantic digest does not match immutable content")
        if self.version == 1 and self.supersedes_relation_version_id is not None:
            raise ValueError("initial relation version cannot supersede another version")
        if self.version > 1 and self.supersedes_relation_version_id is None:
            raise ValueError("later relation version must bind its predecessor")
        support = sum(item.weight for item in self.evidence if item.polarity == 1)
        counter = sum(item.weight for item in self.evidence if item.polarity == -1)
        if self.lifecycle is RelationLifecycle.ACTIVE and support <= counter:
            raise ValueError("counterevidence at parity or majority requires disputed lifecycle")
        return self

    @staticmethod
    def compute_semantic_digest(**values: object) -> str:
        return canonical_sha256(values)


def validate_admissible_relation(
    *,
    kind: RelationKind,
    participants: tuple[RelationParticipant, ...],
    assertion: DirectionAssertion,
    evidence: tuple[RelationEvidence, ...],
) -> None:
    expected_roles = ROLE_SCHEMA[kind]
    by_role = {item.role: item for item in participants}
    if len(participants) != 2 or set(by_role) != set(expected_roles):
        raise ValueError(f"{kind.value} requires exactly roles {expected_roles!r}")
    source, target = (by_role[role] for role in expected_roles)
    if source.model_id == target.model_id:
        raise ValueError("business relation endpoints must be distinct Models")
    if source.model_version_id == target.model_version_id:
        raise ValueError("business relation endpoints must be distinct ModelVersions")
    if assertion.kind is not kind:
        raise ValueError("rationale kind contradicts the proposed relation kind")
    if assertion.polarity != 1:
        raise ValueError("self-negating rationale cannot be admitted")
    if (
        assertion.source_model_version_id != source.model_version_id
        or assertion.target_model_version_id != target.model_version_id
    ):
        raise ValueError("rationale direction contradicts the typed endpoints")
    if not evidence or not any(item.polarity == 1 for item in evidence):
        raise ValueError("admitted relation requires unique positive evidence")


__all__ = [
    "DirectionAssertion", "RelationCandidate", "RelationDisposition",
    "RelationEvidence", "RelationKind", "RelationLifecycle",
    "RelationParticipant", "RelationVersion", "ROLE_SCHEMA",
    "validate_admissible_relation",
]
