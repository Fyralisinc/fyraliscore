"""Pure contracts for claim-local evidence and typed Model scope.

These values deliberately contain no repository or database behavior.  They are
the boundary shared by truth admission, lifecycle commands, and independent
evaluators.  A reference identifies one immutable piece of evidence at an exact
coordinate and records the authority/cutoff under which it was usable.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .kernel import canonical_sha256


class _TruthEvidenceContract(BaseModel):
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


class TruthEvidenceKind(StrEnum):
    OBSERVATION = "observation"
    MODEL_VERSION = "model_version"
    REGISTERED = "registered"


class TruthEvidenceRole(StrEnum):
    SUPPORT = "support"
    COUNTEREVIDENCE = "counterevidence"
    CONTEXT = "context"
    DERIVATION = "derivation"
    AUTHORITY = "authority"


class EvidenceAuthority(_TruthEvidenceContract):
    """The exact grant/policy decision authorizing evidence use."""

    authority_ref: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    authority_epoch: int = Field(ge=1)
    decided_at: datetime
    expires_at: datetime | None = None

    @field_validator("decided_at", "expires_at")
    @classmethod
    def times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def interval_is_valid(self) -> Self:
        if self.expires_at is not None and self.expires_at <= self.decided_at:
            raise ValueError("evidence authority expiry must follow its decision")
        return self

    def is_live_at(self, instant: datetime) -> bool:
        _aware(instant, field_name="instant")
        return self.decided_at <= instant and (
            self.expires_at is None or instant < self.expires_at
        )


class TruthEvidenceCoordinate(_TruthEvidenceContract):
    """Exact source or version coordinate; generic unbounded IDs are forbidden."""

    source_system: str = Field(min_length=1)
    source_object_id: str = Field(min_length=1)
    source_revision: str = Field(min_length=1)
    field_path: str | None = None
    span_start: int | None = Field(default=None, ge=0)
    span_end: int | None = Field(default=None, ge=0)
    time_range_start: datetime | None = None
    time_range_end: datetime | None = None

    @field_validator("time_range_start", "time_range_end")
    @classmethod
    def times_are_aware(cls, value: datetime | None, info) -> datetime | None:
        return _aware(value, field_name=info.field_name) if value else None

    @model_validator(mode="after")
    def ranges_are_complete(self) -> Self:
        if (self.span_start is None) != (self.span_end is None):
            raise ValueError("text coordinates require both span bounds")
        if self.span_start is not None and self.span_end <= self.span_start:
            raise ValueError("span_end must be after span_start")
        if (self.time_range_start is None) != (self.time_range_end is None):
            raise ValueError("time coordinates require both time bounds")
        if self.time_range_start and self.time_range_end <= self.time_range_start:
            raise ValueError("time range end must follow start")
        return self


class TruthEvidenceReference(_TruthEvidenceContract):
    reference_id: UUID
    tenant_id: UUID
    kind: TruthEvidenceKind
    evidence_id: str = Field(min_length=1)
    evidence_version: int = Field(ge=1)
    evidence_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    role: TruthEvidenceRole
    coordinate: TruthEvidenceCoordinate
    authority: EvidenceAuthority
    occurred_at: datetime
    recorded_at: datetime
    cutoff_at: datetime

    @field_validator("occurred_at", "recorded_at", "cutoff_at")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def reference_is_cutoff_and_authority_bound(self) -> Self:
        if self.occurred_at > self.recorded_at:
            raise ValueError("evidence cannot be recorded before it occurred")
        if self.recorded_at > self.cutoff_at:
            raise ValueError("future evidence is outside the admission cutoff")
        if not self.authority.is_live_at(self.cutoff_at):
            raise ValueError("evidence authority is not live at the cutoff")
        return self

    @property
    def reference_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class ScopeSubjectKind(StrEnum):
    PERSON = "person"
    TEAM = "team"
    ORGANIZATION = "organization"
    PROJECT = "project"
    PRODUCT = "product"
    CUSTOMER = "customer"
    ISSUE = "issue"
    WORK_ITEM = "work_item"
    LOCATION = "location"
    OTHER = "other"


class ClaimScopeRole(StrEnum):
    ACTOR = "actor"
    SUBJECT = "subject"
    OBJECT = "object"
    OWNER = "owner"
    BENEFICIARY = "beneficiary"
    AFFECTED = "affected"
    LOCATION = "location"


class ClaimScopeBinding(_TruthEvidenceContract):
    """A typed scope assertion proved by evidence local to this exact claim.

    ``canonical_ref`` is an unresolved, stable extracted coordinate. Its
    presence does not claim that a canonical entity/resource row already
    exists; ``subject_id`` remains the durable UUID identity.
    """

    subject_id: UUID
    subject_kind: ScopeSubjectKind
    role: ClaimScopeRole
    canonical_ref: str | None = Field(default=None, min_length=3, max_length=300)
    display_label: str | None = Field(default=None, min_length=1, max_length=300)
    canonical_ref_status: Literal["provisional", "resolved"] | None = None
    normalization_version: int | None = Field(default=None, ge=1)
    claim_local_evidence_refs: tuple[UUID, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def provenance_is_unique_and_canonical(self) -> Self:
        if len(set(self.claim_local_evidence_refs)) != len(
            self.claim_local_evidence_refs
        ):
            raise ValueError("scope provenance references must be unique")
        if tuple(sorted(self.claim_local_evidence_refs, key=str)) != (
            self.claim_local_evidence_refs
        ):
            raise ValueError("scope provenance references must be sorted")
        if self.canonical_ref:
            prefix, separator, value = self.canonical_ref.partition(":")
            if not separator or not prefix or not value or prefix == "batch":
                raise ValueError(
                    "canonical_ref must be a typed non-batch coordinate"
                )
            compatible = {
                ScopeSubjectKind.PERSON: {"actor", "person"},
                ScopeSubjectKind.PROJECT: {"workstream", "project"},
                ScopeSubjectKind.CUSTOMER: {"customer"},
                ScopeSubjectKind.WORK_ITEM: {
                    "commitment", "decision", "goal", "resource", "work_item"
                },
            }.get(self.subject_kind)
            if compatible is not None and prefix not in compatible:
                raise ValueError(
                    "canonical_ref type must agree with subject_kind"
                )
            if self.canonical_ref_status is None or self.normalization_version is None:
                raise ValueError(
                    "canonical_ref requires provenance status and normalization version"
                )
        elif self.canonical_ref_status is not None or self.normalization_version is not None:
            raise ValueError("canonical provenance metadata requires canonical_ref")
        return self


def validate_claim_local_scope(
    *,
    evidence: tuple[TruthEvidenceReference, ...],
    scope: tuple[ClaimScopeBinding, ...],
    tenant_id: UUID,
) -> None:
    """Validate referential locality and prohibit conflicting entity types."""

    ids = [item.reference_id for item in evidence]
    if len(ids) != len(set(ids)):
        raise ValueError("evidence reference IDs must be unique")
    if any(item.tenant_id != tenant_id for item in evidence):
        raise ValueError("evidence tenant does not match the claim tenant")
    available = set(ids)
    kinds: dict[UUID, ScopeSubjectKind] = {}
    binding_keys: set[tuple[UUID, ClaimScopeRole]] = set()
    for binding in scope:
        if not set(binding.claim_local_evidence_refs) <= available:
            raise ValueError("scope cites evidence outside this claim")
        prior = kinds.setdefault(binding.subject_id, binding.subject_kind)
        if prior is not binding.subject_kind:
            raise ValueError("one scope subject cannot have conflicting entity types")
        key = (binding.subject_id, binding.role)
        if key in binding_keys:
            raise ValueError("claim scope contains a duplicate subject-role binding")
        binding_keys.add(key)


__all__ = [
    "ClaimScopeBinding",
    "ClaimScopeRole",
    "EvidenceAuthority",
    "ScopeSubjectKind",
    "TruthEvidenceCoordinate",
    "TruthEvidenceKind",
    "TruthEvidenceReference",
    "TruthEvidenceRole",
    "validate_claim_local_scope",
]
