"""Typed commands and results for canonical referent lineage changes."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.kernel import canonical_sha256


class _CanonicalReferentContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


def _require_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class CanonicalReferentVersionRef(_CanonicalReferentContract):
    """An immutable, typed physical referent identity."""

    type: str = Field(min_length=1)
    id: str = Field(min_length=1)
    version: int = Field(ge=1)


class CanonicalReferentReplacementCommand(_CanonicalReferentContract):
    """Request one governed predecessor-to-successor lineage transition."""

    transition_kind: Literal["replacement"] = "replacement"
    tenant_id: UUID
    operation_ref: str = Field(min_length=1)
    predecessor: CanonicalReferentVersionRef
    successor: CanonicalReferentVersionRef
    expected_predecessor_version: int = Field(ge=1)
    effective_at: datetime
    authority_ref: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    cause_event_id: UUID | None = None

    @field_validator("effective_at")
    @classmethod
    def effective_time_is_aware(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="effective_at")

    @field_validator("evidence_refs")
    @classmethod
    def evidence_is_nonempty_and_unique(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        normalized = tuple(item.strip() for item in value)
        if any(not item for item in normalized):
            raise ValueError("evidence_refs cannot contain blank references")
        if len(set(normalized)) != len(normalized):
            raise ValueError("evidence_refs cannot contain duplicates")
        return normalized

    @model_validator(mode="after")
    def replacement_has_valid_compare_and_swap(self) -> Self:
        if self.predecessor == self.successor:
            raise ValueError("replacement successor must differ from predecessor")
        if self.predecessor.type != self.successor.type:
            raise ValueError(
                "replacement predecessor and successor must have the same type"
            )
        if self.expected_predecessor_version != self.predecessor.version:
            raise ValueError(
                "expected_predecessor_version must equal predecessor.version"
            )
        return self

    @property
    def request_fingerprint(self) -> str:
        """Stable semantic digest used to reject conflicting idempotent replays."""

        return canonical_sha256(
            {
                "transition_kind": self.transition_kind,
                "tenant_id": str(self.tenant_id),
                "predecessor": self.predecessor.model_dump(mode="json"),
                "successor": self.successor.model_dump(mode="json"),
                "expected_predecessor_version": (
                    self.expected_predecessor_version
                ),
                "effective_at": self.effective_at.isoformat(),
                "authority_ref": self.authority_ref,
                "reason": self.reason,
                "evidence_refs": list(self.evidence_refs),
                "cause_event_id": (
                    str(self.cause_event_id) if self.cause_event_id else None
                ),
            }
        )


class CanonicalReferentReplacementResult(_CanonicalReferentContract):
    """One applied or idempotently replayed replacement transition."""

    transition_kind: Literal["replacement"] = "replacement"
    transition_id: UUID
    tenant_id: UUID
    operation_ref: str = Field(min_length=1)
    request_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    predecessor: CanonicalReferentVersionRef
    successor: CanonicalReferentVersionRef
    effective_at: datetime
    transaction_at: datetime
    applied: bool

    @field_validator("effective_at", "transaction_at")
    @classmethod
    def result_times_are_aware(cls, value: datetime, info) -> datetime:
        return _require_aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def result_preserves_a_real_replacement(self) -> Self:
        if self.predecessor == self.successor:
            raise ValueError("replacement successor must differ from predecessor")
        return self
