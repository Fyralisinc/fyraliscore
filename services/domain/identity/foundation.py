"""Immutable contracts for source references, mentions, and resolver runs."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _hash(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def source_reference_key(
    *, tenant_id: UUID, installation_scope: str, source: str, native_type: str, native_id: str
) -> str:
    return _hash(
        {
            "tenant_id": str(tenant_id),
            "installation_scope": installation_scope,
            "source": source,
            "native_type": native_type,
            "native_id": native_id,
        }
    )


def mention_key(value: "EntityMentionCreate") -> str:
    return _hash(
        {
            "tenant_id": str(value.tenant_id),
            "observation_id": str(value.observation_id) if value.observation_id else None,
            "evidence_id": str(value.evidence_id) if value.evidence_id else None,
            "mention_kind": value.mention_kind,
            "text": value.text,
            "span_start": value.span_start,
            "span_end": value.span_end,
            "source_reference_id": str(value.source_reference_id) if value.source_reference_id else None,
            "context": value.context,
        }
    )


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


ReferenceKind = Literal[
    "principal", "artifact", "container", "conversation", "work_record",
    "scheduled_event", "transcript", "operational_event", "financial_record",
    "employment_record", "external_resource", "url",
]


class SourceReferenceCreate(_Strict):
    tenant_id: UUID
    connector_installation_id: UUID | None = None
    installation_scope: str = Field(min_length=1)
    source: str = Field(min_length=1)
    native_type: str = Field(min_length=1)
    native_id: str = Field(min_length=1)
    reference_kind: ReferenceKind
    attributes: dict[str, Any] = Field(default_factory=dict)
    evidence_id: UUID
    valid_from: datetime | None = None
    valid_to: datetime | None = None
    status: Literal["active", "deleted", "superseded"] = "active"

    @model_validator(mode="after")
    def validate_window(self) -> "SourceReferenceCreate":
        if self.valid_from and self.valid_to and self.valid_to < self.valid_from:
            raise ValueError("source reference valid-time window is reversed")
        return self

    @property
    def computed_stable_key(self) -> str:
        return source_reference_key(
            tenant_id=self.tenant_id,
            installation_scope=self.installation_scope,
            source=self.source,
            native_type=self.native_type,
            native_id=self.native_id,
        )


class SourceReferenceRow(SourceReferenceCreate):
    id: UUID
    stable_key: str
    first_evidence_id: UUID
    latest_evidence_id: UUID
    version: int
    first_seen_at: datetime
    last_seen_at: datetime


class EntityMentionCreate(_Strict):
    tenant_id: UUID
    observation_id: UUID | None = None
    observation_occurred_at: datetime | None = None
    evidence_id: UUID | None = None
    source_reference_id: UUID | None = None
    mention_kind: Literal["structured_reference", "source_actor", "text", "coreference", "query"]
    text: str = Field(min_length=1)
    span_start: int | None = Field(default=None, ge=0)
    span_end: int | None = Field(default=None, ge=0)
    expected_types: tuple[str, ...] = ()
    context: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_origin_and_span(self) -> "EntityMentionCreate":
        if (self.span_start is None) != (self.span_end is None):
            raise ValueError("mention spans require both start and end")
        if self.span_start is not None and self.span_end is not None:
            if self.span_end <= self.span_start:
                raise ValueError("mention span end must follow its start")
        if self.mention_kind != "query" and (
            self.observation_id is None
            or self.observation_occurred_at is None
            or self.evidence_id is None
        ):
            raise ValueError("non-query mentions require observation and evidence")
        return self

    @property
    def computed_mention_key(self) -> str:
        return mention_key(self)


class EntityMentionRow(EntityMentionCreate):
    id: UUID
    mention_key: str
    status: Literal["registered", "superseded"]
    created_at: datetime


class ResolutionRunCreate(_Strict):
    tenant_id: UUID
    input_kind: Literal["observation", "query", "reprocess"]
    input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    resolver_name: str = Field(min_length=1)
    resolver_version: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    capability_snapshot: dict[str, Any]
    observation_id: UUID | None = None
    observation_occurred_at: datetime | None = None
    requester_actor_id: UUID | None = None

    @model_validator(mode="after")
    def validate_origin(self) -> "ResolutionRunCreate":
        if self.input_kind != "query" and (
            self.observation_id is None or self.observation_occurred_at is None
        ):
            raise ValueError("observation and reprocess runs require an observation")
        return self


class ResolutionRunRow(ResolutionRunCreate):
    id: UUID
    status: Literal["running", "completed", "failed"]
    result_hash: str | None = None
    failure: str | None = None
    started_at: datetime
    completed_at: datetime | None = None


__all__ = [
    "EntityMentionCreate",
    "EntityMentionRow",
    "ResolutionRunCreate",
    "ResolutionRunRow",
    "SourceReferenceCreate",
    "SourceReferenceRow",
    "mention_key",
    "source_reference_key",
]
