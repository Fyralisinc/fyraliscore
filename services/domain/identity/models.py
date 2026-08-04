from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IdentityAssertionCreate(_Strict):
    tenant_id: UUID
    source_identity_key: str = Field(min_length=1)
    source_identity_ref: dict[str, Any]
    candidate_entity_ref: dict[str, Any]
    assertion_kind: Literal[
        "same_as", "not_same_as", "refers_to", "represents", "part_of", "version_of"
    ] = "same_as"
    confidence: float = Field(ge=0, le=1)
    evidence_id: UUID | None = None
    mention_id: UUID | None = None
    resolver_run_id: UUID | None = None
    score_components: dict[str, float] = Field(default_factory=dict)
    scope: dict[str, Any] = Field(default_factory=dict)
    access_policy_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    decision_provenance: dict[str, Any] = Field(default_factory=dict)
    valid_from: datetime | None = None


class IdentityAssertionRow(_Strict):
    id: UUID
    tenant_id: UUID
    source_identity_key: str
    source_identity_ref: dict[str, Any]
    candidate_entity_ref: dict[str, Any]
    assertion_kind: Literal[
        "same_as", "not_same_as", "refers_to", "represents", "part_of", "version_of"
    ]
    status: Literal["proposed", "accepted", "rejected", "superseded"]
    confidence: float
    evidence_id: UUID | None = None
    mention_id: UUID | None = None
    resolver_run_id: UUID | None = None
    score_components: dict[str, float]
    scope: dict[str, Any]
    access_policy_hash: str | None = None
    decision_provenance: dict[str, Any]
    valid_from: datetime
    valid_to: datetime | None = None
    version: int
    supersedes_assertion_id: UUID | None = None
    created_at: datetime
    decided_at: datetime | None = None

    @model_validator(mode="after")
    def validate_window(self) -> "IdentityAssertionRow":
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("valid_to must not precede valid_from")
        return self
