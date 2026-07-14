"""Shared contracts for projection workers."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Protocol, Sequence
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class ModelEvent:
    id: UUID
    tenant_id: UUID
    model_id: UUID
    event_type: str
    changed_fields: tuple[str, ...]
    proposition_kind: str | None
    claim_role: str | None
    domain_tags: tuple[str, ...]
    scope_entities: tuple[dict[str, Any], ...]
    semantic_snapshot: dict[str, Any]
    previous_snapshot: dict[str, Any] | None
    source_event_id: UUID | None
    created_at: datetime


@dataclass(frozen=True)
class ProjectionSnapshot:
    tenant_id: UUID
    projection_name: str
    projection_version: str
    subject_key: str
    payload: dict[str, Any]
    confidence: float = 0.0
    severity: str | None = None
    source_model_ids: tuple[UUID, ...] = field(default_factory=tuple)
    source_event_ids: tuple[UUID, ...] = field(default_factory=tuple)


ProjectionRefreshReason = Literal[
    "event_match",
    "dependency_delta",
    "watch_delta",
    "manual",
    "rebuild",
    "contract_change",
]


@dataclass(frozen=True)
class ProjectionDependencyRef:
    """Projection-independent source object a snapshot depends on."""

    ref_kind: str
    ref_value: str
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectionWatchKey:
    """Projection-independent frontier key that may discover new evidence."""

    watch_kind: str
    watch_value: str
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ProjectionSubjectRef:
    projection_name: str
    projection_version: str
    subject_key: str


@dataclass(frozen=True)
class ProjectionRefreshJob:
    id: UUID
    tenant_id: UUID
    projection_name: str
    projection_version: str
    subject_key: str
    reason: ProjectionRefreshReason | str
    event_ids: tuple[UUID, ...] = field(default_factory=tuple)
    dependency_refs: tuple[ProjectionDependencyRef, ...] = field(default_factory=tuple)
    payload: dict[str, Any] = field(default_factory=dict)
    status: str = "pending"
    attempts: int = 0
    max_attempts: int = 5
    scheduled_at: datetime | None = None
    leased_at: datetime | None = None
    processed_at: datetime | None = None
    last_error: str | None = None


@dataclass(frozen=True)
class ProjectionInquiryState:
    tenant_id: UUID
    projection_name: str
    projection_version: str
    subject_key: str
    last_mined_event_id: UUID | None = None
    last_mined_event_created_at: datetime | None = None
    evidence_digest: str | None = None
    watch_fingerprint: str | None = None
    state_payload: dict[str, Any] = field(default_factory=dict)
    updated_at: datetime | None = None


@dataclass(frozen=True)
class ProjectionEvidenceGraph:
    """Evidence produced by projection inquiry before projector reduction."""

    tenant_id: UUID
    projection_name: str
    projection_version: str
    subject_key: str
    models: tuple[Any, ...] = field(default_factory=tuple)
    edges: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    open_questions: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    events: tuple[ModelEvent, ...] = field(default_factory=tuple)
    dependency_refs: tuple[ProjectionDependencyRef, ...] = field(default_factory=tuple)
    watch_keys: tuple[ProjectionWatchKey, ...] = field(default_factory=tuple)
    match_reasons: dict[str, tuple[str, ...]] = field(default_factory=dict)
    omissions: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    freshness: dict[str, Any] = field(default_factory=dict)


class Projector(Protocol):
    name: str
    version: str

    def matches(self, event: ModelEvent) -> bool:
        ...

    async def affected_subjects(
        self,
        conn: asyncpg.Connection,
        event: ModelEvent,
    ) -> Sequence[str]:
        ...

    async def project_subject(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        subject_key: str,
        source_event_ids: Sequence[UUID],
    ) -> ProjectionSnapshot:
        ...
