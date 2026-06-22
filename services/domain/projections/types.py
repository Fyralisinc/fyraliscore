"""Shared contracts for projection workers."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, Sequence
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
