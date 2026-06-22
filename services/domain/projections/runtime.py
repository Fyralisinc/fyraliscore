"""Projection dispatch runtime."""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
import logging
from uuid import UUID

import asyncpg

from services.domain.projections.store import (
    fetch_pending_events,
    upsert_checkpoint,
    upsert_projection_snapshot,
)
from services.domain.projections.types import Projector
from services.domain.projections.types import ProjectionSnapshot


log = logging.getLogger("domain.projections.runtime")


@dataclass(frozen=True)
class ProjectionRunError:
    projection_name: str
    projection_version: str
    event_id: UUID
    model_id: UUID
    stage: str
    message: str


@dataclass(frozen=True)
class ProjectionRunReport:
    processed_events: int = 0
    failed_events: int = 0
    errors: tuple[ProjectionRunError, ...] = field(default_factory=tuple)


class ProjectionRegistry:
    """Small registry for core and extension-provided projectors."""

    def __init__(self, projectors: Iterable[Projector] | None = None) -> None:
        self._projectors: dict[tuple[str, str], Projector] = {}
        for projector in projectors or ():
            self.register(projector)

    def register(self, projector: Projector) -> None:
        key = (projector.name, projector.version)
        if key in self._projectors:
            raise ValueError(f"projector already registered: {key!r}")
        self._projectors[key] = projector

    @property
    def projectors(self) -> tuple[Projector, ...]:
        return tuple(self._projectors.values())


class ProjectionRunner:
    """Consumes Model events and materializes rebuildable projection rows."""

    def __init__(
        self,
        registry: ProjectionRegistry | Iterable[Projector],
    ) -> None:
        if isinstance(registry, ProjectionRegistry):
            self._registry = registry
        else:
            self._registry = ProjectionRegistry(registry)

    async def run_once(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        limit: int = 100,
    ) -> int:
        """Process up to ``limit`` events for each registered projector."""
        report = await self.run_once_detailed(conn, tenant_id=tenant_id, limit=limit)
        return report.processed_events

    async def run_once_detailed(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        limit: int = 100,
    ) -> ProjectionRunReport:
        """Process one pass and return checkpoint/failure details.

        Extension projectors are failure-isolated. If a projector fails for an
        event, that event is not checkpointed and later events for that same
        projector are not processed, preserving cursor order for retries. Other
        projectors still run.
        """
        processed = 0
        failures = 0
        errors: list[ProjectionRunError] = []
        for projector in self._registry.projectors:
            events = await fetch_pending_events(
                conn,
                tenant_id=tenant_id,
                projection_name=projector.name,
                projection_version=projector.version,
                limit=limit,
            )
            for event in events:
                try:
                    matched = projector.matches(event)
                    snapshots: list[ProjectionSnapshot] = []
                    if matched:
                        subject_keys = await projector.affected_subjects(conn, event)
                        for subject_key in subject_keys:
                            snapshot = await projector.project_subject(
                                conn,
                                tenant_id=tenant_id,
                                subject_key=subject_key,
                                source_event_ids=[event.id],
                            )
                            _validate_snapshot(projector, snapshot, tenant_id, subject_key)
                            snapshots.append(snapshot)
                except Exception as exc:  # noqa: BLE001 - isolate projector failures
                    failures += 1
                    errors.append(
                        ProjectionRunError(
                            projection_name=projector.name,
                            projection_version=projector.version,
                            event_id=event.id,
                            model_id=event.model_id,
                            stage="event",
                            message=f"{type(exc).__name__}: {exc}",
                        )
                    )
                    log.error(
                        "projection_runner.event_failed",
                        extra={
                            "projection_name": projector.name,
                            "projection_version": projector.version,
                            "tenant_id": str(tenant_id),
                            "event_id": str(event.id),
                            "model_id": str(event.model_id),
                        },
                        exc_info=True,
                    )
                    break
                for snapshot in snapshots:
                    await upsert_projection_snapshot(conn, snapshot)
                await upsert_checkpoint(
                    conn,
                    event=event,
                    projection_name=projector.name,
                    projection_version=projector.version,
                )
                processed += 1
        return ProjectionRunReport(
            processed_events=processed,
            failed_events=failures,
            errors=tuple(errors),
        )


def _validate_snapshot(
    projector: Projector,
    snapshot: ProjectionSnapshot,
    tenant_id: UUID,
    subject_key: str,
) -> None:
    if snapshot.tenant_id != tenant_id:
        raise ValueError(
            f"projector {projector.name!r} returned snapshot for tenant "
            f"{snapshot.tenant_id}, expected {tenant_id}"
        )
    if snapshot.projection_name != projector.name:
        raise ValueError(
            f"projector {projector.name!r} returned projection "
            f"{snapshot.projection_name!r}"
        )
    if snapshot.projection_version != projector.version:
        raise ValueError(
            f"projector {projector.name!r} returned version "
            f"{snapshot.projection_version!r}, expected {projector.version!r}"
        )
    if snapshot.subject_key != subject_key:
        raise ValueError(
            f"projector {projector.name!r} returned subject "
            f"{snapshot.subject_key!r}, expected {subject_key!r}"
        )
