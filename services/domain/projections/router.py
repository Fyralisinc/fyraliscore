"""Route Model deltas to projection refresh work."""
from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.projections.store import (
    enqueue_projection_refresh_job,
    list_projection_subjects_for_dependency,
    list_projection_subjects_for_watch_key,
)
from services.domain.projections.types import (
    ModelEvent,
    ProjectionDependencyRef,
    ProjectionSubjectRef,
    ProjectionWatchKey,
    Projector,
)


@dataclass(frozen=True)
class ProjectionRouteError:
    projection_name: str
    projection_version: str
    stage: str
    message: str


@dataclass(frozen=True)
class ProjectionRouteReport:
    event_id: UUID
    enqueued_jobs: tuple[UUID, ...] = field(default_factory=tuple)
    direct_matches: int = 0
    dependency_matches: int = 0
    watch_matches: int = 0
    errors: tuple[ProjectionRouteError, ...] = field(default_factory=tuple)


def dependency_refs_for_event(event: ModelEvent) -> tuple[ProjectionDependencyRef, ...]:
    """Return precise source refs changed by a Model event."""
    refs = [
        ProjectionDependencyRef(
            ref_kind="model",
            ref_value=str(event.model_id),
            reason="changed_model",
        ),
        ProjectionDependencyRef(
            ref_kind="model_event",
            ref_value=str(event.id),
            reason="changed_event",
        ),
    ]
    if event.source_event_id is not None:
        refs.append(
            ProjectionDependencyRef(
                ref_kind="model_event",
                ref_value=str(event.source_event_id),
                reason="source_event",
            )
        )
    return _dedupe_dependency_refs(refs)


def watch_keys_for_event(event: ModelEvent) -> tuple[ProjectionWatchKey, ...]:
    """Return broad semantic keys an event may satisfy for future inquiry."""
    keys: list[ProjectionWatchKey] = [
        ProjectionWatchKey("event_type", event.event_type, reason="event_type"),
        ProjectionWatchKey("model", str(event.model_id), reason="model"),
    ]
    if event.proposition_kind:
        keys.append(
            ProjectionWatchKey(
                "proposition_kind",
                event.proposition_kind,
                reason="proposition_kind",
            )
        )
    if event.claim_role:
        keys.append(ProjectionWatchKey("claim_role", event.claim_role, reason="claim_role"))
    keys.extend(
        ProjectionWatchKey("domain_tag", tag, reason="domain_tag")
        for tag in event.domain_tags
    )
    keys.extend(
        ProjectionWatchKey("changed_field", field_name, reason="changed_field")
        for field_name in event.changed_fields
    )
    for entity in event.scope_entities:
        key = _scope_entity_key(entity)
        if key:
            keys.append(ProjectionWatchKey("scope_entity", key, reason="scope_entity"))
    return _dedupe_watch_keys(keys)


async def enqueue_refreshes_for_event(
    conn: asyncpg.Connection,
    event: ModelEvent,
    projectors: Iterable[Projector],
    *,
    dependency_limit: int = 100,
    watch_limit: int = 100,
) -> ProjectionRouteReport:
    """Translate one Model event into deduped projection refresh jobs.

    The router combines three signals:
    - exact dependency refs from previously materialized snapshots;
    - broad watch keys that capture projection inquiry frontiers;
    - the current projector ``matches``/``affected_subjects`` contract for
      cold-start discovery and backwards-compatible materialization.
    """
    projector_list = tuple(projectors)
    allowed_projection_keys = {
        (projector.name, projector.version) for projector in projector_list
    }
    event_refs = dependency_refs_for_event(event)
    event_watch_keys = watch_keys_for_event(event)
    routes: dict[tuple[str, str, str], _Route] = {}
    errors: list[ProjectionRouteError] = []
    direct_matches = 0
    dependency_matches = 0
    watch_matches = 0

    for ref in event_refs:
        subjects = await list_projection_subjects_for_dependency(
            conn,
            tenant_id=event.tenant_id,
            ref_kind=ref.ref_kind,
            ref_value=ref.ref_value,
            limit=dependency_limit,
        )
        dependency_matches += len(subjects)
        for subject in subjects:
            if not _subject_allowed(subject, allowed_projection_keys):
                continue
            route = _route_for(routes, subject)
            route.reasons.add("dependency_delta")
            route.dependency_refs.append(ref)

    for key in event_watch_keys:
        subjects = await list_projection_subjects_for_watch_key(
            conn,
            tenant_id=event.tenant_id,
            watch_kind=key.watch_kind,
            watch_value=key.watch_value,
            limit=watch_limit,
        )
        watch_matches += len(subjects)
        for subject in subjects:
            if not _subject_allowed(subject, allowed_projection_keys):
                continue
            route = _route_for(routes, subject)
            route.reasons.add("watch_delta")
            route.watch_keys.append(key)

    for projector in projector_list:
        try:
            if not projector.matches(event):
                continue
            subject_keys = await projector.affected_subjects(conn, event)
        except Exception as exc:  # noqa: BLE001 - isolate extension projectors
            errors.append(
                ProjectionRouteError(
                    projection_name=projector.name,
                    projection_version=projector.version,
                    stage="match",
                    message=f"{type(exc).__name__}: {exc}",
                )
            )
            continue
        direct_matches += len(subject_keys)
        for subject_key in subject_keys:
            subject = ProjectionSubjectRef(
                projection_name=projector.name,
                projection_version=projector.version,
                subject_key=subject_key,
            )
            route = _route_for(routes, subject)
            route.reasons.add("event_match")
            route.dependency_refs.extend(event_refs)

    enqueued: list[UUID] = []
    for route in routes.values():
        enqueued.append(
            await enqueue_projection_refresh_job(
                conn,
                tenant_id=event.tenant_id,
                projection_name=route.projection_name,
                projection_version=route.projection_version,
                subject_key=route.subject_key,
                reason=_primary_reason(route.reasons),
                event_ids=(event.id,),
                dependency_refs=_dedupe_dependency_refs(route.dependency_refs),
                payload={
                    "route_reasons": sorted(route.reasons),
                    "watch_keys": [
                        {
                            "watch_kind": key.watch_kind,
                            "watch_value": key.watch_value,
                            "reason": key.reason,
                            "metadata": key.metadata,
                        }
                        for key in _dedupe_watch_keys(route.watch_keys)
                    ],
                },
            )
        )

    return ProjectionRouteReport(
        event_id=event.id,
        enqueued_jobs=tuple(enqueued),
        direct_matches=direct_matches,
        dependency_matches=dependency_matches,
        watch_matches=watch_matches,
        errors=tuple(errors),
    )


@dataclass
class _Route:
    projection_name: str
    projection_version: str
    subject_key: str
    reasons: set[str] = field(default_factory=set)
    dependency_refs: list[ProjectionDependencyRef] = field(default_factory=list)
    watch_keys: list[ProjectionWatchKey] = field(default_factory=list)


def _route_for(
    routes: dict[tuple[str, str, str], _Route],
    subject: ProjectionSubjectRef,
) -> _Route:
    key = (subject.projection_name, subject.projection_version, subject.subject_key)
    route = routes.get(key)
    if route is None:
        route = _Route(
            projection_name=subject.projection_name,
            projection_version=subject.projection_version,
            subject_key=subject.subject_key,
        )
        routes[key] = route
    return route


def _scope_entity_key(entity: dict[str, Any]) -> str | None:
    raw_type = entity.get("type") or entity.get("kind") or entity.get("entity_type")
    raw_id = entity.get("id") or entity.get("entity_id") or entity.get("model_id")
    entity_type = _normalize(raw_type)
    entity_id = str(raw_id or "").strip()
    if not entity_type or not entity_id:
        return None
    return f"{entity_type}:{entity_id}"


def _subject_allowed(
    subject: ProjectionSubjectRef,
    allowed_projection_keys: set[tuple[str, str]],
) -> bool:
    return (subject.projection_name, subject.projection_version) in allowed_projection_keys


def _primary_reason(reasons: set[str]) -> str:
    for reason in ("event_match", "dependency_delta", "watch_delta"):
        if reason in reasons:
            return reason
    return "event_match"


def _dedupe_dependency_refs(
    refs: Sequence[ProjectionDependencyRef],
) -> tuple[ProjectionDependencyRef, ...]:
    out: list[ProjectionDependencyRef] = []
    seen: set[tuple[str, str]] = set()
    for ref in refs:
        ref_kind = _normalize(ref.ref_kind)
        ref_value = str(ref.ref_value or "").strip()
        if not ref_kind or not ref_value:
            continue
        key = (ref_kind, ref_value)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            ProjectionDependencyRef(
                ref_kind=ref_kind,
                ref_value=ref_value,
                reason=ref.reason,
                metadata=dict(ref.metadata or {}),
            )
        )
    return tuple(out)


def _dedupe_watch_keys(keys: Sequence[ProjectionWatchKey]) -> tuple[ProjectionWatchKey, ...]:
    out: list[ProjectionWatchKey] = []
    seen: set[tuple[str, str]] = set()
    for key in keys:
        watch_kind = _normalize(key.watch_kind)
        watch_value = _normalize(key.watch_value)
        if not watch_kind or not watch_value:
            continue
        dedupe_key = (watch_kind, watch_value)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(
            ProjectionWatchKey(
                watch_kind=watch_kind,
                watch_value=watch_value,
                reason=key.reason,
                metadata=dict(key.metadata or {}),
            )
        )
    return tuple(out)


def _normalize(value: Any) -> str:
    return str(value or "").strip().casefold()
