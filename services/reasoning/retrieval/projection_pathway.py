"""Projection-first retrieval candidates.

This is an opportunistic pre-pass: typed projection snapshots provide a compact
index into canonical Models, while the existing pathways remain the fallback.
"""
from __future__ import annotations

from typing import Any, Protocol
from uuid import UUID

import asyncpg

from services.domain.projections.repo import ProjectionRepo, ProjectionStaleness
from services.domain.projections.subjects import (
    ProjectionSubjectSeed,
    projection_subject_candidates as _projection_subject_candidates,
)
from services.reasoning.retrieval.pathways import PathwayResult


class ProjectionTrigger(Protocol):
    tenant_id: UUID
    seed_natural_text: str | None
    subkind: str | None
    topology_event_kind: str | None
    seed_signature: dict[str, Any] | None
    region_spec: dict[str, Any] | None


_PROJECTION_VERSION = "v1"
_PROJECTIONS = ProjectionRepo()


def projection_subject_candidates(
    trigger: ProjectionTrigger,
    *,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID] | None = None,
) -> list[tuple[str, str]]:
    """Infer projection subjects from trigger scope and seed text."""
    return _projection_subject_candidates(
        ProjectionSubjectSeed(
            tenant_id=trigger.tenant_id,
            seed_natural_text=trigger.seed_natural_text,
            seed_entities=tuple(effective_seed_entities),
            scope_actors=tuple(effective_scope_actors or ()),
            subkind=trigger.subkind,
            topology_event_kind=trigger.topology_event_kind,
            seed_signature=trigger.seed_signature,
            region_spec=trigger.region_spec,
        )
    )


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    return bool(
        await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL",
            f"public.{table_name}",
        )
    )


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)


async def _freshness_notes(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    projection_names: list[str],
) -> dict[str, Any]:
    if not projection_names:
        return {"available": False, "reason": "no_projection_names"}
    if not await _table_exists(conn, "model_events"):
        return {"available": False, "reason": "model_events_missing"}
    if not await _table_exists(conn, "projection_checkpoints"):
        return {"available": False, "reason": "projection_checkpoints_missing"}

    staleness = await _PROJECTIONS.list_staleness(
        conn,
        tenant_id=tenant_id,
        projection_names=projection_names,
        projection_version=_PROJECTION_VERSION,
    )
    stale_projection_names = [
        entry.projection_name for entry in staleness if entry.is_stale
    ]
    latest_model_event_created_at = _latest_model_event_created_at(staleness)
    return {
        "available": True,
        "is_stale": bool(stale_projection_names),
        "latest_model_event_created_at": _iso(latest_model_event_created_at),
        "stale_projection_names": stale_projection_names,
        "checkpoint_count": sum(
            1 for entry in staleness if entry.checkpoint_event_created_at is not None
        ),
    }


def _latest_model_event_created_at(
    staleness: list[ProjectionStaleness],
) -> Any:
    latest_values = [
        entry.latest_model_event_created_at
        for entry in staleness
        if entry.latest_model_event_created_at is not None
    ]
    return max(latest_values) if latest_values else None


async def pathway_projection_context(
    trigger: ProjectionTrigger,
    tenant_id: UUID,
    conn: asyncpg.Connection,
    *,
    effective_seed_entities: list[dict[str, Any]],
    effective_scope_actors: list[UUID] | None = None,
    max_snapshots: int = 8,
    max_models: int = 24,
) -> PathwayResult:
    """Return Models pointed to by relevant projection snapshots."""
    notes: dict[str, Any] = {
        "projection_first": True,
        "source_pathway": "A",
        "max_snapshots": max_snapshots,
        "max_models": max_models,
    }
    if max_snapshots <= 0 or max_models <= 0:
        return PathwayResult(
            source_pathway="A",
            notes={**notes, "reason": "disabled_by_limit"},
        )

    if not await _table_exists(conn, "projection_snapshots"):
        return PathwayResult(
            source_pathway="A",
            notes={**notes, "reason": "projection_snapshots_missing"},
        )

    candidates = projection_subject_candidates(
        trigger,
        effective_seed_entities=effective_seed_entities,
        effective_scope_actors=effective_scope_actors,
    )
    if not candidates:
        return PathwayResult(
            source_pathway="A",
            notes={**notes, "reason": "no_projection_subject_candidates"},
        )

    projection_names = sorted({name for name, _ in candidates})
    subject_keys = [subject for _, subject in candidates]
    snapshots = await _PROJECTIONS.list_snapshots_for_subjects(
        conn,
        tenant_id=tenant_id,
        subjects=candidates,
        projection_version=_PROJECTION_VERSION,
        limit=max_snapshots,
        require_source_models=True,
    )
    if not snapshots:
        return PathwayResult(
            source_pathway="A",
            notes={
                **notes,
                "reason": "no_projection_snapshots",
                "subject_candidates": len(candidates),
                "subject_keys": subject_keys[:20],
                "projection_names": projection_names,
                "freshness": {
                    "available": False,
                    "reason": "skipped_no_projection_snapshots",
                },
            },
        )

    freshness = await _freshness_notes(
        conn,
        tenant_id=tenant_id,
        projection_names=projection_names,
    )

    model_ids: list[UUID] = []
    seen_model_ids: set[UUID] = set()
    snapshot_notes: list[dict[str, Any]] = []
    for snapshot in snapshots:
        snapshot_model_ids = list(snapshot.source_model_ids)
        snapshot_notes.append(
            {
                "projection_name": snapshot.projection_name,
                "subject_key": snapshot.subject_key,
                "confidence": snapshot.confidence,
                "severity": snapshot.severity,
                "source_model_count": len(snapshot_model_ids),
            }
        )
        for model_id in snapshot_model_ids:
            if model_id in seen_model_ids:
                continue
            seen_model_ids.add(model_id)
            model_ids.append(model_id)
            if len(model_ids) >= max_models:
                break
        if len(model_ids) >= max_models:
            break

    models = await _PROJECTIONS.load_models_by_id(
        conn,
        tenant_id=tenant_id,
        model_ids=model_ids,
    )
    return PathwayResult(
        models=list(models),
        source_pathway="A",
        notes={
            **notes,
            "projection_names": projection_names,
            "subject_candidates": len(candidates),
            "subject_keys": subject_keys[:20],
            "snapshots_returned": len(snapshots),
            "models_returned": len(models),
            "snapshots": snapshot_notes,
            "freshness": freshness,
        },
    )


__all__ = [
    "pathway_projection_context",
    "projection_subject_candidates",
]
