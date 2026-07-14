"""Constraint projection over canonical Models."""
from __future__ import annotations

import json
from collections import Counter
from typing import Any, Sequence
from uuid import UUID

import asyncpg

from services.domain.projections.types import ModelEvent, ProjectionSnapshot


_CONSTRAINT_ROLE_ONLY = (
    "concern",
    "capability",
    "situation",
)

_CONSTRAINT_TAGS = (
    "constraint",
    "runway",
    "capacity",
    "financial_capacity",
    "cash",
    "burn",
    "hiring",
    "onboarding",
    "dependency",
    "bottleneck",
    "obligation",
    "scarcity",
    "risk",
    "blocked",
    "blocker",
)

_FINANCIAL_TAGS = ("financial_capacity", "runway", "cash", "burn")
_CAPACITY_TAGS = ("capacity", "financial_capacity", "hiring", "onboarding", "runway")


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _loads_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


class ConstraintProjector:
    """Materialize the company's operating constraints from Model semantics."""

    name = "constraints"
    version = "v1"

    def matches(self, event: ModelEvent) -> bool:
        tags = {tag.casefold() for tag in event.domain_tags}
        role = (event.claim_role or "").casefold()
        return (
            event.event_type in {"model.created", "model.updated", "model.archived"}
            and (
                role in _CONSTRAINT_ROLE_ONLY
                or bool(tags.intersection(_CONSTRAINT_TAGS))
            )
        )

    async def affected_subjects(
        self,
        conn: asyncpg.Connection,
        event: ModelEvent,
    ) -> Sequence[str]:
        del conn
        tags = {tag.casefold() for tag in event.domain_tags}
        subjects: set[str] = set()

        if "runway" in tags:
            subjects.add("company:runway")
        if tags.intersection(_FINANCIAL_TAGS):
            subjects.add("company:financial_capacity")
        if tags.intersection(_CAPACITY_TAGS):
            subjects.add("company:capacity")

        for entity in event.scope_entities:
            entity_type = str(entity.get("type") or "").strip()
            entity_id = str(entity.get("id") or "").strip()
            if entity_type and entity_id:
                subjects.add(f"{entity_type}:{entity_id}:constraints")

        if not subjects:
            subjects.add(f"tenant:{event.tenant_id}:constraints")
        return sorted(subjects)

    async def project_subject(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        subject_key: str,
        source_event_ids: Sequence[UUID],
    ) -> ProjectionSnapshot:
        rows = await _fetch_constraint_models(
            conn,
            tenant_id=tenant_id,
            subject_key=subject_key,
        )
        if not rows:
            return _empty_snapshot(
                tenant_id=tenant_id,
                subject_key=subject_key,
                source_event_ids=source_event_ids,
            )

        cards = [_constraint_card(row) for row in rows]
        source_model_ids = tuple(row["id"] for row in rows)
        confidence = max(float(row["confidence"]) for row in rows)
        severity = _severity(rows)
        payload = {
            "kind": "constraint_projection",
            "subject_key": subject_key,
            "label": _label(subject_key),
            "status": "active",
            "severity": severity,
            "confidence": confidence,
            "source_model_count": len(source_model_ids),
            "dominant_tags": _dominant_tags(rows),
            "constraints": cards,
        }
        return ProjectionSnapshot(
            tenant_id=tenant_id,
            projection_name=self.name,
            projection_version=self.version,
            subject_key=subject_key,
            payload=payload,
            confidence=confidence,
            severity=severity,
            source_model_ids=source_model_ids,
            source_event_ids=tuple(source_event_ids),
        )


async def _fetch_constraint_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    subject_key: str,
) -> list[asyncpg.Record]:
    params: list[Any] = [
        tenant_id,
        list(_CONSTRAINT_ROLE_ONLY),
        list(_CONSTRAINT_TAGS),
    ]
    where = [
        "tenant_id = $1",
        "status = 'active'",
        "(claim_role = ANY($2::text[]) OR domain_tags && $3::text[])",
    ]

    subject_tags = _subject_tags(subject_key)
    if subject_tags:
        params.append(subject_tags)
        where.append(f"domain_tags && ${len(params)}::text[]")

    entity_filter = _subject_entity_filter(subject_key)
    if entity_filter is not None:
        params.append(_jsonb([entity_filter]))
        where.append(f"scope_entities @> ${len(params)}::jsonb")

    params.append(50)
    return list(
        await conn.fetch(
            f"""
            SELECT
              id, proposition, "natural" AS natural, confidence, falsifier,
              claim_role, domain_tags, scope_entities, created_at
            FROM models
            WHERE {' AND '.join(where)}
            ORDER BY confidence DESC, created_at DESC, id DESC
            LIMIT ${len(params)}
            """,
            *params,
        )
    )


def _subject_tags(subject_key: str) -> list[str]:
    if subject_key == "company:runway":
        return ["runway"]
    if subject_key == "company:financial_capacity":
        return list(_FINANCIAL_TAGS)
    if subject_key == "company:capacity":
        return list(_CAPACITY_TAGS)
    return []


def _subject_entity_filter(subject_key: str) -> dict[str, str] | None:
    parts = subject_key.split(":")
    if len(parts) != 3 or parts[2] != "constraints":
        return None
    entity_type, entity_id, _ = parts
    if entity_type == "tenant":
        return None
    return {"type": entity_type, "id": entity_id}


def _constraint_card(row: asyncpg.Record) -> dict[str, Any]:
    proposition = _loads_json(row["proposition"]) or {}
    falsifier = _loads_json(row["falsifier"])
    return {
        "model_id": str(row["id"]),
        "natural": row["natural"],
        "claim_role": row["claim_role"],
        "confidence": float(row["confidence"]),
        "domain_tags": list(row["domain_tags"] or []),
        "proposition": proposition,
        "falsifier": falsifier,
    }


def _severity(rows: Sequence[asyncpg.Record]) -> str:
    if not rows:
        return "none"
    max_confidence = max(float(row["confidence"]) for row in rows)
    concern_count = sum(1 for row in rows if row["claim_role"] == "concern")
    if max_confidence >= 0.8 or (max_confidence >= 0.7 and concern_count):
        return "high"
    if max_confidence >= 0.6 or len(rows) >= 3:
        return "medium"
    return "low"


def _dominant_tags(rows: Sequence[asyncpg.Record]) -> list[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts.update(str(tag) for tag in row["domain_tags"] or [])
    return [tag for tag, _ in counts.most_common(8)]


def _label(subject_key: str) -> str:
    if subject_key == "company:runway":
        return "Runway constraints"
    if subject_key == "company:financial_capacity":
        return "Financial capacity constraints"
    if subject_key == "company:capacity":
        return "Capacity constraints"
    if subject_key.endswith(":constraints"):
        return f"{subject_key.removesuffix(':constraints')} constraints"
    return "Operating constraints"


def _empty_snapshot(
    *,
    tenant_id: UUID,
    subject_key: str,
    source_event_ids: Sequence[UUID],
) -> ProjectionSnapshot:
    return ProjectionSnapshot(
        tenant_id=tenant_id,
        projection_name=ConstraintProjector.name,
        projection_version=ConstraintProjector.version,
        subject_key=subject_key,
        payload={
            "kind": "constraint_projection",
            "subject_key": subject_key,
            "label": _label(subject_key),
            "status": "empty",
            "severity": "none",
            "confidence": 0.0,
            "source_model_count": 0,
            "dominant_tags": [],
            "constraints": [],
        },
        confidence=0.0,
        severity="none",
        source_model_ids=(),
        source_event_ids=tuple(source_event_ids),
    )
