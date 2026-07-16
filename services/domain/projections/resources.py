"""Resource projection over canonical Models."""
from __future__ import annotations

import json
from collections import Counter
from typing import Any, Sequence
from uuid import UUID

import asyncpg

from services.domain.projections.types import ModelEvent, ProjectionSnapshot
from services.domain.projections.visibility import active_visible_model_predicates


_RESOURCE_ROLES = (
    "capability",
    "situation",
    "concern",
)

_RESOURCE_KIND_TAGS: dict[str, tuple[str, ...]] = {
    "financial": (
        "financial",
        "financial_capacity",
        "runway",
        "cash",
        "burn",
        "budget",
        "revenue",
        "capital",
        "funding",
    ),
    "capacity": (
        "capacity",
        "people",
        "team",
        "employee",
        "employees",
        "hiring",
        "headcount",
        "workload",
        "onboarding",
        "engineering_capacity",
    ),
    "relational": (
        "relational",
        "customer",
        "customers",
        "relationship",
        "renewal",
        "retention",
        "churn",
        "trust",
        "partner",
        "vendor",
    ),
    "infrastructure": (
        "infrastructure",
        "aws",
        "grafana",
        "latency",
        "incident",
        "reliability",
        "deployment",
        "production",
        "system",
    ),
    "regulatory": (
        "regulatory",
        "compliance",
        "policy",
        "audit",
        "security",
    ),
    "ip": (
        "ip",
        "product",
        "code",
        "source_code",
        "patent",
        "brand",
    ),
}
_GENERIC_RESOURCE_TAGS = ("resource", "resources")
_RESOURCE_TAGS = tuple(
    sorted(
        {
            tag
            for tags in (*_RESOURCE_KIND_TAGS.values(), _GENERIC_RESOURCE_TAGS)
            for tag in tags
        }
    )
)
_RESOURCE_ENTITY_TYPES = {
    "asset",
    "customer",
    "employee",
    "partner",
    "person",
    "product",
    "project",
    "system",
    "team",
    "tool",
    "vendor",
}
_PRESSURE_TAGS = {
    "blocked",
    "blocker",
    "bottleneck",
    "burn",
    "churn",
    "constraint",
    "dependency",
    "incident",
    "latency",
    "risk",
    "scarcity",
    "strained",
    "workload",
}
_DEPLETED_TAGS = {
    "depleted",
    "exhausted",
    "out_of_cash",
    "unavailable",
}


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _loads_json(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


class ResourceProjector:
    """Materialize operational resources from Model semantics."""

    name = "resources"
    version = "v1"

    def matches(self, event: ModelEvent) -> bool:
        if event.event_type not in {"model.created", "model.updated", "model.archived"}:
            return False

        tags = {tag.casefold() for tag in event.domain_tags}
        if tags.intersection(_RESOURCE_TAGS):
            return True

        role = (event.claim_role or "").casefold()
        return role in _RESOURCE_ROLES and any(
            str(entity.get("type") or "").casefold() in _RESOURCE_ENTITY_TYPES
            for entity in event.scope_entities
        )

    async def affected_subjects(
        self,
        conn: asyncpg.Connection,
        event: ModelEvent,
    ) -> Sequence[str]:
        del conn
        tags = {tag.casefold() for tag in event.domain_tags}
        subjects: set[str] = set()

        for resource_kind, kind_tags in _RESOURCE_KIND_TAGS.items():
            if tags.intersection(kind_tags):
                subjects.add(f"company:{resource_kind}")

        for entity in event.scope_entities:
            entity_type = str(entity.get("type") or "").strip()
            entity_id = str(entity.get("id") or "").strip()
            if entity_type.casefold() in _RESOURCE_ENTITY_TYPES and entity_id:
                subjects.add(f"{entity_type}:{entity_id}:resources")

        if not subjects:
            subjects.add(f"tenant:{event.tenant_id}:resources")
        return sorted(subjects)

    async def project_subject(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        subject_key: str,
        source_event_ids: Sequence[UUID],
    ) -> ProjectionSnapshot:
        rows = await _fetch_resource_models(
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

        cards = [_resource_card(row) for row in rows]
        source_model_ids = tuple(row["id"] for row in rows)
        confidence = max(float(row["confidence"]) for row in rows)
        state = _state(rows)
        severity = _severity(rows, state)
        payload = {
            "kind": "resource_projection",
            "subject_key": subject_key,
            "label": _label(subject_key),
            "status": "active",
            "resource_kind": _resource_kind(subject_key, rows),
            "state": state,
            "severity": severity,
            "confidence": confidence,
            "source_model_count": len(source_model_ids),
            "dominant_tags": _dominant_tags(rows),
            "resources": cards,
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


async def _fetch_resource_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    subject_key: str,
) -> list[asyncpg.Record]:
    params: list[Any] = [tenant_id]
    where = [
        "tenant_id = $1",
        *active_visible_model_predicates(),
    ]

    subject_tags = _subject_tags(subject_key)
    entity_filter = _subject_entity_filter(subject_key)
    if subject_tags or entity_filter is not None:
        params.append(list(_RESOURCE_ROLES))
        role_param = len(params)
        params.append(list(_RESOURCE_TAGS))
        tag_param = len(params)
        where.append(
            f"(claim_role = ANY(${role_param}::text[]) "
            f"OR domain_tags && ${tag_param}::text[])"
        )
    else:
        params.append(list(_RESOURCE_TAGS))
        where.append(f"domain_tags && ${len(params)}::text[]")

    if subject_tags:
        params.append(subject_tags)
        where.append(f"domain_tags && ${len(params)}::text[]")

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
    parts = subject_key.split(":")
    if len(parts) == 2 and parts[0] == "company":
        return list(_RESOURCE_KIND_TAGS.get(parts[1], ()))
    return []


def _subject_entity_filter(subject_key: str) -> dict[str, str] | None:
    parts = subject_key.split(":")
    if len(parts) != 3 or parts[2] != "resources":
        return None
    entity_type, entity_id, _ = parts
    if entity_type == "tenant":
        return None
    return {"type": entity_type, "id": entity_id}


def _resource_card(row: asyncpg.Record) -> dict[str, Any]:
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


def _state(rows: Sequence[asyncpg.Record]) -> str:
    if not rows:
        return "empty"

    row_tags = [_row_tags(row) for row in rows]
    if any(tags.intersection(_DEPLETED_TAGS) for tags in row_tags):
        return "depleted"

    for row, tags in zip(rows, row_tags, strict=True):
        confidence = float(row["confidence"])
        if row["claim_role"] == "concern" and confidence >= 0.6:
            return "strained"
        if tags.intersection(_PRESSURE_TAGS) and confidence >= 0.55:
            return "strained"

    if any(row["claim_role"] == "capability" for row in rows):
        return "available"
    return "unknown"


def _severity(rows: Sequence[asyncpg.Record], state: str) -> str:
    if not rows:
        return "none"
    max_confidence = max(float(row["confidence"]) for row in rows)
    if state == "depleted":
        return "high"
    if state == "strained":
        return "high" if max_confidence >= 0.8 else "medium"
    if max_confidence >= 0.8:
        return "medium"
    return "low"


def _resource_kind(subject_key: str, rows: Sequence[asyncpg.Record]) -> str:
    parts = subject_key.split(":")
    if len(parts) == 2 and parts[0] == "company" and parts[1] in _RESOURCE_KIND_TAGS:
        return parts[1]

    kinds = {
        kind
        for row in rows
        for kind in _resource_kinds_for_tags(_row_tags(row))
    }
    if len(kinds) == 1:
        return next(iter(kinds))
    if kinds:
        return "mixed"
    return "unknown"


def _resource_kinds_for_tags(tags: set[str]) -> set[str]:
    return {
        kind
        for kind, kind_tags in _RESOURCE_KIND_TAGS.items()
        if tags.intersection(kind_tags)
    }


def _row_tags(row: asyncpg.Record) -> set[str]:
    return {str(tag).casefold() for tag in row["domain_tags"] or []}


def _dominant_tags(rows: Sequence[asyncpg.Record]) -> list[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts.update(str(tag) for tag in row["domain_tags"] or [])
    return [tag for tag, _ in counts.most_common(8)]


def _label(subject_key: str) -> str:
    if subject_key == "company:financial":
        return "Financial resources"
    if subject_key == "company:capacity":
        return "Capacity resources"
    if subject_key == "company:relational":
        return "Relational resources"
    if subject_key == "company:infrastructure":
        return "Infrastructure resources"
    if subject_key == "company:regulatory":
        return "Regulatory resources"
    if subject_key == "company:ip":
        return "IP resources"
    if subject_key.endswith(":resources"):
        return f"{subject_key.removesuffix(':resources')} resources"
    return "Operating resources"


def _empty_snapshot(
    *,
    tenant_id: UUID,
    subject_key: str,
    source_event_ids: Sequence[UUID],
) -> ProjectionSnapshot:
    return ProjectionSnapshot(
        tenant_id=tenant_id,
        projection_name=ResourceProjector.name,
        projection_version=ResourceProjector.version,
        subject_key=subject_key,
        payload={
            "kind": "resource_projection",
            "subject_key": subject_key,
            "label": _label(subject_key),
            "status": "empty",
            "resource_kind": _resource_kind(subject_key, ()),
            "state": "empty",
            "severity": "none",
            "confidence": 0.0,
            "source_model_count": 0,
            "dominant_tags": [],
            "resources": [],
        },
        confidence=0.0,
        severity="none",
        source_model_ids=(),
        source_event_ids=tuple(source_event_ids),
    )
