"""Decision surface projection over canonical Models.

The model graph stores pressure, recommendations, actors, and scoped entities as
ordinary Models. This projector turns those beliefs into an operating surface:
what likely needs a decision, who owns it if known, and what evidence should
refresh it.
"""
from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.projections.types import ModelEvent, ProjectionSnapshot


_DECISION_ROLES = ("concern", "recommendation", "situation")
_DECISION_TAGS = (
    "approval",
    "blocker",
    "blocked",
    "bottleneck",
    "capacity",
    "constraint",
    "decision",
    "decision_pressure",
    "dependency",
    "escalation",
    "owner",
    "pressure",
    "priority",
    "resource",
    "risk",
    "tradeoff",
)
_PRESSURE_TERMS = {
    "capacity",
    "compliance",
    "decision",
    "execution",
    "market",
    "resource",
    "revenue",
    "trust",
}
_REVISIT_TRIGGERS = {
    "source_pressure_changes": "Source pressure materially changes",
    "owner_action_recorded": "Owner action or commitment is recorded",
    "pressure_resolved": "Evidence shows the pressure resolved",
}


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _loads_json(value: Any, fallback: Any) -> Any:
    if value is None:
        return fallback
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return fallback


class DecisionSurfaceProjector:
    """Materialize decision-needing operating surfaces from Model semantics."""

    name = "decision_surfaces"
    version = "v1"

    def matches(self, event: ModelEvent) -> bool:
        if event.event_type not in {"model.created", "model.updated", "model.archived"}:
            return False
        proposition = _loads_json(event.semantic_snapshot.get("proposition"), {})
        tags = {tag.casefold() for tag in event.domain_tags}
        role = (event.claim_role or "").casefold()
        return (
            role in _DECISION_ROLES
            and (
                bool(tags.intersection(_DECISION_TAGS))
                or _pressure_type(proposition, event.domain_tags) is not None
                or _is_decision_pressure_recommendation(proposition)
            )
        )

    async def affected_subjects(
        self,
        conn: asyncpg.Connection,
        event: ModelEvent,
    ) -> Sequence[str]:
        del conn
        subjects: set[str] = set()
        proposition = _loads_json(event.semantic_snapshot.get("proposition"), {})
        pressure_type = _pressure_type(proposition, event.domain_tags)
        if pressure_type:
            subjects.add(f"company:{pressure_type}:decision_surface")

        for entity in event.scope_entities:
            entity_type = str(entity.get("type") or "").strip()
            entity_id = str(entity.get("id") or "").strip()
            if entity_type and entity_id:
                subjects.add(f"{entity_type}:{entity_id}:decision_surface")

        for actor_id in _event_scope_actor_ids(event):
            subjects.add(f"actor:{actor_id}:decision_surface")

        if not subjects:
            subjects.add(f"tenant:{event.tenant_id}:decision_surfaces")
        return sorted(subjects)

    async def project_subject(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        subject_key: str,
        source_event_ids: Sequence[UUID],
    ) -> ProjectionSnapshot:
        rows = await _fetch_decision_surface_models(
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

        surfaces = [_surface_card(row) for row in rows]
        source_model_ids = tuple(row["id"] for row in rows)
        confidence = max(float(row["confidence"]) for row in rows)
        severity = _severity(rows)
        owned_count = sum(1 for surface in surfaces if surface["owner_actor_id"])
        payload = {
            "kind": "decision_surface_projection",
            "subject_key": subject_key,
            "label": _label(subject_key),
            "status": "active",
            "surface_state": "owned" if owned_count else "needs_owner",
            "decision_required": True,
            "severity": severity,
            "confidence": confidence,
            "source_model_count": len(source_model_ids),
            "dominant_tags": _dominant_tags(rows),
            "decision_surfaces": surfaces,
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


async def _fetch_decision_surface_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    subject_key: str,
) -> list[asyncpg.Record]:
    params: list[Any] = [
        tenant_id,
        list(_DECISION_ROLES),
        list(_DECISION_TAGS),
    ]
    where = [
        "tenant_id = $1",
        "status = 'active'",
        """
        (
          claim_role = ANY($2::text[])
          OR domain_tags && $3::text[]
          OR proposition ? 'pressure_type'
          OR proposition #>> '{proposed_change,payload,kind}' = 'decision_pressure'
        )
        """,
    ]

    pressure_type = _subject_pressure_type(subject_key)
    if pressure_type:
        params.append(pressure_type)
        where.append(
            f"""
            (
              ${len(params)} = ANY(domain_tags)
              OR proposition ->> 'pressure_type' = ${len(params)}
              OR proposition #>> '{{proposed_change,payload,source_pressure_type}}'
                 = ${len(params)}
            )
            """
        )

    entity_filter = _subject_entity_filter(subject_key)
    if entity_filter is not None:
        params.append(_jsonb([entity_filter]))
        where.append(f"scope_entities @> ${len(params)}::jsonb")

    actor_id = _subject_actor_id(subject_key)
    if actor_id is not None:
        params.append([actor_id])
        where.append(f"scope_actors && ${len(params)}::uuid[]")

    params.append(50)
    return list(
        await conn.fetch(
            f"""
            SELECT
              id, proposition, "natural" AS natural, confidence,
              claim_role, domain_tags, scope_entities, scope_actors, created_at
            FROM models
            WHERE {' AND '.join(where)}
            ORDER BY confidence DESC, created_at DESC, id DESC
            LIMIT ${len(params)}
            """,
            *params,
        )
    )


def _surface_card(row: asyncpg.Record) -> dict[str, Any]:
    proposition = _loads_json(row["proposition"], {})
    scope_entities = _loads_json(row["scope_entities"], [])
    domain_tags = [str(tag) for tag in row["domain_tags"] or []]
    title = _title(row["natural"], proposition)
    owner_actor_id = _owner_actor_id(row, proposition)
    pressure_type = _pressure_type(proposition, domain_tags) or "decision"
    return {
        "model_id": str(row["id"]),
        "title": title,
        "decision_text": _decision_text(title, proposition),
        "pressure_type": pressure_type,
        "state": "owned_candidate" if owner_actor_id else "needs_owner",
        "owner_actor_id": owner_actor_id,
        "scope_entities": scope_entities if isinstance(scope_entities, list) else [],
        "confidence": float(row["confidence"]),
        "severity": _card_severity(float(row["confidence"])),
        "claim_role": row["claim_role"],
        "domain_tags": domain_tags,
        "why_now": _why_now(row["natural"], proposition),
        "revisit_triggers": dict(_REVISIT_TRIGGERS),
    }


def _title(natural: str | None, proposition: dict[str, Any]) -> str:
    payload = _proposed_change_payload(proposition)
    for key in ("title", "situation", "summary", "assertion", "text"):
        value = payload.get(key) if key == "title" else proposition.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return str(natural or "Decision surface").strip()


def _decision_text(title: str, proposition: dict[str, Any]) -> str:
    payload = _proposed_change_payload(proposition)
    description = payload.get("description")
    if isinstance(description, str) and description.strip():
        return description.strip()
    summary = proposition.get("summary")
    if isinstance(summary, str) and summary.strip():
        return summary.strip()
    return f"Choose the accountable next action for {title}."


def _why_now(natural: str | None, proposition: dict[str, Any]) -> str:
    for key in ("judgment_change", "relationship_summary", "summary", "open_falsifier"):
        value = proposition.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return str(natural or "").strip()


def _owner_actor_id(row: asyncpg.Record, proposition: dict[str, Any]) -> str | None:
    target_actor = proposition.get("target_actor_id")
    if isinstance(target_actor, str) and target_actor.strip():
        return target_actor.strip()
    actors = row["scope_actors"] or []
    if actors:
        return str(actors[0])
    return None


def _pressure_type(
    proposition: dict[str, Any],
    domain_tags: Sequence[str],
) -> str | None:
    payload = _proposed_change_payload(proposition)
    for value in (
        proposition.get("pressure_type"),
        payload.get("source_pressure_type"),
    ):
        if isinstance(value, str) and value.strip():
            return value.strip().casefold()
    for tag in domain_tags:
        normalized = str(tag or "").strip().casefold()
        if normalized in _PRESSURE_TERMS:
            return normalized
    return None


def _is_decision_pressure_recommendation(proposition: dict[str, Any]) -> bool:
    payload = _proposed_change_payload(proposition)
    return str(payload.get("kind") or "").strip().casefold() == "decision_pressure"


def _proposed_change_payload(proposition: dict[str, Any]) -> dict[str, Any]:
    proposed_change = proposition.get("proposed_change")
    if not isinstance(proposed_change, dict):
        return {}
    payload = proposed_change.get("payload")
    return payload if isinstance(payload, dict) else {}


def _event_scope_actor_ids(event: ModelEvent) -> tuple[str, ...]:
    raw = event.semantic_snapshot.get("scope_actors") or ()
    out: list[str] = []
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes, bytearray)):
        for value in raw:
            actor_id = str(value or "").strip()
            if actor_id:
                out.append(actor_id)
    return tuple(out)


def _subject_pressure_type(subject_key: str) -> str | None:
    parts = subject_key.split(":")
    if (
        len(parts) == 3
        and parts[0] == "company"
        and parts[1] in _PRESSURE_TERMS
        and parts[2] == "decision_surface"
    ):
        return parts[1]
    return None


def _subject_entity_filter(subject_key: str) -> dict[str, str] | None:
    parts = subject_key.split(":")
    if len(parts) != 3 or parts[2] != "decision_surface":
        return None
    entity_type, entity_id, _ = parts
    if entity_type == "company" and entity_id in _PRESSURE_TERMS:
        return None
    if entity_type in {"actor", "tenant"}:
        return None
    return {"type": entity_type, "id": entity_id}


def _subject_actor_id(subject_key: str) -> UUID | None:
    parts = subject_key.split(":")
    if len(parts) != 3 or parts[0] != "actor" or parts[2] != "decision_surface":
        return None
    try:
        return UUID(parts[1])
    except ValueError:
        return None


def _severity(rows: Sequence[asyncpg.Record]) -> str:
    if not rows:
        return "none"
    max_confidence = max(float(row["confidence"]) for row in rows)
    if max_confidence >= 0.8 or len(rows) >= 3:
        return "high"
    if max_confidence >= 0.65:
        return "medium"
    return "low"


def _card_severity(confidence: float) -> str:
    if confidence >= 0.8:
        return "high"
    if confidence >= 0.65:
        return "medium"
    return "low"


def _dominant_tags(rows: Sequence[asyncpg.Record]) -> list[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts.update(str(tag) for tag in row["domain_tags"] or [])
    return [tag for tag, _ in counts.most_common(8)]


def _label(subject_key: str) -> str:
    if subject_key.endswith(":decision_surface"):
        return f"{subject_key.removesuffix(':decision_surface')} decision surface"
    if subject_key.endswith(":decision_surfaces"):
        return "Decision surfaces"
    return "Decision surface"


def _empty_snapshot(
    *,
    tenant_id: UUID,
    subject_key: str,
    source_event_ids: Sequence[UUID],
) -> ProjectionSnapshot:
    return ProjectionSnapshot(
        tenant_id=tenant_id,
        projection_name=DecisionSurfaceProjector.name,
        projection_version=DecisionSurfaceProjector.version,
        subject_key=subject_key,
        payload={
            "kind": "decision_surface_projection",
            "subject_key": subject_key,
            "label": _label(subject_key),
            "status": "empty",
            "surface_state": "none",
            "decision_required": False,
            "severity": "none",
            "confidence": 0.0,
            "source_model_count": 0,
            "dominant_tags": [],
            "decision_surfaces": [],
        },
        confidence=0.0,
        severity="none",
        source_model_ids=(),
        source_event_ids=tuple(source_event_ids),
    )
