"""Employee profile projection over canonical Models.

The Model layer stores employee beliefs as ordinary Models. This projection is
the typed operating view: it groups actor-scoped Models into a profile that
planning, retrieval, and actor context can use without adding employee-specific
columns to `models`.
"""
from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.projections.types import ModelEvent, ProjectionSnapshot


_PROFILE_ROLES = ("capability", "concern", "pattern", "relation", "recommendation")
_EMPLOYEE_TAGS = {
    "actor",
    "capacity",
    "employee",
    "employees",
    "mentorship",
    "people",
    "preference",
    "support_need",
    "team",
    "work_pattern",
    "work_style",
    "workload",
}
_SUPPORT_TAGS = {"blocked", "blocker", "support_need", "dependency"}
_RISK_TAGS = {"burnout", "constraint", "overload", "risk", "strained", "workload"}
_WORK_STYLE_TAGS = {"preference", "work_pattern", "work_style"}
_MIN_PROFILE_CONFIDENCE = 0.6
_MAX_MODELS_PER_FACET = 4
_MAX_PROFILE_MODELS = 16


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


class EmployeeProfileProjector:
    """Materialize actor-scoped employee operating profiles."""

    name = "employee_profiles"
    version = "v1"

    def matches(self, event: ModelEvent) -> bool:
        if event.event_type not in {"model.created", "model.updated", "model.archived"}:
            return False
        if not _event_scope_actors(event):
            return False
        tags = {tag.casefold() for tag in event.domain_tags}
        role = (event.claim_role or "").casefold()
        return bool(tags.intersection(_EMPLOYEE_TAGS) or role in _PROFILE_ROLES)

    async def affected_subjects(
        self,
        conn: asyncpg.Connection,
        event: ModelEvent,
    ) -> Sequence[str]:
        del conn
        return tuple(
            f"employee:{actor_id}:profile"
            for actor_id in _event_scope_actors(event)
        )

    async def project_subject(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        subject_key: str,
        source_event_ids: Sequence[UUID],
    ) -> ProjectionSnapshot:
        actor_id = _actor_id_from_subject(subject_key)
        rows = await _fetch_profile_models(conn, tenant_id=tenant_id, actor_id=actor_id)
        if not rows:
            return ProjectionSnapshot(
                tenant_id=tenant_id,
                projection_name=self.name,
                projection_version=self.version,
                subject_key=subject_key,
                payload={
                    "kind": "employee_profile_projection",
                    "subject_key": subject_key,
                    "status": "empty",
                    "facets": {},
                    "role_counts": {},
                    "source_model_count": 0,
                },
                confidence=0.0,
                severity="none",
                source_model_ids=(),
                source_event_ids=tuple(source_event_ids),
            )

        selected = _facet_balanced_rows(rows)
        selected_ids = tuple(row["id"] for row in selected)
        open_questions = await _fetch_open_questions(
            conn,
            tenant_id=tenant_id,
            model_ids=selected_ids,
        )
        evidence_span = await _evidence_span(conn, tenant_id=tenant_id, rows=selected)
        facets = _facets(selected)
        role_counts = Counter(str(row["claim_role"] or "unknown") for row in selected)
        semantic_terms = sorted(
            {
                str(term)
                for row in selected
                for term in (row["semantic_terms"] or [])
                if str(term).strip()
            }
        )
        confidence = max(float(row["confidence"]) for row in selected)
        severity = _severity(facets, confidence)
        return ProjectionSnapshot(
            tenant_id=tenant_id,
            projection_name=self.name,
            projection_version=self.version,
            subject_key=subject_key,
            payload={
                "kind": "employee_profile_projection",
                "subject_key": subject_key,
                "status": "active",
                "facets": facets,
                "role_counts": dict(role_counts),
                "evidence_span": evidence_span,
                "semantic_terms": semantic_terms,
                "source_model_count": len(selected_ids),
                "profile_models": [_model_card(row) for row in selected],
                "open_questions": open_questions,
            },
            confidence=confidence,
            severity=severity,
            source_model_ids=selected_ids,
            source_event_ids=tuple(source_event_ids),
        )


def _event_scope_actors(event: ModelEvent) -> tuple[UUID, ...]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for raw in event.semantic_snapshot.get("scope_actors") or ():
        try:
            actor_id = raw if isinstance(raw, UUID) else UUID(str(raw))
        except (TypeError, ValueError):
            continue
        if actor_id in seen:
            continue
        seen.add(actor_id)
        out.append(actor_id)
    return tuple(out)


def _actor_id_from_subject(subject_key: str) -> UUID:
    parts = subject_key.split(":")
    if len(parts) != 3 or parts[0] != "employee" or parts[2] != "profile":
        raise ValueError(f"invalid employee profile subject: {subject_key!r}")
    return UUID(parts[1])


async def _fetch_profile_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
) -> list[asyncpg.Record]:
    return list(
        await conn.fetch(
            """
            SELECT m.id, m.proposition, m."natural" AS natural, m.confidence,
                   m.activation, m.claim_role, m.domain_tags, m.scope_entities,
                   m.supporting_event_ids, m.created_at,
                   COALESCE(mst.semantic_terms, '{}'::text[]) AS semantic_terms
            FROM models m
            LEFT JOIN model_semantic_terms mst
              ON mst.tenant_id = m.tenant_id AND mst.model_id = m.id
            WHERE m.tenant_id = $1
              AND m.status = 'active'
              AND m.confidence >= $5
              AND m.scope_actors && ARRAY[$2]::uuid[]
              AND (
                m.claim_role = ANY($3::text[])
                OR m.domain_tags && $4::text[]
              )
            ORDER BY m.activation * m.confidence DESC,
                     m.confidence DESC,
                     m.created_at DESC,
                     m.id DESC
            LIMIT 64
            """,
            tenant_id,
            actor_id,
            list(_PROFILE_ROLES),
            sorted(_EMPLOYEE_TAGS),
            _MIN_PROFILE_CONFIDENCE,
        )
    )


def _facet_balanced_rows(rows: Sequence[asyncpg.Record]) -> list[asyncpg.Record]:
    buckets: dict[str, list[asyncpg.Record]] = {
        "capabilities": [],
        "work_style": [],
        "support_needs": [],
        "risk_factors": [],
        "patterns": [],
        "relationships": [],
        "recommendations": [],
        "recent": [],
    }
    for row in rows:
        for facet in _facets_for_row(row):
            buckets.setdefault(facet, []).append(row)
        buckets["recent"].append(row)

    selected: list[asyncpg.Record] = []
    seen: set[UUID] = set()
    for facet in (
        "support_needs",
        "risk_factors",
        "capabilities",
        "work_style",
        "patterns",
        "relationships",
        "recommendations",
        "recent",
    ):
        for row in buckets.get(facet, ())[:_MAX_MODELS_PER_FACET]:
            if row["id"] in seen:
                continue
            seen.add(row["id"])
            selected.append(row)
            if len(selected) >= _MAX_PROFILE_MODELS:
                return selected
    return selected


def _facets_for_row(row: asyncpg.Record) -> tuple[str, ...]:
    role = str(row["claim_role"] or "")
    text = str(row["natural"] or "").casefold()
    tags = _row_tags(row)
    facets: list[str] = []
    if role == "capability":
        facets.append("capabilities")
    if role == "relation":
        if tags.intersection(_WORK_STYLE_TAGS) or _looks_like_preference(text):
            facets.append("work_style")
        else:
            facets.append("relationships")
    if role == "concern":
        if tags.intersection(_SUPPORT_TAGS) or _looks_like_support_need(text):
            facets.append("support_needs")
        else:
            facets.append("risk_factors")
    if role == "pattern":
        facets.append("patterns")
    if role == "recommendation":
        facets.append("recommendations")
    if tags.intersection(_RISK_TAGS) and "risk_factors" not in facets:
        facets.append("risk_factors")
    return tuple(dict.fromkeys(facets or ["recent"]))


def _facets(rows: Sequence[asyncpg.Record]) -> dict[str, list[dict[str, Any]]]:
    facets: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        for facet in _facets_for_row(row):
            facets.setdefault(facet, []).append(_model_card(row))
    return facets


def _model_card(row: asyncpg.Record) -> dict[str, Any]:
    return {
        "model_id": str(row["id"]),
        "natural": row["natural"],
        "claim_role": row["claim_role"],
        "confidence": float(row["confidence"]),
        "activation": float(row["activation"]),
        "domain_tags": list(row["domain_tags"] or []),
        "semantic_terms": list(row["semantic_terms"] or []),
        "supporting_event_ids": [str(v) for v in row["supporting_event_ids"] or []],
        "created_at": _iso(row["created_at"]),
    }


async def _evidence_span(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    rows: Sequence[asyncpg.Record],
) -> dict[str, Any] | None:
    evidence_ids = tuple(
        dict.fromkeys(
            event_id
            for row in rows
            for event_id in list(row["supporting_event_ids"] or [])
        )
    )
    if not evidence_ids:
        return None
    span = await conn.fetchrow(
        """
        SELECT min(occurred_at) AS first_seen,
               max(occurred_at) AS last_seen,
               count(*)::int AS observation_count
        FROM observations
        WHERE tenant_id = $1 AND id = ANY($2::uuid[])
        """,
        tenant_id,
        list(evidence_ids),
    )
    if span is None:
        return None
    return {
        "first_seen": _iso(span["first_seen"]),
        "last_seen": _iso(span["last_seen"]),
        "observation_count": int(span["observation_count"] or 0),
    }


async def _fetch_open_questions(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: Sequence[UUID],
) -> list[dict[str, Any]]:
    if not model_ids:
        return []
    rows = await conn.fetch(
        """
        SELECT id, model_id, question, question_type, rationale, priority,
               expected_resolution_signal, search_signature, next_search_at
        FROM model_open_questions
        WHERE tenant_id = $1
          AND status = 'open'
          AND model_id = ANY($2::uuid[])
        ORDER BY priority DESC, updated_at DESC, id DESC
        LIMIT 12
        """,
        tenant_id,
        list(model_ids),
    )
    return [
        {
            "id": str(row["id"]),
            "model_id": str(row["model_id"]),
            "question": row["question"],
            "question_type": row["question_type"],
            "rationale": row["rationale"],
            "priority": float(row["priority"]),
            "expected_resolution_signal": _loads_json(row["expected_resolution_signal"], {}),
            "search_signature": _loads_json(row["search_signature"], {}),
            "next_search_at": _iso(row["next_search_at"]),
        }
        for row in rows
    ]


def _severity(facets: dict[str, list[dict[str, Any]]], confidence: float) -> str:
    if facets.get("support_needs") or facets.get("risk_factors"):
        return "high" if confidence >= 0.85 else "medium"
    if confidence >= 0.85:
        return "medium"
    return "low"


def _row_tags(row: asyncpg.Record) -> set[str]:
    return {str(tag).casefold() for tag in row["domain_tags"] or []}


def _looks_like_preference(text: str) -> bool:
    return any(
        token in text
        for token in (
            "prefers",
            "preference",
            "works best",
            "strongest",
            "quiet",
            "brief",
            "design window",
        )
    )


def _looks_like_support_need(text: str) -> bool:
    return any(
        token in text
        for token in (
            "needs support",
            "needs help",
            "needs decision",
            "needs clarity",
            "needs owner",
            "needs ownership",
            "ownership clarity",
            "waiting on",
            "blocked",
            "unblock",
            "approval",
            "escalat",
        )
    )


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        return isoformat()
    return str(value)


__all__ = ["EmployeeProfileProjector"]
