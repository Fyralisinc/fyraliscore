"""Neutral Model-layer event outbox.

Model events are the durable connection between the canonical belief
kernel and rebuildable projections. They describe how belief state
changed, but they do not name downstream projections.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable, Sequence
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from lib.shared.types import ModelRow
from services.domain.models.facets import MODEL_FACET_SCHEMA_VERSION, model_facet_names


MODEL_EVENT_CREATED = "model.created"
MODEL_EVENT_UPDATED = "model.updated"
MODEL_EVENT_ARCHIVED = "model.archived"
MODEL_EVENT_RELATION_CHANGED = "model.relation_changed"
MODEL_EVENT_OPEN_QUESTION_CHANGED = "model.open_question_changed"

MODEL_EVENT_TYPES = frozenset(
    {
        MODEL_EVENT_CREATED,
        MODEL_EVENT_UPDATED,
        MODEL_EVENT_ARCHIVED,
        MODEL_EVENT_RELATION_CHANGED,
        MODEL_EVENT_OPEN_QUESTION_CHANGED,
    }
)

_MODEL_EVENT_COLUMNS_SQL = """
    id, tenant_id, proposition, "natural" AS natural,
    scope_actors, scope_entities, scope_temporal,
    confidence, falsifier, supporting_event_ids, supporting_model_ids,
    evidential_weight, status, archived_at, archive_reason, created_at,
    evaluate_at, resolution_criteria, contributing_models,
    proposition_kind, claim_role, abstraction_level, time_mode, modality,
    polarity, domain_tags,
    COALESCE((SELECT mst.semantic_terms
              FROM model_semantic_terms mst
              WHERE mst.model_id = id), '{}'::text[]) AS semantic_terms,
    COALESCE((SELECT jsonb_agg(jsonb_build_object(
                'id', moq.id,
                'question', moq.question,
                'question_type', moq.question_type,
                'rationale', moq.rationale,
                'priority', moq.priority,
                'status', moq.status,
                'expected_resolution_signal', moq.expected_resolution_signal,
                'search_signature', moq.search_signature,
                'source_model_ids', moq.source_model_ids,
                'created_at', moq.created_at,
                'last_searched_at', moq.last_searched_at,
                'next_search_at', moq.next_search_at
              ) ORDER BY moq.priority DESC, moq.created_at ASC)
              FROM model_open_questions moq
              WHERE moq.model_id = models.id
                AND moq.status = 'open'), '[]'::jsonb) AS open_questions,
    memory_grammar_version,
    confirmed_count, contested_count, last_confirmed_at,
    confidence_at_assertion, resolved_at, resolution_outcome,
    activation_coefficient
"""


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    return value


def model_semantic_snapshot(model: ModelRow | asyncpg.Record | dict[str, Any]) -> dict[str, Any]:
    """Return the event payload used by projection dispatch and rebuilds."""
    if isinstance(model, asyncpg.Record):
        raw = dict(model)
    elif isinstance(model, ModelRow):
        raw = model.model_dump(mode="python")
    else:
        raw = dict(model)

    snapshot = {
        "id": raw.get("id"),
        "tenant_id": raw.get("tenant_id"),
        "facet_schema_version": MODEL_FACET_SCHEMA_VERSION,
        "facet_names": model_facet_names(raw),
        "proposition": raw.get("proposition") or {},
        "natural": raw.get("natural"),
        "scope_actors": raw.get("scope_actors") or [],
        "scope_entities": raw.get("scope_entities") or [],
        "scope_temporal": raw.get("scope_temporal") or {},
        "confidence": raw.get("confidence"),
        "falsifier": raw.get("falsifier"),
        "supporting_event_ids": raw.get("supporting_event_ids") or [],
        "supporting_model_ids": raw.get("supporting_model_ids") or [],
        "evidential_weight": raw.get("evidential_weight"),
        "status": raw.get("status"),
        "archived_at": raw.get("archived_at"),
        "archive_reason": raw.get("archive_reason"),
        "created_at": raw.get("created_at"),
        "evaluate_at": raw.get("evaluate_at"),
        "resolution_criteria": raw.get("resolution_criteria"),
        "contributing_models": raw.get("contributing_models") or [],
        "proposition_kind": raw.get("proposition_kind"),
        "claim_role": raw.get("claim_role"),
        "abstraction_level": raw.get("abstraction_level"),
        "time_mode": raw.get("time_mode"),
        "modality": raw.get("modality"),
        "polarity": raw.get("polarity"),
        "domain_tags": raw.get("domain_tags") or [],
        "semantic_terms": raw.get("semantic_terms") or [],
        "open_questions": raw.get("open_questions") or [],
        "memory_grammar_version": raw.get("memory_grammar_version"),
        "confirmed_count": raw.get("confirmed_count"),
        "contested_count": raw.get("contested_count"),
        "last_confirmed_at": raw.get("last_confirmed_at"),
        "confidence_at_assertion": raw.get("confidence_at_assertion"),
        "resolved_at": raw.get("resolved_at"),
        "resolution_outcome": raw.get("resolution_outcome"),
        "activation_coefficient": raw.get("activation_coefficient"),
    }
    return _jsonable(snapshot)


async def emit_model_event(
    conn: asyncpg.Connection,
    *,
    model: ModelRow | asyncpg.Record | dict[str, Any],
    event_type: str,
    changed_fields: Sequence[str],
    previous_snapshot: dict[str, Any] | None = None,
    source_event_id: UUID | None = None,
    event_id: UUID | None = None,
) -> UUID:
    """Append one neutral Model event to the durable outbox."""
    if event_type not in MODEL_EVENT_TYPES:
        raise ValueError(f"unknown model event type: {event_type!r}")

    snapshot = model_semantic_snapshot(model)
    eid = event_id or uuid7()
    await conn.execute(
        """
        INSERT INTO model_events (
          id, tenant_id, model_id, event_type, changed_fields,
          proposition_kind, claim_role, domain_tags, scope_entities,
          semantic_snapshot, previous_snapshot, source_event_id
        ) VALUES (
          $1, $2, $3, $4, $5::text[],
          $6, $7, $8::text[], $9::jsonb,
          $10::jsonb, $11::jsonb, $12
        )
        """,
        eid,
        UUID(str(snapshot["tenant_id"])),
        UUID(str(snapshot["id"])),
        event_type,
        sorted({str(field) for field in changed_fields}),
        snapshot.get("proposition_kind"),
        snapshot.get("claim_role"),
        list(snapshot.get("domain_tags") or []),
        _jsonb(snapshot.get("scope_entities") or []),
        _jsonb(snapshot),
        _jsonb(_jsonable(previous_snapshot)) if previous_snapshot is not None else None,
        source_event_id,
    )
    return eid


async def emit_model_events(
    conn: asyncpg.Connection,
    *,
    models: Iterable[ModelRow],
    event_type: str,
    changed_fields: Sequence[str],
) -> list[UUID]:
    """Append a homogeneous batch of Model events."""
    event_ids: list[UUID] = []
    for model in models:
        event_ids.append(
            await emit_model_event(
                conn,
                model=model,
                event_type=event_type,
                changed_fields=changed_fields,
                source_event_id=model.born_from_event_id,
            )
        )
    return event_ids


async def emit_model_event_from_db(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    event_type: str,
    changed_fields: Sequence[str],
    previous_snapshot: dict[str, Any] | None = None,
    source_event_id: UUID | None = None,
) -> UUID | None:
    """Fetch the current Model row and emit an event for direct update paths."""
    row = await conn.fetchrow(
        f"""
        SELECT {_MODEL_EVENT_COLUMNS_SQL}
        FROM models
        WHERE id = $1
        """,
        model_id,
    )
    if row is None:
        return None
    return await emit_model_event(
        conn,
        model=row,
        event_type=event_type,
        changed_fields=changed_fields,
        previous_snapshot=previous_snapshot,
        source_event_id=source_event_id,
    )


__all__ = [
    "MODEL_EVENT_ARCHIVED",
    "MODEL_EVENT_CREATED",
    "MODEL_EVENT_OPEN_QUESTION_CHANGED",
    "MODEL_EVENT_RELATION_CHANGED",
    "MODEL_EVENT_TYPES",
    "MODEL_EVENT_UPDATED",
    "emit_model_event",
    "emit_model_event_from_db",
    "emit_model_events",
    "model_semantic_snapshot",
]
