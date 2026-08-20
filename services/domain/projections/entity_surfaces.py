"""Entity-first projection surfaces over canonical Models.

These projectors make living organizational objects directly readable without
moving truth out of the model graph. They intentionally share one small
implementation: the families differ by entity vocabulary and summary fields,
not by storage or runtime mechanics.
"""
from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.projections.types import ModelEvent, ProjectionSnapshot


_MODEL_EVENT_TYPES = {"model.created", "model.updated", "model.archived"}
_MAX_MODELS = 32


@dataclass(frozen=True)
class EntityProjectionConfig:
    projection_name: str
    entity_type: str
    subject_suffix: str
    entity_types: tuple[str, ...]
    tags: tuple[str, ...]
    roles: tuple[str, ...]
    summary_key: str


_COMMITMENTS = EntityProjectionConfig(
    projection_name="commitments",
    entity_type="commitment",
    subject_suffix="commitments",
    entity_types=(
        "candidate_commitment",
        "commitment",
        "jira",
        "pr",
        "pull_request",
        "ticket",
        "work_item",
    ),
    tags=(
        "blocked",
        "blocker",
        "commitment",
        "commitments",
        "dependency",
        "deliverable",
        "deadline",
        "handoff",
        "obligation",
        "owner",
        "promise",
        "risk",
        "slip",
    ),
    roles=("concern", "prediction", "recommendation", "relation", "situation"),
    summary_key="commitments",
)

_CUSTOMERS = EntityProjectionConfig(
    projection_name="customers",
    entity_type="customer",
    subject_suffix="customers",
    entity_types=(
        "account",
        "candidate_customer",
        "customer",
        "customer_resource",
        "org",
        "organization",
    ),
    tags=(
        "account",
        "churn",
        "customer",
        "customers",
        "implementation",
        "onboarding",
        "relationship",
        "renewal",
        "retention",
        "revenue",
        "risk",
        "trust",
    ),
    roles=("concern", "prediction", "recommendation", "relation", "situation"),
    summary_key="customer_signals",
)

_GOALS = EntityProjectionConfig(
    projection_name="goals",
    entity_type="goal",
    subject_suffix="goals",
    entity_types=(
        "candidate_goal",
        "goal",
        "initiative",
        "objective",
        "project",
        "workstream",
    ),
    tags=(
        "goal",
        "goals",
        "initiative",
        "milestone",
        "northstar",
        "objective",
        "outcome",
        "priority",
        "progress",
        "roadmap",
    ),
    roles=("capability", "concern", "prediction", "recommendation", "situation"),
    summary_key="goal_signals",
)

_DECISIONS = EntityProjectionConfig(
    projection_name="decisions",
    entity_type="decision",
    subject_suffix="decisions",
    entity_types=("candidate_decision", "choice", "decision"),
    tags=(
        "approval",
        "decision",
        "decision_pressure",
        "decisions",
        "escalation",
        "go/no-go",
        "option",
        "owner",
        "pressure",
        "priority",
        "prioritize",
        "tradeoff",
    ),
    roles=("concern", "recommendation", "situation"),
    summary_key="decision_signals",
)


class EntitySurfaceProjector:
    """Base projector for first-class organizational entity surfaces."""

    version = "v1"

    def __init__(self, config: EntityProjectionConfig) -> None:
        self._config = config
        self.name = config.projection_name

    def matches(self, event: ModelEvent) -> bool:
        if event.event_type not in _MODEL_EVENT_TYPES:
            return False
        tags = _normalized_set(event.domain_tags)
        if tags.intersection(self._config.tags):
            return True
        return any(
            _canonical_entity_type(entity, self._config) is not None
            for entity in event.scope_entities
        )

    async def affected_subjects(
        self,
        conn: asyncpg.Connection,
        event: ModelEvent,
    ) -> Sequence[str]:
        del conn
        subjects: set[str] = set()
        for entity in event.scope_entities:
            canonical = _canonical_entity_type(entity, self._config)
            entity_id = str(entity.get("id") or entity.get("entity_id") or "").strip()
            if canonical and entity_id:
                subjects.add(
                    f"{canonical}:{entity_id}:{self._config.subject_suffix}"
                )
        if not subjects and self.matches(event):
            subjects.add(f"tenant:{event.tenant_id}:{self._config.subject_suffix}")
        return sorted(subjects)

    async def project_subject(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        subject_key: str,
        source_event_ids: Sequence[UUID],
    ) -> ProjectionSnapshot:
        rows = await _fetch_entity_models(
            conn,
            tenant_id=tenant_id,
            subject_key=subject_key,
            config=self._config,
        )
        if not rows:
            return ProjectionSnapshot(
                tenant_id=tenant_id,
                projection_name=self.name,
                projection_version=self.version,
                subject_key=subject_key,
                payload=_empty_payload(
                    config=self._config,
                    subject_key=subject_key,
                    source_event_ids=source_event_ids,
                ),
                confidence=0.0,
                severity="none",
                source_model_ids=(),
                source_event_ids=tuple(source_event_ids),
            )

        cards = [_model_card(row) for row in rows]
        source_model_ids = tuple(_row_value(row, "id") for row in rows)
        evidence_event_ids = _evidence_event_ids(rows, source_event_ids)
        confidence = max(float(_row_value(row, "confidence", 0.0) or 0.0) for row in rows)
        severity = _severity(self._config, cards, confidence)
        payload = _payload(
            config=self._config,
            subject_key=subject_key,
            rows=rows,
            cards=cards,
            confidence=confidence,
            severity=severity,
            source_event_ids=evidence_event_ids,
        )
        return ProjectionSnapshot(
            tenant_id=tenant_id,
            projection_name=self.name,
            projection_version=self.version,
            subject_key=subject_key,
            payload=payload,
            confidence=confidence,
            severity=severity,
            source_model_ids=source_model_ids,
            source_event_ids=evidence_event_ids,
        )


class CommitmentProjector(EntitySurfaceProjector):
    def __init__(self) -> None:
        super().__init__(_COMMITMENTS)


class CustomerProjector(EntitySurfaceProjector):
    def __init__(self) -> None:
        super().__init__(_CUSTOMERS)


class GoalProjector(EntitySurfaceProjector):
    def __init__(self) -> None:
        super().__init__(_GOALS)


class DecisionProjector(EntitySurfaceProjector):
    def __init__(self) -> None:
        super().__init__(_DECISIONS)


async def _fetch_entity_models(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    subject_key: str,
    config: EntityProjectionConfig,
) -> list[asyncpg.Record]:
    params: list[Any] = [tenant_id]
    where = [
        "tenant_id = $1",
        "status = 'active'",
    ]

    entity_id = _subject_entity_id(subject_key, config)
    if entity_id is not None:
        params.append(list(config.roles))
        role_param = len(params)
        params.append(list(config.tags))
        tag_param = len(params)
        where.append(
            f"(claim_role = ANY(${role_param}::text[]) "
            f"OR domain_tags && ${tag_param}::text[])"
        )
        entity_clauses: list[str] = []
        for entity_type in config.entity_types:
            params.append(_jsonb([{"type": entity_type, "id": entity_id}]))
            entity_clauses.append(f"scope_entities @> ${len(params)}::jsonb")
        where.append(f"({' OR '.join(entity_clauses)})")
    else:
        params.append(list(config.tags))
        where.append(f"domain_tags && ${len(params)}::text[]")

    params.append(_MAX_MODELS)
    return list(
        await conn.fetch(
            f"""
            SELECT
              id, proposition, "natural" AS natural, confidence, activation,
              claim_role, domain_tags, scope_entities, scope_actors,
              supporting_event_ids, created_at, evaluate_at
            FROM models
            WHERE {' AND '.join(where)}
            ORDER BY activation * confidence DESC,
                     confidence DESC,
                     created_at DESC,
                     id DESC
            LIMIT ${len(params)}
            """,
            *params,
        )
    )


def _payload(
    *,
    config: EntityProjectionConfig,
    subject_key: str,
    rows: Sequence[Any],
    cards: list[dict[str, Any]],
    confidence: float,
    severity: str,
    source_event_ids: tuple[UUID, ...],
) -> dict[str, Any]:
    related_refs = _related_entity_refs(rows, config=config, subject_key=subject_key)
    owner_actor_ids = _owner_actor_ids(rows)
    last_evidence_at = _last_evidence_at(rows)
    payload: dict[str, Any] = {
        "kind": f"{config.entity_type}_projection",
        "projection_family": config.projection_name,
        "subject_key": subject_key,
        "entity_type": config.entity_type,
        "canonical_label": _canonical_label(subject_key, cards),
        "status": _status(config, cards),
        "confidence": confidence,
        "severity": severity,
        "last_evidence_at": last_evidence_at,
        "evidence_model_ids": [card["model_id"] for card in cards],
        "evidence_event_ids": [str(event_id) for event_id in source_event_ids],
        "source_model_count": len(cards),
        "dominant_tags": _dominant_tags(rows),
        "related_entity_refs": related_refs,
        "open_questions": [],
        "needs_review": _needs_review(cards),
        config.summary_key: cards,
    }
    payload.update(_family_summary(config, cards, owner_actor_ids))
    return payload


def _family_summary(
    config: EntityProjectionConfig,
    cards: Sequence[dict[str, Any]],
    owner_actor_ids: list[str],
) -> dict[str, Any]:
    risk_cards = [
        card
        for card in cards
        if _card_tags(card).intersection(
            {"blocked", "blocker", "churn", "constraint", "risk", "slip"}
        )
        or str(card.get("claim_role") or "").casefold() == "concern"
    ]
    if config.projection_name == "commitments":
        return {
            "owner_actor_ids": owner_actor_ids,
            "blockers": risk_cards[:8],
            "downstream_risks": risk_cards[:8],
        }
    if config.projection_name == "customers":
        return {
            "health": "at_risk" if risk_cards else "active",
            "risk_drivers": risk_cards[:8],
            "owner_actor_ids": owner_actor_ids,
        }
    if config.projection_name == "goals":
        return {
            "progress_state": "at_risk" if risk_cards else "active",
            "blockers": risk_cards[:8],
            "owner_actor_ids": owner_actor_ids,
        }
    if config.projection_name == "decisions":
        return {
            "decision_state": "owned" if owner_actor_ids else "needs_owner",
            "owner_actor_ids": owner_actor_ids,
            "tradeoffs": [
                card for card in cards if "tradeoff" in _card_tags(card)
            ][:8],
        }
    return {}


def _empty_payload(
    *,
    config: EntityProjectionConfig,
    subject_key: str,
    source_event_ids: Sequence[UUID],
) -> dict[str, Any]:
    return {
        "kind": f"{config.entity_type}_projection",
        "projection_family": config.projection_name,
        "subject_key": subject_key,
        "entity_type": config.entity_type,
        "canonical_label": _label_from_subject(subject_key),
        "status": "empty",
        "confidence": 0.0,
        "severity": "none",
        "last_evidence_at": None,
        "evidence_model_ids": [],
        "evidence_event_ids": [str(event_id) for event_id in source_event_ids],
        "source_model_count": 0,
        "dominant_tags": [],
        "related_entity_refs": [],
        "open_questions": [],
        "needs_review": False,
        config.summary_key: [],
    }


def _model_card(row: Any) -> dict[str, Any]:
    proposition = _loads_json(_row_value(row, "proposition"), {})
    scope_entities = _loads_json(_row_value(row, "scope_entities"), [])
    scope_actors = _row_value(row, "scope_actors", ()) or ()
    created_at = _row_value(row, "created_at")
    evaluate_at = _row_value(row, "evaluate_at")
    return {
        "model_id": str(_row_value(row, "id")),
        "natural": str(_row_value(row, "natural", "") or ""),
        "claim_role": _row_value(row, "claim_role"),
        "confidence": float(_row_value(row, "confidence", 0.0) or 0.0),
        "domain_tags": [str(tag) for tag in _row_value(row, "domain_tags", ()) or ()],
        "scope_entities": scope_entities if isinstance(scope_entities, list) else [],
        "scope_actor_ids": [str(actor_id) for actor_id in scope_actors],
        "proposition": proposition if isinstance(proposition, dict) else {},
        "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
        "evaluate_at": evaluate_at.isoformat() if isinstance(evaluate_at, datetime) else None,
    }


def _subject_entity_id(subject_key: str, config: EntityProjectionConfig) -> str | None:
    parts = subject_key.split(":")
    if len(parts) != 3:
        return None
    entity_type, entity_id, suffix = parts
    if suffix != config.subject_suffix:
        return None
    if entity_type == "tenant":
        return None
    if entity_type != config.entity_type:
        return None
    return entity_id or None


def _canonical_entity_type(
    entity: dict[str, Any],
    config: EntityProjectionConfig,
) -> str | None:
    raw = str(
        entity.get("type")
        or entity.get("kind")
        or entity.get("entity_type")
        or ""
    ).strip().casefold()
    return config.entity_type if raw in set(config.entity_types) else None


def _evidence_event_ids(
    rows: Sequence[Any],
    source_event_ids: Sequence[UUID],
) -> tuple[UUID, ...]:
    out: list[UUID] = []
    seen: set[UUID] = set()

    def add(raw: Any) -> None:
        try:
            event_id = raw if isinstance(raw, UUID) else UUID(str(raw))
        except (TypeError, ValueError):
            return
        if event_id in seen:
            return
        seen.add(event_id)
        out.append(event_id)

    for event_id in source_event_ids:
        add(event_id)
    for row in rows:
        for event_id in _row_value(row, "supporting_event_ids", ()) or ():
            add(event_id)
    return tuple(out)


def _related_entity_refs(
    rows: Sequence[Any],
    *,
    config: EntityProjectionConfig,
    subject_key: str,
) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    subject_parts = subject_key.split(":")
    subject_id = subject_parts[1] if len(subject_parts) == 3 else None
    for row in rows:
        scope_entities = _loads_json(_row_value(row, "scope_entities"), [])
        if not isinstance(scope_entities, list):
            continue
        for entity in scope_entities:
            if not isinstance(entity, dict):
                continue
            entity_type = str(entity.get("type") or "").strip()
            entity_id = str(entity.get("id") or "").strip()
            if not entity_type or not entity_id:
                continue
            if entity_type in config.entity_types and entity_id == subject_id:
                continue
            key = (entity_type, entity_id)
            if key in seen:
                continue
            seen.add(key)
            refs.append({"type": entity_type, "id": entity_id})
            if len(refs) >= 24:
                return refs
    return refs


def _owner_actor_ids(rows: Sequence[Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for actor_id in _row_value(row, "scope_actors", ()) or ():
            value = str(actor_id)
            if value in seen:
                continue
            seen.add(value)
            out.append(value)
            if len(out) >= 12:
                return out
    return out


def _dominant_tags(rows: Sequence[Any]) -> list[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        counts.update(str(tag) for tag in _row_value(row, "domain_tags", ()) or ())
    return [tag for tag, _ in counts.most_common(10)]


def _last_evidence_at(rows: Sequence[Any]) -> str | None:
    dates = [
        value
        for row in rows
        if isinstance((value := _row_value(row, "created_at")), datetime)
    ]
    if not dates:
        return None
    return max(dates).isoformat()


def _canonical_label(subject_key: str, cards: Sequence[dict[str, Any]]) -> str:
    for card in cards:
        natural = str(card.get("natural") or "").strip()
        if natural:
            return natural[:180]
    return _label_from_subject(subject_key)


def _label_from_subject(subject_key: str) -> str:
    return subject_key.replace(":", " ")


def _status(config: EntityProjectionConfig, cards: Sequence[dict[str, Any]]) -> str:
    tags = set().union(*(_card_tags(card) for card in cards)) if cards else set()
    if tags.intersection({"done", "fulfilled", "resolved", "shipped"}):
        return "resolved"
    if tags.intersection({"blocked", "blocker", "churn", "risk", "slip"}):
        return "at_risk"
    if config.projection_name == "decisions" and not _owners_from_cards(cards):
        return "needs_owner"
    return "active"


def _severity(
    config: EntityProjectionConfig,
    cards: Sequence[dict[str, Any]],
    confidence: float,
) -> str:
    del config
    tags = set().union(*(_card_tags(card) for card in cards)) if cards else set()
    concern_count = sum(
        1
        for card in cards
        if str(card.get("claim_role") or "").casefold() == "concern"
    )
    if confidence >= 0.8 or tags.intersection({"blocked", "blocker", "churn"}):
        return "high"
    if confidence >= 0.65 or concern_count or tags.intersection({"risk", "slip"}):
        return "medium"
    return "low"


def _needs_review(cards: Sequence[dict[str, Any]]) -> bool:
    tags = set().union(*(_card_tags(card) for card in cards)) if cards else set()
    roles = {str(card.get("claim_role") or "").casefold() for card in cards}
    return (
        "contested" in tags
        or "contradiction" in tags
        or ("concern" in roles and "recommendation" in roles)
    )


def _owners_from_cards(cards: Sequence[dict[str, Any]]) -> list[str]:
    owners: list[str] = []
    seen: set[str] = set()
    for card in cards:
        for actor_id in card.get("scope_actor_ids") or ():
            if actor_id in seen:
                continue
            seen.add(actor_id)
            owners.append(actor_id)
    return owners


def _card_tags(card: dict[str, Any]) -> set[str]:
    return _normalized_set(card.get("domain_tags") or ())


def _normalized_set(values: Sequence[Any]) -> set[str]:
    return {str(value).strip().casefold() for value in values if str(value).strip()}


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


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, IndexError, TypeError):
        return default
