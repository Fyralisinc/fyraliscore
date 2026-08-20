"""Outcome-oracle contracts for authoritative feedback.

This module gives product and ingestion surfaces a small shared shape for
"reality pushed back" events. The first producer is Today human corrections,
but the same contract can carry deploy outcomes, churn/renewal facts, SLA
breaches, and closed commitments without coupling each source directly to
Think internals.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from uuid import UUID

import asyncpg

from services.domain.triggers import enqueue_trigger

AUTHORITATIVE_TRUST_TIER = "authoritative"
ORACLE_REPAIR_TRIGGER_KIND = "T4"
ORACLE_REPAIR_TRIGGER_SUBKIND = "representation_repair"


@dataclass(frozen=True, slots=True)
class OutcomeFact:
    """A normalized authoritative outcome fact from an external truth surface."""

    tenant_id: UUID
    fact_kind: str
    subject_type: str
    subject_id: str
    outcome_type: str
    outcome_value: Any
    source: str
    trust_tier: str = AUTHORITATIVE_TRUST_TIER
    source_channel: str | None = None
    actor_id: UUID | None = None
    evidence_observation_id: UUID | None = None
    occurred_at: datetime | None = None
    payload: Mapping[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "tenant_id": str(self.tenant_id),
            "fact_kind": self.fact_kind,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "outcome_type": self.outcome_type,
            "outcome_value": _json_ready(self.outcome_value),
            "source": self.source,
            "trust_tier": self.trust_tier,
        }
        optional = {
            "source_channel": self.source_channel,
            "actor_id": str(self.actor_id) if self.actor_id else None,
            "evidence_observation_id": (
                str(self.evidence_observation_id)
                if self.evidence_observation_id
                else None
            ),
            "occurred_at": _iso(self.occurred_at),
            "payload": _json_ready(dict(self.payload)) if self.payload else None,
        }
        out.update({k: v for k, v in optional.items() if v is not None})
        return out


@dataclass(frozen=True, slots=True)
class RepairEnqueueResult:
    trigger_id: UUID
    repair_key: str
    deduped: bool


def human_correction_outcome_fact(
    *,
    tenant_id: UUID,
    delta_id: UUID,
    actor_id: UUID | None,
    correction_type: str,
    explanation: str,
    supporting_link: str | None = None,
    apply_to_related: bool = False,
    occurred_at: datetime | None = None,
    main_assertion: str | None = None,
    target_node_kind: str | None = None,
    target_node_id: UUID | str | None = None,
    evidence: Iterable[Any] | None = None,
) -> OutcomeFact:
    """Create the oracle fact emitted by Today correction submissions."""

    clean_explanation = _clean(explanation)
    payload: dict[str, Any] = {
        "correction": {
            "type": _clean(correction_type),
            "explanation": clean_explanation,
            "supporting_link": _clean(supporting_link),
            "apply_to_related": bool(apply_to_related),
        },
        "decision_delta": {
            "id": str(delta_id),
            "main_assertion": _clean(main_assertion),
            "target_node_kind": _clean(target_node_kind),
            "target_node_id": str(target_node_id) if target_node_id else None,
        },
        "evidence": _evidence_payload(evidence),
    }
    payload = _drop_none(payload)
    return OutcomeFact(
        tenant_id=tenant_id,
        fact_kind="human_correction",
        subject_type="decision_delta",
        subject_id=str(delta_id),
        outcome_type="correction_submitted",
        outcome_value=_clean(correction_type),
        source="today_delta_correction",
        source_channel="today",
        trust_tier=AUTHORITATIVE_TRUST_TIER,
        actor_id=actor_id,
        occurred_at=occurred_at or datetime.now(timezone.utc),
        payload=payload,
    )


def representation_repair_payload_for_outcome(
    fact: OutcomeFact,
    *,
    repair_intent: str | None = None,
    audit_warning_code: str | None = None,
) -> dict[str, Any]:
    """Convert an outcome fact into a Think representation-repair payload."""

    intent = repair_intent or _repair_intent_for_fact(fact)
    warning_code = audit_warning_code or _warning_code_for_fact(fact)
    repair_key = _repair_key(fact)
    payload: dict[str, Any] = {
        "repair_key": repair_key,
        "repair_intent": intent,
        "audit_warning_code": warning_code,
        "source_system": fact.source,
        "source_channel": fact.source_channel,
        "source_subject_type": fact.subject_type,
        "source_subject_id": fact.subject_id,
        "oracle_outcome_fact": fact.to_payload(),
        "seed_natural_text": _seed_text_for_fact(fact),
        "seed_occurred_at": _iso(fact.occurred_at),
        "seed_entity_ids": [
            {"type": fact.subject_type, "id": fact.subject_id},
        ],
    }
    if fact.subject_type == "decision_delta":
        payload["source_delta_id"] = fact.subject_id
    if fact.actor_id:
        payload["scope_actors"] = [str(fact.actor_id)]
    return _drop_none(payload)


async def enqueue_outcome_representation_repair(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    fact: OutcomeFact,
    payload: dict[str, Any] | None = None,
    observation_id: UUID | None = None,
    model_id: UUID | None = None,
) -> RepairEnqueueResult:
    """Enqueue a deduped T4 representation repair for an oracle outcome fact."""

    repair_payload = payload or representation_repair_payload_for_outcome(fact)
    repair_key = str(repair_payload["repair_key"])
    existing_id = await conn.fetchval(
        """
        SELECT id
        FROM think_trigger_queue
        WHERE tenant_id = $1
          AND trigger_kind = 'T4'
          AND trigger_subkind = 'representation_repair'
          AND completed_at IS NULL
          AND payload->>'repair_key' = $2
        LIMIT 1
        """,
        tenant_id,
        repair_key,
    )
    if existing_id is not None:
        return RepairEnqueueResult(
            trigger_id=existing_id,
            repair_key=repair_key,
            deduped=True,
        )

    trigger_id = await enqueue_trigger(
        conn,
        tenant_id=tenant_id,
        trigger_kind=ORACLE_REPAIR_TRIGGER_KIND,
        trigger_subkind=ORACLE_REPAIR_TRIGGER_SUBKIND,
        observation_id=observation_id,
        model_id=model_id,
        payload=repair_payload,
    )
    return RepairEnqueueResult(
        trigger_id=trigger_id,
        repair_key=repair_key,
        deduped=False,
    )


def _repair_key(fact: OutcomeFact) -> str:
    value = str(fact.outcome_value or "unknown").strip().lower()
    return (
        f"oracle:{fact.fact_kind}:{fact.subject_type}:"
        f"{fact.subject_id}:{fact.outcome_type}:{value}"
    )


def _repair_intent_for_fact(fact: OutcomeFact) -> str:
    if fact.fact_kind == "human_correction":
        return "apply_human_correction"
    return "apply_authoritative_outcome"


def _warning_code_for_fact(fact: OutcomeFact) -> str:
    if fact.fact_kind == "human_correction":
        return "human_correction_submitted"
    return "authoritative_outcome_feedback"


def _seed_text_for_fact(fact: OutcomeFact) -> str:
    outcome = f"{fact.outcome_type}={fact.outcome_value}"
    parts = [
        f"Authoritative outcome fact from {fact.source}: {outcome}.",
        f"Subject: {fact.subject_type} {fact.subject_id}.",
    ]
    nested = fact.payload or {}
    correction = nested.get("correction") if isinstance(nested, Mapping) else None
    delta = nested.get("decision_delta") if isinstance(nested, Mapping) else None
    if isinstance(delta, Mapping):
        assertion = _clean(delta.get("main_assertion"))
        if assertion:
            parts.append(f"Original assertion: {_truncate(assertion, 260)}")
    if isinstance(correction, Mapping):
        explanation = _clean(correction.get("explanation"))
        ctype = _clean(correction.get("type"))
        if ctype:
            parts.append(f"Correction type: {ctype}.")
        if explanation:
            parts.append(f"Human explanation: {_truncate(explanation, 420)}")
    return " ".join(parts)


def _evidence_payload(evidence: Iterable[Any] | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for item in evidence or []:
        value = _as_mapping(item)
        if value is None:
            continue
        out.append(
            _drop_none(
                {
                    "id": _field(value, "id"),
                    "source": _field(value, "source"),
                    "title": _field(value, "title"),
                    "trust_tier": _field(value, "trust_tier"),
                    "excerpt": _field(value, "excerpt"),
                    "weight": _field(value, "weight"),
                    "ts": _field(value, "ts"),
                }
            )
        )
    return out[:8]


def _as_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if any(hasattr(value, key) for key in ("id", "source", "title")):
        return {
            "id": getattr(value, "id", None),
            "source": getattr(value, "source", None),
            "title": getattr(value, "title", None),
            "trust_tier": getattr(value, "trust_tier", None),
            "excerpt": getattr(value, "excerpt", None),
            "weight": getattr(value, "weight", None),
            "ts": getattr(value, "ts", None),
        }
    return None


def _field(value: Mapping[str, Any], key: str) -> Any:
    return _json_ready(value.get(key))


def _clean(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _json_ready(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return _iso(value)
    if isinstance(value, Mapping):
        return _drop_none({str(k): _json_ready(v) for k, v in value.items()})
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    return value


def _drop_none(value: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, raw in value.items():
        if raw is None:
            continue
        if isinstance(raw, Mapping):
            nested = _drop_none(raw)
            if nested:
                out[key] = nested
            continue
        if isinstance(raw, list):
            compact = [
                _drop_none(item) if isinstance(item, Mapping) else item
                for item in raw
                if item is not None
            ]
            if compact:
                out[key] = compact
            continue
        out[key] = raw
    return out


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "..."


__all__ = [
    "AUTHORITATIVE_TRUST_TIER",
    "OutcomeFact",
    "RepairEnqueueResult",
    "enqueue_outcome_representation_repair",
    "human_correction_outcome_fact",
    "representation_repair_payload_for_outcome",
]
