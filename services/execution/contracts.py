"""Contracts for the Fyralis execution routing gate."""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from lib.shared.ids import uuid7


SignalRoute = Literal[
    "IGNORE_OR_ARCHIVE",
    "DETERMINISTIC_UPDATE",
    "FAST_PATH",
    "DEEP_INQUIRY_PATH",
    "BACKGROUND_PATH",
    "HUMAN_VALIDATION_PATH",
]

DecisionStatus = Literal["shadow", "enforced", "skipped", "failed"]

SignalRefType = Literal[
    "observation",
    "query",
    "scheduled_job",
    "anomaly",
    "internal",
]


@dataclass(slots=True, frozen=True)
class SignalEnvelope:
    """Normalized signal shape consumed by the routing gate.

    This deliberately mirrors the proposal's Signal Intake object while
    staying close to today's `observations` row. Later intake paths for
    queries, scheduled jobs, and worker events can construct the same
    envelope without going through ingestion.
    """

    tenant_id: UUID
    signal_ref_type: SignalRefType
    signal_id: UUID | None = None
    source_channel: str | None = None
    occurred_at: datetime | None = None
    author: UUID | None = None
    summary: str = ""
    raw_content_ref: str | None = None
    trust_tier: str | None = None
    explicit_entities: tuple[dict[str, Any], ...] = ()
    trigger_type: str | None = None
    observation_kind: str | None = None
    signal_type: str | None = None
    content: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_observation(
        cls,
        observation: Any,
        *,
        signal_type: str | None = None,
        trigger_type: str = "T1_EVENT",
    ) -> "SignalEnvelope":
        obs_id = getattr(observation, "id", None)
        return cls(
            tenant_id=getattr(observation, "tenant_id"),
            signal_ref_type="observation",
            signal_id=obs_id,
            source_channel=getattr(observation, "source_channel", None),
            occurred_at=getattr(observation, "occurred_at", None),
            author=getattr(observation, "actor_id", None),
            summary=(getattr(observation, "content_text", None) or "")[:2000],
            raw_content_ref=f"observation:{obs_id}" if obs_id else None,
            trust_tier=getattr(observation, "trust_tier", None),
            explicit_entities=tuple(
                e for e in (getattr(observation, "entities_mentioned", None) or [])
                if isinstance(e, dict)
            ),
            trigger_type=trigger_type,
            observation_kind=getattr(observation, "kind", None),
            signal_type=signal_type,
            content=dict(getattr(observation, "content", None) or {}),
        )


@dataclass(slots=True, frozen=True)
class RoutingDecision:
    """Auditable output of the routing gate."""

    tenant_id: UUID
    signal_ref_type: SignalRefType
    route: SignalRoute
    score: float
    score_breakdown: dict[str, float]
    estimated_cost: dict[str, Any]
    reason: str
    id: UUID = field(default_factory=uuid7)
    signal_ref_id: UUID | None = None
    decision_status: DecisionStatus = "shadow"
    risk_level: str | None = None
    sensitivity: str | None = None
    enqueued_trigger_id: UUID | None = None

    def with_status(
        self,
        status: DecisionStatus,
        *,
        enqueued_trigger_id: UUID | None = None,
    ) -> "RoutingDecision":
        return replace(
            self,
            decision_status=status,
            enqueued_trigger_id=enqueued_trigger_id,
        )
