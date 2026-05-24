"""Ephemeral temporal dynamics over existing substrate data.

No new truth table lives here. Detectors read Models, audit events,
observations, and topology events, then return compact signals that
ReasoningFrame can surface to Think. Important dynamics can later be
promoted into existing proposition kinds such as pattern,
environmental_trend, concern, or situation.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class DynamicSignal:
    dynamic_kind: str
    summary: str
    strength: float
    confidence: float
    subject_model_ids: tuple[UUID, ...] = ()
    subject_actor_ids: tuple[UUID, ...] = ()
    evidence_event_ids: tuple[UUID, ...] = ()
    evidence_topology_event_ids: tuple[UUID, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "dynamic_kind": self.dynamic_kind,
            "summary": self.summary,
            "strength": _clamp(self.strength),
            "confidence": _clamp(self.confidence),
            "subject_model_ids": [str(v) for v in self.subject_model_ids],
            "subject_actor_ids": [str(v) for v in self.subject_actor_ids],
            "evidence_event_ids": [str(v) for v in self.evidence_event_ids],
            "evidence_topology_event_ids": [
                str(v) for v in self.evidence_topology_event_ids
            ],
        }


async def detect_dynamic_signals(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: Iterable[UUID] = (),
    actor_ids: Iterable[UUID] = (),
    reference_time: datetime | None = None,
    audit_window: timedelta = timedelta(days=30),
    observation_window: timedelta = timedelta(days=7),
    limit: int = 8,
) -> list[DynamicSignal]:
    ref = reference_time or datetime.now(timezone.utc)
    model_tuple = tuple(dict.fromkeys(model_ids))
    actor_tuple = tuple(dict.fromkeys(actor_ids))

    signals: list[DynamicSignal] = []
    if model_tuple:
        signals.extend(
            await _detect_audit_dynamics(
                conn,
                tenant_id=tenant_id,
                model_ids=model_tuple,
                since=ref - audit_window,
            )
        )
        signals.extend(
            await _detect_stale_model_dynamics(
                conn,
                tenant_id=tenant_id,
                model_ids=model_tuple,
                reference_time=ref,
            )
        )
        signals.extend(
            await _detect_topology_dynamics(
                conn,
                tenant_id=tenant_id,
                model_ids=model_tuple,
                since=ref - audit_window,
            )
        )
    if actor_tuple:
        signals.extend(
            await _detect_actor_activity_dynamics(
                conn,
                tenant_id=tenant_id,
                actor_ids=actor_tuple,
                since=ref - observation_window,
                reference_time=ref,
            )
        )
    ordered = sorted(
        signals,
        key=lambda s: (-_clamp(s.strength), -_clamp(s.confidence), s.summary),
    )
    return ordered[: max(1, int(limit))]


async def _detect_audit_dynamics(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: tuple[UUID, ...],
    since: datetime,
) -> list[DynamicSignal]:
    rows = await conn.fetch(
        """
        SELECT model_id,
               count(*) AS event_count,
               count(*) FILTER (WHERE re_asserts_event_id IS NOT NULL)
                 AS reassert_count,
               count(*) FILTER (WHERE cause_type = 'confidence_update')
                 AS confidence_updates,
               array_agg(cause_id) FILTER (WHERE cause_id IS NOT NULL)
                 AS cause_ids
        FROM audit_events
        WHERE tenant_id = $1
          AND model_id = ANY($2::uuid[])
          AND occurred_at >= $3
        GROUP BY model_id
        """,
        tenant_id,
        list(model_ids),
        since,
    )
    out: list[DynamicSignal] = []
    for row in rows:
        model_id = row["model_id"]
        reassert_count = int(row["reassert_count"] or 0)
        confidence_updates = int(row["confidence_updates"] or 0)
        event_count = int(row["event_count"] or 0)
        evidence = tuple(row["cause_ids"] or ())
        if reassert_count > 0:
            out.append(
                DynamicSignal(
                    dynamic_kind="oscillating",
                    summary=(
                        f"Model {model_id} has re-asserted a prior state "
                        f"{reassert_count} time(s) in the recent audit chain."
                    ),
                    strength=min(1.0, 0.55 + 0.15 * reassert_count),
                    confidence=0.75,
                    subject_model_ids=(model_id,),
                    evidence_event_ids=evidence,
                )
            )
        elif confidence_updates >= 2 or event_count >= 4:
            out.append(
                DynamicSignal(
                    dynamic_kind="recurring_update",
                    summary=(
                        f"Model {model_id} has repeated audit movement "
                        f"({event_count} events, {confidence_updates} confidence updates)."
                    ),
                    strength=min(1.0, 0.35 + 0.08 * event_count),
                    confidence=0.65,
                    subject_model_ids=(model_id,),
                    evidence_event_ids=evidence,
                )
            )
    return out


async def _detect_stale_model_dynamics(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: tuple[UUID, ...],
    reference_time: datetime,
) -> list[DynamicSignal]:
    rows = await conn.fetch(
        """
        SELECT id, "natural", activation, last_retrieved_at, created_at
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
          AND status = 'active'
        """,
        tenant_id,
        list(model_ids),
    )
    out: list[DynamicSignal] = []
    for row in rows:
        last_seen = row["last_retrieved_at"] or row["created_at"]
        age_days = (reference_time - last_seen).total_seconds() / 86400.0
        activation = float(row["activation"] or 0.0)
        if age_days < 30 or activation >= 0.12:
            continue
        out.append(
            DynamicSignal(
                dynamic_kind="stale",
                summary=(
                    f"Model {row['id']} has low activation ({activation:.2f}) "
                    f"and has not been retrieved for {age_days:.0f} days."
                ),
                strength=min(1.0, 0.35 + age_days / 120.0),
                confidence=0.7,
                subject_model_ids=(row["id"],),
            )
        )
    return out


async def _detect_topology_dynamics(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: tuple[UUID, ...],
    since: datetime,
) -> list[DynamicSignal]:
    rows = await conn.fetch(
        """
        SELECT id, kind, member_model_ids, magnitude, named_signature
        FROM topology_events
        WHERE tenant_id = $1
          AND occurred_at >= $2
          AND member_model_ids && $3::uuid[]
        ORDER BY occurred_at DESC
        LIMIT 5
        """,
        tenant_id,
        since,
        list(model_ids),
    )
    out: list[DynamicSignal] = []
    for row in rows:
        magnitude = row["magnitude"]
        strength = float(magnitude) if magnitude is not None else 0.5
        members = tuple(row["member_model_ids"] or ())
        out.append(
            DynamicSignal(
                dynamic_kind="phase_shift",
                summary=(
                    f"Topology {row['kind']} event touched selected memory"
                    + (
                        f" ({row['named_signature']})."
                        if row["named_signature"]
                        else "."
                    )
                ),
                strength=strength,
                confidence=0.65,
                subject_model_ids=members,
                evidence_topology_event_ids=(row["id"],),
            )
        )
    return out


async def _detect_actor_activity_dynamics(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_ids: tuple[UUID, ...],
    since: datetime,
    reference_time: datetime,
) -> list[DynamicSignal]:
    rows = await conn.fetch(
        """
        SELECT actor_id, count(*) AS n
        FROM observations
        WHERE tenant_id = $1
          AND actor_id = ANY($2::uuid[])
          AND occurred_at >= $3
          AND occurred_at <= $4
          AND kind != 'state_change'
        GROUP BY actor_id
        """,
        tenant_id,
        list(actor_ids),
        since,
        reference_time,
    )
    out: list[DynamicSignal] = []
    for row in rows:
        count = int(row["n"] or 0)
        if count < 5:
            continue
        out.append(
            DynamicSignal(
                dynamic_kind="high_activity",
                summary=(
                    f"Actor {row['actor_id']} has {count} recent "
                    "non-state-change observations in the reasoning window."
                ),
                strength=min(1.0, 0.35 + count / 20.0),
                confidence=0.6,
                subject_actor_ids=(row["actor_id"],),
            )
        )
    return out


def _clamp(v: float) -> float:
    return max(0.0, min(1.0, float(v)))


__all__ = ["DynamicSignal", "detect_dynamic_signals"]
