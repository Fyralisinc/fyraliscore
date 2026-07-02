"""Ephemeral temporal dynamics over existing substrate data.

No new truth table lives here. Detectors read Models, audit events,
observations, and topology events, then return compact signals that
ReasoningFrame can surface to Think. Important dynamics can later be
promoted into existing proposition kinds such as pattern,
environmental_trend, concern, or situation.
"""
from __future__ import annotations

import json
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
        signals.extend(
            await _detect_missing_transition_anomalies(
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


# =====================================================================
# Missing-transition (imaginary-node) anomaly
#
# When the substrate's invariant `event_i.new_state == event_j.previous_state`
# (for consecutive audit_events on the same Model) is violated, *something
# mutated the Model between event_i and event_j without emitting a matching
# audit_event*. That "something" is precisely the off-system event class
# the imaginary-node pattern targets: a decision, hand-off, or hallway
# correction the system never observed.
#
# The detector itself only emits the *signal*. The hypothesis imputer
# (services/reasoning/dynamics/hypothesis_imputer.py) takes the signal plus the
# enriched discontinuity payload and synthesizes a low-confidence
# `claim_role='hypothesis'` Model that a CEO can Approve / Correct /
# Other / Dismiss.
#
# Volatile fields (activation, last_retrieved_at, embedding-derived
# columns) are excluded from the diff so the detector never fires on
# pure reconsolidation activity.
# =====================================================================


_VOLATILE_AUDIT_FIELDS: frozenset[str] = frozenset(
    {
        "activation",
        "last_retrieved_at",
        "retrieval_count",
        "embedding",
        "embedding_pending",
        "updated_at",
    }
)


_REINFORCEMENT_AUDIT_FIELDS: frozenset[str] = frozenset(
    {
        "confidence",
        "confidence_at_assertion",
        "confirmed_count",
        "contested_count",
        "last_confirmed_at",
        "signal_readings",
        "reading_contestable",
        "supporting_event_ids",
        "supporting_model_ids",
        "evidential_weight",
        "contributing_models",
        "domain_tags",
        "semantic_terms",
        "open_questions",
        "activation_coefficient",
    }
)


_IDENTITY_AUDIT_FIELDS: frozenset[str] = frozenset(
    {
        "id",
        "tenant_id",
        "born_from_event_id",
        "created_at",
    }
)


_MISSING_TRANSITION_IGNORED_FIELDS: frozenset[str] = frozenset(
    {
        *_VOLATILE_AUDIT_FIELDS,
        *_REINFORCEMENT_AUDIT_FIELDS,
        *_IDENTITY_AUDIT_FIELDS,
    }
)


_FULL_SNAPSHOT_MARKER_FIELDS: frozenset[str] = frozenset(
    {
        "id",
        "tenant_id",
        "proposition",
        "natural",
        "confidence",
        "status",
    }
)


@dataclass(frozen=True)
class MissingTransitionDiscontinuity:
    """Structured payload describing one detected substrate discontinuity.

    Carries enough context for the hypothesis imputer to synthesize a
    natural-language hypothesis without re-querying the substrate.

    `differing_fields` is the symmetric-difference set of keys whose value
    changed between `prev_event_new_state` and `next_event_previous_state`
    (volatile fields excluded). It will always be non-empty for an emitted
    discontinuity — a discontinuity with no material diff is suppressed.
    """

    model_id: UUID
    prev_event_id: int | None
    next_event_id: int | None
    prev_event_occurred_at: datetime
    next_event_occurred_at: datetime
    prev_event_cause_id: UUID | None
    next_event_cause_id: UUID | None
    prev_state: dict[str, Any]
    next_state: dict[str, Any]
    differing_fields: tuple[str, ...]

    @property
    def gap(self) -> timedelta:
        return self.next_event_occurred_at - self.prev_event_occurred_at

    @property
    def gap_seconds(self) -> float:
        return self.gap.total_seconds()


def _coerce_state_jsonb(value: Any) -> dict[str, Any]:
    """audit_events.{previous,next}_state may come back as dict, bytes, or
    JSON-encoded string depending on the asyncpg codec path. Normalize."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except UnicodeDecodeError:
            return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _material_diff(
    prev_new: dict[str, Any], next_prev: dict[str, Any]
) -> tuple[str, ...]:
    """Return material fields that differ between two audit observations.

    Audit events are not all the same shape: model creation records a full
    snapshot, while most Think/applier updates record only the fields that
    operation touched. A missing transition can only be inferred across fields
    both events actually observed, unless both sides are full model snapshots.

    Support/reinforcement fields are intentionally ignored. They describe the
    evidence history of a memory, not a hidden semantic or lifecycle transition
    that T3 should ask Think to explain.
    """
    prev_keys = set(prev_new.keys())
    next_keys = set(next_prev.keys())
    if _is_full_model_snapshot(prev_new) and _is_full_model_snapshot(next_prev):
        keys = prev_keys | next_keys
    else:
        keys = prev_keys & next_keys
    differing: list[str] = []
    for key in keys:
        if key in _MISSING_TRANSITION_IGNORED_FIELDS:
            continue
        if prev_new.get(key) != next_prev.get(key):
            differing.append(key)
    return tuple(sorted(differing))


def _is_full_model_snapshot(state: dict[str, Any]) -> bool:
    """Best-effort discriminator for full Model snapshots.

    The audit table does not carry an explicit full/partial flag. These fields
    are present in normal `model_state_snapshot` output and absent from sparse
    update diffs, giving the detector enough structure to avoid comparing a
    full create snapshot against a later partial update as if missing keys were
    semantic deletes.
    """
    return _FULL_SNAPSHOT_MARKER_FIELDS.issubset(state.keys())


def _missing_transition_strength(
    differing_fields: tuple[str, ...], gap_seconds: float
) -> float:
    """Stronger signal when (a) more fields diverged, and (b) the gap is
    small enough that the missed mutation likely sat between observed
    activity. A 1-second gap with one differing field still counts —
    a deterministic write path always emits a matching audit_event."""
    base = 0.45 + 0.10 * min(5, len(differing_fields))
    # Gap penalty: huge gaps weaken the signal (the system may have
    # been unattended; later mutations are not necessarily missed
    # transitions in the imaginary-node sense). Cap at 30 days.
    gap_days = max(0.0, gap_seconds) / 86400.0
    decay = max(0.0, 1.0 - gap_days / 30.0)
    return _clamp(base * (0.5 + 0.5 * decay))


def _missing_transition_summary(
    model_id: UUID,
    differing_fields: tuple[str, ...],
    gap: timedelta,
) -> str:
    fields_str = ", ".join(differing_fields[:3])
    if len(differing_fields) > 3:
        fields_str += f", +{len(differing_fields) - 3} more"
    hours = max(0.0, gap.total_seconds() / 3600.0)
    if hours < 24:
        gap_str = f"{hours:.1f}h"
    else:
        gap_str = f"{hours / 24:.1f}d"
    return (
        f"Model {model_id} shows a state discontinuity across consecutive "
        f"audit events ({gap_str} apart, fields: {fields_str}); an "
        f"unrecorded mutation likely occurred between them."
    )


async def _detect_missing_transition_anomalies(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_ids: tuple[UUID, ...],
    since: datetime,
) -> list[DynamicSignal]:
    """Detect substrate state-jump anomalies and emit signals.

    Algorithm:
      1. Fetch material audit_events for the given Models ordered by
         (model_id, occurred_at, event_id) — event_id breaks same-instant
         ties deterministically.
      2. For each Model, walk consecutive pairs (event_i, event_j).
         The substrate invariant requires
         `event_i.new_state == event_j.previous_state` (modulo volatile
         fields). When the invariant is violated, an unrecorded mutation
         happened between the two events.
      3. Emit one DynamicSignal per discontinuity, carrying the diff +
         bracketing events so the imputer can construct a hypothesis Model
         without re-querying.

    Excluded by design:
      - `confidence_update` events (confidence-only changes are tracked
        by recurring_update; they're not state discontinuities).
      - Fields absent from the next sparse event after `create`; creation is a
        full snapshot, while most later updates are partial observations.
      - Volatile/reinforcement fields (`activation`, `confirmed_count`,
        `supporting_event_ids`, etc.) — these churn under normal
        reconsolidation and evidence attachment, and would otherwise create
        false positives.
      - Non-overlapping sparse snapshots — an absent key in a partial update is
        "not observed," not a semantic deletion.
    """
    rows = await conn.fetch(
        """
        SELECT model_id, event_id, occurred_at, cause_id, cause_type,
               previous_state, new_state
        FROM audit_events
        WHERE tenant_id = $1
          AND model_id = ANY($2::uuid[])
          AND occurred_at >= $3
          AND cause_type IN ('create', 'field_update', 'reconciliation_merge')
        ORDER BY model_id, occurred_at, event_id
        """,
        tenant_id,
        list(model_ids),
        since,
    )
    if not rows:
        return []

    per_model: dict[UUID, list[asyncpg.Record]] = {}
    for row in rows:
        per_model.setdefault(row["model_id"], []).append(row)

    out: list[DynamicSignal] = []
    for model_id, events in per_model.items():
        if len(events) < 2:
            continue
        for i in range(len(events) - 1):
            prev = events[i]
            curr = events[i + 1]
            prev_new = _coerce_state_jsonb(prev["new_state"])
            curr_prev = _coerce_state_jsonb(curr["previous_state"])
            # `create` events have NULL previous_state but their new_state
            # bootstraps the chain. We compare against the *next* event's
            # previous_state. If the next event is also a `create` (which
            # shouldn't happen on the same model_id but the substrate
            # allows it via reconciliation_merge), we skip rather than
            # raise a false positive.
            if not prev_new and not curr_prev:
                continue
            differing = _material_diff(prev_new, curr_prev)
            if not differing:
                continue
            gap = curr["occurred_at"] - prev["occurred_at"]
            evidence: list[UUID] = []
            if prev["cause_id"] is not None:
                evidence.append(prev["cause_id"])
            if curr["cause_id"] is not None:
                evidence.append(curr["cause_id"])
            out.append(
                DynamicSignal(
                    dynamic_kind="missing_transition",
                    summary=_missing_transition_summary(
                        model_id, differing, gap
                    ),
                    strength=_missing_transition_strength(
                        differing, gap.total_seconds()
                    ),
                    confidence=0.55,
                    subject_model_ids=(model_id,),
                    evidence_event_ids=tuple(evidence),
                )
            )
    return out


async def fetch_missing_transition_discontinuity(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    since: datetime,
) -> MissingTransitionDiscontinuity | None:
    """Re-derive the most recent discontinuity for one Model and return
    the enriched payload the imputer needs.

    Why a separate function: detect_dynamic_signals returns the lightweight
    DynamicSignal envelope (UUIDs + summary + strength), but the imputer
    needs the actual bracketing JSONB snapshots. Rather than fattening the
    DynamicSignal dataclass with raw audit state (which would leak through
    every downstream consumer), the imputer calls this targeted query.

    Returns None when the discontinuity has been resolved (e.g., a later
    audit_event filled the gap) or when the Model no longer has at least
    two material audit events in the window.
    """
    rows = await conn.fetch(
        """
        SELECT event_id, occurred_at, cause_id, cause_type,
               previous_state, new_state
        FROM audit_events
        WHERE tenant_id = $1
          AND model_id = $2
          AND occurred_at >= $3
          AND cause_type IN ('create', 'field_update', 'reconciliation_merge')
        ORDER BY occurred_at DESC, event_id DESC
        LIMIT 16
        """,
        tenant_id,
        model_id,
        since,
    )
    if len(rows) < 2:
        return None

    # rows are DESC; walk pairs (newer=rows[i], older=rows[i+1]) to find
    # the most-recent discontinuity. This makes "what does the imputer
    # explain?" deterministic: always the latest unrecorded mutation.
    for i in range(len(rows) - 1):
        newer = rows[i]
        older = rows[i + 1]
        older_new = _coerce_state_jsonb(older["new_state"])
        newer_prev = _coerce_state_jsonb(newer["previous_state"])
        if not older_new and not newer_prev:
            continue
        differing = _material_diff(older_new, newer_prev)
        if not differing:
            continue
        return MissingTransitionDiscontinuity(
            model_id=model_id,
            prev_event_id=older["event_id"],
            next_event_id=newer["event_id"],
            prev_event_occurred_at=older["occurred_at"],
            next_event_occurred_at=newer["occurred_at"],
            prev_event_cause_id=older["cause_id"],
            next_event_cause_id=newer["cause_id"],
            prev_state=older_new,
            next_state=newer_prev,
            differing_fields=differing,
        )
    return None


__all__ = [
    "DynamicSignal",
    "MissingTransitionDiscontinuity",
    "detect_dynamic_signals",
    "fetch_missing_transition_discontinuity",
]
