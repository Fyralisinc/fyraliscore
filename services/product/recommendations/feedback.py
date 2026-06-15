"""Recommendation feedback aggregates used by the action-list ranker."""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg

from services.domain.feedback_stats import record_feedback_stat


FeedbackAction = Literal["acted", "dismissed"]


def pattern_key_for_proposition(proposition: dict[str, Any]) -> str:
    """Stable coarse key for one recommendation pattern.

    The key intentionally ignores freeform prose and IDs that would make every
    card unique. Founder feedback should adjust the rank of similar future
    proposals, not mutate belief content.
    """
    target = proposition.get("target_act_ref") or {}
    proposed = proposition.get("proposed_change") or {}
    payload = proposed.get("payload") or {}
    scope = {
        "claim_role": proposition.get("claim_role") or "recommendation",
        "kind": proposition.get("kind") or "norm",
        "target_type": target.get("type"),
        "operation": proposed.get("operation"),
        "new_state": payload.get("new_state") or payload.get("state"),
        "payload_keys": sorted(str(k) for k in payload.keys())[:12],
        "impact_band": _impact_band(proposition.get("expected_impact")),
        "qualitative": _token_fingerprint(proposition.get("qualitative_impact")),
    }
    raw = json.dumps(scope, sort_keys=True, default=str)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"rec:{digest}"


def ranking_multiplier(
    *,
    acted_count: int = 0,
    dismissed_count: int = 0,
    last_acted_at: datetime | None = None,
    last_dismissed_at: datetime | None = None,
    now: datetime | None = None,
) -> float:
    """Bounded, decaying feedback multiplier for recommendation ranking."""
    now = now or datetime.now(timezone.utc)
    positive = _decayed_count(acted_count, last_acted_at, now=now)
    negative = _decayed_count(dismissed_count, last_dismissed_at, now=now)
    if positive == 0.0 and negative == 0.0:
        return 1.0
    raw = 1.0 + (0.18 * positive) - (0.22 * negative)
    return max(0.35, min(1.6, raw))


async def record_recommendation_feedback(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    target_actor_id: UUID,
    proposition: dict[str, Any],
    action: FeedbackAction,
    reason: str | None = None,
) -> str:
    pattern_key = pattern_key_for_proposition(proposition)
    if action == "acted":
        await conn.execute(
            """
            INSERT INTO recommendation_feedback_stats (
              tenant_id, target_actor_id, pattern_key, acted_count,
              last_acted_at, last_reason, updated_at
            )
            VALUES ($1, $2, $3, 1, now(), $4, now())
            ON CONFLICT (tenant_id, target_actor_id, pattern_key)
            DO UPDATE SET
              acted_count = recommendation_feedback_stats.acted_count + 1,
              last_acted_at = now(),
              last_reason = EXCLUDED.last_reason,
              updated_at = now()
            """,
            tenant_id,
            target_actor_id,
            pattern_key,
            reason,
        )
    else:
        await conn.execute(
            """
            INSERT INTO recommendation_feedback_stats (
              tenant_id, target_actor_id, pattern_key, dismissed_count,
              last_dismissed_at, last_reason, updated_at
            )
            VALUES ($1, $2, $3, 1, now(), $4, now())
            ON CONFLICT (tenant_id, target_actor_id, pattern_key)
            DO UPDATE SET
              dismissed_count = recommendation_feedback_stats.dismissed_count + 1,
              last_dismissed_at = now(),
              last_reason = EXCLUDED.last_reason,
              updated_at = now()
            """,
            tenant_id,
            target_actor_id,
            pattern_key,
            reason,
        )
    try:
        await record_feedback_stat(
            conn,
            tenant_id=tenant_id,
            surface="recommendation_feedback",
            op_type="recommendation",
            op_kind=action,
            outcome="success",
            reason=reason,
            payload={
                "pattern_key": pattern_key,
                "target_actor_id": str(target_actor_id),
            },
        )
    except asyncpg.PostgresError:
        pass
    return pattern_key


async def bump_supporting_model_confirmations(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    supporting_model_ids: list[UUID],
) -> int:
    if not supporting_model_ids:
        return 0
    return int(
        await conn.fetchval(
            """
            WITH updated AS (
              UPDATE models
              SET confirmed_count = confirmed_count + 1,
                  last_confirmed_at = now()
              WHERE tenant_id = $1
                AND id = ANY($2::uuid[])
                AND status = 'active'
              RETURNING 1
            )
            SELECT count(*) FROM updated
            """,
            tenant_id,
            supporting_model_ids,
        )
        or 0
    )


def _impact_band(value: Any) -> str | None:
    try:
        impact = float(value)
    except (TypeError, ValueError):
        return None
    if impact <= 0:
        return "none"
    if impact < 10_000:
        return "small"
    if impact < 100_000:
        return "medium"
    if impact < 1_000_000:
        return "large"
    return "strategic"


def _token_fingerprint(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    tokens = [
        token
        for token in value.lower().replace("_", " ").split()
        if len(token) > 3
    ]
    return " ".join(sorted(set(tokens))[:8]) or None


def _decayed_count(count: int, last_at: datetime | None, *, now: datetime) -> float:
    n = max(0, int(count or 0))
    if n == 0 or last_at is None:
        return 0.0
    if last_at.tzinfo is None:
        last_at = last_at.replace(tzinfo=timezone.utc)
    age_days = max(0.0, (now - last_at.astimezone(timezone.utc)).total_seconds() / 86_400.0)
    half_life_days = 30.0
    return float(n) * math.pow(0.5, age_days / half_life_days)


__all__ = [
    "bump_supporting_model_confirmations",
    "pattern_key_for_proposition",
    "ranking_multiplier",
    "record_recommendation_feedback",
]
