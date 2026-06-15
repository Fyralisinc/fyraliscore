"""Durable aggregate feedback counters for learning-loop surfaces."""
from __future__ import annotations

import json
from typing import Any, Literal
from uuid import UUID

import asyncpg


FeedbackOutcome = Literal["attempt", "success", "dropped", "failure"]

_COUNT_COLUMN: dict[FeedbackOutcome, str] = {
    "attempt": "attempt_count",
    "success": "success_count",
    "dropped": "dropped_count",
    "failure": "failure_count",
}


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, default=str)


async def record_feedback_stat(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    surface: str,
    op_type: str,
    op_kind: str,
    outcome: FeedbackOutcome,
    reason: str | None = None,
    payload: dict[str, Any] | None = None,
    count: int = 1,
) -> None:
    """Increment a bounded aggregate feedback counter.

    ``surface`` names the subsystem emitting feedback, ``op_type`` and
    ``op_kind`` describe the operation, and ``reason`` gives the coarse
    learning label. The helper intentionally stores only aggregate counts plus
    the latest compact payload so it can be called on hot paths.
    """

    n = max(1, int(count or 1))
    column = _COUNT_COLUMN[outcome]
    await conn.execute(
        f"""
        INSERT INTO think_feedback_stats (
          tenant_id, surface, op_type, op_kind, reason,
          {column}, last_payload, first_seen_at, last_seen_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now(), now())
        ON CONFLICT (tenant_id, surface, op_type, op_kind, reason)
        DO UPDATE SET
          {column} = think_feedback_stats.{column} + EXCLUDED.{column},
          last_payload = EXCLUDED.last_payload,
          last_seen_at = now()
        """,
        tenant_id,
        surface.strip()[:80],
        op_type.strip()[:80],
        op_kind.strip()[:80],
        (reason or "").strip()[:160],
        n,
        _jsonb(payload),
    )


__all__ = ["FeedbackOutcome", "record_feedback_stat"]
