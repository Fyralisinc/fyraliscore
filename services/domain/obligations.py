"""Generic future-work obligations for the Think feedback loop."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7

from .triggers import enqueue_trigger


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, default=str)


@dataclass(frozen=True, slots=True)
class ObligationSweepReport:
    claimed: int
    fired: int
    trigger_ids: tuple[UUID, ...]


async def open_obligation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    kind: str,
    object_kind: str,
    object_id: UUID | None = None,
    due_at: datetime | None = None,
    trigger_kind: str = "T4",
    trigger_subkind: str | None = None,
    observation_id: UUID | None = None,
    model_id: UUID | None = None,
    payload: dict[str, Any] | None = None,
    max_fires: int = 1,
    obligation_id: UUID | None = None,
) -> UUID:
    """Open or return one pending obligation for an object.

    The partial unique index deduplicates open object obligations. This keeps
    model reevaluation, prediction checks, and future policy digests from
    spawning duplicate future work while a prior obligation is still pending.
    """

    new_id = obligation_id or uuid7()
    inserted = await conn.fetchval(
        """
        INSERT INTO think_obligations (
          id, tenant_id, kind, object_kind, object_id, due_at,
          trigger_kind, trigger_subkind, observation_id, model_id,
          payload, max_fires
        )
        VALUES (
          $1, $2, $3, $4, $5, COALESCE($6, now()),
          $7, $8, $9, $10, $11::jsonb, GREATEST(1, $12)
        )
        ON CONFLICT DO NOTHING
        RETURNING id
        """,
        new_id,
        tenant_id,
        kind,
        object_kind,
        object_id,
        due_at,
        trigger_kind,
        trigger_subkind,
        observation_id,
        model_id,
        _jsonb(payload),
        max_fires,
    )
    if inserted is not None:
        return inserted
    existing = await conn.fetchval(
        """
        SELECT id
        FROM think_obligations
        WHERE tenant_id = $1
          AND kind = $2
          AND object_kind = $3
          AND object_id IS NOT DISTINCT FROM $4
          AND status = 'open'
        ORDER BY due_at ASC
        LIMIT 1
        """,
        tenant_id,
        kind,
        object_kind,
        object_id,
    )
    return existing or new_id


async def open_model_reeval_obligation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    cause_kind: str,
    cause_model_id: UUID | None = None,
    due_at: datetime | None = None,
) -> UUID:
    """Compatibility obligation mirroring ``model_reeval_queue`` intent."""

    return await open_obligation(
        conn,
        tenant_id=tenant_id,
        kind="model_reeval",
        object_kind="model",
        object_id=model_id,
        due_at=due_at,
        trigger_kind="T4",
        trigger_subkind="model_reeval",
        model_id=model_id,
        payload={
            "cause_kind": cause_kind,
            "cause_model_id": str(cause_model_id) if cause_model_id else None,
            "source": "think_obligations",
        },
    )


async def sweep_due_obligations(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID | None = None,
    limit: int = 100,
) -> ObligationSweepReport:
    """Fire due open obligations into ``think_trigger_queue``."""

    if tenant_id is None:
        rows = await conn.fetch(
            """
            SELECT *
            FROM think_obligations
            WHERE status = 'open'
              AND due_at <= now()
            ORDER BY due_at ASC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            """,
            max(1, int(limit)),
        )
    else:
        rows = await conn.fetch(
            """
            SELECT *
            FROM think_obligations
            WHERE status = 'open'
              AND due_at <= now()
              AND tenant_id = $2
            ORDER BY due_at ASC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            """,
            max(1, int(limit)),
            tenant_id,
        )

    trigger_ids: list[UUID] = []
    for row in rows:
        payload = _coerce_obj(row["payload"])
        payload.update({
            "obligation_id": str(row["id"]),
            "obligation_kind": row["kind"],
            "obligation_object_kind": row["object_kind"],
            "obligation_object_id": str(row["object_id"]) if row["object_id"] else None,
        })
        trigger_id = await enqueue_trigger(
            conn,
            tenant_id=row["tenant_id"],
            trigger_kind=row["trigger_kind"],
            trigger_subkind=row["trigger_subkind"],
            observation_id=row["observation_id"],
            model_id=row["model_id"],
            payload=payload,
        )
        trigger_ids.append(trigger_id)
        status = "fired" if int(row["fires"] or 0) + 1 >= int(row["max_fires"] or 1) else "open"
        await conn.execute(
            """
            UPDATE think_obligations
            SET fires = fires + 1,
                status = $2,
                last_trigger_id = $3,
                updated_at = now(),
                completed_at = CASE WHEN $2 = 'open' THEN completed_at ELSE now() END
            WHERE id = $1
            """,
            row["id"],
            status,
            trigger_id,
        )

    return ObligationSweepReport(
        claimed=len(rows),
        fired=len(trigger_ids),
        trigger_ids=tuple(trigger_ids),
    )


def _coerce_obj(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


__all__ = [
    "ObligationSweepReport",
    "open_model_reeval_obligation",
    "open_obligation",
    "sweep_due_obligations",
]
