"""Shared helpers for durable Think trigger enqueueing.

The learning-loop architecture wants a single choke point for future-work
creation. This module keeps the current ``think_trigger_queue`` contract intact
while removing copy-pasted INSERT statements from producers.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7

_log = structlog.get_logger(__name__)


def _jsonb(value: Any) -> str:
    return json.dumps(value if value is not None else {}, default=str)


def _trigger_lock_key(*parts: object) -> int:
    digest = hashlib.sha256("\0".join(map(str, parts)).encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF


async def enqueue_trigger(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    trigger_kind: str,
    trigger_subkind: str | None = None,
    observation_id: UUID | None = None,
    model_id: UUID | None = None,
    payload: dict[str, Any] | None = None,
    scheduled_for: datetime | None = None,
    locked_by: str | None = None,
    trigger_id: UUID | None = None,
) -> UUID:
    """Insert one row into ``think_trigger_queue`` and return its id."""

    new_id = trigger_id or uuid7()
    if locked_by is not None:
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
                id, tenant_id, trigger_kind, trigger_subkind,
                observation_id, model_id, payload, scheduled_for,
                locked_by, locked_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::jsonb,
                COALESCE($8, now()), $9, now()
            )
            """,
            new_id,
            tenant_id,
            trigger_kind,
            trigger_subkind,
            observation_id,
            model_id,
            _jsonb(payload),
            scheduled_for,
            locked_by,
        )
    elif scheduled_for is None:
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
                id, tenant_id, trigger_kind, trigger_subkind,
                observation_id, model_id, payload
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
            """,
            new_id,
            tenant_id,
            trigger_kind,
            trigger_subkind,
            observation_id,
            model_id,
            _jsonb(payload),
        )
    else:
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
                id, tenant_id, trigger_kind, trigger_subkind,
                observation_id, model_id, payload, scheduled_for
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            """,
            new_id,
            tenant_id,
            trigger_kind,
            trigger_subkind,
            observation_id,
            model_id,
            _jsonb(payload),
            scheduled_for,
        )
    return new_id


async def ensure_event_arrival_trigger(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    observation_id: UUID,
    payload: dict[str, Any] | None = None,
    scheduled_for: datetime | None = None,
    trigger_id: UUID | None = None,
) -> UUID:
    """Return the idempotent T1:event_arrival trigger for an observation.

    This is the producer-side metabolism invariant: once an observation exists,
    duplicate ingest may reuse the observation row, but it must not silently skip
    the Think trigger that metabolizes the signal.
    """

    lock_key = _trigger_lock_key(tenant_id, observation_id, "T1", "event_arrival")
    await conn.execute("SELECT pg_advisory_lock($1)", lock_key)
    try:
        existing = await conn.fetchval(
            """
            SELECT id
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND observation_id = $2
              AND trigger_kind = 'T1'
              AND trigger_subkind = 'event_arrival'
            ORDER BY
              CASE WHEN completed_at IS NULL THEN 0 ELSE 1 END,
              enqueued_at ASC
            LIMIT 1
            """,
            tenant_id,
            observation_id,
        )
        if existing is not None:
            return existing
        return await enqueue_trigger(
            conn,
            tenant_id=tenant_id,
            trigger_kind="T1",
            trigger_subkind="event_arrival",
            observation_id=observation_id,
            payload=payload,
            scheduled_for=scheduled_for,
            trigger_id=trigger_id,
        )
    finally:
        await conn.execute("SELECT pg_advisory_unlock($1)", lock_key)


async def enqueue_model_reeval(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    cause_kind: str,
    cause_model_id: UUID | None = None,
    row_id: UUID | None = None,
) -> UUID:
    """Compatibility wrapper for the legacy ``model_reeval_queue``.

    New obligation plumbing will eventually replace this table. Until then,
    callers use this helper instead of raw INSERTs so the migration has one
    compatibility surface.
    """

    new_id = row_id or uuid7()
    inserted = await conn.fetchval(
        """
        INSERT INTO model_reeval_queue (
            id, tenant_id, model_id, cause_model_id, cause_kind
        ) VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT ON CONSTRAINT model_reeval_queue_dedup DO NOTHING
        RETURNING id
        """,
        new_id,
        tenant_id,
        model_id,
        cause_model_id,
        cause_kind,
    )
    if inserted is not None:
        await _mirror_model_reeval_obligation(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            cause_kind=cause_kind,
            cause_model_id=cause_model_id,
        )
        return inserted
    existing = await conn.fetchval(
        """
        SELECT id
        FROM model_reeval_queue
        WHERE tenant_id = $1
          AND model_id = $2
          AND cause_model_id IS NOT DISTINCT FROM $3
          AND processed_at IS NULL
        ORDER BY enqueued_at ASC
        LIMIT 1
        """,
        tenant_id,
        model_id,
        cause_model_id,
    )
    if existing is not None:
        await _mirror_model_reeval_obligation(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            cause_kind=cause_kind,
            cause_model_id=cause_model_id,
        )
        return existing
    return new_id


async def _mirror_model_reeval_obligation(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    model_id: UUID,
    cause_kind: str,
    cause_model_id: UUID | None,
) -> None:
    try:
        from .obligations import open_model_reeval_obligation

        await open_model_reeval_obligation(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            cause_kind=cause_kind,
            cause_model_id=cause_model_id,
        )
    except asyncpg.PostgresError as exc:
        _log.warning(
            "model_reeval_obligation_mirror_failed",
            tenant_id=str(tenant_id),
            model_id=str(model_id),
            cause_kind=cause_kind,
            error=str(exc),
        )


__all__ = ["enqueue_model_reeval", "enqueue_trigger", "ensure_event_arrival_trigger"]
