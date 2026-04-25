"""
services/access_control/audit.py — access_override_log writes.

Every time `can_read` grants access via an override path (admin,
leadership, first-person), Gateway and Retrieval callers are expected
to append a row here. Writes are best-effort; structlog records any
failure but never aborts the request.
"""
from __future__ import annotations

import logging
from typing import Literal
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


log = logging.getLogger(__name__)


OverrideKind = Literal["admin", "first_person", "leadership", "system"]


async def record_override(
    actor_id: UUID,
    entity_type: str,
    entity_id: UUID | None,
    override_kind: OverrideKind,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    reason: str | None = None,
) -> UUID | None:
    """Insert a row into access_override_log. Returns the new id on
    success, None on failure (logged but not raised).
    """
    new_id = uuid7()
    try:
        await conn.execute(
            """
            INSERT INTO access_override_log (
                id, tenant_id, actor_id, entity_type, entity_id,
                override_kind, reason, occurred_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, now())
            """,
            new_id, tenant_id, actor_id, entity_type, entity_id,
            override_kind, reason,
        )
        return new_id
    except Exception as e:
        log.warning(
            "access_override_log insert failed",
            extra={
                "actor_id": str(actor_id),
                "entity_type": entity_type,
                "entity_id": str(entity_id) if entity_id else None,
                "override_kind": override_kind,
                "error": str(e),
            },
        )
        return None


__all__ = ["OverrideKind", "record_override"]
