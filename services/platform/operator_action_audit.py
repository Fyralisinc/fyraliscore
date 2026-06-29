"""Shared operator_action_log writes for tenant-scoped admin actions."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


async def record_operator_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    action: str,
    resource_type: str,
    resource_id: UUID | None = None,
    metadata: dict[str, Any] | None = None,
) -> UUID:
    """Insert a bounded operator action audit row and return its id."""
    action_id = uuid7()
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now())
        """,
        action_id,
        tenant_id,
        actor_id,
        action,
        resource_type,
        resource_id,
        json.dumps(metadata or {}, default=str, sort_keys=True),
    )
    return action_id


__all__ = ["record_operator_action"]
