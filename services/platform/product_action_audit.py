"""Tenant-scoped audit writes for user-facing product actions."""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


_MAX_METADATA_KEYS = 24
_MAX_METADATA_STRING_CHARS = 160
_MAX_METADATA_LIST_ITEMS = 16
_MAX_METADATA_DEPTH = 3


async def record_product_action(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    actor_id: UUID,
    action: str,
    resource_type: str,
    resource_id: UUID | None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Append a product action audit row.

    Callers should pass only bounded operational metadata, never raw source
    payloads, prompts, user notes, or free-form customer text. This helper
    bounds the JSON shape and sets `app.current_tenant` for strict-RLS tables;
    it is intended to run inside the caller's mutation transaction.
    """
    await conn.execute(
        "SELECT set_config('app.current_tenant', $1::text, true)",
        str(tenant_id),
    )
    await conn.execute(
        """
        INSERT INTO product_action_audit_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now())
        """,
        uuid7(),
        tenant_id,
        actor_id,
        action,
        resource_type,
        resource_id,
        json.dumps(_bounded_metadata(metadata or {}), default=str, sort_keys=True),
    )


def _bounded_metadata(value: Any, *, depth: int = 0) -> Any:
    if depth > _MAX_METADATA_DEPTH:
        return "<truncated>"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, str):
        compact = " ".join(value.split())
        if len(compact) > _MAX_METADATA_STRING_CHARS:
            return compact[:_MAX_METADATA_STRING_CHARS] + "..."
        return compact
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in list(value.items())[:_MAX_METADATA_KEYS]:
            key_text = str(key)[:_MAX_METADATA_STRING_CHARS]
            out[key_text] = _bounded_metadata(item, depth=depth + 1)
        return out
    if isinstance(value, (list, tuple, set)):
        return [
            _bounded_metadata(item, depth=depth + 1)
            for item in list(value)[:_MAX_METADATA_LIST_ITEMS]
        ]
    return str(value)[:_MAX_METADATA_STRING_CHARS]


__all__ = ["record_product_action"]
