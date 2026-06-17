"""services/platform/extensions/audit.py — extension read/write audit log (E3.4).

Append-only record of what each extension did, per tenant. Best-effort: an audit
failure must never break the request (it's observability, not a gate), so callers
fire-and-forget and swallow errors.
"""
from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from lib.shared.ids import uuid7

log = logging.getLogger("extensions.audit")


class AuditLog:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def record(
        self, *, extension_id: str, action: str, tenant_id: UUID | None = None,
        item_count: int = 0, detail: dict[str, Any] | None = None,
    ) -> None:
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO extension_audit_log "
                    "(id, extension_id, tenant_id, action, detail, item_count) "
                    "VALUES ($1,$2,$3,$4,$5::jsonb,$6)",
                    uuid7(), extension_id, tenant_id, action,
                    json.dumps(detail or {}), item_count,
                )
        except Exception:  # noqa: BLE001 — audit is best-effort, never blocks
            log.warning("extension_audit_failed ext=%s action=%s", extension_id, action, exc_info=True)

    async def recent(
        self, *, extension_id: str | None = None, tenant_id: UUID | None = None, limit: int = 100,
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT extension_id, tenant_id, action, detail, item_count, at "
                "FROM extension_audit_log "
                "WHERE ($1::text IS NULL OR extension_id=$1) "
                "  AND ($2::uuid IS NULL OR tenant_id=$2) "
                "ORDER BY at DESC LIMIT $3",
                extension_id, tenant_id, limit,
            )
        return [dict(r) for r in rows]


__all__ = ["AuditLog"]
