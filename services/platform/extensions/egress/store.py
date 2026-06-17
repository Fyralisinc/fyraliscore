"""services/platform/extensions/egress/store.py — outbox read/write for the egress plane.

Append-only writes (the delivery worker materializing the Kafka projection) and the
cursor read the PULL endpoint serves. Idempotent on (extension_id, observation_id).
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID


class EgressStore:
    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def append(
        self, *, extension_id: str, tenant_id: UUID, observation_id: UUID,
        source_channel: str, payload: dict[str, Any], enqueue_webhook: bool = False,
    ) -> int | None:
        """Insert one outbox row (idempotent). Returns its seq, or None if the
        (extension, observation) was already projected. Optionally enqueues a
        pending webhook delivery for the same item."""
        from lib.shared.ids import uuid7
        async with self.pool.acquire() as conn:
            seq = await conn.fetchval(
                "INSERT INTO extension_egress "
                "(extension_id, tenant_id, observation_id, source_channel, payload) "
                "VALUES ($1,$2,$3,$4,$5::jsonb) "
                "ON CONFLICT (extension_id, observation_id) DO NOTHING RETURNING seq",
                extension_id, tenant_id, observation_id, source_channel, json.dumps(payload),
            )
            if seq is not None and enqueue_webhook:
                await conn.execute(
                    "INSERT INTO extension_webhook_delivery (id, egress_seq, extension_id) "
                    "VALUES ($1,$2,$3)",
                    uuid7(), seq, extension_id,
                )
        return seq

    async def read(
        self, *, extension_id: str, tenant_id: UUID, after_seq: int, limit: int = 100,
    ) -> tuple[list[dict[str, Any]], int]:
        """Return (items, next_cursor). Each item is the redacted payload + its seq.
        next_cursor is the max seq returned (== after_seq when empty)."""
        limit = max(1, min(int(limit), 1000))
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT seq, payload FROM extension_egress "
                "WHERE extension_id=$1 AND tenant_id=$2 AND seq > $3 "
                "ORDER BY seq LIMIT $4",
                extension_id, tenant_id, after_seq, limit,
            )
        items: list[dict[str, Any]] = []
        cursor = after_seq
        for r in rows:
            payload = r["payload"]
            if isinstance(payload, str):
                payload = json.loads(payload)
            items.append({"seq": r["seq"], **payload})
            cursor = r["seq"]
        return items, cursor


__all__ = ["EgressStore"]
