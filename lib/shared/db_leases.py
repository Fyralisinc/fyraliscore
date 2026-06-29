"""Postgres-backed leases for pgbouncer-safe singleton workers."""
from __future__ import annotations

import json
import socket
import uuid
from typing import Any

import asyncpg


DEFAULT_LEASE_TTL_SECONDS = 30.0


def default_lease_holder_id(prefix: str = "fyralis") -> str:
    return f"{prefix}@{socket.gethostname()}:{uuid.uuid4()}"


class PostgresLease:
    """A crash-tolerant row lease with explicit expiry.

    The implementation avoids session-scoped advisory locks so it remains safe
    when app traffic is routed through transaction-mode pgbouncer.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        lease_name: str,
        holder_id: str | None = None,
        ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be greater than zero")
        self._pool = pool
        self._lease_name = lease_name
        self._holder_id = holder_id or default_lease_holder_id(lease_name)
        self._ttl_seconds = float(ttl_seconds)
        self._metadata_json = json.dumps(metadata or {})
        self._held = False

    @property
    def lease_name(self) -> str:
        return self._lease_name

    @property
    def holder_id(self) -> str:
        return self._holder_id

    @property
    def ttl_seconds(self) -> float:
        return self._ttl_seconds

    def is_held(self) -> bool:
        return self._held

    async def acquire(self) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO scheduler_leases (
                    lease_name,
                    holder_id,
                    expires_at,
                    acquired_at,
                    refreshed_at,
                    metadata
                )
                VALUES (
                    $1,
                    $2,
                    now() + ($3::double precision * interval '1 second'),
                    now(),
                    now(),
                    $4::jsonb
                )
                ON CONFLICT (lease_name) DO UPDATE
                SET holder_id = EXCLUDED.holder_id,
                    expires_at = EXCLUDED.expires_at,
                    acquired_at = CASE
                        WHEN scheduler_leases.holder_id = EXCLUDED.holder_id
                        THEN scheduler_leases.acquired_at
                        ELSE now()
                    END,
                    refreshed_at = now(),
                    metadata = EXCLUDED.metadata
                WHERE scheduler_leases.holder_id = EXCLUDED.holder_id
                   OR scheduler_leases.expires_at <= now()
                RETURNING holder_id
                """,
                self._lease_name,
                self._holder_id,
                self._ttl_seconds,
                self._metadata_json,
            )
        acquired = row is not None
        self._held = acquired
        return acquired

    async def refresh(self) -> bool:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE scheduler_leases
                   SET expires_at = now() + (
                           $3::double precision * interval '1 second'
                       ),
                       refreshed_at = now(),
                       metadata = $4::jsonb
                 WHERE lease_name = $1
                   AND holder_id = $2
                   AND expires_at > now()
                RETURNING holder_id
                """,
                self._lease_name,
                self._holder_id,
                self._ttl_seconds,
                self._metadata_json,
            )
        refreshed = row is not None
        self._held = refreshed
        return refreshed

    async def release(self) -> bool:
        async with self._pool.acquire() as conn:
            status = await conn.execute(
                """
                DELETE FROM scheduler_leases
                 WHERE lease_name = $1
                   AND holder_id = $2
                """,
                self._lease_name,
                self._holder_id,
            )
        released = status.endswith(" 1")
        self._held = False
        return released


__all__ = ["PostgresLease", "default_lease_holder_id"]
