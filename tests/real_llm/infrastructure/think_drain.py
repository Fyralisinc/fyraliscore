"""Helpers to wait for the Think trigger queue to drain and to load active Models."""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

import asyncpg

from lib.shared.types import ModelRow
from services.models.repo import ModelsRepo


async def wait_for_think_to_drain(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    *,
    timeout_seconds: int = 120,
    poll_interval_s: float = 0.5,
) -> None:
    """Poll think_trigger_queue until no incomplete rows remain for the tenant."""
    deadline = time.monotonic() + timeout_seconds
    last_pending = -1
    while True:
        async with pool.acquire() as conn:
            pending = await conn.fetchval(
                """
                SELECT COUNT(*)::bigint
                FROM think_trigger_queue
                WHERE tenant_id = $1
                  AND completed_at IS NULL
                """,
                tenant_id,
            )
        pending = int(pending or 0)
        if pending == 0:
            return
        last_pending = pending
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Think trigger queue did not drain for tenant {tenant_id} "
                f"within {timeout_seconds}s; {last_pending} row(s) still pending"
            )
        await asyncio.sleep(poll_interval_s)


async def load_active_models(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    *,
    scope_entity_id: UUID | None = None,
    scope_entity_type: str | None = None,
    scope_actor_id: UUID | None = None,
) -> list[ModelRow]:
    """Load active Models for the tenant via ModelsRepo.search_by_scope."""
    repo = ModelsRepo(pool)
    scope_actors: list[UUID] | None = (
        [scope_actor_id] if scope_actor_id is not None else None
    )
    scope_entities: list[dict] | None = None
    if scope_entity_id is not None or scope_entity_type is not None:
        entry: dict = {}
        if scope_entity_type is not None:
            entry["type"] = scope_entity_type
        if scope_entity_id is not None:
            entry["id"] = str(scope_entity_id)
        scope_entities = [entry]
    return await repo.search_by_scope(
        tenant_id=tenant_id,
        scope_actors=scope_actors,
        scope_entities=scope_entities,
        status="active",
    )


__all__ = [
    "wait_for_think_to_drain",
    "load_active_models",
]
