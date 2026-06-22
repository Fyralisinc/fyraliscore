"""Helpers to wait for the Think trigger queue to drain and to load active Models."""

from __future__ import annotations

import asyncio
import os
import time
from uuid import UUID

import asyncpg

from lib.shared.types import ModelRow
from services.domain.models.repo import ModelsRepo


_BACKGROUND_POST_COMMIT_ACTIONS = {
    "discover_model_edges",
    "search_open_questions",
}


def _env_truthy(name: str, default: str) -> bool:
    return os.environ.get(name, default).strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


async def wait_for_think_to_drain(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    *,
    timeout_seconds: int = 120,
    poll_interval_s: float = 0.5,
    include_background: bool | None = None,
) -> None:
    """Poll until product-facing Think/post-commit work settles for the tenant.

    By default this waits for T1/T2/T3 work and non-background post-commit
    actions. T4 topology/open-question/model-reeval work is intentionally
    treated as background maintenance so product assertions do not block on
    exploratory fanout. Set include_background=True or
    REAL_LLM_THINK_DRAIN_INCLUDE_BACKGROUND=1 for a full maintenance drain.
    """
    override = os.environ.get("REAL_LLM_THINK_DRAIN_TIMEOUT_SECONDS")
    if override:
        try:
            timeout_seconds = max(timeout_seconds, int(override))
        except ValueError:
            pass
    if include_background is None:
        include_background = _env_truthy(
            "REAL_LLM_THINK_DRAIN_INCLUDE_BACKGROUND",
            "0",
        )
    deadline = time.monotonic() + timeout_seconds
    last_think_pending = -1
    last_post_commit_pending = -1
    last_diagnostics = ""
    while True:
        async with pool.acquire() as conn:
            think_pending = await _pending_think_count(
                conn,
                tenant_id,
                include_background=include_background,
            )
            post_commit_pending = await _pending_post_commit_count(
                conn,
                tenant_id,
                include_background=include_background,
            )
            if time.monotonic() >= deadline:
                last_diagnostics = await _queue_diagnostics(conn, tenant_id)
        think_pending = int(think_pending or 0)
        post_commit_pending = int(post_commit_pending or 0)
        if think_pending == 0 and post_commit_pending == 0:
            return
        last_think_pending = think_pending
        last_post_commit_pending = post_commit_pending
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Think/post-commit queues did not settle for tenant {tenant_id} "
                f"within {timeout_seconds}s; "
                f"think_pending={last_think_pending}, "
                f"post_commit_pending={last_post_commit_pending}, "
                f"include_background={include_background}. "
                f"{last_diagnostics}"
            )
        await asyncio.sleep(poll_interval_s)


async def _pending_think_count(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    include_background: bool,
) -> int:
    if include_background:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NULL
              AND batch_parent_id IS NULL
            """,
            tenant_id,
        )
    else:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_trigger_queue
            WHERE tenant_id = $1
              AND completed_at IS NULL
              AND batch_parent_id IS NULL
              AND trigger_kind != 'T4'
            """,
            tenant_id,
        )
    return int(value or 0)


async def _pending_post_commit_count(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    include_background: bool,
) -> int:
    if include_background:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND processed_at IS NULL
              AND dead_lettered_at IS NULL
            """,
            tenant_id,
        )
    else:
        value = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM pending_post_commit_actions
            WHERE tenant_id = $1
              AND processed_at IS NULL
              AND dead_lettered_at IS NULL
              AND action_kind <> ALL($2::text[])
            """,
            tenant_id,
            sorted(_BACKGROUND_POST_COMMIT_ACTIONS),
        )
    return int(value or 0)


async def _queue_diagnostics(conn: asyncpg.Connection, tenant_id: UUID) -> str:
    think_rows = await conn.fetch(
        """
        SELECT trigger_kind, COALESCE(trigger_subkind, '') AS trigger_subkind,
               COUNT(*)::bigint AS count
        FROM think_trigger_queue
        WHERE tenant_id = $1
          AND completed_at IS NULL
          AND batch_parent_id IS NULL
        GROUP BY trigger_kind, trigger_subkind
        ORDER BY trigger_kind, trigger_subkind
        """,
        tenant_id,
    )
    action_rows = await conn.fetch(
        """
        SELECT action_kind, COUNT(*)::bigint AS count
        FROM pending_post_commit_actions
        WHERE tenant_id = $1
          AND processed_at IS NULL
          AND dead_lettered_at IS NULL
        GROUP BY action_kind
        ORDER BY action_kind
        """,
        tenant_id,
    )
    think = {
        f"{row['trigger_kind']}:{row['trigger_subkind']}".rstrip(":"): int(
            row["count"] or 0
        )
        for row in think_rows
    }
    actions = {
        str(row["action_kind"]): int(row["count"] or 0)
        for row in action_rows
    }
    return f"pending_think_by_kind={think}; pending_post_commit_by_kind={actions}"


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
