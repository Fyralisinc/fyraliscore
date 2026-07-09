"""Facebook Pages coverage reconciler."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from services.ingest.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
)
from services.ingest.ingestion.workflows.state import load_state


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.facebook_pages: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


async def _cursor_for_shard(pool: Any, shard_id: Any) -> dict[str, Any]:
    state = await load_state(pool, "shard_fetch", str(shard_id))
    if state is None or not state.state_data:
        return {}
    cursor = state.state_data.get("cursor")
    return cursor if isinstance(cursor, dict) else {}


def _install_id_for_shard(shard: asyncpg.Record) -> UUID | None:
    raw = shard["shard_identifier"]
    identifier: Any = raw
    if isinstance(raw, str):
        try:
            identifier = json.loads(raw)
        except json.JSONDecodeError:
            return None
    if not isinstance(identifier, dict):
        return None
    value = identifier.get("installation_id")
    if not value:
        return None
    try:
        return UUID(str(value))
    except ValueError:
        return None


async def reconcile_facebook_pages(
    shards: list[asyncpg.Record],
    run: asyncpg.Record,
) -> ReconciliationDecision:
    pool = _get_pool()
    done = [s for s in shards if s["state"] == "done"]
    if not done:
        return ReconciliationDecision(has_gaps=False)

    oldest: datetime | None = None
    message_count = 0
    conversation_count = 0
    reasons: set[str] = set()
    install_ids: set[UUID] = set()
    for shard in done:
        install_id = _install_id_for_shard(shard)
        if install_id:
            install_ids.add(install_id)
        cursor = await _cursor_for_shard(pool, shard["id"])
        candidate = _parse_ts(cursor.get("oldest_message_at"))
        if candidate is not None and (oldest is None or candidate < oldest):
            oldest = candidate
        message_count += int(cursor.get("message_count") or 0)
        conversation_count += int(cursor.get("conversation_count") or 0)
        reason = cursor.get("exhausted_reason")
        if isinstance(reason, str) and reason:
            reasons.add(reason)

    reason_text = (
        ", ".join(sorted(reasons))
        if reasons
        else "all_available_history_graph_pagination_exhausted"
    )
    await pool.execute(
        """
        UPDATE facebook_page_installations
           SET oldest_message_at = COALESCE($2, oldest_message_at),
               backfill_exhausted_at = now(),
               backfill_exhausted_reason = $3,
               conversation_count = GREATEST(conversation_count, $4),
               message_count = GREATEST(message_count, $5),
               updated_at = now()
         WHERE tenant_id = $1
           AND enabled = true
           AND (
               cardinality($6::uuid[]) = 0
               OR id = ANY($6::uuid[])
           )
        """,
        run["tenant_id"],
        oldest,
        reason_text,
        conversation_count,
        message_count,
        list(install_ids),
    )
    return ReconciliationDecision(
        has_gaps=False,
        message=(
            "Facebook Page Messages coverage complete: Graph pagination "
            "exhausted for every accessible conversation."
        ),
    )


RECONCILER_DISPATCH["facebook_pages"] = reconcile_facebook_pages


__all__ = ["reconcile_facebook_pages", "set_pool_provider"]
