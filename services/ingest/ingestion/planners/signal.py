"""services/ingest/ingestion/planners/signal.py — Signal planner (IN-SIGNAL).

Per ingestion LLD §3 + the Telegram/Jira loader precedent (A18.2): the install
record is account-scoped, but the planner needs the 1-to-N thread list to emit
one shard per thread. The enrichment lives in `signal_threads`; the
SourceOnboarding loader JSON-aggregates it onto `ctx.install["threads"]` so the
planner stays stateless (no DB I/O), exactly like Telegram's dialog aggregation.

Each shard is one thread's message history. The fetcher pages it backwards via
the thread-history call cursored on `offset_id`, warm-starting from the
per-thread `offset_id_cursor` high-water for an incremental re-walk.

`ctx.source_client` is None — threads are read from DB state (populated at
install time by `SignalClient.iter_threads` via `finalize_install`).
"""
from __future__ import annotations

import logging
from typing import Any

import orjson

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_THREAD_HISTORY = "signal_thread_history"


def _decode_threads(install: Any) -> list[dict[str, Any]]:
    """Decode the JSON-aggregated `threads` column on the install record."""
    raw = install["threads"] if "threads" in install else None
    if raw is None:
        return []
    if isinstance(raw, (str, bytes)):
        try:
            decoded = orjson.loads(raw)
        except orjson.JSONDecodeError:
            return []
    elif isinstance(raw, list):
        decoded = raw
    else:
        return []
    return [t for t in decoded if isinstance(t, dict)]


async def plan_shards_signal(ctx: PlannerContext) -> list[Shard]:
    """One `signal_thread_history` shard per active thread.

    Reads DB state only (threads pre-aggregated by the loader), so
    `ctx.source_client` is None — same as Telegram/Jira.
    """
    install_id = str(ctx.install["id"])
    threads = _decode_threads(ctx.install)

    shards: list[Shard] = []
    for t in threads:
        thread_id = t.get("thread_id")
        if not isinstance(thread_id, int):
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_THREAD_HISTORY,
            shard_identifier={
                "shard_kind": SHARD_KIND_THREAD_HISTORY,
                "thread_id": thread_id,
                "thread_kind": t.get("thread_kind") or "direct",
                "thread_title": t.get("title"),
                "installation_id": install_id,
                # The per-thread offset_id high-water — None on first full sync.
                "offset_id_cursor": t.get("offset_id_cursor"),
            },
            recency_score=1.0,
            window_start=None, window_end=None,
        ))

    log.info(
        "planners.signal.planned",
        extra={"thread_shards": len(shards), "installation_id": install_id},
    )
    return shards


PLANNER_DISPATCH["signal"] = plan_shards_signal


__all__ = ["SHARD_KIND_THREAD_HISTORY", "plan_shards_signal"]
