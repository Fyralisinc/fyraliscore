"""services/ingest/ingestion/planners/fireflies.py — Fireflies planner.

Per the Jira/Calendar loader precedent (A18.2): Fireflies' install record is
workspace-scoped with NO sharded sub-resource — a workspace's meeting
transcripts are a single newest-first stream — so the planner emits exactly ONE
`fireflies_transcripts` shard per workspace install. The workspace_id is carried
on the install row (loaded by the SourceOnboarding loader into
`ctx.install["workspace_id"]`) so the planner stays stateless (no DB I/O).

The shard is the workspace's transcript stream. The fetcher walks
`GET /transcripts` on first run, then incrementally via the per-workspace
transcript high-water cursor.

`ctx.source_client` is None — the workspace_id is read from DB state (populated
at seed/install time by `FirefliesClient.get_workspace`).

TODO(human): confirm Fireflies resource taxonomy to shard on. This clones
Brex's per-install shard model but collapsed to a single workspace stream
(Fireflies exposes no per-account split). If a workspace's transcripts must be
sharded (e.g. per channel / per host) once the surface is confirmed, fan the
shard list out here.
"""
from __future__ import annotations

import logging

from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingest.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_TRANSCRIPTS = "fireflies_transcripts"


async def plan_shards_fireflies(ctx: PlannerContext) -> list[Shard]:
    """One `fireflies_transcripts` shard per workspace install.

    Reads DB state only (workspace_id on the install row), so
    `ctx.source_client` is None — same as Jira/Brex/Calendar.
    """
    install_id = str(ctx.install["id"])
    workspace_id = ctx.install["workspace_id"] if "workspace_id" in ctx.install else None
    if not isinstance(workspace_id, str) or not workspace_id:
        log.info(
            "planners.fireflies.no_workspace",
            extra={"installation_id": install_id},
        )
        return []

    # The high-water transcript cursor — None on first sync. Carried on the
    # install row by the loader for warm-started incremental syncs.
    txn_cursor = ctx.install["transcript_cursor"] if "transcript_cursor" in ctx.install else None

    shard = Shard(
        shard_kind=SHARD_KIND_TRANSCRIPTS,
        shard_identifier={
            "shard_kind": SHARD_KIND_TRANSCRIPTS,
            "workspace_id": workspace_id,
            "installation_id": install_id,
            # High-water transcript-date cursor — None on first sync.
            "transcript_cursor": txn_cursor,
        },
        recency_score=1.0,
        window_start=None, window_end=None,
    )

    log.info(
        "planners.fireflies.planned",
        extra={"workspace_id": workspace_id, "installation_id": install_id},
    )
    return [shard]


PLANNER_DISPATCH["fireflies"] = plan_shards_fireflies


__all__ = ["SHARD_KIND_TRANSCRIPTS", "plan_shards_fireflies"]
