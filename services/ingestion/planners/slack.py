"""services/ingestion/planners/slack.py — Slack backfill planner (M6.5).

Per A18 + A18.6 (PlannerContext). One Shard per active channel via
`conversations.list`. No DB-cached channel list (Slack steady-state
doesn't materialize one); planner enumerates at plan time.
"""
from __future__ import annotations

import logging

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "slack_channel_window"


async def plan_shards_slack(ctx: PlannerContext) -> list[Shard]:
    """Enumerate channels via Slack client, emit one Shard per channel."""
    if ctx.source_client is None:
        raise RuntimeError(
            "Slack planner: source_client=None. The PlannerContext "
            "factory must supply a SlackClient. See "
            "_build_source_client in source_onboarding.py."
        )
    channels = await ctx.source_client.conversations_list()
    install_id = str(ctx.install["installation_id"])
    shards: list[Shard] = []
    for ch in channels:
        cid = ch.get("id")
        if not cid:
            continue
        shards.append(Shard(
            shard_kind=SHARD_KIND_CHANNEL_WINDOW,
            shard_identifier={
                "shard_kind": SHARD_KIND_CHANNEL_WINDOW,
                "channel_id": cid,
                "channel_name": ch.get("name"),
                "team_id": ch.get("team_id") or install_id,
                "installation_id": install_id,
            },
            recency_score=1.0,
        ))
    return shards


PLANNER_DISPATCH["slack"] = plan_shards_slack


__all__ = ["SHARD_KIND_CHANNEL_WINDOW", "plan_shards_slack"]
