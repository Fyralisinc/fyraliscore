"""services/ingestion/planners/slack.py — Slack backfill planner (M6.5).

Per A18 + A18.6 (PlannerContext). Emits TWO shard families:

  * `slack_channel_window` — one shard per public/private channel the BOT can
    see, enumerated via `ctx.source_client.conversations_list()` (the bot
    client; M6.5 original).
  * `slack_dm_window` — one shard per human↔human DM / group-DM conversation,
    enumerated PER CONSENTING USER. A bot token can never read DMs, so DM
    coverage is consent-based: each row in `slack_dm_installations` is a user
    who granted an xoxp token. For each, a `SlackUserClient` lists their
    `im`/`mpim` conversations (`conversations.list(types=im,mpim)`) and one
    shard is emitted per conversation. The fetcher then reads that
    conversation's history under the SAME user token.

DM shards carry the consenting `user_id` in their identifier so the fetcher /
reconciler resolve the right per-user token. Both families share the
`slack:message` handler + `external_id="{channel}:{ts}"` dedup, so a DM
backfilled here and its live webhook twin collapse to one observation —
identical to the inline backfill path.

No DB-cached conversation list (Slack steady-state doesn't materialize one);
the planner enumerates at plan time (the GitHub-repo-enumeration pattern).
"""
from __future__ import annotations

import logging
from typing import Any

from services.ingestion.planners import PLANNER_DISPATCH, Shard
from services.ingestion.planners.context import PlannerContext


log = logging.getLogger(__name__)


SHARD_KIND_CHANNEL_WINDOW = "slack_channel_window"
SHARD_KIND_DM_WINDOW = "slack_dm_window"

# Active consenting-user enumeration (per-user DM grain; migration 0065).
# Worker role bypasses RLS (rolbypassrls), so the explicit tenant filter is
# the isolation boundary here — same as the mercury/jira install loaders.
_LOAD_DM_INSTALLS_SQL = """
SELECT id, team_id, user_id, base_url
  FROM slack_dm_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 ORDER BY user_id
"""


async def _open_slack_user_client(
    *, tenant_id: Any, team_id: str, user_id: str, base_url: str | None,
):  # noqa: ANN202
    """Per-user DM client factory seam. Real builder resolves the xoxp token
    (or presets the spammer token in spammer mode). The X3 mock harness
    monkeypatches this symbol to inject a MockSlackUserClient."""
    from services.ingestion.fetchers._clients import build_slack_user_client
    return await build_slack_user_client(
        tenant_id=tenant_id, team_id=team_id,
        user_id=user_id, base_url=base_url,
    )


async def _plan_channel_shards(ctx: PlannerContext) -> list[Shard]:
    """One Shard per bot-visible channel (M6.5 original)."""
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


async def _plan_dm_shards(ctx: PlannerContext) -> list[Shard]:
    """One Shard per DM/group-DM conversation, per consenting user.

    Enumeration runs under each user's own xoxp token. A revoked / errored
    token for ONE user must not fail the whole plan — it is logged and that
    user's DMs are skipped (consent-shaped coverage gap), and the remaining
    users + the channel shards still proceed.
    """
    if ctx.conn is None:
        # No DB connection (e.g. a channel-only planner unit test) → no DM
        # grain to read. Production always supplies the in-transaction conn.
        return []
    install_id = str(ctx.install["installation_id"])
    rows = await ctx.conn.fetch(_LOAD_DM_INSTALLS_SQL, ctx.tenant_id)
    shards: list[Shard] = []
    for row in rows:
        team_id = row["team_id"]
        user_id = row["user_id"]
        try:
            client = await _open_slack_user_client(
                tenant_id=ctx.tenant_id, team_id=team_id,
                user_id=user_id, base_url=row["base_url"],
            )
            conversations = await client.conversations_list(types="im,mpim")
        except Exception as exc:  # noqa: BLE001 — one user's failure is partial
            log.warning(
                "slack_planner.dm_enumeration_failed",
                extra={
                    "tenant_id": str(ctx.tenant_id), "user_id": user_id,
                    "error": str(exc)[:200],
                },
            )
            continue
        for conv in conversations:
            cid = conv.get("id")
            if not cid:
                continue
            shards.append(Shard(
                shard_kind=SHARD_KIND_DM_WINDOW,
                shard_identifier={
                    "shard_kind": SHARD_KIND_DM_WINDOW,
                    "channel_id": cid,
                    "channel_type": conv.get("channel_type"),
                    # The consenting user whose token reads this conversation.
                    "consenting_user_id": user_id,
                    # im counterpart (the OTHER human); None for group DMs.
                    "counterpart_user_id": conv.get("user"),
                    "team_id": conv.get("team_id") or team_id,
                    "installation_id": install_id,
                },
                # DMs are higher-signal recency than bulk channel history.
                recency_score=1.25,
            ))
    return shards


async def plan_shards_slack(ctx: PlannerContext) -> list[Shard]:
    """Emit channel shards (bot) + DM shards (per consenting user)."""
    shards = await _plan_channel_shards(ctx)
    shards.extend(await _plan_dm_shards(ctx))
    return shards


PLANNER_DISPATCH["slack"] = plan_shards_slack


__all__ = [
    "SHARD_KIND_CHANNEL_WINDOW",
    "SHARD_KIND_DM_WINDOW",
    "plan_shards_slack",
]
