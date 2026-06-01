"""Slack per-user DM workspace fixture generator.

`make_slack_dm_workspace(team_id=..., user_id=..., ...)` produces a fixture
shaped for the spammer's `_SlackStore` DM support (and `MockSlackUserClient`):
the consenting user's `im` (1:1) + `mpim` (group) conversations plus a couple
of bot-visible `channels`. Feeds the worker-fetch DM backfill in spammer mode.

DM channel ids are derived with the SAME blake2b scheme the gateway DM debug
console uses (`services/gateway/slack_router.py:_dm_channel`/`_mpim_channel`),
so a conversation backfilled through the worker chain and one driven through
the inline console collapse to the same `external_id` — "identical
observations to the inline backfill."

Timestamps anchor on `base_ts` (default a recent epoch); the demo passes a
near-now value so `occurred_at` lands inside the `observations` table's ±2-month
partition window (older bases raise a missing-partition CheckViolation — see
A28 / ticket #44).
"""
from __future__ import annotations

import hashlib
from typing import Any


# A recent default so out-of-the-box fixtures land in-partition; the demo
# overrides with a now-anchored value.
_DEFAULT_BASE_TS = 1_780_000_000  # 2026-05-28T…Z

_DM_TEMPLATES = (
    "hey, did you get a chance to look at the deploy?",
    "lunch at 1?",
    "can you review my PR when you have a sec",
    "the client call moved to 3pm",
    "did you see the thread in #general lol",
    "I'll send over the deck tonight",
    "are we still on for friday?",
    "thanks for covering standup today",
)
_MPIM_TEMPLATES = (
    "group ping: standup notes are in the doc",
    "who's on the incident? prod looks degraded",
    "+1 to shipping tomorrow morning",
    "moving this to a huddle, join when you can",
)
_CHANNEL_TEMPLATES = (
    "deploy is green :white_check_mark:",
    "reminder: retro at 4pm in the main room",
)


def _dm_channel(user_id: str, counterpart: str) -> str:
    a, b = sorted((user_id, counterpart))
    h = hashlib.blake2b(
        f"{a}:{b}".encode("utf-8"), digest_size=6,
    ).hexdigest().upper()
    return f"D{h}"


def _mpim_channel(user_id: str) -> str:
    h = hashlib.blake2b(
        f"mpim:{user_id}".encode("utf-8"), digest_size=6,
    ).hexdigest().upper()
    return f"G{h}"


def _msg(channel: str, sender: str, text: str, ts: float) -> dict[str, Any]:
    return {
        "ts": f"{ts:.6f}",
        "user": sender,
        "type": "message",
        "team": None,  # filled by caller's team scope when needed
        "channel": channel,
        "text": text,
    }


def make_slack_dm_workspace(
    *,
    team_id: str,
    user_id: str = "U_ALICE",
    counterparts: tuple[str, ...] = ("U_BOB", "U_CAROL", "U_FRIEND"),
    mpim_users: tuple[str, ...] = ("U_BOB", "U_CAROL", "U_DAVE"),
    messages_per_dm: int = 6,
    messages_per_mpim: int = 4,
    channels: int = 1,
    messages_per_channel: int = 2,
    base_ts: float = _DEFAULT_BASE_TS,
) -> dict[str, Any]:
    """Build a Slack DM workspace fixture for one consenting user.

    Returns a fixture dict with `team_id`, `channels` (bot-visible), and
    `dm_users` (the consenting user's im/mpim conversations), consumable by
    the spammer `_SlackStore` and `MockSlackUserClient`.
    """
    # Spread ts backwards from base_ts at 600s intervals; a single global
    # counter keeps every message ts unique across all conversations.
    step = 600.0
    counter = 0

    def _next_ts() -> float:
        nonlocal counter
        ts = base_ts - 60 - counter * step
        counter += 1
        return ts

    conversations: list[dict[str, Any]] = []

    # 1:1 DMs (human↔human) with each counterpart.
    for ci, cp in enumerate(counterparts):
        ch = _dm_channel(user_id, cp)
        msgs = []
        for i in range(messages_per_dm):
            sender = cp if i % 2 == 0 else user_id  # alternate direction
            text = _DM_TEMPLATES[(ci + i) % len(_DM_TEMPLATES)]
            m = _msg(ch, sender, text, _next_ts())
            m["team"] = team_id
            msgs.append(m)
        conversations.append({
            "id": ch, "channel_type": "im", "user": cp,
            "name": None, "messages": msgs,
        })

    # One group DM (mpim).
    gch = _mpim_channel(user_id)
    gmsgs = []
    for i in range(messages_per_mpim):
        sender = mpim_users[i % len(mpim_users)]
        text = _MPIM_TEMPLATES[i % len(_MPIM_TEMPLATES)]
        m = _msg(gch, sender, text, _next_ts())
        m["team"] = team_id
        gmsgs.append(m)
    conversations.append({
        "id": gch, "channel_type": "mpim", "user": None,
        "name": f"mpdm-{user_id}", "messages": gmsgs,
    })

    # A couple of bot-visible channels (channel signals land ALONGSIDE DMs).
    channel_list: list[dict[str, Any]] = []
    for c in range(channels):
        cid = f"C0DMDEMO{c:02d}"
        cmsgs = []
        for i in range(messages_per_channel):
            sender = mpim_users[i % len(mpim_users)]
            text = _CHANNEL_TEMPLATES[i % len(_CHANNEL_TEMPLATES)]
            m = _msg(cid, sender, text, _next_ts())
            m["team"] = team_id
            cmsgs.append(m)
        channel_list.append({
            "id": cid, "name": f"dm-demo-{c}", "team_id": team_id,
            "messages": cmsgs,
        })

    return {
        "team_id": team_id,
        "channels": channel_list,
        "dm_users": [{"user_id": user_id, "conversations": conversations}],
        "page_size": 50,
    }


__all__ = ["make_slack_dm_workspace"]
