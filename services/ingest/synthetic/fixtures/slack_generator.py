"""Slack workspace fixture generator.

`make_slack_workspace(team_id=..., channels=N, messages_per_channel=M)`
produces a deterministic workspace shaped to feed `MockSlackClient`.
"""
from __future__ import annotations

import hashlib
from typing import Any


def make_slack_workspace(
    *,
    team_id: str,
    channels: int = 3,
    messages_per_channel: int = 50,
    page_size: int = 10,
) -> dict[str, Any]:
    """Build a Slack workspace fixture.

    Args:
      team_id: Slack team / workspace identifier.
      channels: Number of channels (all public, type='channel').
      messages_per_channel: Messages per channel.
      page_size: Mock client's default page size for
        `conversations_history`.

    Returns:
      Fixture dict consumable by `MockSlackClient(fixture=...)`.
    """
    channel_list: list[dict[str, Any]] = []
    for c in range(channels):
        cid = f"C_{_digest(team_id, c, 'ch')[:10]}".upper()
        # Each channel's messages span 60-second intervals.
        msgs = [
            _slack_message(team_id, cid, m)
            for m in range(messages_per_channel)
        ]
        channel_list.append({
            "id": cid,
            "name": f"channel-{c}",
            "team_id": team_id,
            "messages": msgs,
        })

    return {
        "team_id": team_id,
        "channels": channel_list,
        "page_size": page_size,
    }


def _slack_message(
    team_id: str, channel_id: str, idx: int,
) -> dict[str, Any]:
    # Slack ts format: "<unix_seconds>.<microseconds>".
    # Base = 2026-01-01T00:00:00Z (1_767_225_600). Must fall within the
    # `observations` table's partition coverage (monthly partitions);
    # an out-of-range occurred_at makes the writer's INSERT raise a
    # missing-partition CheckViolation. Real backfill of historical data
    # can legitimately produce older timestamps — see A28 + ticket #44
    # for the partition-coverage / DLQ handling of that production case.
    base = 1_767_225_600.0 + idx * 60.0
    ts = f"{base:.6f}"
    return {
        "ts": ts,
        "user": f"U_{_digest(team_id, idx, 'user')[:8]}".upper(),
        "type": "message",
        "team": team_id,
        "channel": channel_id,
        "text": f"synthetic slack message #{idx}",
    }


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(str(p).encode())
        h.update(b"|")
    return h.hexdigest()
