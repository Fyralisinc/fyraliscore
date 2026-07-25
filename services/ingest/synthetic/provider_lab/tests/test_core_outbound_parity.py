"""Unique legacy outbound-loop coverage retained in Provider Lab.

Normal Slack, GitHub, and Gmail production-client conformance lives in
``test_production_clients.py``. Discord Gateway conformance lives in
``test_discord_gateway.py``. This module preserves the remaining distinct
Slack DM, Discord REST pagination, and client-side 429 retry behavior.
"""
from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from services.ingest.integrations.discord.client import DiscordClient
from services.ingest.integrations.slack.client import SlackClient, SlackUserClient
from services.ingest.synthetic.fixtures import (
    make_discord_guild,
    make_slack_dm_workspace,
    make_slack_workspace,
)
from services.ingest.synthetic.provider_lab import build_provider_lab_app


_TEAM = "T_PROVIDER_LAB_DM"
_USER = "U_ALICE"
_GUILD = "900000000000000001"


def _transport(app) -> httpx.ASGITransport:  # noqa: ANN001
    return httpx.ASGITransport(app=app, client=("127.0.0.1", 43123))


async def test_slack_user_client_lists_and_pages_direct_messages() -> None:
    fixture = make_slack_dm_workspace(
        team_id=_TEAM,
        user_id=_USER,
        messages_per_dm=5,
    )
    app = build_provider_lab_app(fixtures={"slack": [fixture]})
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    client = SlackUserClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id=_TEAM,
        user_id=_USER,
        base_url="http://provider-lab/slack/api",
        http_client=http_client,
    )
    client._user_token_cache.set(  # type: ignore[attr-defined]
        f"lab-slack-user::{_TEAM}::{_USER}",
        ttl_seconds=float("inf"),
    )

    try:
        conversations = await client.conversations_list(types="im,mpim")
        direct = next(
            item for item in conversations if item["channel_type"] == "im"
        )
        seen: list[str] = []
        cursor = None
        while True:
            messages, cursor = await client.conversations_history(
                channel=direct["id"],
                cursor=cursor,
                limit=2,
            )
            seen.extend(message["ts"] for message in messages)
            if not cursor:
                break
    finally:
        await client.aclose()

    assert {item["channel_type"] for item in conversations} == {"im", "mpim"}
    assert len(seen) == 5


async def test_slack_bot_token_cannot_enumerate_user_direct_messages() -> None:
    fixture = make_slack_dm_workspace(team_id=_TEAM, user_id=_USER)
    app = build_provider_lab_app(fixtures={"slack": [fixture]})
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    client = SlackClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id=_TEAM,
        base_url="http://provider-lab/slack/api",
        http_client=http_client,
    )
    client._bot_token_cache.set(  # type: ignore[attr-defined]
        f"lab-slack::{_TEAM}",
        ttl_seconds=float("inf"),
    )

    try:
        channels = await client.conversations_list()
    finally:
        await client.aclose()

    direct_ids = {
        conversation["id"]
        for conversation in fixture["dm_users"][0]["conversations"]
    }
    assert {channel["id"] for channel in channels}.isdisjoint(direct_ids)


async def test_discord_rest_client_pages_snowflakes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DISCORD_BOT_TOKEN", f"lab-discord::{_GUILD}")
    fixture = make_discord_guild(
        guild_id=_GUILD,
        channels=1,
        messages_per_channel=5,
    )
    channel_id = fixture["channels"][0]["id"]
    app = build_provider_lab_app(fixtures={"discord": [fixture]})
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    client = DiscordClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        guild_id=_GUILD,
        base_url="http://provider-lab/discord/api/v10",
        http_client=http_client,
    )

    try:
        guilds = await client.list_guilds()
        channels = await client.list_guild_channels(_GUILD)
        seen: list[str] = []
        before = None
        while True:
            messages = await client.get_messages(
                channel_id=channel_id,
                before=before,
                limit=2,
            )
            if not messages:
                break
            seen.extend(message["id"] for message in messages)
            before = messages[-1]["id"]
            if len(messages) < 2:
                break
    finally:
        await client.aclose()

    assert guilds == [{"id": _GUILD}]
    assert [channel["id"] for channel in channels] == [channel_id]
    assert len(set(seen)) == 5


async def test_slack_client_retries_periodic_provider_lab_429() -> None:
    fixture = make_slack_workspace(
        team_id=_TEAM,
        channels=1,
        messages_per_channel=3,
    )
    channel_id = fixture["channels"][0]["id"]
    app = build_provider_lab_app(fixtures={"slack": [fixture]})
    app.state.provider_lab.faults.create(
        source="slack",
        route_id="slack.conversations_history",
        status_code=429,
        body={"ok": False, "error": "ratelimited"},
        headers={"Retry-After": "0"},
        after_requests=1,
        every=2,
        max_hits=1,
    )
    http_client = httpx.AsyncClient(
        transport=_transport(app),
        base_url="http://provider-lab",
    )
    client = SlackClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id=_TEAM,
        base_url="http://provider-lab/slack/api",
        http_client=http_client,
    )
    client._bot_token_cache.set(  # type: ignore[attr-defined]
        f"lab-slack::{_TEAM}",
        ttl_seconds=float("inf"),
    )

    try:
        await client.conversations_history(channel=channel_id, limit=10)
        messages, _cursor = await client.conversations_history(
            channel=channel_id,
            limit=10,
        )
    finally:
        await client.aclose()

    assert len(messages) == 3
    assert app.state.provider_lab.faults.snapshot()[0]["hits"] == 1
