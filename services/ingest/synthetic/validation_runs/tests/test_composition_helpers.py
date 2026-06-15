from __future__ import annotations

from uuid import UUID

import pytest
from fastapi import FastAPI

from services.ingest.synthetic.validation_runs.composition import (
    HMAC_PROVIDERS,
    LiveTarget,
    SigningSecrets,
    _LiveCutoverDeps,
    _attach_cutover_state,
    _build_discord_guild_bindings,
    _gmail_tenant_ids_by_email,
    _hmac_secret_map,
    _split_live_targets,
)


_TENANT = UUID("aaaaaaaa-1111-7777-8888-bbbbbbbbbbbb")


def test_split_live_targets_groups_core_sources() -> None:
    gmail = LiveTarget(
        tenant_id=_TENANT,
        source="gmail",
        slug="acme",
        email="alice@example.com",
    )
    discord = LiveTarget(
        tenant_id=_TENANT,
        source="discord",
        slug="acme",
        guild_id="guild-1",
        channel_id="channel-1",
    )
    slack = LiveTarget(
        tenant_id=_TENANT,
        source="slack",
        slug="acme",
        team_id="team-1",
    )

    groups = _split_live_targets([gmail, discord, slack])

    assert groups.gmail == [gmail]
    assert groups.discord == [discord]
    assert groups.present == {"gmail", "discord", "slack"}


def test_attach_cutover_state_threads_shared_dependencies() -> None:
    app = FastAPI()
    cutover = _LiveCutoverDeps(
        kafka_producer=object(),
        s3_raw_client=object(),
        tenant_flags=object(),
    )

    _attach_cutover_state(app, cutover)

    assert app.state.kafka_producer is cutover.kafka_producer
    assert app.state.s3_raw_client is cutover.s3_raw_client
    assert app.state.tenant_flags is cutover.tenant_flags


def test_gmail_tenant_ids_by_email_uses_lowercase_mailbox_keys() -> None:
    target = LiveTarget(
        tenant_id=_TENANT,
        source="gmail",
        slug="acme",
        email="Alice@Example.com",
    )

    assert _gmail_tenant_ids_by_email([target]) == {
        "alice@example.com": _TENANT,
    }


def test_hmac_secret_map_covers_all_hmac_providers() -> None:
    secrets = SigningSecrets()

    secret_by_provider = _hmac_secret_map(secrets)

    assert set(HMAC_PROVIDERS) <= set(secret_by_provider)
    assert secret_by_provider["jira"] == secrets.jira
    assert secret_by_provider["quickbooks"] == secrets.quickbooks
    assert secret_by_provider["ashby"] == secrets.ashby


@pytest.mark.asyncio
async def test_discord_guild_bindings_preserve_target_channel() -> None:
    bindings = _build_discord_guild_bindings([
        LiveTarget(
            tenant_id=_TENANT,
            source="discord",
            slug="acme",
            guild_id="guild-1",
            channel_id="channel-1",
        ),
    ])

    binding = bindings["guild-1"]

    assert binding.guild_id == "guild-1"
    channels = await binding.mock_client.list_guild_channels(guild_id="guild-1")
    assert channels[0]["id"] == "channel-1"
