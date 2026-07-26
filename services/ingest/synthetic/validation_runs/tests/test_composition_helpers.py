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
    _partition_probe_contract,
    _split_live_targets,
    live_target_for,
    seed_contract_live_only_targets,
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


def test_meta_live_targets_use_exact_provider_scope() -> None:
    whatsapp = live_target_for(
        _TENANT,
        "whatsapp",
        "wa-acme",
        {"phone_number_id": "15551234567"},
    )
    facebook = live_target_for(
        _TENANT,
        "facebook_pages",
        "fb-acme",
        {},
    )

    assert whatsapp.whatsapp_phone_number_id == "15551234567"
    assert facebook.facebook_page_id == "x3-fb-acme-facebook_pages"


def test_whatsapp_live_target_requires_explicit_phone_scope() -> None:
    with pytest.raises(ValueError, match="explicit phone_number_id"):
        live_target_for(_TENANT, "whatsapp", "wa-acme", {})


class _RecordingPool:
    def __init__(self) -> None:
        self.executions: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> None:
        self.executions.append((query, args))


@pytest.mark.asyncio
async def test_live_only_target_bootstrap_is_contract_derived_and_secretless() -> None:
    pool = _RecordingPool()

    targets = await seed_contract_live_only_targets(  # type: ignore[arg-type]
        pool,
        tenants_per_source=2,
    )

    assert [target.source for target in targets] == ["whatsapp", "whatsapp"]
    assert len({target.tenant_id for target in targets}) == 2
    assert all(target.whatsapp_phone_number_id for target in targets)
    install_queries = [
        (query, args)
        for query, args in pool.executions
        if "INSERT INTO whatsapp_installations" in query
    ]
    assert len(install_queries) == 2
    for query, _args in install_queries:
        assert "app_secret_ref" in query
        assert "NULL, NULL, NULL, NULL, NULL, NULL, TRUE" in query
        assert "ON CONFLICT" not in query


def test_partition_probe_metadata_comes_from_every_source_contract() -> None:
    from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS

    resolved = {
        definition.source_id: _partition_probe_contract(
            definition.source_id,
        )
        for definition in SOURCE_DEFINITIONS
    }

    assert set(resolved) == {
        definition.source_id for definition in SOURCE_DEFINITIONS
    }
    for definition in SOURCE_DEFINITIONS:
        ingress, channel, trust, kind = resolved[definition.source_id]
        assert channel in definition.normalization_inputs
        assert ingress in {
            route.ingress_kind for route in definition.ingress_routes
        }
        assert trust == definition.default_trust_tier
        assert kind in definition.allowed_observation_kinds
