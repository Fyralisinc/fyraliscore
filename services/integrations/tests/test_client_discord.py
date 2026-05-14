"""IN-09 US5: outbound Discord REST client tests.

Covers rate-limit budget, retry-after handling, missing-env-var error,
the no-guild_id-in-logs invariant (SC-006), and — added post-IN-09 —
verification that the app-level Bot Token from `DISCORD_BOT_TOKEN` env
flows into the `Authorization: Bot …` header instead of the
per-installation OAuth Bearer that used to live in encrypted_secrets.
"""
from __future__ import annotations

import time
from uuid import UUID, uuid4

import asyncpg
import pytest
import respx
import structlog
from cryptography.fernet import Fernet

from lib.shared.errors import DiscordApiError
from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.integrations.discord import metrics as discord_metrics
from services.integrations.discord.client import DiscordClient


pytestmark = pytest.mark.integration


_GUILD_ID = "G_US5_700000000000000001"
_USER_ID = "U_US5_700000000000000002"
_BOT_TOKEN_ENV = "test-bot-token-env-not-the-oauth-bearer"


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    discord_metrics.reset()


@pytest.fixture(autouse=True)
def _set_bot_token_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISCORD_BOT_TOKEN", _BOT_TOKEN_ENV)


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"discord-us5-{tid.hex[:8]}",
    )
    return tid


async def _seed_install(
    fresh_db: asyncpg.Pool, tenant_id: UUID, secret_store,
) -> UUID:
    public_key_ref = await secret_store.put(
        b"a" * 32, label=f"discord_public_key:{_GUILD_ID}", tenant_id=tenant_id,
    )
    await secret_store.put(
        b"discord-bot-token-US5",
        label=f"discord_bot_token:{_GUILD_ID}",
        tenant_id=tenant_id,
    )
    row_id = uuid7()
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'discord', $3, $4, TRUE)",
        row_id, tenant_id, _GUILD_ID, public_key_ref,
    )
    return row_id


async def test_429_retry_within_budget(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_id = await _seed_install(fresh_db, _tenant, secret_store)

    call_count = {"n": 0}

    def _handler(request):
        import httpx
        call_count["n"] += 1
        if call_count["n"] == 1:
            return httpx.Response(
                429, json={"message": "rate"}, headers={"Retry-After": "1"},
            )
        return httpx.Response(200, json={"user": {"id": _USER_ID}})

    with respx.mock(base_url="https://discord.com") as router:
        router.get(
            f"/api/v10/guilds/{_GUILD_ID}/members/{_USER_ID}",
        ).mock(side_effect=_handler)

        client = DiscordClient(
            pool=fresh_db, secret_store=secret_store,
            tenant_id=_tenant, installation_row_id=install_id,
            guild_id=_GUILD_ID,
        )
        start = time.monotonic()
        result = await client.get_guild_member(_USER_ID)
        elapsed = time.monotonic() - start
        await client.aclose()

    assert call_count["n"] == 2
    assert result["user"]["id"] == _USER_ID
    # We slept for the Retry-After=1, so elapsed should be ≥ ~0.9s
    # and well under the 30s budget.
    assert elapsed >= 0.9, f"expected ≥0.9s sleep, got {elapsed:.2f}s"
    assert elapsed < 30.0


async def test_budget_exhausted_raises_rate_limited(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_id = await _seed_install(fresh_db, _tenant, secret_store)

    with respx.mock(base_url="https://discord.com") as router:
        # Always return 429 with a tiny Retry-After so retries don't blow the wall clock.
        router.get(
            f"/api/v10/guilds/{_GUILD_ID}/members/{_USER_ID}",
        ).respond(429, json={"message": "rate"}, headers={"Retry-After": "0.05"})

        client = DiscordClient(
            pool=fresh_db, secret_store=secret_store,
            tenant_id=_tenant, installation_row_id=install_id,
            guild_id=_GUILD_ID,
            max_attempts=3,
        )
        with pytest.raises(DiscordApiError) as exc_info:
            await client.get_guild_member(_USER_ID)
        await client.aclose()
        assert exc_info.value.code == "discord_api_rate_limited"
        assert exc_info.value.context["attempts"] <= 3


async def test_missing_bot_token_env_raises_discord_secret_unavailable(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The app-level Bot Token lives in `DISCORD_BOT_TOKEN`. If the env
    var is unset or empty, `_resolve_bot_token` must fail fast with
    `discord_secret_unavailable` — without falling back to the OAuth
    Bearer in encrypted_secrets (which can't authorize bot-scope API
    calls anyway)."""
    monkeypatch.delenv("DISCORD_BOT_TOKEN", raising=False)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_id = await _seed_install(fresh_db, _tenant, secret_store)

    client = DiscordClient(
        pool=fresh_db, secret_store=secret_store,
        tenant_id=_tenant, installation_row_id=install_id,
        guild_id=_GUILD_ID,
    )
    with pytest.raises(DiscordApiError) as exc_info:
        await client.get_guild_member(_USER_ID)
    await client.aclose()
    assert exc_info.value.code == "discord_secret_unavailable"


async def test_env_bot_token_flows_to_authorization_header(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    """Integration check: the value of `DISCORD_BOT_TOKEN` env (set by
    the autouse fixture to `_BOT_TOKEN_ENV`) must appear in the
    outbound `Authorization: Bot <token>` header — NOT the OAuth
    `access_token` stored per-guild in encrypted_secrets. Seed a
    deliberately different per-guild bot-token row to prove the env
    path wins."""
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    # Seed a per-guild secret with a value that MUST NOT appear in the header.
    poisoned_oauth_bearer = "oauth-bearer-DO-NOT-USE"
    public_key_ref = await secret_store.put(
        b"a" * 32, label=f"discord_public_key:{_GUILD_ID}", tenant_id=_tenant,
    )
    await secret_store.put(
        poisoned_oauth_bearer.encode("utf-8"),
        label=f"discord_bot_token:{_GUILD_ID}",
        tenant_id=_tenant,
    )
    install_id = uuid7()
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'discord', $3, $4, TRUE)",
        install_id, _tenant, _GUILD_ID, public_key_ref,
    )

    captured_auth: dict[str, str] = {}

    def _handler(request):
        import httpx
        captured_auth["value"] = request.headers.get("Authorization", "")
        return httpx.Response(200, json={"user": {"id": _USER_ID}})

    with respx.mock(base_url="https://discord.com") as router:
        router.get(
            f"/api/v10/guilds/{_GUILD_ID}/members/{_USER_ID}",
        ).mock(side_effect=_handler)

        client = DiscordClient(
            pool=fresh_db, secret_store=secret_store,
            tenant_id=_tenant, installation_row_id=install_id,
            guild_id=_GUILD_ID,
        )
        await client.get_guild_member(_USER_ID)
        await client.aclose()

    assert captured_auth["value"] == f"Bot {_BOT_TOKEN_ENV}", (
        f"expected env-var bot token, got {captured_auth['value']!r}"
    )
    assert poisoned_oauth_bearer not in captured_auth["value"], (
        "OAuth Bearer from encrypted_secrets leaked into Authorization header"
    )


async def test_no_guild_id_in_structured_logs(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    """SC-006: structured log records emitted by `_request` MUST NOT
    carry the raw guild_id. The endpoint label is the unsubstituted
    template (`/guilds/{guild_id}/members/{user_id}`), not the URL we
    actually hit."""
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_id = await _seed_install(fresh_db, _tenant, secret_store)

    with respx.mock(base_url="https://discord.com") as router:
        router.get(
            f"/api/v10/guilds/{_GUILD_ID}/members/{_USER_ID}",
        ).respond(200, json={"user": {"id": _USER_ID}})

        client = DiscordClient(
            pool=fresh_db, secret_store=secret_store,
            tenant_id=_tenant, installation_row_id=install_id,
            guild_id=_GUILD_ID,
        )
        # structlog.testing.capture_logs is the canonical capture API.
        with structlog.testing.capture_logs() as captured:
            await client.get_guild_member(_USER_ID)
        await client.aclose()

    api_events = [e for e in captured if e.get("event") == "discord_api_request"]
    assert len(api_events) >= 1, f"no discord_api_request events: {captured}"

    for event in api_events:
        rendered = " ".join(f"{k}={v}" for k, v in event.items())
        assert _GUILD_ID not in rendered, (
            f"guild_id {_GUILD_ID!r} leaked into structured log event: {event}"
        )
        assert "{guild_id}" in event["endpoint"], (
            f"endpoint should be unsubstituted template; got {event['endpoint']!r}"
        )
