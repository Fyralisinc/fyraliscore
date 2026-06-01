"""IN-09 US3: bot-kick chokepoint tests.

The chokepoint fires from the outbound client on 401 (or 403 with
code=50001). Idempotent under concurrent races (Clarifications Q1).
The disabled installation then causes the next inbound interaction
for the same guild_id to return 401 unknown_installation.
"""
from __future__ import annotations

import asyncio
import json
import time
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import respx
from cryptography.fernet import Fernet

from lib.shared.errors import DiscordApiError
from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.main import build_app
from services.gateway.rate_limit import RateLimiter
from services.integrations.discord import metrics as discord_metrics
from services.integrations.discord.client import DiscordClient
from services.webhooks.tests.conftest import discord_keypair


pytestmark = pytest.mark.integration


_GUILD_ID = "G_US3_700000000000000001"
_USER_ID = "U_US3_700000000000000002"
_INTERACTION_ID = "I_US3_700000000000000003"
_APP_ID = "A_US3"


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    discord_metrics.reset()


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"discord-us3-{tid.hex[:8]}",
    )
    return tid


async def _seed_install(
    fresh_db: asyncpg.Pool, tenant_id: UUID, secret_store,
    pub_hex: str = "a" * 64,
) -> UUID:
    public_key_ref = await secret_store.put(
        pub_hex.encode("utf-8"),
        label=f"discord_public_key:{_GUILD_ID}",
        tenant_id=tenant_id,
    )
    await secret_store.put(
        b"discord-bot-token-seed",
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


async def test_401_disables_installation_and_zeroes_token(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_id = await _seed_install(fresh_db, _tenant, secret_store)

    with respx.mock(base_url="https://discord.com") as router:
        router.get(
            f"/api/v10/guilds/{_GUILD_ID}/members/{_USER_ID}",
        ).respond(401, json={"message": "401: Unauthorized", "code": 0})

        client = DiscordClient(
            pool=fresh_db, secret_store=secret_store,
            tenant_id=_tenant, installation_row_id=install_id,
            guild_id=_GUILD_ID,
        )
        with pytest.raises(DiscordApiError) as exc_info:
            await client.get_guild_member(_USER_ID)
        await client.aclose()
        assert exc_info.value.code == "discord_api_unauthorized"

    # Installation disabled.
    row = await fresh_db.fetchrow(
        "SELECT enabled FROM provider_installations WHERE id=$1", install_id,
    )
    assert row is not None and row["enabled"] is False
    # Bot token secret deleted.
    bot_count = await fresh_db.fetchval(
        "SELECT count(*) FROM encrypted_secrets "
        "WHERE tenant_id=$1 AND label=$2",
        _tenant, f"discord_bot_token:{_GUILD_ID}",
    )
    assert bot_count == 0
    # Audit row.
    audit = await fresh_db.fetchrow(
        "SELECT action, status FROM installation_audit_log "
        "WHERE installation_row_id=$1 ORDER BY created_at DESC LIMIT 1",
        install_id,
    )
    assert audit is not None
    assert audit["action"] == "uninstall"
    assert audit["status"] == "ok"


async def test_concurrent_401s_are_idempotent(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    """Clarifications Q1: two parallel 401s on the same installation
    are safe to fire — UPDATE on already-disabled row is a no-op,
    SecretNotFoundError on second delete is suppressed, ≤ 2 audit rows."""
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_id = await _seed_install(fresh_db, _tenant, secret_store)

    # assert_all_called=False because the second concurrent observer
    # may short-circuit at the secret-resolution step (the bot token
    # was deleted by the first chokepoint fire — exactly the
    # idempotent-re-runs behaviour Clarifications Q1 specifies).
    with respx.mock(base_url="https://discord.com", assert_all_called=False) as router:
        router.get(
            f"/api/v10/guilds/{_GUILD_ID}/members/{_USER_ID}",
        ).respond(401, json={"message": "401"})
        router.get(
            f"/api/v10/channels/{_USER_ID}",
        ).respond(401, json={"message": "401"})

        c1 = DiscordClient(
            pool=fresh_db, secret_store=secret_store,
            tenant_id=_tenant, installation_row_id=install_id,
            guild_id=_GUILD_ID,
        )
        c2 = DiscordClient(
            pool=fresh_db, secret_store=secret_store,
            tenant_id=_tenant, installation_row_id=install_id,
            guild_id=_GUILD_ID,
        )

        async def fire(client, fn):
            try:
                await fn(client)
            except DiscordApiError:
                pass

        await asyncio.gather(
            fire(c1, lambda c: c.get_guild_member(_USER_ID)),
            fire(c2, lambda c: c.get_channel(_USER_ID)),
        )
        await c1.aclose()
        await c2.aclose()

    # Final state: enabled=false.
    row = await fresh_db.fetchrow(
        "SELECT enabled FROM provider_installations WHERE id=$1", install_id,
    )
    assert row is not None and row["enabled"] is False
    # Secret gone.
    bot_count = await fresh_db.fetchval(
        "SELECT count(*) FROM encrypted_secrets "
        "WHERE tenant_id=$1 AND label=$2",
        _tenant, f"discord_bot_token:{_GUILD_ID}",
    )
    assert bot_count == 0
    # Audit row count is in [1, 2] per Clarifications Q1.
    audit_count = await fresh_db.fetchval(
        "SELECT count(*) FROM installation_audit_log "
        "WHERE installation_row_id=$1 AND action='uninstall'",
        install_id,
    )
    assert audit_count in (1, 2), (
        f"expected 1 or 2 audit rows under concurrent fires, got {audit_count}"
    )


async def test_403_code_50001_triggers_chokepoint(
    fresh_db: asyncpg.Pool, _tenant: UUID,
) -> None:
    """A 403 with Discord error code 50001 (Missing Access) is
    equivalent to a 401 — the chokepoint fires and the installation
    is disabled."""
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_id = await _seed_install(fresh_db, _tenant, secret_store)

    with respx.mock(base_url="https://discord.com") as router:
        router.get(
            f"/api/v10/guilds/{_GUILD_ID}/members/{_USER_ID}",
        ).respond(403, json={"message": "Missing Access", "code": 50001})

        client = DiscordClient(
            pool=fresh_db, secret_store=secret_store,
            tenant_id=_tenant, installation_row_id=install_id,
            guild_id=_GUILD_ID,
        )
        with pytest.raises(DiscordApiError) as exc_info:
            await client.get_guild_member(_USER_ID)
        await client.aclose()
        assert exc_info.value.code == "discord_api_unauthorized"

    row = await fresh_db.fetchrow(
        "SELECT enabled FROM provider_installations WHERE id=$1", install_id,
    )
    assert row is not None and row["enabled"] is False


async def test_disabled_installation_rejects_next_inbound(
    fresh_db: asyncpg.Pool, _tenant: UUID,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """After the chokepoint disables the installation, the next signed
    Discord interaction returns 401 unknown_installation, and the
    guild_id does NOT leak into logs (SC-006)."""
    pub_hex, sk = discord_keypair()
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", pub_hex)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    install_id = await _seed_install(fresh_db, _tenant, secret_store, pub_hex)

    # Manually flip enabled=FALSE.
    await fresh_db.execute(
        "UPDATE provider_installations SET enabled=FALSE WHERE id=$1",
        install_id,
    )
    # And delete the secrets (mimicking what the chokepoint would have done).
    await fresh_db.execute(
        "DELETE FROM encrypted_secrets WHERE tenant_id=$1 AND label=$2",
        _tenant, f"discord_bot_token:{_GUILD_ID}",
    )
    await fresh_db.execute(
        "DELETE FROM encrypted_secrets WHERE tenant_id=$1 AND label=$2",
        _tenant, f"discord_public_key:{_GUILD_ID}",
    )

    app = build_app(
        pool=fresh_db,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )
    app.state.secret_store = secret_store

    body = json.dumps({
        "id": _INTERACTION_ID,
        "type": 2,
        "application_id": _APP_ID,
        "guild_id": _GUILD_ID,
        "data": {"name": "fyralis", "options": [{"name": "ask", "type": 3, "value": "q"}]},
        "member": {"user": {"id": _USER_ID}},
    }).encode("utf-8")
    ts = int(time.time())
    sig = sk.sign(str(ts).encode("utf-8") + body).signature.hex()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.post(
            "/webhooks/discord/events",
            content=body,
            headers={
                "X-Signature-Ed25519": sig,
                "X-Signature-Timestamp": str(ts),
                "Content-Type": "application/json",
            },
        )

    # 401 unknown_installation (or secret_not_configured — both are
    # tenant-resolution failures with no observation written).
    assert r.status_code == 401, r.text
    body_json = r.json()
    assert body_json["context"]["provider"] == "discord"

    # NO observation row.
    obs_count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id=$1", _tenant,
    )
    assert obs_count == 0

    # SC-006: guild_id MUST NOT appear in structured log records.
    leaked = [r for r in caplog.records if _GUILD_ID in r.getMessage()]
    assert leaked == [], (
        f"guild_id leaked into structured logs: {[r.getMessage() for r in leaked]}"
    )
