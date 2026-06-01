"""IN-09 US1: Discord interaction ingestion tests.

End-to-end through the webhooks router: a signed Discord
`INTERACTION_CREATE` (type=2 ApplicationCommand) arrives for a guild
with a valid `provider_installations` row + encrypted public key,
and produces exactly one Observation under the right tenant with
`source_channel='discord:interaction'`, `content.text=<query verbatim>`,
no `token` in content metadata, and idempotency on the interaction id.

The PING test asserts that type=1 returns `{"type": 1}` (after Ed25519
signature verification using the env-var public-key fallback) without
producing any Observation.
"""
from __future__ import annotations

import json
import time
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.main import build_app
from services.gateway.rate_limit import RateLimiter
from services.webhooks.tests.conftest import discord_keypair


pytestmark = pytest.mark.integration


_GUILD_ID = "guild_test_US1_700000000000000001"
_USER_ID = "user_test_US1_700000000000000002"
_INTERACTION_ID = "interaction_test_US1_700000000000000003"
_APP_ID = "app_test_US1_700000000000000004"
_CHANNEL_ID = "channel_test_US1_700000000000000005"
_INTERACTION_TOKEN = "discord-interaction-credential-token-DO-NOT-LEAK"


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"discord-us1-{tid.hex[:8]}",
    )
    return tid


async def _seed_install(
    fresh_db: asyncpg.Pool, tenant_id: UUID, secret_store, pub_hex: str,
) -> tuple[UUID, str]:
    """Insert provider_installations row + encrypted public key + bot
    token in encrypted_secrets so a signed interaction can flow
    end-to-end. Returns (install_row_id, bot_ref)."""
    public_key_ref = await secret_store.put(
        pub_hex.encode("utf-8"),
        label=f"discord_public_key:{_GUILD_ID}",
        tenant_id=tenant_id,
    )
    bot_ref = await secret_store.put(
        b"discord-bot-token-test",
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
    return row_id, bot_ref


def _build_test_app(fresh_db: asyncpg.Pool, secret_store) -> FastAPI:
    app = build_app(
        pool=fresh_db,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )
    app.state.secret_store = secret_store
    return app


def _build_interaction_body(
    *,
    query: str = "smoke test from US1",
    interaction_id: str = _INTERACTION_ID,
    include_token: bool = True,
) -> bytes:
    body: dict = {
        "id": interaction_id,
        "type": 2,
        "application_id": _APP_ID,
        "guild_id": _GUILD_ID,
        "channel_id": _CHANNEL_ID,
        "member": {
            "user": {"id": _USER_ID, "username": "testuser"},
        },
        "data": {
            "id": "cmd_id",
            "name": "fyralis",
            "options": [{"name": "ask", "type": 3, "value": query}],
        },
    }
    if include_token:
        body["token"] = _INTERACTION_TOKEN
    return json.dumps(body).encode("utf-8")


def _sign(sk, ts: int, body: bytes) -> str:
    """Discord Ed25519: signature over `timestamp || body`, hex-encoded."""
    return sk.sign(str(ts).encode("utf-8") + body).signature.hex()


async def _post_interaction(
    app: FastAPI, sk, body: bytes, ts: int | None = None,
) -> httpx.Response:
    ts = ts if ts is not None else int(time.time())
    sig = _sign(sk, ts, body)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        return await c.post(
            "/webhooks/discord/events",
            content=body,
            headers={
                "X-Signature-Ed25519": sig,
                "X-Signature-Timestamp": str(ts),
                "Content-Type": "application/json",
            },
        )


async def test_interaction_lands_as_observation(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pub_hex, sk = discord_keypair()
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", pub_hex)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    await _seed_install(fresh_db, _tenant, secret_store, pub_hex)
    app = _build_test_app(fresh_db, secret_store)

    body = _build_interaction_body(query="What's our churn rate?")
    r = await _post_interaction(app, sk, body)

    # The router emits Discord's interaction-ack shape for type=2;
    # substrate metadata lands in response headers.
    assert r.status_code == 200, r.text
    body_json = r.json()
    assert body_json.get("type") == 4
    assert body_json.get("data", {}).get("flags") == 64  # EPHEMERAL
    assert "X-Observation-Id" in r.headers
    assert r.headers["X-Deduped"] == "false"

    row = await fresh_db.fetchrow(
        "SELECT source_channel, content_text, external_id, trust_tier, source_actor_ref "
        "FROM observations WHERE tenant_id=$1 AND source_channel='discord:interaction' "
        "ORDER BY occurred_at DESC LIMIT 1",
        _tenant,
    )
    assert row is not None
    assert row["source_channel"] == "discord:interaction"
    assert row["content_text"] == "What's our churn rate?"
    assert row["external_id"] == f"discord:{_INTERACTION_ID}"
    assert row["trust_tier"] == "attested_agent"
    assert row["source_actor_ref"] == f"discord:{_USER_ID}"


async def test_duplicate_interaction_id_is_idempotent(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pub_hex, sk = discord_keypair()
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", pub_hex)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    await _seed_install(fresh_db, _tenant, secret_store, pub_hex)
    app = _build_test_app(fresh_db, secret_store)

    body = _build_interaction_body(query="dup test")
    # Two posts of the same interaction (Discord retry-style). Re-sign
    # each with a fresh timestamp; signature differs but the interaction
    # id is identical → dedup on (source_channel, external_id).
    r1 = await _post_interaction(app, sk, body, ts=int(time.time()))
    r2 = await _post_interaction(app, sk, body, ts=int(time.time()))

    assert r1.status_code == 200
    assert r2.status_code == 200
    # Each Discord interaction returns the ack shape; dedup state is
    # in the X-Deduped response header.
    flags = sorted([r1.headers.get("X-Deduped"), r2.headers.get("X-Deduped")])
    assert flags == ["false", "true"], (r1.headers, r2.headers)

    count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id=$1 "
        "AND source_channel='discord:interaction' "
        "AND external_id=$2",
        _tenant, f"discord:{_INTERACTION_ID}",
    )
    assert count == 1


async def test_token_stripped_from_content_metadata(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pub_hex, sk = discord_keypair()
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", pub_hex)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    await _seed_install(fresh_db, _tenant, secret_store, pub_hex)
    app = _build_test_app(fresh_db, secret_store)

    body = _build_interaction_body(query="token strip test", include_token=True)
    r = await _post_interaction(app, sk, body)
    assert r.status_code in (200, 201), r.text

    persisted = await fresh_db.fetchval(
        "SELECT content::text FROM observations WHERE tenant_id=$1 "
        "AND source_channel='discord:interaction' "
        "ORDER BY occurred_at DESC LIMIT 1",
        _tenant,
    )
    assert persisted is not None
    # The literal token string MUST NOT appear anywhere in the
    # persisted content (FR-001 / Clarifications Q3).
    assert _INTERACTION_TOKEN not in persisted, (
        f"interaction token leaked into content jsonb: "
        f"{persisted[:200]}..."
    )


async def test_ping_returns_pong_via_router(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pub_hex, sk = discord_keypair()
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", pub_hex)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    # NO seed of provider_installations — PING precedes any install.
    app = _build_test_app(fresh_db, secret_store)

    body = json.dumps({"id": "ping_id", "application_id": _APP_ID, "type": 1}).encode("utf-8")
    r = await _post_interaction(app, sk, body)

    assert r.status_code == 200, r.text
    assert r.json() == {"type": 1}

    # No observation should have been written.
    count = await fresh_db.fetchval(
        "SELECT count(*) FROM observations WHERE tenant_id=$1",
        _tenant,
    )
    assert count == 0
