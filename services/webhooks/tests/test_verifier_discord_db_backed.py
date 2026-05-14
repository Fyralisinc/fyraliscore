"""IN-09 polish: regression tests for the IN-08 DB-backed secret-store
path applied to Discord.

T046: signed Discord interaction whose `provider_installations.secret_ref`
      points at the `discord_public_key:<guild_id>` row in encrypted_secrets
      verifies successfully, with `secret_label='installation:<ref>'` in
      the verifier context.
T047: a PING (type=1) with no matching install row still verifies via
      the env-var WEBHOOK_SECRET_DISCORD fallback (FR-003).
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


_GUILD_ID = "G_REG_700000000000000001"
_APP_ID = "A_REG_700000000000000002"
_INTERACTION_ID = "I_REG_700000000000000003"
_USER_ID = "U_REG_700000000000000004"


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"discord-reg-{tid.hex[:8]}",
    )
    return tid


def _make_app(fresh_db: asyncpg.Pool, secret_store) -> FastAPI:
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


def _sign(sk, ts: int, body: bytes) -> str:
    return sk.sign(str(ts).encode("utf-8") + body).signature.hex()


async def test_signed_interaction_resolves_via_db_backed_public_key(
    fresh_db: asyncpg.Pool, _tenant: UUID, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A signed Discord interaction is verified using the per-installation
    public key stored in encrypted_secrets — NOT the env-var fallback."""
    pub_hex, sk = discord_keypair()
    # Set a DIFFERENT env-var public key so any accidental fallback
    # would fail verification — proving the DB path was used.
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", "b" * 64)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())

    public_key_ref = await secret_store.put(
        pub_hex.encode("utf-8"),
        label=f"discord_public_key:{_GUILD_ID}",
        tenant_id=_tenant,
    )
    await secret_store.put(
        b"discord-bot-token",
        label=f"discord_bot_token:{_GUILD_ID}",
        tenant_id=_tenant,
    )
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'discord', $3, $4, TRUE)",
        uuid7(), _tenant, _GUILD_ID, public_key_ref,
    )

    body = json.dumps({
        "id": _INTERACTION_ID,
        "type": 2,
        "application_id": _APP_ID,
        "guild_id": _GUILD_ID,
        "channel_id": "C_TEST",
        "member": {"user": {"id": _USER_ID}},
        "data": {
            "name": "fyralis",
            "options": [{"name": "ask", "type": 3, "value": "regression test"}],
        },
    }).encode("utf-8")
    ts = int(time.time())
    sig = _sign(sk, ts, body)

    app = _make_app(fresh_db, secret_store)
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

    # Verification succeeded → observation written.
    assert r.status_code in (200, 201), r.text
    body_json = r.json()
    assert body_json.get("observation_id")
    # The secret label in the response identifies the DB-backed path
    # (label prefix is 'installation:<ref>' per services/webhooks/secrets.py).
    secret_label = body_json.get("secret_label", "")
    assert secret_label.startswith("installation:"), (
        f"expected DB-backed path (installation:<ref>), got {secret_label!r}"
    )


async def test_ping_uses_env_var_public_key_when_no_install_row(
    fresh_db: asyncpg.Pool, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """FR-003: PING precedes any provider_installations row. Signature
    verification falls back to the env-var WEBHOOK_SECRET_DISCORD."""
    pub_hex, sk = discord_keypair()
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", pub_hex)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())

    body = json.dumps({"id": "ping_xyz", "application_id": "A_X", "type": 1}).encode("utf-8")
    ts = int(time.time())
    sig = _sign(sk, ts, body)

    app = _make_app(fresh_db, secret_store)
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

    assert r.status_code == 200, r.text
    assert r.json() == {"type": 1}


async def test_unknown_guild_returns_unknown_installation_no_leak(
    fresh_db: asyncpg.Pool, _tenant: UUID,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """T048: a signed interaction for a guild_id with no install row
    returns 401 unknown_installation; the guild_id MUST NOT appear in
    structured log records (SC-006)."""
    pub_hex, sk = discord_keypair()
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", pub_hex)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())

    secret_guild = "G_UNKNOWN_70999999"
    body = json.dumps({
        "id": "ix-unknown",
        "type": 2,
        "application_id": "A_X",
        "guild_id": secret_guild,
        "member": {"user": {"id": "U_X"}},
        "data": {"name": "fyralis", "options": [{"name": "ask", "type": 3, "value": "q"}]},
    }).encode("utf-8")
    ts = int(time.time())
    sig = _sign(sk, ts, body)

    app = _make_app(fresh_db, secret_store)
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

    assert r.status_code == 401, r.text
    body_json = r.json()
    assert body_json["context"]["provider"] == "discord"
    # The unknown guild_id MUST NOT appear in any structured log record.
    leaked = [r for r in caplog.records if secret_guild in r.getMessage()]
    assert leaked == [], (
        f"guild_id leaked into logs: {[r.getMessage() for r in leaked]}"
    )
