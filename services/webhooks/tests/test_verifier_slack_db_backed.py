"""IN-08 US1 end-to-end: a Slack webhook is verified using a signing
secret pulled from `encrypted_secrets` (via `provider_installations
.secret_ref`), with the env-var fallback explicitly disabled.

Covers FR-001..FR-005, SC-001, SC-002.

The router still uses the IN-06 env-var tenant_resolver in US1
(US2 swaps that to the DB resolver). So the test mixes the two
mechanisms — DB-backed secrets, env-mapped tenant — exactly the
state immediately after US1 ships and before US2 lands.
"""
from __future__ import annotations

import json
import time
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
from cryptography.fernet import Fernet

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.gateway.main import build_app
from services.gateway.rate_limit import RateLimiter
from services.webhooks.tests.conftest import slack_sign


pytestmark = pytest.mark.integration


@pytest.fixture
async def _tenant(fresh_db: asyncpg.Pool) -> UUID:
    tenant_id = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id,
        f"in08-us1-{tenant_id.hex[:8]}",
    )
    return tenant_id


async def test_signed_slack_verifies_via_db_secret_ref(
    fresh_db: asyncpg.Pool,
    _tenant: UUID,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end: a Slack webhook signed with a secret stored in
    `encrypted_secrets` verifies via the DB-backed `load_secrets`
    path, with the env-var fallback explicitly OFF."""
    # SC-002 prerequisite: env-var fallback OFF. The webhooks conftest
    # turns it on by autouse; explicitly disable here.
    monkeypatch.delenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", raising=False)
    monkeypatch.delenv("WEBHOOK_SECRET_SLACK", raising=False)

    # Seed: secret in the store, installation row pointing at it.
    signing_secret = "DB-backed-slack-signing-secret-7F"
    store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    ref = await store.put(
        signing_secret.encode("utf-8"),
        label="slack_signing_secret:app",
        tenant_id=_tenant,
    )
    team_id = "T_IN08_US1"
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, $3, $4, $5, TRUE)",
        uuid7(),
        _tenant,
        "slack",
        team_id,
        ref,
    )

    # Build the app, but inject OUR store so the test's plaintext is
    # the one the router resolves through. Without this, build_app's
    # lifespan would construct its own store with a fresh KEK and
    # could not decrypt our ciphertext.
    app = build_app(
        pool=fresh_db,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )
    app.state.secret_store = store

    # US1-era tenant resolution still uses the env-var path; US2
    # replaces this with the DB resolver. Until then, map team_id →
    # tenant via env var so the router can complete resolution.
    monkeypatch.setenv(
        f"WEBHOOK_TENANT_SLACK_{team_id.upper()}", str(_tenant),
    )

    # Build and sign a Slack message payload.
    body = json.dumps(
        {
            "team_id": team_id,
            "event": {
                "type": "message",
                "text": "db-backed-secret-works",
                "ts": str(time.time()),
                "channel": "C_TEST",
                "user": "U_TEST",
            },
        }
    ).encode("utf-8")
    ts = int(time.time())
    sig = slack_sign(signing_secret, body, ts)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver",
    ) as client:
        r = await client.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )

    assert r.status_code in (200, 201), r.text
    response_body = r.json()
    # SC-001: secret_label confirms the secret came from `encrypted_secrets`
    # (the DB-backed path uses `installation:<ref>` labels; the env-var
    # path would use the env-var label or None).
    assert response_body.get("secret_label", "").startswith("installation:")


async def test_db_ref_unresolvable_returns_401_no_env_leak(
    fresh_db: asyncpg.Pool,
    _tenant: UUID,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the installation row's `secret_ref` cannot be resolved
    (dangling pointer) AND the env-var fallback is off, the verifier
    sees no secret and the router returns 401 `secret_not_configured`.

    Crucially, it MUST NOT silently fall back to env vars even if
    `WEBHOOK_SECRET_SLACK` is set (that would be the SC-002 violation
    this story prevents).
    """
    monkeypatch.delenv("WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW", raising=False)
    monkeypatch.setenv("WEBHOOK_SECRET_SLACK", "this-would-be-a-leak")

    team_id = "T_IN08_DANGLE"
    # Dangling ref: looks like a UUID, doesn't resolve to a row.
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, $3, $4, $5, TRUE)",
        uuid7(),
        _tenant,
        "slack",
        team_id,
        str(uuid7()),  # ref that points to nothing
    )

    app = build_app(
        pool=fresh_db,
        actor_repo=ActorRepo(fresh_db),
        alias_repo=EntityAliasRepo(fresh_db),
        embedder=None,
        rate_limiter=RateLimiter(),
        configure_logging=False,
    )
    app.state.secret_store = FernetSecretStore(
        fresh_db, master_kek=Fernet.generate_key(),
    )
    monkeypatch.setenv(
        f"WEBHOOK_TENANT_SLACK_{team_id.upper()}", str(_tenant),
    )

    body = json.dumps({"team_id": team_id, "event": {"type": "message"}}).encode()
    ts = int(time.time())
    # Sign with the env-var "leak" secret — if the router fell back
    # to env, this signature would verify.
    sig = slack_sign("this-would-be-a-leak", body, ts)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver",
    ) as client:
        r = await client.post(
            "/webhooks/slack/events",
            content=body,
            headers={
                "Content-Type": "application/json",
                "X-Slack-Request-Timestamp": str(ts),
                "X-Slack-Signature": sig,
            },
        )

    # 401 secret_not_configured — env fallback did NOT take effect.
    assert r.status_code == 401
    err = r.json()
    assert err["context"]["reason"] == "secret_not_configured"
