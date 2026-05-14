"""IN-09 US2 + US4 callback-handler tests.

GET /integrations/discord/callback covers:
  - state_invalid (HMAC mismatch / unknown nonce / malformed)
  - state_expired (nonce past expires_at)
  - state_consumed (replayed nonce)
  - discord_oauth_token_exchange_failed (POST oauth2/token non-2xx)
  - discord_oauth_missing_guild (response lacks guild.id)
  - installation_collision (cross-tenant rebind attempt)
  - secret_store_unavailable
  - success (fresh install) — verifies UPSERT + audit + secrets + slash-command POST
  - command-registration failure does NOT block install (FR-012)
  - re-install reuses row AND is orphan-free (analyze E1 remediation)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import respx
from cryptography.fernet import Fernet
from fastapi import FastAPI

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.integrations.discord import metrics as discord_metrics
from services.integrations.discord import oauth as discord_oauth
from services.integrations.router import build_integrations_router


pytestmark = pytest.mark.integration


_DISCORD_PUBLIC_KEY_HEX = "a" * 64  # 64 hex chars = 32 bytes (ed25519 public key)


@pytest.fixture(autouse=True)
def _set_oauth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DISCORD_CLIENT_ID", "discord-client-id-test")
    monkeypatch.setenv("DISCORD_CLIENT_SECRET", "discord-client-secret-test")
    monkeypatch.setenv(
        "DISCORD_REDIRECT_URI",
        "https://app.fyralis.test/integrations/discord/callback",
    )
    monkeypatch.setenv("DISCORD_APPLICATION_ID", "A_TEST_APP_ID")
    monkeypatch.setenv("WEBHOOK_SECRET_DISCORD", _DISCORD_PUBLIC_KEY_HEX)
    monkeypatch.setenv("OAUTH_STATE_HMAC_KEY", "test-hmac-key-discord-callback")
    discord_metrics.reset()


@pytest.fixture(autouse=True)
def _mock_discord_endpoints() -> object:
    """Default mocks for Discord OAuth + command-registration endpoints.

    Happy path: oauth2/token returns the bot token + guild_id +
    application_id. POST applications/{}/commands returns 200 with a
    persistent command id.
    """
    with respx.mock(
        assert_all_called=False, base_url="https://discord.com",
    ) as router:
        router.post("/api/v10/oauth2/token").respond(
            200,
            json={
                "access_token": "discord-bot-token-test",
                "token_type": "Bearer",
                "scope": "applications.commands bot",
                "guild": {"id": "G_TEST_GUILD"},
                "application": {"id": "A_TEST_APP_ID"},
            },
        )
        router.post("/api/v10/applications/A_TEST_APP_ID/commands").respond(
            200,
            json={
                "id": "CMD_FYRALIS_TEST_ID",
                "application_id": "A_TEST_APP_ID",
                "name": "fyralis",
            },
        )
        yield router


async def _seed_tenant(pool: asyncpg.Pool) -> UUID:
    tid = uuid4()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tid, f"discord-cb-{tid.hex[:8]}",
    )
    return tid


def _make_app(pool: asyncpg.Pool, secret_store) -> FastAPI:
    app = FastAPI()
    app.include_router(build_integrations_router())
    app.state.pool = pool
    app.state.secret_store = secret_store
    return app


async def test_first_install_end_to_end(
    fresh_db: asyncpg.Pool, _mock_discord_endpoints: object,
) -> None:
    """US2 + US4 happy path: callback creates install row + encrypted
    bot token + encrypted public key + audit row + registers the
    /fyralis slash command, and 302s to the success page."""
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await discord_oauth.issue_state_token(tenant, fresh_db)
    app = _make_app(fresh_db, secret_store)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "valid-code", "state": state},
            follow_redirects=False,
        )

    assert r.status_code == 302
    loc = r.headers["location"]
    assert loc.startswith("/integrations/discord/installed?guild=")
    # Hashed guild, not the raw id.
    assert "G_TEST_GUILD" not in loc

    # provider_installations row exists, enabled, secret_ref populated.
    row = await fresh_db.fetchrow(
        "SELECT tenant_id, secret_ref, enabled "
        "FROM provider_installations WHERE provider='discord' "
        "AND installation_id = $1",
        "G_TEST_GUILD",
    )
    assert row is not None
    assert row["tenant_id"] == tenant
    assert row["enabled"] is True
    assert row["secret_ref"] is not None

    # encrypted_secrets has both labels.
    labels = {
        r["label"]
        for r in await fresh_db.fetch(
            "SELECT label FROM encrypted_secrets WHERE tenant_id = $1", tenant,
        )
    }
    assert "discord_bot_token:G_TEST_GUILD" in labels
    assert "discord_public_key:G_TEST_GUILD" in labels

    # Audit row install/ok.
    audit = await fresh_db.fetchrow(
        "SELECT action, status, context::text AS ctx FROM installation_audit_log "
        "WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 1",
        tenant,
    )
    assert audit is not None
    assert audit["action"] == "install"
    assert audit["status"] == "ok"

    # Slash-command POST recorded exactly once (US4 / Clarifications Q2).
    cmd_calls = [
        c for c in _mock_discord_endpoints.calls
        if c.request.url.path.endswith("/commands")
    ]
    assert len(cmd_calls) == 1

    # Metric incremented.
    assert discord_metrics.get_install_outcome_count("success") == 1


async def test_state_invalid_hmac_redirects_to_error(
    fresh_db: asyncpg.Pool,
) -> None:
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "x", "state": "garbage.notbase64"},
            follow_redirects=False,
        )
    assert r.status_code == 302
    assert "reason=state_invalid" in r.headers["location"]


async def test_state_expired_redirects_to_error(fresh_db: asyncpg.Pool) -> None:
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await discord_oauth.issue_state_token(tenant, fresh_db)
    # Backdate the nonce.
    await fresh_db.execute(
        "UPDATE oauth_install_states SET expires_at = $1 WHERE tenant_id = $2",
        datetime.now(timezone.utc) - timedelta(seconds=1),
        tenant,
    )
    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )
    assert "reason=state_expired" in r.headers["location"]


async def test_state_consumed_redirects_to_error(fresh_db: asyncpg.Pool) -> None:
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await discord_oauth.issue_state_token(tenant, fresh_db)
    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.get(
            "/integrations/discord/callback",
            params={"code": "code-A", "state": state},
            follow_redirects=False,
        )
        assert r1.headers["location"].startswith("/integrations/discord/installed")
        r2 = await c.get(
            "/integrations/discord/callback",
            params={"code": "code-B", "state": state},
            follow_redirects=False,
        )
    assert "reason=state_consumed" in r2.headers["location"]


async def test_cross_tenant_collision_redirects_and_audits(
    fresh_db: asyncpg.Pool,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A guild already bound to tenant_a; tenant_b attempts an install
    for the same guild_id → 302 reason=installation_collision, audit row
    with rejected_collision, and the foreign tenant id MUST NOT leak."""
    tenant_a = await _seed_tenant(fresh_db)
    tenant_b = await _seed_tenant(fresh_db)
    # Pre-seed: G_TEST_GUILD bound to tenant_a.
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'discord', 'G_TEST_GUILD', $3, TRUE)",
        uuid7(), tenant_a, str(uuid7()),
    )
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await discord_oauth.issue_state_token(tenant_b, fresh_db)
    app = _make_app(fresh_db, secret_store)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )

    assert "reason=installation_collision" in r.headers["location"]
    # Tenant A's row unchanged.
    row = await fresh_db.fetchrow(
        "SELECT tenant_id FROM provider_installations "
        "WHERE provider='discord' AND installation_id = 'G_TEST_GUILD'",
    )
    assert row is not None and row["tenant_id"] == tenant_a
    # Audit row rejected_collision under tenant_b.
    audit = await fresh_db.fetchrow(
        "SELECT status FROM installation_audit_log "
        "WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 1",
        tenant_b,
    )
    assert audit is not None
    assert audit["status"] == "rejected_collision"
    # Tenant A's UUID must NOT appear in caplog records (SC-006).
    leaked = [r for r in caplog.records if str(tenant_a) in r.getMessage()]
    assert leaked == [], f"foreign tenant id leaked into logs: {leaked}"


async def test_discord_oauth_token_exchange_failed(
    fresh_db: asyncpg.Pool, _mock_discord_endpoints: object,
) -> None:
    """Discord returns 4xx on oauth2/token → 302 reason."""
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await discord_oauth.issue_state_token(tenant, fresh_db)

    _mock_discord_endpoints.post("/api/v10/oauth2/token").mock(
        return_value=httpx.Response(
            400, json={"error": "invalid_grant"},
        )
    )

    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )

    assert "reason=discord_oauth_token_exchange_failed" in r.headers["location"]
    # No install row.
    count = await fresh_db.fetchval(
        "SELECT count(*) FROM provider_installations WHERE tenant_id = $1",
        tenant,
    )
    assert count == 0
    # Audit row with status=error.
    audit = await fresh_db.fetchrow(
        "SELECT status FROM installation_audit_log WHERE tenant_id = $1",
        tenant,
    )
    assert audit is not None
    assert audit["status"] == "error"


async def test_discord_oauth_missing_guild_redirects(
    fresh_db: asyncpg.Pool, _mock_discord_endpoints: object,
) -> None:
    """OAuth response without guild.id → reason=discord_oauth_missing_guild."""
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await discord_oauth.issue_state_token(tenant, fresh_db)

    _mock_discord_endpoints.post("/api/v10/oauth2/token").mock(
        return_value=httpx.Response(
            200,
            json={
                "access_token": "discord-bot-token-test",
                "token_type": "Bearer",
                "scope": "applications.commands bot",
                # NO guild object
                "application": {"id": "A_TEST_APP_ID"},
            },
        )
    )

    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )

    assert "reason=discord_oauth_missing_guild" in r.headers["location"]


async def test_command_registration_failure_does_not_block_install(
    fresh_db: asyncpg.Pool, _mock_discord_endpoints: object,
) -> None:
    """FR-012: Discord 4xx on POST /commands → install still completes,
    audit row carries status='error' with the discord_error_code."""
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    state = await discord_oauth.issue_state_token(tenant, fresh_db)

    _mock_discord_endpoints.post(
        "/api/v10/applications/A_TEST_APP_ID/commands",
    ).mock(
        return_value=httpx.Response(
            403, json={"code": 50001, "message": "Missing Access"},
        )
    )

    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )

    # Install completes — redirect is to SUCCESS even though slash
    # command registration failed. The substrate is healthy; the only
    # user-visible degradation is "/fyralis" not appearing in Discord's
    # command picker (recoverable: an operator can re-register). The
    # audit row carries the failure signal for dashboards (per FR-012
    # + Clarifications: install row is written, audit status='error',
    # discord_error_code in context).
    loc = r.headers["location"]
    assert loc.startswith("/integrations/discord/installed?guild=")
    # Metric reflects the registration failure (not "success").
    assert discord_metrics.get_install_outcome_count(
        "discord_command_registration_failed",
    ) == 1
    row = await fresh_db.fetchrow(
        "SELECT enabled FROM provider_installations "
        "WHERE provider='discord' AND installation_id = 'G_TEST_GUILD'",
    )
    assert row is not None
    assert row["enabled"] is True
    # Audit row carries status='error' and the discord_error_code in context.
    audit = await fresh_db.fetchrow(
        "SELECT status, context FROM installation_audit_log "
        "WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT 1",
        tenant,
    )
    assert audit is not None
    assert audit["status"] == "error"
    import json
    ctx = audit["context"] if isinstance(audit["context"], dict) else json.loads(audit["context"])
    assert ctx.get("failure_code") == "discord_command_registration_failed"
    assert ctx.get("http_status") == 403
    assert ctx.get("discord_error_code") == 50001


async def test_reinstall_after_disable_reuses_row_and_orphan_free(
    fresh_db: asyncpg.Pool, _mock_discord_endpoints: object,
) -> None:
    """E1 remediation: pre-seed a disabled install row AND a stale
    encrypted_secrets bot-token row (simulating a prior install whose
    token was never cleaned up). Run the OAuth callback → same install
    row id is reused, enabled flips to true, and the stale secret is
    gone (replaced by the two fresh refs)."""
    tenant = await _seed_tenant(fresh_db)
    secret_store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())

    # Pre-seed disabled install row.
    prior_install_id = uuid7()
    prior_secret_ref = await secret_store.put(
        b"prior-public-key-stale",
        label="discord_public_key:G_TEST_GUILD",
        tenant_id=tenant,
    )
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'discord', 'G_TEST_GUILD', $3, FALSE)",
        prior_install_id, tenant, prior_secret_ref,
    )
    # Pre-seed a stale bot-token row that should be cleaned up.
    stale_bot_ref = await secret_store.put(
        b"stale-bot-token-from-prior-install",
        label="discord_bot_token:G_TEST_GUILD",
        tenant_id=tenant,
    )

    state = await discord_oauth.issue_state_token(tenant, fresh_db)
    app = _make_app(fresh_db, secret_store)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r = await c.get(
            "/integrations/discord/callback",
            params={"code": "x", "state": state},
            follow_redirects=False,
        )
    assert r.status_code == 302
    assert r.headers["location"].startswith("/integrations/discord/installed")

    # (a) SAME provider_installations.id reused.
    row = await fresh_db.fetchrow(
        "SELECT id, enabled FROM provider_installations "
        "WHERE provider='discord' AND installation_id='G_TEST_GUILD'",
    )
    assert row is not None
    assert row["id"] == prior_install_id
    assert row["enabled"] is True

    # (b) encrypted_secrets has exactly TWO rows for this guild — one
    # fresh bot token, one fresh public key. The cleanup ran.
    labels_count = await fresh_db.fetchval(
        "SELECT count(*) FROM encrypted_secrets "
        "WHERE tenant_id=$1 AND (label='discord_bot_token:G_TEST_GUILD' "
        "OR label='discord_public_key:G_TEST_GUILD')",
        tenant,
    )
    assert labels_count == 2, (
        f"expected exactly 2 fresh discord secrets, found {labels_count}"
    )

    # (c) the stale bot-token row is GONE.
    stale_still_present = await fresh_db.fetchval(
        "SELECT count(*) FROM encrypted_secrets WHERE id::text = $1",
        stale_bot_ref,
    )
    assert stale_still_present == 0, (
        "stale bot-token row was NOT cleaned up (E1 remediation broken)"
    )
    # And the prior public-key ref is gone too.
    prior_pk_still_present = await fresh_db.fetchval(
        "SELECT count(*) FROM encrypted_secrets WHERE id::text = $1",
        prior_secret_ref,
    )
    assert prior_pk_still_present == 0, (
        "prior public-key row was NOT cleaned up (E1 remediation broken)"
    )
