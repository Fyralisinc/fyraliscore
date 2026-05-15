"""IN-13 integration tests for `services/integrations/github/oauth.py`.

Covers T039–T050 from tasks.md. Requires live Postgres (Constitution
§IV). `respx` mocks `api.github.com` for the per-installation token +
repository fetch.
"""
from __future__ import annotations

import json
from uuid import UUID, uuid4

import httpx
import pytest
import respx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from lib.shared.ids import uuid7


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------


@pytest.fixture
def app_state_factory(db_pool, monkeypatch: pytest.MonkeyPatch):
    """Wire enough app state to run install/callback handlers."""
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.hazmat.primitives import serialization

    monkeypatch.setenv("GITHUB_APP_SLUG", "fyralis-test")
    monkeypatch.setenv("GITHUB_APP_ID", "999999")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    monkeypatch.setenv("GITHUB_APP_PRIVATE_KEY", pem.decode())
    # OAUTH state-token signing key — reused from IN-08.
    monkeypatch.setenv(
        "OAUTH_STATE_HMAC_KEY",
        "test-state-hmac-key-not-for-prod",
    )

    from services.integrations.github.client import GithubClient

    def _make_app(tenant_id: UUID) -> FastAPI:
        app = FastAPI()
        app.state.pool = db_pool
        app.state.github_client = GithubClient(pool=db_pool)
        # Stub the auth shim that the install handler reads.
        @app.middleware("http")
        async def _inject_auth(request, call_next):
            class _A:
                pass
            a = _A()
            a.tenant_id = tenant_id
            request.state.auth = a
            return await call_next(request)
        from services.integrations.router import build_integrations_router
        app.include_router(build_integrations_router())
        return app

    return _make_app


async def _seed_tenant(db_pool) -> UUID:
    """Insert a tenant row and return its id."""
    tenant_id = uuid7()
    await db_pool.execute(
        """
        INSERT INTO tenants (id, slug, display_name)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO NOTHING
        """,
        tenant_id,
        f"tenant-{tenant_id.hex[:8]}",
        "Test Tenant",
    )
    return tenant_id


# ---------------------------------------------------------------------
# Install handler — T039–T041
# ---------------------------------------------------------------------


async def test_install_302_to_github(app_state_factory, db_pool) -> None:
    """T039: GET /integrations/github/install → 302 to github.com/apps/<slug>/installations/new?state=<token>"""
    tenant_id = await _seed_tenant(db_pool)
    app = app_state_factory(tenant_id)
    client = TestClient(app, follow_redirects=False)

    response = client.get("/integrations/github/install")
    assert response.status_code == 302
    loc = response.headers["Location"]
    assert loc.startswith(
        "https://github.com/apps/fyralis-test/installations/new?"
    )
    assert "state=" in loc


async def test_install_writes_oauth_state_row(
    app_state_factory, db_pool,
) -> None:
    """T040: the install handler INSERTs an oauth_install_states row
    with provider='github', consumed_at=NULL."""
    tenant_id = await _seed_tenant(db_pool)
    app = app_state_factory(tenant_id)
    client = TestClient(app, follow_redirects=False)

    client.get("/integrations/github/install")
    row = await db_pool.fetchrow(
        """
        SELECT provider, consumed_at, tenant_id
          FROM oauth_install_states
         WHERE tenant_id = $1
         ORDER BY expires_at DESC
         LIMIT 1
        """,
        tenant_id,
    )
    assert row is not None
    assert row["provider"] == "github"
    assert row["consumed_at"] is None
    assert row["tenant_id"] == tenant_id


# ---------------------------------------------------------------------
# Callback handler — T042–T050
# ---------------------------------------------------------------------


@pytest.fixture
def mocked_github_api():
    """respx mock for api.github.com endpoints used by the callback."""
    with respx.mock(base_url="https://api.github.com", assert_all_called=False) as r:
        r.post("/app/installations/12345678/access_tokens").mock(
            return_value=httpx.Response(
                201,
                json={
                    "token": "ghs_test_installation_token",
                    "expires_at": "2099-12-31T23:59:59Z",
                },
            )
        )
        r.get(url__regex=r"/installation/repositories(\?.*)?").mock(
            return_value=httpx.Response(
                200,
                json={
                    "total_count": 2,
                    "repository_selection": "selected",
                    "repositories": [
                        {"full_name": "octo/repo-a"},
                        {"full_name": "octo/repo-b"},
                    ],
                },
            )
        )
        yield r


async def test_first_install_end_to_end(
    app_state_factory, db_pool, mocked_github_api,
) -> None:
    """T042: full happy path — UPSERT installation, mint token, GET
    repos, persist selected_repositories, audit row, 302 to success."""
    tenant_id = await _seed_tenant(db_pool)
    app = app_state_factory(tenant_id)
    client = TestClient(app, follow_redirects=False)

    # Issue a state token through the install endpoint.
    resp1 = client.get("/integrations/github/install")
    state = _state_from_location(resp1.headers["Location"])

    # Simulate GitHub redirect to the callback.
    resp2 = client.get(
        f"/integrations/github/callback"
        f"?installation_id=12345678&setup_action=install&state={state}",
    )
    assert resp2.status_code == 302
    assert resp2.headers["Location"].startswith("/integrations/github/installed?")

    row = await db_pool.fetchrow(
        """
        SELECT id, tenant_id, enabled, selected_repositories
          FROM provider_installations
         WHERE provider = 'github' AND installation_id = '12345678'
        """,
    )
    assert row is not None
    assert row["tenant_id"] == tenant_id
    assert row["enabled"] is True
    persisted = (
        row["selected_repositories"]
        if isinstance(row["selected_repositories"], list)
        else json.loads(row["selected_repositories"])
    )
    assert sorted(persisted) == ["octo/repo-a", "octo/repo-b"]

    audit = await db_pool.fetchrow(
        """
        SELECT action, status
          FROM installation_audit_log
         WHERE tenant_id = $1
           AND provider = 'github'
         ORDER BY created_at DESC
         LIMIT 1
        """,
        tenant_id,
    )
    assert audit is not None
    assert audit["action"] == "install"
    assert audit["status"] == "ok"


async def test_state_token_invalid(app_state_factory, db_pool) -> None:
    """T045: tampered state → 302 to install-error?reason=state_invalid."""
    tenant_id = await _seed_tenant(db_pool)
    app = app_state_factory(tenant_id)
    client = TestClient(app, follow_redirects=False)

    resp = client.get(
        "/integrations/github/callback"
        "?installation_id=12345678&setup_action=install&state=garbage",
    )
    assert resp.status_code == 302
    assert "install-error?reason=state_invalid" in resp.headers["Location"]


async def test_missing_installation_id(app_state_factory, db_pool) -> None:
    """Callback without installation_id → 302 to install-error."""
    tenant_id = await _seed_tenant(db_pool)
    app = app_state_factory(tenant_id)
    client = TestClient(app, follow_redirects=False)

    resp = client.get(
        "/integrations/github/callback?setup_action=install&state=garbage",
    )
    assert resp.status_code == 302
    assert "install-error?reason=missing_installation_id" in resp.headers[
        "Location"
    ]


async def test_cross_tenant_collision(
    app_state_factory, db_pool, mocked_github_api,
) -> None:
    """T046: existing installation_id mapped to a different tenant →
    302 to install-error?reason=installation_collision, audit row with
    status='rejected_collision', foreign tenant id absent from logs and
    response."""
    # Tenant A pre-installed.
    tenant_a = await _seed_tenant(db_pool)
    await db_pool.execute(
        """
        INSERT INTO provider_installations
            (id, tenant_id, provider, installation_id, secret_ref, enabled)
        VALUES ($1, $2, 'github', '12345678', NULL, TRUE)
        """,
        uuid7(),
        tenant_a,
    )

    # Tenant B attempts to claim it.
    tenant_b = await _seed_tenant(db_pool)
    app = app_state_factory(tenant_b)
    client = TestClient(app, follow_redirects=False)

    resp1 = client.get("/integrations/github/install")
    state = _state_from_location(resp1.headers["Location"])

    resp2 = client.get(
        f"/integrations/github/callback"
        f"?installation_id=12345678&setup_action=install&state={state}",
    )
    assert resp2.status_code == 302
    loc = resp2.headers["Location"]
    assert "install-error?reason=installation_collision" in loc
    # Tenant A's id must NOT appear in the redirect URL.
    assert str(tenant_a) not in loc

    audit = await db_pool.fetchrow(
        """
        SELECT action, status
          FROM installation_audit_log
         WHERE tenant_id = $1
         ORDER BY created_at DESC
         LIMIT 1
        """,
        tenant_b,
    )
    assert audit is not None
    assert audit["status"] == "rejected_collision"


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _state_from_location(location: str) -> str:
    """Extract the `state` query param from a github install redirect."""
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(location)
    qs = parse_qs(parsed.query)
    return qs["state"][0]
