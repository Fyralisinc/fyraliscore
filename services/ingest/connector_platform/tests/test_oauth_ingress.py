from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
import respx
from fastapi import FastAPI, Request

from services.ingest.connector_platform import oauth_ingress
from services.ingest.connector_platform.catalog import build_connector_runtime
from services.ingest.connector_platform.oauth_ingress import (
    execute_configuration_install,
    execute_oauth_callback,
    execute_oauth_install,
)


class _Pool:
    def __init__(self, tenant_id: UUID, source: str) -> None:
        self.tenant_id = tenant_id
        self.source = source
        self.connector_id = f"fyralis/{source}"
        self.installation_id: UUID | None = None
        self.queries: list[str] = []

    async def execute(self, query, *args):
        self.queries.append(query)
        if "INSERT INTO source_connector_installations" in query:
            self.installation_id = args[0]
        return "INSERT 0 1"

    async def fetchrow(self, query, *args):
        self.queries.append(query)
        if "FROM source_connector_authority_grants" in query:
            manifest = build_connector_runtime().registry.for_source(
                self.source
            ).manifest
            return {
                "installation_id": self.installation_id,
                "tenant_id": self.tenant_id,
                "connector_id": self.connector_id,
                "authority_generation": 1,
                "credential_owner": "connector_bootstrap",
                "granted_slot_names": list(manifest.spec.permissions.secret_slots),
                "granted_scopes": list(manifest.spec.permissions.requested_scopes),
                "granted_outbound_hosts": list(
                    manifest.spec.permissions.outbound_hosts
                ),
                "maximum_trust_tier": manifest.spec.trust.maximum_tier,
                "provenance": {},
                "granted_at": datetime.now(timezone.utc),
                "revoked_at": None,
            }
        if "SELECT tenant_id" in query and "source_connector_installations" in query:
            return None
        raise AssertionError(query)

    async def fetch(self, query, *args):
        self.queries.append(query)
        if "FROM source_connector_credentials" in query:
            return []
        raise AssertionError(query)

    def acquire(self):
        return self

    def transaction(self):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None


class _Secrets:
    def __init__(self) -> None:
        self.labels: list[str] = []

    async def put(self, _value, *, label, tenant_id):
        assert tenant_id
        self.labels.append(label)
        return str(uuid4())

    async def get(self, _ref, *, tenant_id):
        raise AssertionError("installation ingress must not read stored credentials")


def _state_app(pool: _Pool, secrets: _Secrets) -> FastAPI:
    app = FastAPI()
    app.state.pool = pool
    app.state.secret_store = secrets
    app.state.source_connector_runtime = build_connector_runtime()
    return app


@pytest.mark.asyncio
async def test_install_authorization_redirect_is_connector_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    pool = _Pool(tenant_id, "slack")
    app = _state_app(pool, _Secrets())
    monkeypatch.setenv("SLACK_CLIENT_ID", "client")
    monkeypatch.setenv("SLACK_REDIRECT_URI", "https://app.test/slack/callback")

    @app.get("/install")
    async def install(request: Request):
        request.state.auth = SimpleNamespace(tenant_id=tenant_id)
        return await execute_oauth_install(request, provider="slack")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/install", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["location"].startswith(
        "https://slack.com/oauth/v2/authorize?"
    )
    assert any("oauth_install_states" in query for query in pool.queries)
    assert any("source_connector_authority_grants" in query for query in pool.queries)


@pytest.mark.asyncio
async def test_callback_persists_common_installation_and_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    pool = _Pool(tenant_id, "slack")
    secrets = _Secrets()
    app = _state_app(pool, secrets)
    monkeypatch.setenv("SLACK_CLIENT_ID", "client")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "signing-secret")
    monkeypatch.setenv("SLACK_REDIRECT_URI", "https://app.test/slack/callback")

    async def consume(_state, _pool, *, provider):
        assert provider == "slack"
        return tenant_id, {}

    monkeypatch.setattr(oauth_ingress, "verify_and_consume_state", consume)

    @app.get("/callback")
    async def callback(request: Request):
        return await execute_oauth_callback(request, provider="slack")

    with respx.mock(assert_all_called=True) as mock:
        mock.post("https://slack.com/api/oauth.v2.access").respond(
            200,
            json={
                "ok": True,
                "access_token": "xoxb-token",
                "scope": "channels:read,channels:history,groups:read,groups:history,users:read,team:read",
                "team": {"id": "T1", "name": "Acme"},
                "authed_user": {
                    "id": "U1",
                    "scope": "im:read,im:history,mpim:read,mpim:history",
                    "access_token": "xoxp-token",
                },
            },
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.get(
                "/callback",
                params={"code": "code", "state": "state-token"},
                follow_redirects=False,
            )

    assert response.status_code == 302
    assert response.headers["location"].startswith(
        "/integrations/slack/installed?installation="
    )
    assert "webhook_endpoint=" in response.headers["location"]
    assert any("source_connector_credentials" in query for query in pool.queries)
    assert any("source_connector_callbacks" in query for query in pool.queries)
    assert set(secrets.labels) >= {
        "source_connector:slack:oauth_access_token",
        "source_connector:slack:oauth_user_access_token",
        "source_connector:slack:webhook_signing_secret",
    }


@pytest.mark.asyncio
async def test_aws_configuration_accepts_declared_specialized_namespace() -> None:
    tenant_id = uuid4()
    pool = _Pool(tenant_id, "aws")
    app = _state_app(pool, _Secrets())

    @app.post("/configure")
    async def configure(request: Request):
        request.state.auth = SimpleNamespace(tenant_id=tenant_id)
        return await execute_configuration_install(request, provider="aws")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/configure",
            json={
                "external_installation_id": "aws-acme",
                "credentials": {
                    "aws_access_key_id": "AKIAEXAMPLE",
                    "aws_secret_access_key": "secret",
                },
                "configuration": {"selected_resources": ["us-east-1"]},
                "installation_data": {
                    "aws": {"region": "us-east-1", "regions": ["us-east-1"]}
                },
            },
        )

    assert response.status_code == 201
    assert response.json()["phase"] == "Ready"
    assert any("source_connector_installation_data" in query for query in pool.queries)
