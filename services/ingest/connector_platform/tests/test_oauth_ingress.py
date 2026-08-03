from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
import respx
from fastapi import FastAPI, Request

from services.ingest.connector_platform.oauth_ingress import (
    execute_oauth_callback,
    execute_oauth_install,
)
from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_runtime.policy import ExecutionMode, RoutingPolicy


class _Pool:
    def __init__(self, tenant_id: UUID) -> None:
        self.tenant_id = tenant_id
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
            return {
                "installation_id": self.installation_id,
                "tenant_id": self.tenant_id,
                "connector_id": "fyralis/slack",
                "authority_generation": 1,
                "credential_owner": "connector_oauth_bootstrap",
                "granted_secret_slots": [
                    "oauth_access_token",
                    "webhook_signing_secret",
                ],
                "granted_scopes": [
                    "channels:read",
                    "channels:history",
                    "groups:read",
                    "groups:history",
                    "users:read",
                    "team:read",
                ],
                "granted_outbound_hosts": ["slack.com"],
                "maximum_trust_tier": "attested_agent",
                "provenance": {},
                "granted_at": datetime.now(timezone.utc),
                "revoked_at": None,
            }
        if "INSERT INTO provider_installations" in query:
            return {"id": uuid4(), "was_inserted": True}
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
        return uuid4()

    async def get(self, _ref, *, tenant_id):
        raise AssertionError("OAuth completion must not read tenant secrets")


def _state_app(pool: _Pool, secrets: _Secrets) -> FastAPI:
    app = FastAPI()
    app.state.pool = pool
    app.state.secret_store = secrets
    app.state.source_connector_runtime = build_pilot_composition(
        RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)
    )
    return app


@pytest.mark.asyncio
async def test_install_authorization_redirect_is_connector_owned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    pool = _Pool(tenant_id)
    app = _state_app(pool, _Secrets())
    monkeypatch.setenv("SLACK_CLIENT_ID", "client")
    monkeypatch.setenv("SLACK_REDIRECT_URI", "https://app.test/slack/callback")

    async def issue_state(*_args, **_kwargs):
        return "connector-state-token-at-least-sixteen"

    from services.ingest.integrations.slack import oauth as slack_oauth

    monkeypatch.setattr(slack_oauth, "issue_state_token", issue_state)

    @app.get("/install")
    async def install(request: Request):
        request.state.auth = SimpleNamespace(tenant_id=tenant_id)

        async def legacy():
            raise AssertionError("legacy install handler was selected")

        return await execute_oauth_install(
            request,
            provider="slack",
            legacy_handler=legacy,
        )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.get("/install", follow_redirects=False)

    assert response.status_code == 302
    assert response.headers["location"].startswith(
        "https://slack.com/oauth/v2/authorize?"
    )
    assert any("source_connector_authority_grants" in query for query in pool.queries)


@pytest.mark.asyncio
async def test_callback_exchanges_through_capability_and_persists_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    pool = _Pool(tenant_id)
    secrets = _Secrets()
    app = _state_app(pool, secrets)
    monkeypatch.setenv("SLACK_CLIENT_ID", "client")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "signing-secret")
    monkeypatch.setenv("SLACK_REDIRECT_URI", "https://app.test/slack/callback")

    from services.ingest.integrations.slack import oauth as slack_oauth

    async def consume(_state, _pool):
        return tenant_id, {}

    monkeypatch.setattr(slack_oauth, "verify_and_consume_state", consume)

    @app.get("/callback")
    async def callback(request: Request):
        async def legacy():
            raise AssertionError("legacy callback handler was selected")

        return await execute_oauth_callback(
            request,
            provider="slack",
            legacy_handler=legacy,
        )

    with respx.mock(assert_all_called=True) as mock:
        mock.post("https://slack.com/api/oauth.v2.access").respond(
            200,
            json={
                "ok": True,
                "access_token": "xoxb-token",
                "scope": "channels:read,channels:history,users:read,team:read",
                "team": {"id": "T1", "name": "Acme"},
                "authed_user": {
                    "id": "U1",
                    "scope": "im:read,im:history",
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
        "/integrations/slack/installed?team="
    )
    assert set(secrets.labels) == {
        "slack_bot_token:T1",
        "slack_signing_secret:app",
        "slack_user_token:T1:U1",
    }
    assert any("source_connector_credentials" in query for query in pool.queries)
