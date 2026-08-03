import json
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import pytest

from services.ingest.connector_platform.pilots import (
    NOTION_CONNECTOR_ID,
    SLACK_CONNECTOR_ID,
    build_pilot_composition,
)
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.source_contract.capabilities import (
    OAUTH2_LIFECYCLE_V1,
    OAUTH2_V1,
)
from services.ingest.source_contract.capabilities.installation import (
    OAuthBeginRequest,
    OAuthCompleteRequest,
    OAuthRevokeRequest,
)
from services.ingest.source_contract.connector import (
    BindingContext,
    GrantedAuthority,
    OperationContext,
)
from services.ingest.source_contract.host_services import SecretValue
from services.ingest.source_contract.models import InstallationRef


def _authority(connector_id: str) -> GrantedAuthority:
    if connector_id == SLACK_CONNECTOR_ID:
        return GrantedAuthority(
            secret_slots=frozenset({"oauth_access_token", "webhook_signing_secret"}),
            outbound_hosts=frozenset({"slack.com"}),
            scopes=frozenset(
                {
                    "channels:read",
                    "channels:history",
                    "groups:read",
                    "groups:history",
                    "users:read",
                    "team:read",
                }
            ),
            maximum_trust_tier="attested_agent",
        )
    return GrantedAuthority(
        secret_slots=frozenset({"oauth_access_token"}),
        outbound_hosts=frozenset({"api.notion.com"}),
        maximum_trust_tier="attested_agent",
    )


def _context(connector_id: str, services) -> BindingContext:
    return BindingContext(
        installation=InstallationRef(
            id=uuid4(),
            tenant_id=uuid4(),
            connector_id=connector_id,
            generation=1,
        ),
        authority=_authority(connector_id),
        services=services,
    )


def _operation(services) -> OperationContext:
    return OperationContext(
        invocation_id=uuid4(),
        deadline=datetime.now(timezone.utc) + timedelta(seconds=30),
        services=services,
    )


@pytest.mark.asyncio
async def test_slack_oauth_is_registry_resolved_and_returns_secret_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SLACK_CLIENT_ID", "client")
    monkeypatch.setenv("SLACK_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("SLACK_SIGNING_SECRET", "signing-secret")
    monkeypatch.setenv("SLACK_REDIRECT_URI", "https://fyralis.test/slack/callback")

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "slack.com"
        return httpx.Response(
            200,
            json={
                "ok": True,
                "access_token": "xoxb-token",
                "scope": "channels:read,channels:history,groups:read,groups:history,users:read,team:read",
                "team": {"id": "T123", "name": "Acme"},
            },
        )

    async def secret_reader(_installation, _slot):
        return SecretValue.from_text("xoxb-token")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        services = HostServicesFactory(
            http_client=client,
            secret_reader=secret_reader,
        ).build(
            uuid4(),
            _authority(SLACK_CONNECTOR_ID),
            connector_id=SLACK_CONNECTOR_ID,
        )
        binding = build_pilot_composition().registry.resolve_for_install(
            _context(SLACK_CONNECTOR_ID, services)
        )
        oauth = binding.require(OAUTH2_V1)
        redirect = await oauth.begin(
            OAuthBeginRequest(
                redirect_uri="https://fyralis.test/slack/callback",
                state="state-token-at-least-sixteen",
            ),
            _operation(services),
        )
        assert redirect.url.startswith("https://slack.com/oauth/v2/authorize?")
        result, candidates = await oauth.complete(
            OAuthCompleteRequest(
                code="code",
                redirect_uri="https://fyralis.test/slack/callback",
            ),
            _operation(services),
        )
        assert result.external_installation_id == "T123"
        assert {str(item.slot) for item in candidates} == {
            "oauth_access_token",
            "webhook_signing_secret",
        }
        lifecycle = binding.require(OAUTH2_LIFECYCLE_V1)
        revoked = await lifecycle.revoke(
            OAuthRevokeRequest(operation_id="revoke-1"),
            _operation(services),
        )
        assert revoked.remote_revoked


@pytest.mark.asyncio
async def test_notion_oauth_uses_governed_http_and_long_lived_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NOTION_CLIENT_ID", "client")
    monkeypatch.setenv("NOTION_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv("NOTION_REDIRECT_URI", "https://fyralis.test/notion/callback")

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert body["code"] == "code"
        return httpx.Response(
            200,
            json={
                "access_token": "notion-token",
                "workspace_id": "workspace-1",
                "workspace_name": "Acme",
                "bot_id": "bot-1",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        services = HostServicesFactory(http_client=client).build(
            uuid4(),
            _authority(NOTION_CONNECTOR_ID),
            connector_id=NOTION_CONNECTOR_ID,
        )
        binding = build_pilot_composition().registry.resolve_for_install(
            _context(NOTION_CONNECTOR_ID, services)
        )
        result, candidates = await binding.require(OAUTH2_V1).complete(
            OAuthCompleteRequest(
                code="code",
                redirect_uri="https://fyralis.test/notion/callback",
            ),
            _operation(services),
        )
        assert result.external_installation_id == "workspace-1"
        assert len(candidates) == 1
        assert str(candidates[0].slot) == "oauth_access_token"
