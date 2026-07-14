from __future__ import annotations

import json

import httpx
import pytest
import respx

from services.ingest.integrations.slack import byoc_app


pytestmark = pytest.mark.integration


def test_configuration_token_from_inputs_accepts_ui_key() -> None:
    assert (
        byoc_app.configuration_token_from_inputs(
            {"slack_app_config_token": " test-config-token "}
        )
        == "test-config-token"
    )


async def test_create_app_from_manifest_maps_slack_credentials() -> None:
    with respx.mock(base_url="https://slack.com") as router:
        route = router.post("/api/apps.manifest.create").mock(
            return_value=httpx.Response(
                200,
                json={
                    "ok": True,
                    "app_id": "A_BYOC",
                    "credentials": {
                        "client_id": "123.456",
                        "client_secret": "client-secret",
                        "verification_token": "verify-token",
                        "signing_secret": "signing-secret",
                    },
                    "oauth_authorize_url": "https://slack.com/oauth/v2/authorize",
                },
            )
        )

        token = "test-config-token"
        credentials = await byoc_app.create_app_from_manifest(
            configuration_token=token,
            oauth_redirect_url="https://customer.example/integrations/slack/callback",
            events_request_url="https://customer.example/webhooks/slack/events",
        )

    request_payload = json.loads(route.calls.last.request.content)
    manifest = json.loads(request_payload["manifest"])
    assert request_payload["token"] == token
    assert route.calls.last.request.headers["authorization"] == f"Bearer {token}"
    assert manifest["oauth_config"]["redirect_urls"] == [
        "https://customer.example/integrations/slack/callback"
    ]
    assert manifest["settings"]["event_subscriptions"]["request_url"] == (
        "https://customer.example/webhooks/slack/events"
    )
    user_scopes = manifest["oauth_config"]["scopes"]["user"]
    assert "channels:history" in user_scopes
    assert "groups:history" in user_scopes
    assert "im:history" in user_scopes
    assert credentials.app_id == "A_BYOC"
    assert credentials.client_id == "123.456"
    assert credentials.client_secret == "client-secret"
    assert credentials.signing_secret == "signing-secret"
