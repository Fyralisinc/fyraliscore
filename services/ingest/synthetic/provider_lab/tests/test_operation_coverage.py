"""Exact source-contract operation ownership in Provider Lab."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Mapping
from uuid import uuid4

import httpx
import pytest

from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    SOURCE_OPERATION_POLICY_CATALOG,
)
from services.ingest.integrations.gmail.pubsub import PubsubAdmin
from services.ingest.integrations.oauth_refresh import (
    REFRESH_CONFIGS,
    refresh_access_token,
)
from services.ingest.synthetic.provider_lab import (
    AdapterRegistry,
    ProviderOperationBinding,
    ProviderRequest,
    ProviderResponse,
    ProviderRoute,
    build_lab_adapter_registry,
    build_provider_lab_app,
)
from services.ingest.synthetic.provider_lab.adapters import SlackAdapter


class _SlackCoverageAdapter:
    source = "slack"
    protocol_surfaces = ()

    def __init__(self, routes: tuple[ProviderRoute, ...]) -> None:
        self.routes = routes

    def default_state(self) -> Mapping[str, Any]:
        return {}

    def resolve_scope(self, request: ProviderRequest) -> str:
        del request
        return "global"

    async def handle(self, request: ProviderRequest) -> ProviderResponse:
        del request
        return ProviderResponse.json({"ok": True})


class _PubsubTokenMinter:
    service_account_email = "provider-lab@project.test"

    async def mint(self, **kwargs: Any) -> str:
        del kwargs
        return "lab-gmail::provider-lab@project.test"


def test_inventory_owns_exact_catalog_operation_union_per_source() -> None:
    registry = build_lab_adapter_registry()
    inventory = registry.inventory()

    assert tuple(item["source"] for item in inventory) == CANONICAL_SOURCE_IDS
    assert sum(len(item["owned_operation_ids"]) for item in inventory) == sum(
        len(operation_ids)
        for operation_ids in SOURCE_OPERATION_POLICY_CATALOG.values()
    )
    for item in inventory:
        source_id = item["source"]
        expected = set(SOURCE_OPERATION_POLICY_CATALOG[source_id])
        route_owned = {
            operation_id
            for route in item["routes"]
            for operation_id in route["operation_ids"]
        }
        protocol_owned = {
            operation_id
            for surface in item["protocol_surfaces"]
            for operation_id in surface["operation_ids"]
        }
        assert set(item["expected_operation_ids"]) == expected
        assert set(item["owned_operation_ids"]) == expected
        assert route_owned | protocol_owned == expected
        assert route_owned.isdisjoint(protocol_owned)

    by_source = {item["source"]: item for item in inventory}
    assert by_source["discord"]["protocol_surfaces"] == [
        {
            "surface_id": "discord.gateway",
            "transport": "websocket",
            "operation_ids": [],
        }
    ]
    assert {
        surface["surface_id"]
        for surface in by_source["telegram"]["protocol_surfaces"]
    } == {
        "telegram.session_transport",
        "telegram.gateway_transport",
    }
    assert by_source["signal"]["routes"][0]["transport"] == "json_rpc"
    assert by_source["signal"]["routes"][1]["transport"] == "sse"
    assert by_source["aws"]["routes"][0]["transport"] == "aws_sigv4"


def test_registry_rejects_missing_and_unknown_catalog_operations() -> None:
    routes = SlackAdapter.routes
    missing = _SlackCoverageAdapter(
        (
            replace(routes[0], operation_ids=()),
            *routes[1:],
        )
    )
    with pytest.raises(ValueError, match=r"missing=\['conversations.list'\]"):
        AdapterRegistry(
            {"slack": missing},
            expected_sources=("slack",),
            expected_operations={
                "slack": SOURCE_OPERATION_POLICY_CATALOG["slack"],
            },
        )

    unknown = _SlackCoverageAdapter(
        (
            replace(
                routes[0],
                operation_ids=(
                    *routes[0].operation_ids,
                    "not.a.catalog.operation",
                ),
                operation_bindings=(
                    ProviderOperationBinding(
                        operation_id="conversations.list",
                        method="GET",
                    ),
                    ProviderOperationBinding(
                        operation_id="not.a.catalog.operation",
                        method="GET",
                    ),
                ),
            ),
            *routes[1:],
        )
    )
    with pytest.raises(
        ValueError,
        match=r"unknown=\['not.a.catalog.operation'\]",
    ):
        AdapterRegistry(
            {"slack": unknown},
            expected_sources=("slack",),
            expected_operations={
                "slack": SOURCE_OPERATION_POLICY_CATALOG["slack"],
            },
        )


async def test_added_catalog_used_http_surfaces_are_strictly_routable() -> None:
    app = build_provider_lab_app()
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
    )
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
    ) as client:
        responses = {
            "slack_post": await client.post(
                "/slack/api/chat.postMessage",
                json={"channel": "C_PROVIDER_LAB", "text": "hello"},
            ),
            "slack_history_after_post": await client.get(
                "/slack/api/conversations.history",
                params={"channel": "C_PROVIDER_LAB"},
            ),
            "slack_oauth": await client.post(
                "/slack/api/oauth.v2.access",
                data={"code": "oauth-code"},
            ),
            "discord_member": await client.get(
                "/discord/api/v10/guilds/G1/members/U1",
            ),
            "discord_channel": await client.get(
                "/discord/api/v10/channels/C1",
            ),
            "discord_command": await client.post(
                "/discord/api/v10/applications/A1/commands",
                json={"name": "fyralis", "description": "Provider Lab"},
            ),
            "discord_followup": await client.post(
                "/discord/api/v10/webhooks/A1/token",
                json={"content": "done"},
            ),
            "discord_oauth": await client.post(
                "/discord/api/v10/oauth2/token",
                data={"code": "oauth-code"},
            ),
            "figma_oauth": await client.post(
                "/figma/v1/oauth/token",
                data={"grant_type": "refresh_token"},
            ),
            "quickbooks_oauth": await client.post(
                "/quickbooks/oauth2/v1/tokens/bearer",
                data={"grant_type": "refresh_token"},
            ),
            "gusto_oauth": await client.post(
                "/gusto/oauth/token",
                data={"grant_type": "refresh_token"},
            ),
            "linkedin_oauth": await client.post(
                "/linkedin/oauth/v2/accessToken",
                data={"grant_type": "refresh_token"},
            ),
            "calendar_directory": await client.get(
                "/gcal/admin/directory/v1/users",
            ),
            "drive_directory": await client.get(
                "/gdrive/admin/directory/v1/users",
            ),
        }

    assert responses["discord_channel"].status_code == 404
    for name, response in responses.items():
        if name != "discord_channel":
            assert response.status_code in {200, 201}, (
                name,
                response.text,
            )
    assert responses["slack_history_after_post"].json()["messages"] == [
        {
            "type": "message",
            "channel": "C_PROVIDER_LAB",
            "text": "hello",
            "ts": "1.000000",
        }
    ]


async def test_gmail_pubsub_admin_surface_preserves_iam_policy() -> None:
    app = build_provider_lab_app()
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
    )
    topic = "/gmail/v1/projects/P1/topics/T1"
    subscription = "/gmail/v1/projects/P1/subscriptions/S1"
    policy = {
        "bindings": [
            {
                "role": "roles/pubsub.publisher",
                "members": [
                    "serviceAccount:gmail-api-push@system.gserviceaccount.com"
                ],
            }
        ]
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
    ) as client:
        assert (await client.put(topic, json={})).status_code == 200
        assert (
            await client.put(
                subscription,
                json={"topic": "projects/P1/topics/T1"},
            )
        ).status_code == 200
        assert (
            await client.post(
                f"{topic}:setIamPolicy",
                json={"policy": policy},
            )
        ).json() == policy
        assert (
            await client.post(
                f"{topic}:getIamPolicy",
                json={},
            )
        ).json() == policy
        assert (await client.delete(subscription)).status_code == 200
        assert (await client.delete(topic)).status_code == 200


async def test_production_pubsub_admin_uses_the_catalog_owned_lab_routes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = build_provider_lab_app()
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
    )
    monkeypatch.setenv(
        "GMAIL_PUBSUB_API_BASE_URL",
        "http://provider-lab/gmail/v1",
    )
    monkeypatch.setenv("GMAIL_PUBSUB_PROJECT_ID", "provider-lab-project")
    monkeypatch.setenv(
        "GMAIL_PUBSUB_PUSH_ENDPOINT",
        "https://fyralis.test/webhooks/gmail/pubsub",
    )
    monkeypatch.setenv(
        "GMAIL_PUBSUB_PUSH_OIDC_SA",
        "push@provider-lab-project.iam.gserviceaccount.com",
    )
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
    ) as http:
        admin = PubsubAdmin(
            _PubsubTokenMinter(),  # type: ignore[arg-type]
            http_client=http,
            allow_unlimited_local=True,
        )
        tenant_id = uuid4()
        resources = await admin.provision(
            tenant_id,
            installation_id=uuid4(),
        )
        await admin.teardown(
            tenant_id,
            installation_id=uuid4(),
        )

    entries = app.state.provider_lab.ledger.list(
        source="gmail",
        limit=100,
    )
    assert resources.topic_name.startswith(
        "projects/provider-lab-project/topics/gmail-"
    )
    assert [entry["route_id"] for entry in entries] == [
        "gmail.pubsub_topic",
        "gmail.pubsub_iam_get",
        "gmail.pubsub_iam_set",
        "gmail.pubsub_subscription",
        "gmail.pubsub_subscription",
        "gmail.pubsub_topic",
    ]


@pytest.mark.parametrize(
    ("source", "path"),
    (
        ("quickbooks", "/oauth2/v1/tokens/bearer"),
        ("ramp", "/token"),
        ("gusto", "/oauth/token"),
        ("carta", "/o/access_token/"),
        ("linkedin", "/oauth/v2/accessToken"),
    ),
)
async def test_shared_production_oauth_refresh_uses_lab_token_surfaces(
    source: str,
    path: str,
) -> None:
    app = build_provider_lab_app()
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
    )
    config = replace(
        REFRESH_CONFIGS[source],
        token_url=f"http://provider-lab/{source}{path}",
    )
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
    ) as http:
        token = await refresh_access_token(
            http,
            config,
            client_id="provider-lab-client",
            client_secret="provider-lab-secret",
            refresh_token=(
                "provider-lab-refresh"
                if config.grant_type == "refresh_token"
                else None
            ),
        )

    assert token.access_token
    entries = app.state.provider_lab.ledger.list(
        source=source,
        limit=10,
    )
    assert len(entries) == 1
    assert entries[0]["status_code"] == 200
