from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from services.ingest.connector_platform.execution import LegacyExecutionRouter
from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.authority import InstallationAuthority
from services.ingest.connector_runtime.policy import ExecutionMode, RoutingPolicy
from services.ingest.connector_runtime.shadow import InMemoryShadowReportSink
from services.ingest.source_contract.errors import BindingError
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingest.source_contract.errors import PermissionDeniedError
from services.ingest.source_contract.host_services import SecretValue


def _install(source: str) -> dict:
    return {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "installation_id": f"{source}-workspace",
        "secret_ref": str(uuid4()),
        "enabled": True,
    }


@pytest.mark.asyncio
async def test_planner_and_fetcher_execute_end_to_end_through_registry() -> None:
    install = _install("slack")
    planner_context = SimpleNamespace(install=install)
    policy = RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)
    composition = build_pilot_composition(policy)
    metrics: list[tuple[str, tuple]] = []

    def increment(name, _value, attributes):
        metrics.append((name, attributes))

    async def provider(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("conversations.list"):
            return httpx.Response(
                200,
                json={
                    "ok": True,
                    "channels": [{"id": "C1", "name": "general"}],
                    "response_metadata": {"next_cursor": ""},
                },
            )
        return httpx.Response(
            200,
            json={
                "ok": True,
                "messages": [
                    {
                        "type": "message",
                        "ts": "1700000000.000001",
                        "text": "hello",
                    }
                ],
                "response_metadata": {"next_cursor": ""},
            },
        )

    async def read_secret(_installation, slot):
        if str(slot) == "oauth_user_access_token":
            raise PermissionDeniedError("fixture has no user token")
        return SecretValue.from_text("xoxb")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        router = LegacyExecutionRouter(
            composition,
            HostServicesFactory(
                http_client=client,
                metric_incrementer=increment,
                secret_reader=read_secret,
            ),
        )
        shards = await router.plan("slack", planner_context)
        page = await router.fetch(
            "slack",
            install,
            shards[0].shard_identifier,
            None,
            shard_kind=shards[0].shard_kind,
        )

    assert shards[0].shard_kind == "slack_channel_window"
    assert page.end_of_data
    assert page.records[0]["event"]["text"] == "hello"
    completed = [item for item in metrics if item[0].endswith(".completed")]
    assert len(completed) == 2
    assert all(
        dict(attributes)["connector_id"] == "fyralis/slack"
        for _, attributes in completed
    )


@pytest.mark.asyncio
async def test_shadow_fetch_compares_cursor_and_publication_without_cutover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install = _install("notion")
    calls = 0

    async def fetcher(_install, _identifier, _cursor):
        nonlocal calls
        calls += 1
        return FetchResult(
            records=[{"object": "page", "_fyralis_workspace_id": "w1"}],
            next_cursor={
                "stack": [],
                "items_seen": 1,
                "last_edited_at": None,
                "seeded": True,
            },
            end_of_data=True,
        )

    monkeypatch.setitem(FETCHER_DISPATCH, "notion", fetcher)
    composition = build_pilot_composition(
        RoutingPolicy(global_mode=ExecutionMode.SHADOW)
    )
    sink = InMemoryShadowReportSink()

    async def provider(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "results": [{"object": "page"}],
                "has_more": False,
                "next_cursor": None,
            },
        )

    async def read_secret(_installation, _slot):
        return SecretValue.from_text("notion-token")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        router = LegacyExecutionRouter(
            composition,
            HostServicesFactory(http_client=client, secret_reader=read_secret),
            shadow_sink=sink,
        )
        result = await router.fetch(
            "notion",
            install,
            {"shard_kind": "notion_page_tree", "workspace_id": "w1"},
            None,
            shadow_safe=True,
        )

    assert calls == 1
    assert result.records == [{"object": "page", "_fyralis_workspace_id": "w1"}]
    assert sink.reports[0].matches


@pytest.mark.asyncio
async def test_pilot_binding_rejects_install_without_credential_grant() -> None:
    install = _install("slack")
    install["secret_ref"] = None
    planner_context = SimpleNamespace(install=install)
    async with httpx.AsyncClient() as client:
        router = LegacyExecutionRouter(
            build_pilot_composition(RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)),
            HostServicesFactory(http_client=client),
        )
        with pytest.raises(BindingError):
            await router.plan("slack", planner_context)


@pytest.mark.asyncio
async def test_router_uses_durable_authority_instead_of_install_inference() -> None:
    install = _install("slack")
    install["secret_ref"] = None
    planner_context = SimpleNamespace(install=install)

    class Repository:
        async def load(self, installation_id):
            return InstallationAuthority(
                installation_id=installation_id,
                tenant_id=install["tenant_id"],
                connector_id="fyralis/slack",
                generation=1,
                credential_owner="oauth_callback",
                secret_slots=frozenset(
                    {
                        "oauth_access_token",
                        "oauth_user_access_token",
                        "webhook_signing_secret",
                    }
                ),
                outbound_hosts=frozenset({"slack.com"}),
                scopes=frozenset(
                    {
                        "channels:read",
                        "channels:history",
                        "groups:read",
                        "groups:history",
                        "users:read",
                        "team:read",
                        "im:read",
                        "im:history",
                        "mpim:read",
                        "mpim:history",
                    }
                ),
                maximum_trust_tier="attested_agent",
            )

        async def grant(self, authority):
            raise AssertionError("not used")

        async def revoke(self, installation_id, *, revoked_at, reason):
            raise AssertionError("not used")

    async def provider(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"ok": True, "channels": [], "response_metadata": {"next_cursor": ""}},
        )

    async def read_secret(_installation, slot):
        if str(slot) == "oauth_user_access_token":
            raise PermissionDeniedError("fixture has no user token")
        return SecretValue.from_text("xoxb")

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
        router = LegacyExecutionRouter(
            build_pilot_composition(RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)),
            HostServicesFactory(http_client=client, secret_reader=read_secret),
            authority_repository=Repository(),
            require_durable_authority=True,
        )
        assert await router.plan("slack", planner_context) == []
