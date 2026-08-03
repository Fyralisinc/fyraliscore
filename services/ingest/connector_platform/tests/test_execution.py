from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from services.ingest.connector_platform.execution import LegacyExecutionRouter
from services.ingest.connector_platform.pilots import build_pilot_composition
from services.ingest.connector_runtime.host_services import HostServicesFactory
from services.ingest.connector_runtime.policy import ExecutionMode, RoutingPolicy
from services.ingest.connector_runtime.shadow import InMemoryShadowReportSink
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingest.ingestion.planners import PLANNER_DISPATCH, Shard


def _install(source: str) -> dict:
    return {
        "id": uuid4(),
        "tenant_id": uuid4(),
        "installation_id": f"{source}-workspace",
        "enabled": True,
    }


@pytest.mark.asyncio
async def test_planner_and_fetcher_execute_end_to_end_through_registry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    install = _install("slack")
    planner_context = SimpleNamespace(install=install)
    planner_calls = 0
    fetch_calls = 0

    async def planner(_context):
        nonlocal planner_calls
        planner_calls += 1
        return [
            Shard(
                shard_kind="slack_channel_window",
                shard_identifier={"channel_id": "C1"},
            )
        ]

    async def fetcher(_install, _identifier, _cursor):
        nonlocal fetch_calls
        fetch_calls += 1
        return FetchResult(
            records=[
                {
                    "type": "event_callback",
                    "event": {
                        "type": "message",
                        "channel": "C1",
                        "ts": "1700000000.000001",
                        "text": "hello",
                    },
                }
            ],
            end_of_data=True,
        )

    monkeypatch.setitem(PLANNER_DISPATCH, "slack", planner)
    monkeypatch.setitem(FETCHER_DISPATCH, "slack", fetcher)
    policy = RoutingPolicy(global_mode=ExecutionMode.CONNECTOR)
    composition = build_pilot_composition(policy)
    metrics: list[tuple[str, tuple]] = []

    def increment(name, _value, attributes):
        metrics.append((name, attributes))

    async with httpx.AsyncClient() as client:
        router = LegacyExecutionRouter(
            composition,
            HostServicesFactory(
                http_client=client,
                metric_incrementer=increment,
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

    assert planner_calls == 1
    assert fetch_calls == 1
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
            records=[{"object": "page", "id": "page-1"}],
            next_cursor={"cursor": "next"},
            end_of_data=False,
        )

    monkeypatch.setitem(FETCHER_DISPATCH, "notion", fetcher)
    composition = build_pilot_composition(
        RoutingPolicy(global_mode=ExecutionMode.SHADOW)
    )
    sink = InMemoryShadowReportSink()
    async with httpx.AsyncClient() as client:
        router = LegacyExecutionRouter(
            composition,
            HostServicesFactory(http_client=client),
            shadow_sink=sink,
        )
        result = await router.fetch(
            "notion",
            install,
            {"shard_kind": "notion_page_tree", "workspace_id": "w1"},
            None,
            shadow_safe=True,
        )

    assert calls == 2
    assert result.records == [{"object": "page", "id": "page-1"}]
    assert sink.reports[0].matches
