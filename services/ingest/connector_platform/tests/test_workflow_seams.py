from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from services.ingest.connector_platform.workflow_wiring import (
    build_workflow_connector_wiring,
)
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest
from services.ingest.ingestion.fetchers import FETCHER_DISPATCH, FetchResult
from services.ingest.ingestion.workflows.shard_fetch import (
    _FetchLoopContext,
    _fetch_page,
)


@pytest.mark.asyncio
async def test_non_pilot_fetch_keeps_legacy_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    async def fetcher(_install, _identifier, _cursor):
        nonlocal calls
        calls += 1
        return FetchResult(end_of_data=True)

    class PilotOnlyRouter:
        def supports(self, source: str) -> bool:
            return source in {"slack", "notion"}

        async def fetch(self, *_args, **_kwargs):
            raise AssertionError("non-pilot source reached connector router")

    monkeypatch.setitem(FETCHER_DISPATCH, "github", fetcher)
    context = _FetchLoopContext(
        shard_id=uuid4(),
        tenant_id=uuid4(),
        source="github",
        shard_kind="github_repo_events",
        shard_identifier={"repo": "fyralis/core"},
        loop_started_at=datetime.now(timezone.utc),
    )

    result = await _fetch_page(
        None,
        context,
        install={"installation_id": "1"},  # type: ignore[arg-type]
        cursor=None,
        connector_router=PilotOnlyRouter(),
    )

    assert result.end_of_data
    assert calls == 1


@pytest.mark.asyncio
async def test_workflow_startup_applies_strict_artifact_quarantine(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Pool:
        async def fetchrow(self, query, *args):
            return None

        async def fetch(self, query, *args):
            return []

        async def execute(self, query, *args):
            return "UPDATE 0"

    monkeypatch.setenv("SOURCE_CONNECTOR_REQUIRE_SIGNED_ARTIFACTS", "true")
    wiring = build_workflow_connector_wiring(
        pool=Pool(),
        secret_store=object(),
    )
    try:
        await wiring.refresh_routing()
        decision = wiring.composition.routing.resolve(
            RouteRequest(
                uuid4(),
                "fyralis/slack",
                "slack",
                "ingestion.historical_pull",
            )
        )
        assert decision.mode is ExecutionMode.LEGACY
        assert decision.matched_scope == "artifact_quarantine"
    finally:
        await wiring.close()


@pytest.mark.asyncio
async def test_database_free_owner_fails_closed_when_signing_is_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOURCE_CONNECTOR_REQUIRE_SIGNED_ARTIFACTS", "true")
    wiring = build_workflow_connector_wiring(
        routing_config='{"revision":2,"global":"connector"}'
    )
    try:
        decision = wiring.composition.routing.resolve(
            RouteRequest(
                uuid4(),
                "fyralis/slack",
                "slack",
                "semantic.normalization",
            )
        )
        assert decision.mode is ExecutionMode.LEGACY
        assert decision.matched_scope == "artifact_quarantine"
    finally:
        await wiring.close()
