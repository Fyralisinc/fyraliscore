from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

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
