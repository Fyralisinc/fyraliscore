"""Quota pauses must not produce a cursor result for these fetchers."""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.ingestion.fetchers import ashby, grafana, hibob


pytestmark = pytest.mark.asyncio


def _retry(source: str, operation: str) -> RetryLater:
    return RetryLater.after(
        request_context=RequestContext(
            source=source,
            operation=operation,
            tenant_id=str(uuid4()),
            installation_id=str(uuid4()),
        ),
        delay_seconds=60,
        reason=RetryReason.RATE_LIMIT,
    )


async def _open(client):  # noqa: ANN001, ANN202
    async def close() -> None:
        return None

    return client, close


async def test_grafana_retry_later_preserves_persisted_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def list_annotations(self, **kwargs):  # noqa: ANN003, ANN202
            raise _retry("grafana", "annotations.list")

    async def open_client(install):  # noqa: ANN001, ANN202, ARG001
        return await _open(_Client())

    monkeypatch.setattr(grafana, "_open_grafana_client", open_client)
    cursor = grafana.GrafanaCursor(
        high_water_time_ms=100,
        page_to_ms=90,
        floor_ms=10,
        annotations_seen=5,
        seeded=True,
    ).model_dump(mode="json")
    original = dict(cursor)

    with pytest.raises(RetryLater):
        await grafana.fetch_page_grafana(
            {"base_url": "https://grafana.test"},
            {"shard_kind": grafana.SHARD_KIND_ORG_ANNOTATIONS},
            cursor,
        )

    assert cursor == original


async def test_hibob_retry_later_preserves_persisted_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def list_entities(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            raise _retry("hibob", "people.salaries.list")

    async def open_client(install):  # noqa: ANN001, ANN202, ARG001
        return await _open(_Client())

    monkeypatch.setattr(hibob, "_open_hibob_client", open_client)
    cursor = hibob.HibobCursor(
        offset=5,
        page_cursor="cursor-1",
        high_water_updated="2026-01-01T00:00:00Z",
        incremental_floor="2026-01-01T00:00:00Z",
        rows_seen=5,
        seeded=True,
    ).model_dump(mode="json")
    original = dict(cursor)

    with pytest.raises(RetryLater):
        await hibob.fetch_page_hibob(
            {"company_id": "company-1"},
            {
                "shard_kind": hibob.SHARD_KIND_ENTITY,
                "entity_type": "payroll",
            },
            cursor,
        )

    assert cursor == original


async def test_ashby_retry_later_preserves_persisted_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def list_entities(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            raise _retry("ashby", "entities.list")

    async def open_client(install):  # noqa: ANN001, ANN202, ARG001
        return await _open(_Client())

    monkeypatch.setattr(ashby, "_open_ashby_client", open_client)
    cursor = ashby.AshbyCursor(
        cursor="cursor-1",
        sync_token="sync-1",
        high_water_updated="2026-01-01T00:00:00Z",
        rows_seen=5,
        seeded=True,
    ).model_dump(mode="json")
    original = dict(cursor)

    with pytest.raises(RetryLater):
        await ashby.fetch_page_ashby(
            {"org_id": "org-1"},
            {
                "shard_kind": ashby.SHARD_KIND_ENTITY,
                "entity_type": "candidate",
            },
            cursor,
        )

    assert cursor == original
