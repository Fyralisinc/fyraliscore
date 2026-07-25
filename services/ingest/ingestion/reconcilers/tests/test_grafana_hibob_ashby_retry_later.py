"""Quota pauses must reach the durable reconciler scheduler."""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.ingestion.reconcilers import ashby, grafana, hibob


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


async def test_grafana_probe_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def high_water(pool, shard_id):  # noqa: ANN001, ANN202, ARG001
        return 100

    class _Client:
        async def has_annotations_since(self, **kwargs):  # noqa: ANN003, ANN202
            raise _retry("grafana", "annotations.list")

    monkeypatch.setattr(grafana, "_load_shard_high_water", high_water)
    with pytest.raises(RetryLater):
        await grafana._check_one_shard_for_gap(
            pool=object(),
            client=_Client(),
            shard={
                "id": uuid4(),
                "shard_identifier": {
                    "shard_kind": grafana.SHARD_KIND_ORG_ANNOTATIONS,
                },
            },
        )


async def test_hibob_probe_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def high_water(pool, shard_id):  # noqa: ANN001, ANN202, ARG001
        return "2026-01-01T00:00:00Z"

    class _Client:
        async def list_entities(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            raise _retry("hibob", "people.search")

    monkeypatch.setattr(hibob, "_load_shard_high_water", high_water)
    with pytest.raises(RetryLater):
        await hibob._check_one_shard_for_gap(
            pool=object(),
            client=_Client(),
            shard={
                "id": uuid4(),
                "shard_identifier": {
                    "shard_kind": hibob.SHARD_KIND_ENTITY,
                    "entity_type": "employee",
                },
            },
        )


async def test_ashby_probe_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def cursor(pool, shard_id):  # noqa: ANN001, ANN202, ARG001
        return {
            "sync_token": "sync-1",
            "high_water_updated": "2026-01-01T00:00:00Z",
        }

    class _Client:
        async def list_entities(self, *args, **kwargs):  # noqa: ANN002, ANN003, ANN202
            raise _retry("ashby", "entities.list")

    monkeypatch.setattr(ashby, "_load_shard_cursor", cursor)
    with pytest.raises(RetryLater):
        await ashby._check_one_shard_for_gap(
            pool=object(),
            client=_Client(),
            shard={
                "id": uuid4(),
                "shard_identifier": {
                    "shard_kind": ashby.SHARD_KIND_ENTITY,
                    "entity_type": "candidate",
                },
            },
        )
