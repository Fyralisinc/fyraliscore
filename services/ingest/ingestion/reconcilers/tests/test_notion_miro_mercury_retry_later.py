"""Quota pauses must reach the durable reconciler scheduler."""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.ingestion.reconcilers import mercury, miro, notion


pytestmark = pytest.mark.asyncio


def _retry(source: str, operation: str) -> RetryLater:
    return RetryLater.after(
        request_context=RequestContext(
            source=source,
            operation=operation,
            tenant_id=str(uuid4()),
            installation_id=str(uuid4()),
        ),
        delay_seconds=90,
        reason=RetryReason.RATE_LIMIT,
    )


async def test_notion_probe_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def high_water(pool, shard_id):  # noqa: ANN001, ANN202, ARG001
        return "2026-01-01T00:00:00Z"

    class _Client:
        async def latest_database_edit(self, database_id):  # noqa: ANN001, ANN202, ARG002
            raise _retry("notion", "databases.query")

    monkeypatch.setattr(notion, "_load_shard_high_water", high_water)
    with pytest.raises(RetryLater):
        await notion._check_one_shard_for_gap(
            pool=object(),
            client=_Client(),
            shard={
                "id": uuid4(),
                "shard_identifier": {
                    "shard_kind": notion.SHARD_KIND_DATABASE,
                    "database_id": "database-1",
                },
            },
        )


async def test_miro_probe_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def high_water(pool, shard_id):  # noqa: ANN001, ANN202, ARG001
        return "2026-01-01T00:00:00Z"

    class _Client:
        async def list_items(self, board_id, **kwargs):  # noqa: ANN001, ANN202, ARG002
            raise _retry("miro", "board_items.list")

    monkeypatch.setattr(miro, "_load_shard_high_water", high_water)
    with pytest.raises(RetryLater):
        await miro._check_one_shard_for_gap(
            pool=object(),
            client=_Client(),
            shard={
                "id": uuid4(),
                "shard_identifier": {
                    "shard_kind": miro.SHARD_KIND_BOARD_ITEMS,
                    "board_id": "board-1",
                },
            },
        )


async def test_mercury_probe_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def high_water(pool, shard_id):  # noqa: ANN001, ANN202, ARG001
        return "2026-01-01T00:00:00Z"

    class _Client:
        async def list_transactions(self, account_id, **kwargs):  # noqa: ANN001, ANN202, ARG002
            raise _retry("mercury", "transactions.list")

    monkeypatch.setattr(mercury, "_load_shard_high_water", high_water)
    with pytest.raises(RetryLater):
        await mercury._check_one_shard_for_gap(
            pool=object(),
            client=_Client(),
            shard={
                "id": uuid4(),
                "shard_identifier": {
                    "shard_kind": mercury.SHARD_KIND_ACCOUNT_TXNS,
                    "account_id": "account-1",
                },
            },
        )
