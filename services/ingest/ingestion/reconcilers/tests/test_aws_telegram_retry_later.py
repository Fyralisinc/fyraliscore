"""AWS and Telegram reconcilers must not mask provider cooldowns as no gaps."""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)
from services.ingest.ingestion.reconcilers import aws, telegram


pytestmark = pytest.mark.asyncio


def _retry(source: str, operation: str) -> RetryLater:
    return RetryLater.after(
        request_context=RequestContext(
            source=source,
            operation=operation,
        ),
        delay_seconds=30,
        reason=RetryReason.RATE_LIMIT,
    )


async def test_aws_reconciler_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _high_water(_pool, _shard_id):  # noqa: ANN001
        return 1000

    class _Client:
        async def has_events_since(self, **_kwargs):
            raise _retry("aws", "cloudtrail.lookup_events")

    monkeypatch.setattr(aws, "_load_shard_high_water", _high_water)
    shard = {
        "id": uuid4(),
        "shard_identifier": {
            "shard_kind": aws.SHARD_KIND_ACCOUNT_EVENTS,
        },
    }
    with pytest.raises(RetryLater):
        await aws._check_one_shard_for_gap(
            pool=object(),
            client=_Client(),
            shard=shard,
            account_id="123456789012",
            region="us-east-1",
        )


async def test_telegram_reconciler_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _high_water(_pool, _shard_id):  # noqa: ANN001
        return 10

    class _Client:
        async def has_history_since(self, **_kwargs):
            raise _retry("telegram", "has_history_since")

    monkeypatch.setattr(telegram, "_load_shard_high_water", _high_water)
    shard = {
        "id": uuid4(),
        "shard_identifier": {
            "shard_kind": telegram.SHARD_KIND_DIALOG_HISTORY,
            "dialog_id": 7,
            "dialog_kind": "chat",
        },
    }
    with pytest.raises(RetryLater):
        await telegram._check_one_shard_for_gap(
            pool=object(),
            client=_Client(),
            shard=shard,
        )
