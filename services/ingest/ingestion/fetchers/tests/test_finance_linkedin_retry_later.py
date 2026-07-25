"""Backfill fetchers must hand RetryLater to durable shard scheduling."""
from __future__ import annotations

import importlib

import pytest

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)


pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    ("source", "entity_type", "installation"),
    [
        ("quickbooks", "Invoice", {"realm_id": "realm-1"}),
        ("ramp", "transaction", {"business_id": "business-1"}),
        ("gusto", "employee", {"company_uuid": "company-1"}),
        (
            "linkedin",
            "post",
            {"organization_urn": "urn:li:organization:123"},
        ),
    ],
)
async def test_fetcher_propagates_retry_later_without_empty_page(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    entity_type: str,
    installation: dict[str, str],
) -> None:
    module = importlib.import_module(
        f"services.ingest.ingestion.fetchers.{source}",
    )
    retry = RetryLater.after(
        request_context=RequestContext(
            source=source,
            operation="backfill.page",
        ),
        delay_seconds=60,
        reason=RetryReason.RATE_LIMIT,
    )

    class _Client:
        async def query(self, *args: object, **kwargs: object) -> None:
            raise retry

        async def list_transactions(
            self,
            *args: object,
            **kwargs: object,
        ) -> None:
            raise retry

        async def list_employees(
            self,
            *args: object,
            **kwargs: object,
        ) -> None:
            raise retry

        async def list_posts(
            self,
            *args: object,
            **kwargs: object,
        ) -> None:
            raise retry

    async def open_client(install: object) -> tuple[object, object]:
        del install

        async def close() -> None:
            return None

        return _Client(), close

    monkeypatch.setattr(module, f"_open_{source}_client", open_client)
    fetch = getattr(module, f"fetch_page_{source}")
    shard = {
        "shard_kind": module.SHARD_KIND_ENTITY,
        "entity_type": entity_type,
    }

    with pytest.raises(RetryLater) as caught:
        await fetch(installation, shard, None)
    assert caught.value is retry
