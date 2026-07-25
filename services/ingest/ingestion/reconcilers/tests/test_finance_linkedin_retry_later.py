"""Reconciler probes must not translate RetryLater into a false no-gap result."""
from __future__ import annotations

import importlib
from uuid import uuid4

import pytest

from lib.shared.provider_transport import (
    RequestContext,
    RetryLater,
    RetryReason,
)


pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    ("source", "entity_type"),
    [
        ("quickbooks", "Invoice"),
        ("ramp", "transaction"),
        ("gusto", "payroll"),
        ("linkedin", "post"),
    ],
)
async def test_reconciler_probe_propagates_retry_later(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    entity_type: str,
) -> None:
    module = importlib.import_module(
        f"services.ingest.ingestion.reconcilers.{source}",
    )
    retry = RetryLater.after(
        request_context=RequestContext(
            source=source,
            operation="reconcile.probe",
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

        async def list_payrolls(
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

    async def high_water(pool: object, shard_id: object) -> object:
        del pool, shard_id
        return 1 if source == "linkedin" else "2026-07-25"

    monkeypatch.setattr(module, "_load_shard_high_water", high_water)
    shard = {
        "id": uuid4(),
        "shard_identifier": {
            "shard_kind": module.SHARD_KIND_ENTITY,
            "entity_type": entity_type,
        },
    }

    with pytest.raises(RetryLater) as caught:
        await module._check_one_shard_for_gap(  # noqa: SLF001
            pool=object(),
            client=_Client(),
            shard=shard,
        )
    assert caught.value is retry
