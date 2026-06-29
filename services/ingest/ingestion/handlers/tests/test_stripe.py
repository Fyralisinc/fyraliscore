from __future__ import annotations

import pytest

from services.ingest.ingestion.handlers.stripe import handle_stripe_webhook


pytestmark = pytest.mark.asyncio


def _event(*, event_id: str = "evt_retry_1", status: str = "paid") -> dict:
    return {
        "id": event_id,
        "type": "invoice.paid",
        "created": 1_700_000_000,
        "data": {
            "object": {
                "id": "in_1",
                "object": "invoice",
                "customer": "cus_1",
                "status": status,
            }
        },
    }


async def test_webhook_retry_dedups_on_event_id() -> None:
    first = await handle_stripe_webhook(_event(status="paid"), {})
    retry = await handle_stripe_webhook(_event(status="void"), {})

    assert first.source_channel == "stripe:webhook"
    assert first.external_id == retry.external_id == "evt_retry_1"


async def test_distinct_events_get_distinct_external_ids() -> None:
    first = await handle_stripe_webhook(_event(event_id="evt_1"), {})
    second = await handle_stripe_webhook(_event(event_id="evt_2"), {})

    assert first.external_id == "evt_1"
    assert second.external_id == "evt_2"
