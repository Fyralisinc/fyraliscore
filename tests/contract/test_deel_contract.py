"""Contract tests for the real Deel REST v2 client surface."""
from __future__ import annotations

import httpx
import pytest

from services.ingest.integrations.deel.client import DeelClient

pytestmark = pytest.mark.asyncio


def _client(http: httpx.AsyncClient) -> DeelClient:
    return DeelClient(
        base_url="https://api.letsdeel.com",
        api_token="test-token",
        http_client=http,
        api_version="2026-01-01",
    )


@pytest.mark.contract
async def test_deel_contracts_use_rest_v2_and_version_header():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={
            "data": [{"id": "con-1", "name": "Agreement"}],
            "page": {"cursor": None, "total_rows": 1},
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        contracts = await _client(http).list_contracts()

    assert contracts == [{"id": "con-1", "name": "Agreement"}]
    assert seen[0].url.path == "/rest/v2/contracts"
    assert seen[0].headers["X-Version"] == "2026-01-01"


@pytest.mark.contract
async def test_deel_payments_are_read_from_invoices_stream():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={
            "data": [
                {"id": "inv-1", "contract_id": "con-1", "total_amount": "-10.00"},
                {"id": "inv-2", "contract_id": "con-2", "total_amount": "-20.00"},
            ],
            "page": {"cursor": None, "total_rows": 2},
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        payments, next_offset, total = await _client(http).list_payments(
            "con-1", start="2026-05-01T00:00:00Z",
        )

    assert seen[0].url.path == "/rest/v2/invoices"
    query = dict(httpx.QueryParams(seen[0].url.query))
    assert query["contract_id"] == "con-1"
    assert query["created_after"] == "2026-05-01T00:00:00Z"
    assert [p["id"] for p in payments] == ["inv-1"]
    assert next_offset is None
    assert total == 1
