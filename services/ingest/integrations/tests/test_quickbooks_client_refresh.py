"""The QuickBooks read client reactively re-mints its access token on a 401
(token expired mid-fetch), persists the rotated tokens, and retries — and on a
failed refresh raises the 401 (which shard_fetch records as a degraded shard).
"""
from __future__ import annotations

import httpx
import pytest

from lib.shared.errors import QuickBooksApiError
from services.ingest.integrations.quickbooks.client import QuickBooksClient

pytestmark = pytest.mark.asyncio

TENANT = "11111111-1111-1111-1111-111111111111"
INSTALL = "22222222-2222-2222-2222-222222222222"


class FakeStore:
    def __init__(self, initial: dict) -> None:
        self._data = dict(initial)
        self._n = 0

    async def get(self, ref, *, tenant_id):
        return self._data[ref].encode("utf-8")

    async def put(self, plaintext, *, label, tenant_id):
        self._n += 1
        ref = f"new-{self._n}"
        self._data[ref] = (
            plaintext.decode("utf-8") if isinstance(plaintext, bytes) else plaintext
        )
        return ref


class FakePool:
    def __init__(self) -> None:
        self.executed: list = []

    async def execute(self, sql, *args):
        self.executed.append((sql, args))


def _client(http: httpx.AsyncClient, pool, store) -> QuickBooksClient:
    return QuickBooksClient(
        base_url="https://quickbooks.api.intuit.com",
        realm_id="realm-1",
        pool=pool,
        secret_store=store,
        tenant_id=TENANT,
        secret_ref="access-ref",
        access_token=None,           # force resolution from the store
        http_client=http,
        install_row_id=INSTALL,
        refresh_secret_ref="refresh-ref",
    )


async def test_quickbooks_client_remints_on_401_and_retries(monkeypatch):
    monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "cid")
    monkeypatch.setenv("QUICKBOOKS_CLIENT_SECRET", "csec")
    store = FakeStore({"access-ref": "stale-token", "refresh-ref": "the-refresh"})
    pool = FakePool()
    state = {"api_calls": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "tokens/bearer" in url:  # Intuit token endpoint → mint a fresh token
            return httpx.Response(200, json={
                "access_token": "fresh-token",
                "refresh_token": "rotated-refresh",
                "expires_in": 3600,
            })
        # QBO query endpoint: 401 with the stale token, 200 with the fresh one.
        state["api_calls"] += 1
        if "Bearer fresh-token" in request.headers.get("authorization", ""):
            return httpx.Response(200, json={
                "QueryResponse": {"Invoice": [{"Id": "1"}], "maxResults": 1},
            })
        return httpx.Response(401, json={"fault": {"type": "AUTHENTICATION"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = _client(http, pool, store)
        rows, _next = await client.query("Invoice")

    assert rows == [{"Id": "1"}]            # the retry after re-mint succeeded
    assert state["api_calls"] == 2          # 401, then 200
    assert len(pool.executed) == 1          # tokens persisted to the install row
    assert "UPDATE quickbooks_installations" in pool.executed[0][0]


async def test_quickbooks_client_failed_refresh_raises_degraded(monkeypatch):
    monkeypatch.setenv("QUICKBOOKS_CLIENT_ID", "cid")
    monkeypatch.setenv("QUICKBOOKS_CLIENT_SECRET", "csec")
    store = FakeStore({"access-ref": "stale-token", "refresh-ref": "revoked"})
    pool = FakePool()

    def handler(request: httpx.Request) -> httpx.Response:
        if "tokens/bearer" in str(request.url):
            return httpx.Response(400, json={"error": "invalid_grant"})
        return httpx.Response(401, json={"fault": {"type": "AUTHENTICATION"}})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = _client(http, pool, store)
        with pytest.raises(QuickBooksApiError) as exc:
            await client.query("Invoice")

    assert exc.value.code == "quickbooks_api_unauthorized"
    assert pool.executed == []  # nothing persisted on a failed refresh
