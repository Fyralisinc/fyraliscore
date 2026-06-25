from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.integrations.brex.client import BrexClient
from services.ingest.integrations.jira.client import JiraClient
from services.ingest.integrations.secret_cache import (
    SECRET_CACHE_TTL_ENV,
    SecretValueCache,
)


pytestmark = pytest.mark.asyncio


class _Store:
    def __init__(self, value: str) -> None:
        self.value = value
        self.calls: list[tuple[str, object]] = []

    async def get(self, ref: str, *, tenant_id):
        self.calls.append((ref, tenant_id))
        return self.value.encode("utf-8")


async def test_secret_value_cache_reloads_after_ttl() -> None:
    now = 100.0

    def clock() -> float:
        return now

    store = _Store("first")
    tenant_id = uuid4()
    cache = SecretValueCache(ttl_seconds=5.0, clock=clock)

    first = await cache.resolve(
        lock=_AsyncLock(),
        secret_store=store,
        secret_ref="ref",
        tenant_id=tenant_id,
        missing_error=lambda: RuntimeError("missing"),
    )
    store.value = "second"
    cached = await cache.resolve(
        lock=_AsyncLock(),
        secret_store=store,
        secret_ref="ref",
        tenant_id=tenant_id,
        missing_error=lambda: RuntimeError("missing"),
    )
    now = 106.0
    refreshed = await cache.resolve(
        lock=_AsyncLock(),
        secret_store=store,
        secret_ref="ref",
        tenant_id=tenant_id,
        missing_error=lambda: RuntimeError("missing"),
    )

    assert (first, cached, refreshed) == ("first", "first", "second")
    assert len(store.calls) == 2


async def test_brex_client_reloads_secret_ref_when_cache_ttl_is_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SECRET_CACHE_TTL_ENV, "0")
    store = _Store("brex-one")
    tenant_id = uuid4()
    client = BrexClient(
        base_url="https://platform.brexapis.com",
        secret_store=store,
        tenant_id=tenant_id,
        secret_ref="brex-ref",
    )

    first = await client._token()
    store.value = "brex-two"
    second = await client._token()

    assert (first, second) == ("brex-one", "brex-two")
    assert store.calls == [("brex-ref", tenant_id), ("brex-ref", tenant_id)]


async def test_jira_client_reloads_secret_ref_when_cache_ttl_is_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SECRET_CACHE_TTL_ENV, "0")
    store = _Store("jira-one")
    tenant_id = uuid4()
    client = JiraClient(
        base_url="https://acme.atlassian.net",
        account_email="admin@example.com",
        secret_store=store,
        tenant_id=tenant_id,
        secret_ref="jira-ref",
    )

    first = await client._token()
    store.value = "jira-two"
    second = await client._token()

    assert (first, second) == ("jira-one", "jira-two")
    assert store.calls == [("jira-ref", tenant_id), ("jira-ref", tenant_id)]


class _AsyncLock:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *args):
        return False
