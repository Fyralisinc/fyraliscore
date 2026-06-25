from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.integrations.brex.client import BrexClient
from services.ingest.integrations.ashby.client import AshbyClient
from services.ingest.integrations.deel.client import DeelClient
from services.ingest.integrations.figma.client import FigmaClient
from services.ingest.integrations.fireflies.client import FirefliesClient
from services.ingest.integrations.grafana.client import GrafanaClient
from services.ingest.integrations.hibob.client import HibobClient
from services.ingest.integrations.jira.client import JiraClient
from services.ingest.integrations.mercury.client import MercuryClient
from services.ingest.integrations.miro.client import MiroClient
from services.ingest.integrations.notion.client import NotionClient
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


@pytest.mark.parametrize(
    ("factory", "method_name", "secret_ref"),
    [
        (
            lambda store, tenant_id, ref: NotionClient(
                secret_store=store, tenant_id=tenant_id, secret_ref=ref,
            ),
            "_token",
            "notion-ref",
        ),
        (
            lambda store, tenant_id, ref: MercuryClient(
                base_url="https://api.mercury.com",
                secret_store=store,
                tenant_id=tenant_id,
                secret_ref=ref,
            ),
            "_token",
            "mercury-ref",
        ),
        (
            lambda store, tenant_id, ref: GrafanaClient(
                base_url="https://grafana.example",
                secret_store=store,
                tenant_id=tenant_id,
                secret_ref=ref,
            ),
            "_token",
            "grafana-ref",
        ),
        (
            lambda store, tenant_id, ref: DeelClient(
                base_url="https://api.letsdeel.com/rest/v2",
                secret_store=store,
                tenant_id=tenant_id,
                secret_ref=ref,
            ),
            "_token",
            "deel-ref",
        ),
        (
            lambda store, tenant_id, ref: FirefliesClient(
                base_url="https://api.fireflies.ai/graphql",
                secret_store=store,
                tenant_id=tenant_id,
                secret_ref=ref,
            ),
            "_token",
            "fireflies-ref",
        ),
        (
            lambda store, tenant_id, ref: FigmaClient(
                base_url="https://api.figma.com",
                secret_store=store,
                tenant_id=tenant_id,
                secret_ref=ref,
            ),
            "_token",
            "figma-ref",
        ),
        (
            lambda store, tenant_id, ref: MiroClient(
                base_url="https://api.miro.com/v2",
                secret_store=store,
                tenant_id=tenant_id,
                secret_ref=ref,
            ),
            "_token",
            "miro-ref",
        ),
        (
            lambda store, tenant_id, ref: HibobClient(
                base_url="https://api.hibob.com",
                company_id="company-1",
                service_user_id="service-user",
                secret_store=store,
                tenant_id=tenant_id,
                secret_ref=ref,
            ),
            "_token_value",
            "hibob-ref",
        ),
        (
            lambda store, tenant_id, ref: AshbyClient(
                base_url="https://api.ashbyhq.com",
                org_id="org-1",
                secret_store=store,
                tenant_id=tenant_id,
                secret_ref=ref,
            ),
            "_key",
            "ashby-ref",
        ),
    ],
)
async def test_static_api_clients_reload_secret_ref_when_cache_ttl_is_zero(
    monkeypatch: pytest.MonkeyPatch,
    factory,
    method_name: str,
    secret_ref: str,
) -> None:
    monkeypatch.setenv(SECRET_CACHE_TTL_ENV, "0")
    store = _Store("first-token")
    tenant_id = uuid4()
    client = factory(store, tenant_id, secret_ref)
    resolver = getattr(client, method_name)

    first = await resolver()
    store.value = "second-token"
    second = await resolver()

    assert (first, second) == ("first-token", "second-token")
    assert store.calls == [(secret_ref, tenant_id), (secret_ref, tenant_id)]


class _AsyncLock:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *args):
        return False
