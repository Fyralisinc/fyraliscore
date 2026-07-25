"""ProviderTransport cutover tests for Notion, Miro, and Mercury."""
from __future__ import annotations

from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI

from lib.shared.provider_transport import (
    ProviderTransport,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
)
from services.ingest.integrations.mercury.client import MercuryClient
from services.ingest.integrations.miro.client import MiroClient
from services.ingest.integrations.notion.client import NotionClient


pytestmark = pytest.mark.asyncio


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        assert isinstance(policy, RequestPolicy)
        self.contexts.append(context)
        return await call()


def _quota(
    source: str,
    operation: str,
    tenant_id: str | None,
    installation_id: str | None,
    dimensions: dict[str, str],
) -> tuple[QuotaRequirement, ...]:
    assert tenant_id is not None
    assert installation_id is not None
    assert dimensions == {}
    return (
        QuotaRequirement(
            scope="installation",
            bucket_key=f"{source}:{operation}:{installation_id}",
            capacity=10,
            refill_per_second=10.0,
        ),
    )


def _tenant_quota(
    source: str,
    operation: str,
    tenant_id: str | None,
    installation_id: str | None,
    dimensions: dict[str, str],
) -> tuple[QuotaRequirement, ...]:
    assert tenant_id is not None
    assert installation_id is None
    assert dimensions == {}
    return (
        QuotaRequirement(
            scope="tenant",
            bucket_key=f"{source}:{operation}:{tenant_id}",
            capacity=10,
            refill_per_second=10.0,
        ),
    )


async def test_notion_attempts_use_exact_binding_and_semantic_operations() -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/users/me"):
            return httpx.Response(200, json={"object": "user", "id": "bot-1"})
        if "/pages/" in path:
            return httpx.Response(
                200,
                json={
                    "object": "page",
                    "id": path.rsplit("/", 1)[-1],
                    "last_edited_time": "2026-01-01T00:00:00Z",
                    "parent": {"type": "workspace"},
                },
            )
        if path.endswith("/comments"):
            return httpx.Response(
                200,
                json={"results": [], "next_cursor": None, "has_more": False},
            )
        if "/blocks/" in path:
            return httpx.Response(
                200,
                json={"results": [], "next_cursor": None, "has_more": False},
            )
        if "/databases/" in path:
            return httpx.Response(
                200,
                json={
                    "results": [
                        {"last_edited_time": "2026-01-01T00:00:00Z"},
                    ],
                    "next_cursor": None,
                    "has_more": False,
                },
            )
        return httpx.Response(
            200,
            json={
                "results": [
                    {
                        "object": "page",
                        "id": "page-1",
                        "last_edited_time": "2026-01-01T00:00:00Z",
                        "parent": {"type": "workspace"},
                    },
                ],
                "next_cursor": None,
                "has_more": False,
            },
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = NotionClient(
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            workspace_id="workspace-1",
            bot_token="token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await client.search()
        await client.query_database("database-1")
        await client.list_block_children("page-1")
        await client.list_comments("page-1")
        await client.latest_database_edit("database-1")
        await client.latest_page_edit()
        await client.retrieve_page("page-1")
        await client.retrieve_bot_user()

    assert [context.operation for context in recorder.contexts] == [
        "search",
        "databases.query",
        "blocks.children.list",
        "comments.list",
        "databases.query",
        "search",
        "pages.retrieve",
        "users.me",
    ]
    assert all(
        context.source == "notion"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


async def test_miro_and_mercury_attempts_use_exact_bindings() -> None:
    tenant_id = uuid4()
    miro_installation_id = uuid4()
    mercury_installation_id = uuid4()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/miro/boards":
            return httpx.Response(
                200,
                json={
                    "data": [{"id": "board-1"}],
                    "total": 1,
                    "size": 1,
                    "offset": 0,
                    "limit": 50,
                },
            )
        if path == "/miro/boards/board-1":
            return httpx.Response(200, json={"id": "board-1"})
        if path == "/miro/boards/board-1/items":
            return httpx.Response(200, json={"data": [], "total": 0})
        if path == "/mercury/accounts":
            return httpx.Response(200, json={"accounts": [{"id": "account-1"}]})
        if path == "/mercury/account/account-1":
            return httpx.Response(200, json={"id": "account-1"})
        if path == "/mercury/account/account-1/transactions":
            return httpx.Response(
                200,
                json={"transactions": [], "total": 0},
            )
        return httpx.Response(404)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        miro = MiroClient(
            base_url="https://provider.test/miro",
            tenant_id=tenant_id,
            installation_row_id=miro_installation_id,
            api_token="miro-token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        mercury = MercuryClient(
            base_url="https://provider.test/mercury",
            tenant_id=tenant_id,
            installation_row_id=mercury_installation_id,
            api_token="mercury-token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await miro.list_boards()
        await miro.get_board("board-1")
        await miro.list_items("board-1")
        await mercury.list_accounts()
        await mercury.get_account("account-1")
        await mercury.list_transactions("account-1")

    operations = [
        (context.source, context.operation, context.installation_id)
        for context in recorder.contexts
    ]
    assert operations == [
        ("miro", "boards.list", str(miro_installation_id)),
        ("miro", "boards.get", str(miro_installation_id)),
        ("miro", "board_items.list", str(miro_installation_id)),
        ("mercury", "accounts.list", str(mercury_installation_id)),
        ("mercury", "accounts.get", str(mercury_installation_id)),
        ("mercury", "transactions.list", str(mercury_installation_id)),
    ]
    assert all(context.tenant_id == str(tenant_id) for context in recorder.contexts)


async def test_production_builders_thread_exact_installation_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.ingestion.fetchers import _clients as builders

    tenant_id = uuid4()
    installation_ids = {
        "notion": uuid4(),
        "miro": uuid4(),
        "mercury": uuid4(),
    }
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/notion/v1/search":
            return httpx.Response(
                200,
                json={"results": [], "next_cursor": None, "has_more": False},
            )
        if request.url.path == "/miro/boards":
            return httpx.Response(200, json={"data": [], "total": 0})
        if request.url.path == "/mercury/accounts":
            return httpx.Response(200, json={"accounts": []})
        return httpx.Response(404)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))

    async def get_http() -> httpx.AsyncClient:
        return http

    async def effective_pool(pool, *, provider_lab):  # noqa: ANN001, ANN202
        assert provider_lab is True
        return pool

    monkeypatch.setenv("PROVIDER_LAB_URL", "http://127.0.0.1:8787")
    monkeypatch.setenv("NOTION_API_BASE_URL", "http://127.0.0.1:8787/notion")
    monkeypatch.setenv("MIRO_API_BASE_URL", "http://127.0.0.1:8787/miro")
    monkeypatch.setenv("MERCURY_API_BASE_URL", "http://127.0.0.1:8787/mercury")
    monkeypatch.setattr(builders, "_get_http", get_http)
    monkeypatch.setattr(builders, "_effective_pool", effective_pool)
    monkeypatch.setattr(
        builders,
        "_provider_transport_kwargs",
        lambda: {
            "provider_transport": recorder,
            "quota_resolver": _quota,
            "allow_unlimited_local": False,
        },
    )

    try:
        notion = await builders.build_notion_client(
            {
                "id": installation_ids["notion"],
                "tenant_id": tenant_id,
                "installation_id": "workspace-1",
                "secret_ref": None,
            }
        )
        miro = await builders.build_miro_client(
            {
                "id": installation_ids["miro"],
                "tenant_id": tenant_id,
                "base_url": "https://api.miro.com/v2",
                "secret_ref": None,
            }
        )
        mercury = await builders.build_mercury_client(
            {
                "id": installation_ids["mercury"],
                "tenant_id": tenant_id,
                "base_url": "https://api.mercury.com/api/v1",
                "secret_ref": None,
            }
        )
        await notion.search()
        await miro.list_boards()
        await mercury.list_accounts()
    finally:
        await http.aclose()

    assert {
        context.source: context.installation_id
        for context in recorder.contexts
    } == {
        source: str(installation_id)
        for source, installation_id in installation_ids.items()
    }
    assert all(context.tenant_id == str(tenant_id) for context in recorder.contexts)


@pytest.mark.parametrize("source", ["notion", "miro", "mercury"])
async def test_connect_probe_binding_is_tenant_scoped_before_install(
    source: str,
) -> None:
    tenant_id = uuid4()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        if source == "notion":
            return httpx.Response(
                200,
                json={"results": [], "next_cursor": None, "has_more": False},
            )
        if source == "miro":
            return httpx.Response(200, json={"data": [], "total": 0})
        return httpx.Response(200, json={"accounts": []})

    common = {
        "tenant_id": tenant_id,
        "http_client": httpx.AsyncClient(
            transport=httpx.MockTransport(handler),
        ),
        "provider_transport": recorder,
        "quota_resolver": _tenant_quota,
        "allow_unlimited_local": False,
        "require_tenant_installation": False,
    }
    try:
        if source == "notion":
            await NotionClient(
                bot_token="token",
                api_base_url="https://provider.test",
                **common,
            ).search()
        elif source == "miro":
            await MiroClient(
                base_url="https://provider.test",
                api_token="token",
                **common,
            ).list_boards()
        else:
            await MercuryClient(
                base_url="https://provider.test",
                api_token="token",
                **common,
            ).list_accounts()
    finally:
        await common["http_client"].aclose()  # type: ignore[union-attr]

    assert recorder.contexts
    assert all(
        context.source == source
        and context.tenant_id == str(tenant_id)
        and context.installation_id is None
        for context in recorder.contexts
    )


@pytest.mark.parametrize(
    ("source", "client_name", "list_method"),
    [
        ("miro", "MiroClient", "list_boards"),
        ("mercury", "MercuryClient", "list_accounts"),
    ],
)
async def test_connect_routes_pass_authenticated_tenant_to_probe_client(
    source: str,
    client_name: str,
    list_method: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = __import__(
        f"services.ingest.integrations.{source}.oauth",
        fromlist=["oauth"],
    )
    tenant_id = uuid4()
    captured: dict[str, object] = {}

    def binding(tenant: UUID) -> dict[str, object]:
        captured["tenant"] = tenant
        return {
            "tenant_id": tenant,
            "allow_unlimited_local": True,
            "require_tenant_installation": False,
        }

    class _Probe:
        def __init__(self, **kwargs: object) -> None:
            captured["kwargs"] = kwargs

        async def list_boards(self) -> list[dict[str, object]]:
            return []

        async def list_accounts(self) -> list[dict[str, object]]:
            return []

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(module, "tenant_preinstall_transport_kwargs", binding)
    monkeypatch.setattr(module, client_name, _Probe)
    app = FastAPI()

    @app.middleware("http")
    async def _auth(request, call_next):  # noqa: ANN001, ANN202
        request.state.auth = type("Auth", (), {"tenant_id": tenant_id})()
        return await call_next(request)

    app.include_router(module.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            f"/integrations/{source}/connect/preflight",
            json={"api_token": "token"},
        )

    assert response.status_code == 200
    assert captured["tenant"] == tenant_id
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["tenant_id"] == tenant_id
    assert kwargs["require_tenant_installation"] is False
    assert hasattr(_Probe, list_method)


@pytest.mark.parametrize(
    ("source", "operation"),
    [
        ("notion", "search"),
        ("miro", "boards.list"),
        ("mercury", "accounts.list"),
    ],
)
async def test_long_429_becomes_retry_later(
    source: str,
    operation: str,
) -> None:
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(  # noqa: ARG005
                429,
                headers={"Retry-After": "120"},
            )
        )
    )
    common = {
        "tenant_id": uuid4(),
        "installation_row_id": uuid4(),
        "http_client": http,
        "provider_transport": ProviderTransport(),
        "request_policy": RequestPolicy(
            max_attempts=1,
            max_inline_retry_after_seconds=0,
        ),
        "allow_unlimited_local": True,
    }
    try:
        with pytest.raises(RetryLater) as raised:
            if source == "notion":
                await NotionClient(
                    bot_token="token",
                    api_base_url="https://provider.test",
                    **common,
                ).search()
            elif source == "miro":
                await MiroClient(
                    base_url="https://provider.test",
                    api_token="token",
                    **common,
                ).list_boards()
            else:
                await MercuryClient(
                    base_url="https://provider.test",
                    api_token="token",
                    **common,
                ).list_accounts()
    finally:
        await http.aclose()

    assert raised.value.reason is RetryReason.RATE_LIMIT
    assert raised.value.request_context.source == source
    assert raised.value.request_context.operation == operation
    assert raised.value.retry_after_seconds == 120


@pytest.mark.parametrize("source", ["notion", "miro", "mercury"])
async def test_client_fails_closed_without_transport_in_production(
    source: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    common = {
        "tenant_id": UUID("00000000-0000-0000-0000-000000000001"),
        "installation_row_id": UUID(
            "00000000-0000-0000-0000-000000000002"
        ),
        "http_client": httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json={}),  # noqa: ARG005
            )
        ),
    }
    try:
        with pytest.raises(RuntimeError, match="requires ProviderTransport"):
            if source == "notion":
                NotionClient(
                    bot_token="token",
                    api_base_url="https://provider.test",
                    **common,
                )
            elif source == "miro":
                MiroClient(
                    base_url="https://provider.test",
                    api_token="token",
                    **common,
                )
            else:
                MercuryClient(
                    base_url="https://provider.test",
                    api_token="token",
                    **common,
                )
    finally:
        await common["http_client"].aclose()  # type: ignore[union-attr]
