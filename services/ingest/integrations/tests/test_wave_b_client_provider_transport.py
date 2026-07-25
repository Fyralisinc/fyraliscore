"""Transport-contract tests for Brex, Carta, Deel, and Fireflies clients."""
from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest

from lib.shared.provider_transport import (
    ProviderTransport,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
    parse_retry_after,
)
from services.ingest.integrations.brex.client import BrexClient
from services.ingest.integrations.carta.client import CartaClient
from services.ingest.integrations.deel.client import DeelClient
from services.ingest.integrations.fireflies.client import FirefliesClient
from services.ingest.integrations.provider_transport import (
    tenant_preinstall_transport_kwargs,
)
from services.ingest.integrations.provider_transport_runtime import (
    reset_provider_transport_runtime_for_tests,
)


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


def _identity() -> tuple[UUID, UUID]:
    return uuid4(), uuid4()


async def test_brex_attempts_use_exact_binding_and_finite_operations() -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/accounts/cash"):
            return httpx.Response(200, json={"items": [], "next_cursor": None})
        return httpx.Response(200, json={"items": []})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = BrexClient(
            base_url="https://brex.test",
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            api_token="token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await client.list_accounts()

    assert [
        (
            context.source,
            context.operation,
            context.tenant_id,
            context.installation_id,
        )
        for context in recorder.contexts
    ] == [
        (
            "brex",
            "accounts.cash.list",
            str(tenant_id),
            str(installation_id),
        ),
        (
            "brex",
            "accounts.card.list",
            str(tenant_id),
            str(installation_id),
        ),
    ]


async def test_carta_attempts_and_token_mint_use_exact_binding() -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={"issuers": []}),
        ),
    ) as http:
        client = CartaClient(
            base_url="https://carta.test",
            tenant_id=tenant_id,
            install_row_id=installation_id,
            access_token="token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await client.list_issuers()

    [context] = recorder.contexts
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == (
        "carta",
        "issuers.list",
        str(tenant_id),
        str(installation_id),
    )


async def test_carta_reactive_token_mint_is_a_transport_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()
    monkeypatch.setenv("CARTA_CLIENT_ID", "client-id")

    class _Store:
        async def get(self, ref: str, *, tenant_id: UUID) -> bytes:
            assert ref == "refresh-ref"
            return b"client-secret"

        async def put(
            self,
            value: str,
            *,
            label: str,
            tenant_id: UUID,
        ) -> str:
            assert value == "fresh-token"
            assert label.startswith("carta_access_token:")
            return "fresh-access-ref"

    class _Pool:
        async def execute(self, query: str, *args: object) -> str:
            assert "UPDATE carta_installations" in query
            assert args[-2:] == (installation_id, tenant_id)
            return "UPDATE 1"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/o/access_token/"):
            return httpx.Response(
                200,
                json={"access_token": "fresh-token", "expires_in": 3600},
            )
        if request.headers.get("Authorization") == "Bearer stale-token":
            return httpx.Response(401, json={"error": "expired"})
        return httpx.Response(200, json={"issuers": []})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = CartaClient(
            base_url="https://carta.test",
            tenant_id=tenant_id,
            install_row_id=installation_id,
            pool=_Pool(),
            secret_store=_Store(),
            secret_ref="access-ref",
            refresh_secret_ref="refresh-ref",
            access_token="stale-token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await client.list_issuers()

    assert [context.operation for context in recorder.contexts] == [
        "issuers.list",
        "oauth.token.mint",
        "issuers.list",
    ]
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


async def test_deel_attempts_use_exact_binding_and_operation() -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={"data": [], "page": {"cursor": None}},
            ),
        ),
    ) as http:
        client = DeelClient(
            base_url="https://deel.test",
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            api_token="token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await client.list_contracts()

    [context] = recorder.contexts
    assert (
        context.source,
        context.operation,
        context.tenant_id,
        context.installation_id,
    ) == (
        "deel",
        "contracts.list",
        str(tenant_id),
        str(installation_id),
    )


async def test_fireflies_attempts_use_semantic_graphql_operations() -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        query = str(request.content)
        if "transcripts" in query:
            return httpx.Response(200, json={"data": {"transcripts": []}})
        return httpx.Response(
            200,
            json={"data": {"user": {"id": "user-1"}}},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = FirefliesClient(
            base_url="https://fireflies.test",
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            api_token="token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await client.get_workspace()
        await client.list_transcripts_graphql()

    assert [context.operation for context in recorder.contexts] == [
        "user.get",
        "transcripts.list",
    ]
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


async def test_connect_probe_clients_are_tenant_bound_before_install_exists() -> None:
    tenant_id = uuid4()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith(("/accounts/cash", "/accounts/card")):
            return httpx.Response(200, json={"items": [], "next_cursor": None})
        if path.endswith("/v1alpha1/issuers"):
            return httpx.Response(200, json={"issuers": []})
        if path.endswith("/contracts"):
            return httpx.Response(
                200,
                json={"data": [], "page": {"cursor": None}},
            )
        return httpx.Response(
            200,
            json={"data": {"user": {"id": "workspace-1"}}},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        common = {
            "tenant_id": tenant_id,
            "http_client": http,
            "provider_transport": recorder,
            "quota_resolver": _tenant_quota,
            "allow_unlimited_local": False,
            "require_tenant_installation": False,
        }
        await BrexClient(
            base_url="https://brex.test",
            api_token="token",
            **common,
        ).list_accounts()
        await CartaClient(
            base_url="https://carta.test",
            access_token="token",
            **common,
        ).list_issuers()
        await DeelClient(
            base_url="https://deel.test",
            api_token="token",
            **common,
        ).list_contracts()
        await FirefliesClient(
            base_url="https://fireflies.test",
            api_token="token",
            **common,
        ).get_workspace()

    assert {context.source for context in recorder.contexts} == {
        "brex",
        "carta",
        "deel",
        "fireflies",
    }
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id is None
        for context in recorder.contexts
    )


async def test_preinstall_transport_helper_fails_closed_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reset_provider_transport_runtime_for_tests()
    monkeypatch.setenv("FYRALIS_ENV", "production")
    monkeypatch.delenv("FYRALIS_PROVIDER_QUOTAS_JSON", raising=False)
    monkeypatch.delenv("REDIS_URL", raising=False)

    with pytest.raises(
        RuntimeError,
        match="provider transport runtime is incomplete",
    ):
        tenant_preinstall_transport_kwargs(uuid4())


@pytest.mark.parametrize(
    ("module_name", "client_name", "payload"),
    [
        (
            "services.ingest.integrations.brex.oauth",
            "BrexClient",
            {"api_token": "token"},
        ),
        (
            "services.ingest.integrations.carta.oauth",
            "CartaClient",
            {"access_token": "token"},
        ),
        (
            "services.ingest.integrations.deel.oauth",
            "DeelClient",
            {"api_token": "token"},
        ),
        (
            "services.ingest.integrations.fireflies.oauth",
            "FirefliesClient",
            {"api_token": "token"},
        ),
    ],
)
async def test_connect_preflight_routes_pass_authenticated_tenant_binding(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    client_name: str,
    payload: dict[str, str],
) -> None:
    module = importlib.import_module(module_name)
    tenant_id = uuid4()
    captured: dict[str, object] = {}

    def binding(tenant: UUID) -> dict[str, object]:
        captured["binding_tenant"] = tenant
        return {
            "tenant_id": tenant,
            "allow_unlimited_local": True,
            "require_tenant_installation": False,
        }

    class _ProbeClient:
        def __init__(self, **kwargs: object) -> None:
            captured["client_kwargs"] = kwargs

        async def list_accounts(self) -> list[dict[str, object]]:
            return []

        async def list_issuers(
            self,
            *,
            page_size: int,
        ) -> tuple[list[dict[str, object]], None]:
            assert page_size == 50
            return [], None

        async def list_contracts(self) -> list[dict[str, object]]:
            return []

        async def get_workspace(self) -> dict[str, object]:
            return {"id": "workspace-1"}

        async def aclose(self) -> None:
            return None

    class _Request:
        state = SimpleNamespace(auth=SimpleNamespace(tenant_id=tenant_id))

        async def json(self) -> dict[str, str]:
            return payload

    monkeypatch.setattr(module, "tenant_preinstall_transport_kwargs", binding)
    monkeypatch.setattr(module, client_name, _ProbeClient)

    response = await module.connect_preflight(_Request())

    assert response.status_code == 200
    assert captured["binding_tenant"] == tenant_id
    kwargs = captured["client_kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["tenant_id"] == tenant_id
    assert kwargs["require_tenant_installation"] is False


@pytest.mark.parametrize("header_value", ["120", "Sat, 25 Jul 2026 12:02:00 GMT"])
async def test_brex_429_parses_delta_and_http_date_into_retry_later(
    monkeypatch: pytest.MonkeyPatch,
    header_value: str,
) -> None:
    tenant_id, installation_id = _identity()
    fixed_now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    transport = ProviderTransport(now=lambda: fixed_now)
    monkeypatch.setattr(
        "services.ingest.integrations.brex.client.parse_retry_after",
        lambda value: parse_retry_after(value, now=fixed_now),
    )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                429,
                headers={"Retry-After": header_value},
            ),
        ),
    ) as http:
        client = BrexClient(
            base_url="https://brex.test",
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            api_token="token",
            http_client=http,
            provider_transport=transport,
            request_policy=RequestPolicy(
                max_attempts=1,
                max_inline_retry_after_seconds=0,
            ),
            allow_unlimited_local=True,
        )
        with pytest.raises(RetryLater) as caught:
            await client.list_accounts()

    assert caught.value.reason is RetryReason.RATE_LIMIT
    assert caught.value.retry_after_seconds == pytest.approx(120.0)
    assert caught.value.not_before == fixed_now + timedelta(seconds=120)


@pytest.mark.parametrize("source", ["brex", "carta", "deel", "fireflies"])
@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        ("timeout", RetryReason.TIMEOUT),
        ("transport", RetryReason.TRANSIENT),
        ("http_5xx", RetryReason.TRANSIENT),
    ],
)
async def test_retryable_failures_propagate_retry_later(
    source: str,
    failure: str,
    reason: RetryReason,
) -> None:
    tenant_id, installation_id = _identity()

    def handler(request: httpx.Request) -> httpx.Response:
        if failure == "timeout":
            raise httpx.ReadTimeout("timed out", request=request)
        if failure == "transport":
            raise httpx.ConnectError("connection lost", request=request)
        return httpx.Response(503, json={"error": "unavailable"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        common = {
            "tenant_id": tenant_id,
            "http_client": http,
            "provider_transport": ProviderTransport(),
            "request_policy": RequestPolicy(max_attempts=1),
            "allow_unlimited_local": True,
        }
        if source == "brex":
            client = BrexClient(
                base_url="https://provider.test",
                installation_row_id=installation_id,
                api_token="token",
                **common,
            )
            operation = client.list_accounts()
        elif source == "carta":
            client = CartaClient(
                base_url="https://provider.test",
                install_row_id=installation_id,
                access_token="token",
                **common,
            )
            operation = client.list_issuers()
        elif source == "deel":
            client = DeelClient(
                base_url="https://provider.test",
                installation_row_id=installation_id,
                api_token="token",
                **common,
            )
            operation = client.list_contracts()
        else:
            client = FirefliesClient(
                base_url="https://provider.test",
                installation_row_id=installation_id,
                api_token="token",
                **common,
            )
            operation = client.get_workspace()

        with pytest.raises(RetryLater) as caught:
            await operation

    assert caught.value.reason is reason
    assert caught.value.request_context.source == source
    assert caught.value.request_context.tenant_id == str(tenant_id)
    assert (
        caught.value.request_context.installation_id
        == str(installation_id)
    )


async def test_fireflies_graphql_rate_error_propagates_retry_later() -> None:
    tenant_id, installation_id = _identity()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                json={
                    "errors": [
                        {
                            "message": "quota exhausted",
                            "extensions": {"code": "TOO_MANY_REQUESTS"},
                        },
                    ],
                },
            ),
        ),
    ) as http:
        client = FirefliesClient(
            base_url="https://fireflies.test",
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            api_token="token",
            http_client=http,
            provider_transport=ProviderTransport(),
            request_policy=RequestPolicy(max_attempts=1),
            allow_unlimited_local=True,
        )
        with pytest.raises(RetryLater) as caught:
            await client.get_workspace()

    assert caught.value.reason is RetryReason.RATE_LIMIT
    assert caught.value.request_context.operation == "user.get"


@pytest.mark.parametrize(
    "client_factory",
    [
        lambda http, tenant: BrexClient(
            base_url="https://provider.test",
            tenant_id=tenant,
            api_token="token",
            http_client=http,
            provider_transport=_Recorder(),
            quota_resolver=_quota,
            allow_unlimited_local=False,
        ),
        lambda http, tenant: CartaClient(
            base_url="https://provider.test",
            tenant_id=tenant,
            access_token="token",
            http_client=http,
            provider_transport=_Recorder(),
            quota_resolver=_quota,
            allow_unlimited_local=False,
        ),
        lambda http, tenant: DeelClient(
            base_url="https://provider.test",
            tenant_id=tenant,
            api_token="token",
            http_client=http,
            provider_transport=_Recorder(),
            quota_resolver=_quota,
            allow_unlimited_local=False,
        ),
        lambda http, tenant: FirefliesClient(
            base_url="https://provider.test",
            tenant_id=tenant,
            api_token="token",
            http_client=http,
            provider_transport=_Recorder(),
            quota_resolver=_quota,
            allow_unlimited_local=False,
        ),
    ],
)
async def test_production_missing_installation_fails_before_http(
    monkeypatch: pytest.MonkeyPatch,
    client_factory,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    sent = False

    def unexpected(request: httpx.Request) -> httpx.Response:
        nonlocal sent
        sent = True
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(unexpected),
    ) as http:
        client = client_factory(http, uuid4())
        with pytest.raises(Exception, match="missing exact tenant/installation"):
            if isinstance(client, BrexClient):
                await client.list_accounts()
            elif isinstance(client, CartaClient):
                await client.list_issuers()
            elif isinstance(client, DeelClient):
                await client.list_contracts()
            else:
                await client.get_workspace()

    assert sent is False
