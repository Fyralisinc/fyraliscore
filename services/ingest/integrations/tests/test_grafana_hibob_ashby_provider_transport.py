"""ProviderTransport cutover tests for Grafana, HiBob, and Ashby."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
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
from services.ingest.integrations.ashby.client import AshbyClient
from services.ingest.integrations.grafana.client import GrafanaClient
from services.ingest.integrations.hibob.client import HibobClient


pytestmark = pytest.mark.asyncio


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        assert isinstance(policy, RequestPolicy)
        self.contexts.append(context)
        return await call()


class _QuotaRecorder:
    def __init__(self) -> None:
        self.calls: list[
            tuple[str, str, str | None, str | None, dict[str, str]]
        ] = []

    def __call__(
        self,
        source: str,
        operation: str,
        tenant_id: str | None,
        installation_id: str | None,
        dimensions: dict[str, str],
    ) -> tuple[QuotaRequirement, ...]:
        self.calls.append(
            (
                source,
                operation,
                tenant_id,
                installation_id,
                dict(dimensions),
            )
        )
        return (
            QuotaRequirement(
                scope="installation",
                bucket_key=f"{source}:{installation_id}",
                capacity=10,
                refill_per_second=10.0,
            ),
        )


def _success_handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path == "/grafana/api/annotations":
        return httpx.Response(200, json=[{"id": 1, "time": 1}])
    if path == "/grafana/api/org":
        return httpx.Response(200, json={"id": 1, "name": "Grafana"})
    if path == "/hibob/v1/people/search":
        return httpx.Response(200, json={"employees": [{"id": "person-1"}]})
    if path == "/hibob/v1/timeoff/requests/changes":
        return httpx.Response(200, json={"requests": [{"id": "leave-1"}]})
    if path == "/hibob/v1/bulk/people/salaries":
        return httpx.Response(
            200,
            json={"results": [{"id": "salary-1"}]},
        )
    if path == "/hibob/v1/bulk/people/work":
        return httpx.Response(
            200,
            json={"results": [{"id": "work-1"}]},
        )
    if path == "/ashby/candidate.list":
        return httpx.Response(
            200,
            json={
                "success": True,
                "results": [{"id": "candidate-1"}],
                "moreDataAvailable": False,
            },
        )
    if path == "/ashby/candidate.info":
        return httpx.Response(
            200,
            json={"success": True, "results": {"id": "candidate-1"}},
        )
    return httpx.Response(404)


async def test_all_used_operations_have_exact_request_contexts() -> None:
    tenant_id = uuid4()
    installation_ids = {
        "grafana": uuid4(),
        "hibob": uuid4(),
        "ashby": uuid4(),
    }
    recorder = _Recorder()
    quotas = _QuotaRecorder()
    http = httpx.AsyncClient(transport=httpx.MockTransport(_success_handler))
    common = {
        "tenant_id": tenant_id,
        "http_client": http,
        "provider_transport": recorder,
        "quota_resolver": quotas,
        "allow_unlimited_local": False,
    }
    grafana = GrafanaClient(
        base_url="https://provider.test/grafana",
        api_token="token",
        installation_row_id=installation_ids["grafana"],
        **common,
    )
    hibob = HibobClient(
        base_url="https://provider.test/hibob",
        company_id="company-1",
        service_user_id="service-user",
        token="token",
        installation_row_id=installation_ids["hibob"],
        **common,
    )
    ashby = AshbyClient(
        base_url="https://provider.test/ashby",
        org_id="org-1",
        api_key="token",
        installation_row_id=installation_ids["ashby"],
        **common,
    )
    try:
        await grafana.list_annotations()
        await grafana.get_org()
        await hibob.company_info()
        await hibob.list_entities("timeoff")
        await hibob.list_entities("payroll")
        await hibob.list_entities("lifecycle")
        await ashby.list_entities("candidate")
        await ashby.get_entity("candidate", "candidate-1")
    finally:
        await http.aclose()

    assert {
        source: {
            context.operation
            for context in recorder.contexts
            if context.source == source
        }
        for source in installation_ids
    } == {
        "grafana": {"annotations.list", "org.get"},
        "hibob": {
            "people.search",
            "timeoff.changes.list",
            "people.salaries.list",
            "people.work.list",
        },
        "ashby": {"entities.list", "entities.info"},
    }
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id
        == str(installation_ids[context.source])
        for context in recorder.contexts
    )
    assert {
        (source, operation, tuple(sorted(dimensions.items())))
        for source, operation, _, _, dimensions in quotas.calls
        if source == "ashby"
    } == {
        ("ashby", "entities.list", (("rpc_method", "candidate.list"),)),
        ("ashby", "entities.info", (("rpc_method", "candidate.info"),)),
    }


async def test_production_builders_thread_installation_row_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.ingestion.fetchers import _clients as builders

    tenant_id = uuid4()
    installation_ids = {
        "grafana": uuid4(),
        "hibob": uuid4(),
        "ashby": uuid4(),
    }
    recorder = _Recorder()
    quotas = _QuotaRecorder()
    http = httpx.AsyncClient(transport=httpx.MockTransport(_success_handler))

    async def get_http() -> httpx.AsyncClient:
        return http

    async def effective_pool(pool, *, provider_lab):  # noqa: ANN001, ANN202
        assert provider_lab is True
        return pool

    monkeypatch.setenv("PROVIDER_LAB_URL", "http://127.0.0.1:8787")
    monkeypatch.setenv("GRAFANA_API_BASE_URL", "http://127.0.0.1:8787/grafana")
    monkeypatch.setenv("HIBOB_API_BASE_URL", "http://127.0.0.1:8787/hibob")
    monkeypatch.setenv("ASHBY_API_BASE_URL", "http://127.0.0.1:8787/ashby")
    monkeypatch.setattr(builders, "_get_http", get_http)
    monkeypatch.setattr(builders, "_effective_pool", effective_pool)
    monkeypatch.setattr(
        builders,
        "_provider_transport_kwargs",
        lambda: {
            "provider_transport": recorder,
            "quota_resolver": quotas,
            "allow_unlimited_local": False,
        },
    )
    try:
        grafana = await builders.build_grafana_client(
            {
                "id": installation_ids["grafana"],
                "tenant_id": tenant_id,
                "base_url": "https://grafana.test",
                "secret_ref": None,
            }
        )
        hibob = await builders.build_hibob_client(
            {
                "id": installation_ids["hibob"],
                "tenant_id": tenant_id,
                "base_url": "https://api.hibob.com",
                "company_id": "company-1",
                "service_user_id": "service-user",
                "secret_ref": None,
            }
        )
        ashby = await builders.build_ashby_client(
            {
                "id": installation_ids["ashby"],
                "tenant_id": tenant_id,
                "base_url": "https://api.ashbyhq.com",
                "org_id": "org-1",
                "secret_ref": None,
            }
        )
        await grafana.get_org()
        await hibob.company_info()
        await ashby.list_entities("candidate")
    finally:
        await http.aclose()

    assert {
        context.source: context.installation_id
        for context in recorder.contexts
    } == {
        source: str(installation_id)
        for source, installation_id in installation_ids.items()
    }


@pytest.mark.parametrize("source", ["grafana", "hibob", "ashby"])
async def test_preinstall_routes_bind_authenticated_tenant(
    source: str,
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

        async def get_org(self) -> dict[str, object]:
            return {"id": "org-1"}

        async def company_info(self) -> dict[str, object]:
            return {"id": "company-1"}

        async def list_entities(self, *args, **kwargs):  # noqa: ANN002, ANN003
            return [], None, None

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(module, "tenant_preinstall_transport_kwargs", binding)
    client_name = {
        "grafana": "GrafanaClient",
        "hibob": "HibobClient",
        "ashby": "AshbyClient",
    }[source]
    monkeypatch.setattr(module, client_name, _Probe)
    app = FastAPI()

    @app.middleware("http")
    async def _auth(request, call_next):  # noqa: ANN001, ANN202
        request.state.auth = type("Auth", (), {"tenant_id": tenant_id})()
        return await call_next(request)

    app.include_router(module.router)
    payloads = {
        "grafana": {
            "base_url": "https://grafana.test",
            "service_account_token": "token",
        },
        "hibob": {
            "company_id": "company-1",
            "service_user_id": "service-user",
            "service_user_token": "token",
        },
        "ashby": {"org_id": "org-1", "api_token": "token"},
    }
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            f"/integrations/{source}/connect/preflight",
            json=payloads[source],
        )

    assert response.status_code == 200
    assert captured["tenant"] == tenant_id
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs["tenant_id"] == tenant_id
    assert kwargs["require_tenant_installation"] is False


def _client_for_fault(
    source: str,
    *,
    http: httpx.AsyncClient,
    policy: RequestPolicy,
):
    common = {
        "tenant_id": uuid4(),
        "installation_row_id": uuid4(),
        "http_client": http,
        "provider_transport": ProviderTransport(),
        "request_policy": policy,
        "allow_unlimited_local": True,
    }
    if source == "grafana":
        return GrafanaClient(
            base_url="https://provider.test",
            api_token="token",
            **common,
        )
    if source == "hibob":
        return HibobClient(
            base_url="https://provider.test",
            company_id="company-1",
            service_user_id="service-user",
            token="token",
            **common,
        )
    return AshbyClient(
        base_url="https://provider.test",
        org_id="org-1",
        api_key="token",
        **common,
    )


async def _invoke_fault(client, source: str) -> None:  # noqa: ANN001
    if source == "grafana":
        await client.get_org()
    elif source == "hibob":
        await client.company_info()
    else:
        await client.list_entities("candidate")


@pytest.mark.parametrize("source", ["grafana", "hibob", "ashby"])
@pytest.mark.parametrize("header_kind", ["delta", "http_date"])
async def test_delta_and_http_date_retry_after_become_retry_later(
    source: str,
    header_kind: str,
) -> None:
    retry_after = (
        "120"
        if header_kind == "delta"
        else format_datetime(
            datetime.now(timezone.utc) + timedelta(seconds=120),
            usegmt=True,
        )
    )
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(  # noqa: ARG005
                429,
                headers={"Retry-After": retry_after},
            )
        )
    )
    client = _client_for_fault(
        source,
        http=http,
        policy=RequestPolicy(
            max_attempts=1,
            max_inline_retry_after_seconds=0,
        ),
    )
    try:
        with pytest.raises(RetryLater) as raised:
            await _invoke_fault(client, source)
    finally:
        await http.aclose()

    assert raised.value.reason is RetryReason.RATE_LIMIT
    assert raised.value.request_context.source == source
    assert 118 <= raised.value.retry_after_seconds <= 120


@pytest.mark.parametrize("source", ["grafana", "hibob", "ashby"])
@pytest.mark.parametrize(
    ("fault", "expected_reason"),
    [
        ("timeout", RetryReason.TIMEOUT),
        ("transport", RetryReason.TRANSIENT),
        ("server", RetryReason.TRANSIENT),
    ],
)
async def test_retryable_failures_become_typed_retry_later(
    source: str,
    fault: str,
    expected_reason: RetryReason,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if fault == "timeout":
            raise httpx.ReadTimeout("timeout", request=request)
        if fault == "transport":
            raise httpx.ConnectError("connection lost", request=request)
        return httpx.Response(503)

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = _client_for_fault(
        source,
        http=http,
        policy=RequestPolicy(max_attempts=1),
    )
    try:
        with pytest.raises(RetryLater) as raised:
            await _invoke_fault(client, source)
    finally:
        await http.aclose()

    assert raised.value.reason is expected_reason
    assert raised.value.request_context.source == source


@pytest.mark.parametrize("source", ["grafana", "hibob", "ashby"])
async def test_clients_fail_closed_without_transport_in_production(
    source: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FYRALIS_ENV", "production")
    http = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json={}),  # noqa: ARG005
        )
    )
    common = {
        "tenant_id": UUID("00000000-0000-0000-0000-000000000001"),
        "installation_row_id": UUID(
            "00000000-0000-0000-0000-000000000002"
        ),
        "http_client": http,
    }
    try:
        with pytest.raises(RuntimeError, match="requires ProviderTransport"):
            if source == "grafana":
                GrafanaClient(
                    base_url="https://provider.test",
                    api_token="token",
                    **common,
                )
            elif source == "hibob":
                HibobClient(
                    base_url="https://provider.test",
                    company_id="company-1",
                    service_user_id="service-user",
                    token="token",
                    **common,
                )
            else:
                AshbyClient(
                    base_url="https://provider.test",
                    org_id="org-1",
                    api_key="token",
                    **common,
                )
    finally:
        await http.aclose()
