"""ProviderTransport contracts for QuickBooks, Ramp, Gusto, and LinkedIn."""
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
)
from services.ingest.integrations import oauth_refresh
from services.ingest.integrations.gusto.client import GustoClient
from services.ingest.integrations.linkedin.client import LinkedinClient
from services.ingest.integrations.quickbooks.client import QuickBooksClient
from services.ingest.integrations.ramp.client import RampClient


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


def _identity() -> tuple[UUID, UUID]:
    return uuid4(), uuid4()


def _bound_kwargs(
    recorder: _Recorder,
    tenant_id: UUID,
    installation_id: UUID,
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "install_row_id": installation_id,
        "provider_transport": recorder,
        "quota_resolver": _quota,
        "allow_unlimited_local": False,
    }


async def test_quickbooks_attempts_use_exact_binding_and_finite_operations() -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        if "/companyinfo/" in request.url.path:
            return httpx.Response(200, json={"CompanyInfo": {"Id": "realm-1"}})
        return httpx.Response(
            200,
            json={"QueryResponse": {"Invoice": [], "maxResults": 0}},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = QuickBooksClient(
            base_url="https://quickbooks.test",
            realm_id="realm-1",
            access_token="token",
            http_client=http,
            **_bound_kwargs(recorder, tenant_id, installation_id),
        )
        await client.query("Invoice")
        await client.company_info()

    assert [context.operation for context in recorder.contexts] == [
        "entities.query",
        "company_info.get",
    ]
    assert all(
        context.source == "quickbooks"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


async def test_ramp_attempts_use_exact_binding_and_finite_operations() -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/token"):
            return httpx.Response(
                200,
                json={"access_token": "minted", "expires_in": 3600},
            )
        if request.url.path.endswith("/business"):
            return httpx.Response(200, json={"id": "business-1"})
        return httpx.Response(200, json={"data": [], "page": {"next": None}})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = RampClient(
            base_url="https://ramp.test",
            business_id="business-1",
            client_id="client-id",
            client_secret="client-secret",
            http_client=http,
            **_bound_kwargs(recorder, tenant_id, installation_id),
        )
        await client.mint_token()
        await client.list_transactions()
        await client.list_reimbursements()
        await client.list_cards()
        await client.list_users()
        await client.business()

    assert [context.operation for context in recorder.contexts] == [
        "oauth.token.mint",
        "transactions.list",
        "reimbursements.list",
        "cards.list",
        "users.list",
        "business.get",
    ]
    assert all(
        context.source == "ramp"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


async def test_gusto_attempts_use_exact_binding_and_finite_operations() -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/employees"):
            return httpx.Response(200, json=[])
        if request.url.path.endswith("/payrolls"):
            return httpx.Response(200, json=[])
        return httpx.Response(200, json={"uuid": "company-1"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = GustoClient(
            base_url="https://gusto.test",
            company_uuid="company-1",
            access_token="token",
            http_client=http,
            **_bound_kwargs(recorder, tenant_id, installation_id),
        )
        await client.list_employees()
        await client.list_payrolls()
        await client.company()

    assert [context.operation for context in recorder.contexts] == [
        "employees.list",
        "payrolls.list",
        "companies.get",
    ]
    assert all(
        context.source == "gusto"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


async def test_linkedin_attempts_use_exact_binding_and_finite_operations() -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        if "/organizations/" in request.url.path:
            return httpx.Response(200, json={"id": 123})
        return httpx.Response(200, json={"elements": [], "paging": {}})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = LinkedinClient(
            base_url="https://linkedin.test",
            organization_urn="urn:li:organization:123",
            access_token="token",
            http_client=http,
            **_bound_kwargs(recorder, tenant_id, installation_id),
        )
        await client.list_posts()
        await client.share_statistics()
        await client.follower_statistics()
        await client.get_organization()

    assert [context.operation for context in recorder.contexts] == [
        "posts.list",
        "share_statistics.list",
        "follower_statistics.list",
        "organizations.get",
    ]
    assert all(
        context.source == "linkedin"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


@pytest.mark.parametrize(
    "source",
    ["quickbooks", "ramp", "gusto", "linkedin"],
)
async def test_rate_limit_becomes_exact_durable_retry_later(source: str) -> None:
    tenant_id, installation_id = _identity()
    fixed_now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    transport = ProviderTransport(now=lambda: fixed_now)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                429,
                headers={"Retry-After": "120"},
            ),
        ),
    ) as http:
        common = {
            "tenant_id": tenant_id,
            "http_client": http,
            "provider_transport": transport,
            "request_policy": RequestPolicy(
                max_attempts=1,
                max_inline_retry_after_seconds=0,
            ),
            "allow_unlimited_local": True,
        }
        if source == "quickbooks":
            client = QuickBooksClient(
                base_url="https://provider.test",
                realm_id="realm-1",
                install_row_id=installation_id,
                access_token="token",
                **common,
            )
            call = client.company_info()
            operation = "company_info.get"
        elif source == "ramp":
            client = RampClient(
                base_url="https://provider.test",
                business_id="business-1",
                install_row_id=installation_id,
                access_token="token",
                **common,
            )
            call = client.business()
            operation = "business.get"
        elif source == "gusto":
            client = GustoClient(
                base_url="https://provider.test",
                company_uuid="company-1",
                install_row_id=installation_id,
                access_token="token",
                **common,
            )
            call = client.company()
            operation = "companies.get"
        else:
            client = LinkedinClient(
                base_url="https://provider.test",
                organization_urn="urn:li:organization:123",
                install_row_id=installation_id,
                access_token="token",
                **common,
            )
            call = client.get_organization()
            operation = "organizations.get"

        with pytest.raises(RetryLater) as caught:
            await call

    retry = caught.value
    assert retry.reason is RetryReason.RATE_LIMIT
    assert retry.retry_after_seconds == pytest.approx(120.0)
    assert retry.not_before == fixed_now + timedelta(seconds=120)
    assert retry.request_context.source == source
    assert retry.request_context.operation == operation
    assert retry.request_context.tenant_id == str(tenant_id)
    assert retry.request_context.installation_id == str(installation_id)


async def test_quickbooks_reactive_refresh_uses_the_same_exact_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id, installation_id = _identity()
    recorder = _Recorder()
    renewal_calls: list[dict[str, object]] = []

    async def through_durable_renewal(**kwargs: object) -> str:
        renewal_calls.append(kwargs)
        return "fresh-token"

    monkeypatch.setattr(
        oauth_refresh,
        "_refresh_through_renewal_job",
        through_durable_renewal,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/tokens/bearer"):
            return httpx.Response(
                200,
                json={
                    "access_token": "fresh-token",
                    "refresh_token": "rotated-refresh",
                    "expires_in": 3600,
                },
            )
        if request.headers.get("Authorization") == "Bearer stale-token":
            return httpx.Response(401, json={"error": "expired"})
        return httpx.Response(
            200,
            json={"QueryResponse": {"Invoice": [], "maxResults": 0}},
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = QuickBooksClient(
            base_url="https://quickbooks.test",
            realm_id="realm-1",
            pool=object(),
            secret_store=object(),
            tenant_id=tenant_id,
            install_row_id=installation_id,
            secret_ref="access-ref",
            refresh_secret_ref="refresh-ref",
            access_token="stale-token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await client.query("Invoice")

    assert [context.operation for context in recorder.contexts] == [
        "entities.query",
        "entities.query",
    ]
    assert all(
        context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )
    assert len(renewal_calls) == 1
    renewal = renewal_calls[0]
    binding = renewal["request_binding"]
    assert getattr(binding, "_source") == "quickbooks"
    assert getattr(binding, "_tenant_id") == str(tenant_id)
    assert getattr(binding, "_installation_id") == str(installation_id)
    assert getattr(binding, "_transport") is recorder


@pytest.mark.parametrize(
    ("module_name", "client_name", "payload"),
    [
        (
            "services.ingest.integrations.quickbooks.oauth",
            "QuickBooksClient",
            {"realm_id": "realm-1", "access_token": "token"},
        ),
        (
            "services.ingest.integrations.ramp.oauth",
            "RampClient",
            {"access_token": "token"},
        ),
        (
            "services.ingest.integrations.gusto.oauth",
            "GustoClient",
            {"company_uuid": "company-1", "access_token": "token"},
        ),
        (
            "services.ingest.integrations.linkedin.oauth",
            "LinkedinClient",
            {
                "organization_urn": "urn:li:organization:123",
                "access_token": "token",
            },
        ),
    ],
)
async def test_onboarding_probes_bind_authenticated_tenant_before_install(
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

        async def company_info(self) -> dict[str, object]:
            return {"CompanyInfo": {"CompanyName": "Lab Co"}}

        async def business(self) -> dict[str, object]:
            return {"id": "business-1", "business_name_legal": "Lab Co"}

        async def company(self) -> dict[str, object]:
            return {"uuid": "company-1", "name": "Lab Co"}

        async def get_organization(self) -> dict[str, object]:
            return {"id": 123, "localizedName": "Lab Co"}

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
