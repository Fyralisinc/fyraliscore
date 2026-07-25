"""ProviderTransport contracts for the Figma and Jira production clients."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import httpx
import pytest

from lib.shared.provider_transport import (
    ProviderPermanentError,
    ProviderTransport,
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
    parse_retry_after,
)
from services.ingest.integrations.figma import oauth as figma_oauth
from services.ingest.integrations.figma.client import FigmaClient
from services.ingest.integrations.jira.client import JiraClient


pytestmark = pytest.mark.asyncio


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001, ANN202
        assert isinstance(policy, RequestPolicy)
        self.contexts.append(context)
        return await call()


def _installation_quota(
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


async def test_figma_attempts_use_exact_binding_and_semantic_operations() -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/v1/teams/team-1/projects":
            return httpx.Response(
                200,
                json={"projects": [{"id": "project-1", "name": "Product"}]},
            )
        if path == "/v1/projects/project-1/files":
            return httpx.Response(200, json={"files": [{"key": "file-1"}]})
        if path == "/v1/me":
            return httpx.Response(200, json={"id": "user-1"})
        if path == "/v1/files/file-1/versions":
            return httpx.Response(200, json={"versions": []})
        if path == "/v1/files/file-1/comments":
            return httpx.Response(200, json={"comments": []})
        if path == "/v1/files/file-1":
            return httpx.Response(200, json={"name": "Checkout"})
        return httpx.Response(404, json={"error": "unexpected"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = FigmaClient(
            base_url="https://figma.test",
            tenant_id=tenant_id,
            install_row_id=installation_id,
            api_token="token",
            team_id="team-1",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_installation_quota,
            allow_unlimited_local=False,
        )
        await client.list_files()
        await client.get_current_user()
        await client.get_file("file-1")
        await client.list_events("file-1")

    assert [context.operation for context in recorder.contexts] == [
        "teams.projects.list",
        "projects.files.list",
        "users.me.get",
        "files.get",
        "file_versions.list",
        "file_comments.list",
    ]
    assert all(
        context.source == "figma"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


async def test_jira_attempts_use_exact_binding_and_semantic_operations() -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/search/jql"):
            return httpx.Response(200, json={"issues": [], "isLast": True})
        if path.endswith("/project/search"):
            return httpx.Response(
                200,
                json={"values": [], "isLast": True, "total": 0},
            )
        if path.endswith("/search/approximate-count"):
            return httpx.Response(200, json={"count": 0})
        if path.endswith("/myself"):
            return httpx.Response(200, json={"accountId": "account-1"})
        return httpx.Response(404, json={"error": "unexpected"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = JiraClient(
            base_url="https://jira.test",
            account_email="admin@example.test",
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            api_token="token",
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_installation_quota,
            allow_unlimited_local=False,
        )
        await client.search_issues(jql='project = "ENG"')
        await client.list_projects()
        await client.has_updates_since(
            project_key="ENG",
            updated_min_jql="2026/07/25 12:00",
        )
        await client.myself()

    assert [context.operation for context in recorder.contexts] == [
        "issues.search",
        "projects.list",
        "issues.approximate_count",
        "users.myself.get",
    ]
    assert all(
        context.source == "jira"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


@pytest.mark.parametrize(
    ("source", "header_value"),
    [
        ("figma", "120"),
        ("jira", "Sat, 25 Jul 2026 12:02:00 GMT"),
    ],
)
async def test_rate_headers_become_durable_retry_later(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
    header_value: str,
) -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    fixed_now = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)
    transport = ProviderTransport(now=lambda: fixed_now)
    monkeypatch.setattr(
        f"services.ingest.integrations.{source}.client.parse_retry_after",
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
        if source == "figma":
            client = FigmaClient(
                base_url="https://provider.test",
                install_row_id=installation_id,
                api_token="token",
                **common,
            )
            call = client.get_current_user()
        else:
            client = JiraClient(
                base_url="https://provider.test",
                account_email="admin@example.test",
                installation_row_id=installation_id,
                api_token="token",
                **common,
            )
            call = client.myself()
        with pytest.raises(RetryLater) as caught:
            await call

    assert caught.value.reason is RetryReason.RATE_LIMIT
    assert caught.value.retry_after_seconds == pytest.approx(120.0)
    assert caught.value.not_before == fixed_now + timedelta(seconds=120)


@pytest.mark.parametrize("source", ["figma", "jira"])
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
    tenant_id = uuid4()
    installation_id = uuid4()

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
        if source == "figma":
            client = FigmaClient(
                base_url="https://provider.test",
                install_row_id=installation_id,
                api_token="token",
                **common,
            )
            call = client.get_current_user()
        else:
            client = JiraClient(
                base_url="https://provider.test",
                account_email="admin@example.test",
                installation_row_id=installation_id,
                api_token="token",
                **common,
            )
            call = client.myself()
        with pytest.raises(RetryLater) as caught:
            await call

    assert caught.value.reason is reason
    assert caught.value.request_context.source == source
    assert caught.value.request_context.tenant_id == str(tenant_id)
    assert caught.value.request_context.installation_id == str(installation_id)


@pytest.mark.parametrize("source", ["figma", "jira"])
async def test_production_missing_installation_fails_before_http(
    monkeypatch: pytest.MonkeyPatch,
    source: str,
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
        common = {
            "tenant_id": uuid4(),
            "http_client": http,
            "provider_transport": _Recorder(),
            "quota_resolver": _installation_quota,
            "allow_unlimited_local": False,
        }
        if source == "figma":
            client = FigmaClient(
                base_url="https://provider.test",
                api_token="token",
                **common,
            )
            call = client.get_current_user()
        else:
            client = JiraClient(
                base_url="https://provider.test",
                account_email="admin@example.test",
                api_token="token",
                **common,
            )
            call = client.myself()
        with pytest.raises(
            ProviderPermanentError,
            match="missing exact tenant/installation",
        ):
            await call

    assert sent is False


async def test_figma_oauth_exchange_and_refresh_are_transport_operations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FIGMA_CLIENT_ID", "client-id")
    monkeypatch.setenv("FIGMA_CLIENT_SECRET", "client-secret")
    monkeypatch.setenv(
        "FIGMA_REDIRECT_URI",
        "https://gateway.test/integrations/figma/oauth/callback",
    )
    monkeypatch.setenv(
        "FIGMA_OAUTH_REFRESH_URL",
        "https://api.figma.test/v1/oauth/token",
    )
    tenant_id = uuid4()
    installation_id = uuid4()
    recorder = _Recorder()
    response_number = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal response_number
        response_number += 1
        if response_number == 1:
            return httpx.Response(
                200,
                json={
                    "access_token": "access",
                    "refresh_token": "refresh",
                },
            )
        return httpx.Response(200, json={"access_token": "fresh"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        await figma_oauth._exchange_oauth_code(
            "authorization-code",
            "pkce-verifier",
            tenant_id=tenant_id,
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_tenant_quota,
            allow_unlimited_local=False,
        )
        await figma_oauth._exchange_oauth_refresh(
            "refresh",
            http,
            tenant_id=tenant_id,
            installation_id=installation_id,
            provider_transport=recorder,
            quota_resolver=_installation_quota,
            allow_unlimited_local=False,
        )

    assert [context.operation for context in recorder.contexts] == [
        "oauth.token.exchange",
        "oauth.token.refresh",
    ]
    assert recorder.contexts[0].tenant_id == str(tenant_id)
    assert recorder.contexts[0].installation_id is None
    assert recorder.contexts[1].tenant_id == str(tenant_id)
    assert recorder.contexts[1].installation_id == str(installation_id)
