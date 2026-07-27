from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from lib.shared.provider_transport import (
    QuotaRequirement,
    RequestContext,
    RequestPolicy,
    RetryLater,
    RetryReason,
)
from services.ingest.integrations.facebook_pages.client import (
    FacebookPagesClient,
)
from services.ingest.integrations.facebook_pages.token_lifecycle import (
    DEGRADED,
    RecoverySchedule,
)
from services.ingest.synthetic.provider_lab import build_provider_lab_app


pytestmark = pytest.mark.asyncio


class _Recorder:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []

    async def execute(self, context, policy, call):  # noqa: ANN001,ANN202
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
    assert source == "facebook_pages"
    assert tenant_id is not None
    assert installation_id is not None
    assert dimensions == {}
    return (
        QuotaRequirement(
            scope="installation",
            bucket_key=f"{source}:{operation}:{installation_id}",
            capacity=10,
            refill_per_second=10,
        ),
    )


async def test_every_graph_call_uses_exact_binding_and_finite_operation() -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    recorder = _Recorder()

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.endswith("/oauth/access_token"):
            return httpx.Response(200, json={"access_token": "user-token"})
        if path.endswith("/me/accounts"):
            return httpx.Response(
                200,
                json={"data": [{"id": "page-1", "access_token": "page-token"}]},
            )
        if path.endswith("/subscribed_apps"):
            return httpx.Response(200, json={"success": True})
        if path.endswith("/conversations"):
            return httpx.Response(200, json={"data": [{"id": "conversation-1"}]})
        return httpx.Response(200, json={"data": [{"id": "message-1"}]})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = FacebookPagesClient(
            base_url="https://graph.test/v23.0",
            access_token="page-token",
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        await client.exchange_code(
            code="code",
            client_id="client",
            client_secret="secret",
            redirect_uri="https://fyralis.test/callback",
        )
        await client.exchange_long_lived_user_token(
            short_lived_user_access_token="user-token",
            client_id="client",
            client_secret="secret",
        )
        await client.list_pages("user-token")
        await client.subscribe_page(
            page_id="page-1",
            page_access_token="page-token",
        )
        await client.list_conversations(page_id="page-1")
        await client.list_messages(conversation_id="conversation-1")

    assert [context.operation for context in recorder.contexts] == [
        "oauth.token.exchange",
        "oauth.user_token.extend",
        "pages.list",
        "pages.subscribe",
        "conversations.list",
        "messages.list",
    ]
    assert all(
        context.source == "facebook_pages"
        and context.tenant_id == str(tenant_id)
        and context.installation_id == str(installation_id)
        for context in recorder.contexts
    )


async def test_long_graph_cooldown_returns_retry_later_without_looping() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(429, headers={"Retry-After": "120"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
    ) as http:
        client = FacebookPagesClient(
            base_url="https://graph.test/v23.0",
            access_token="page-token",
            http_client=http,
        )
        with pytest.raises(RetryLater) as exc:
            await client.list_conversations(page_id="page-1")

    assert attempts == 1
    assert exc.value.reason is RetryReason.RATE_LIMIT
    assert exc.value.request_context.operation == "conversations.list"
    assert exc.value.retry_after_seconds == 120


async def test_graph_code_190_recovers_exact_installation_and_retries_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    expected_tenant_id = tenant_id
    installation_id = uuid4()
    old_ref = "secret://old-page"
    observed_tokens: list[str] = []
    lifecycle_calls: list[tuple[str, object]] = []

    class _Secrets:
        async def get(self, ref, *, tenant_id):  # noqa: ANN001,ANN202
            assert ref == old_ref
            assert tenant_id == expected_tenant_id
            return b"old-token"

    async def _current(*_args, **kwargs):  # noqa: ANN003,ANN202
        lifecycle_calls.append(("load", kwargs["installation_row_id"]))
        return "old-token", old_ref

    async def _schedule(*_args, **kwargs):  # noqa: ANN003,ANN202
        lifecycle_calls.append(("schedule", kwargs["installation_row_id"]))
        assert kwargs["tenant_id"] == tenant_id
        assert kwargs["expected_page_token_ref"] == old_ref
        assert kwargs["graph_error_subcode"] == 463
        return RecoverySchedule(state=DEGRADED, not_before=None)

    async def _recover(*_args, **kwargs):  # noqa: ANN003,ANN202
        lifecycle_calls.append(("recover", kwargs["installation_row_id"]))
        assert kwargs["tenant_id"] == tenant_id
        return "replacement-token"

    monkeypatch.setattr(
        "services.ingest.integrations.facebook_pages.token_lifecycle."
        "page_access_token_for_request",
        _current,
    )
    monkeypatch.setattr(
        "services.ingest.integrations.facebook_pages.token_lifecycle."
        "schedule_page_token_recovery",
        _schedule,
    )
    monkeypatch.setattr(
        "services.ingest.integrations.facebook_pages.token_lifecycle."
        "recover_page_access_token",
        _recover,
    )

    def handler(request: httpx.Request) -> httpx.Response:
        token = request.url.params["access_token"]
        observed_tokens.append(token)
        if token == "old-token":
            return httpx.Response(
                400,
                json={
                    "error": {
                        "type": "OAuthException",
                        "code": 190,
                        "error_subcode": 463,
                    },
                },
            )
        return httpx.Response(200, json={"data": [{"id": "conversation-1"}]})

    recorder = _Recorder()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = FacebookPagesClient(
            base_url="https://graph.test/v23.0",
            page_access_token_ref=old_ref,
            pool=object(),
            secret_store=_Secrets(),
            tenant_id=tenant_id,
            installation_row_id=installation_id,
            http_client=http,
            provider_transport=recorder,
            quota_resolver=_quota,
            allow_unlimited_local=False,
        )
        conversations, cursor = await client.list_conversations(page_id="page-1")

    assert conversations == [{"id": "conversation-1"}]
    assert cursor is None
    assert observed_tokens == ["old-token", "replacement-token"]
    assert lifecycle_calls == [
        ("load", installation_id),
        ("schedule", installation_id),
        ("recover", installation_id),
    ]
    assert [context.operation for context in recorder.contexts] == [
        "conversations.list",
        "conversations.list",
    ]


async def test_production_client_conforms_to_provider_lab_used_surface() -> None:
    app = build_provider_lab_app()
    transport = httpx.ASGITransport(
        app=app,
        client=("127.0.0.1", 43123),
    )
    state = {
        "pages": {
            "page-1": {
                "id": "page-1",
                "name": "Test Page",
                "access_token": "page-token",
            },
        },
        "user_pages": {},
        "conversations": {
            "page-1": [{"id": "conversation-1", "message_count": 1}],
        },
        "messages": {
            "conversation-1": [{"id": "message-1", "message": "hello"}],
        },
        "verify_tokens": [],
        "app_secrets": {},
        "installations": {"page-1": {"enabled": True}},
    }
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://provider-lab",
    ) as http:
        seeded = await http.put(
            "/_lab/sources/facebook_pages/state",
            json=state,
        )
        assert seeded.status_code == 200
        client = FacebookPagesClient(
            base_url="http://provider-lab/facebook_pages/v23.0",
            access_token="page-token",
            http_client=http,
        )
        token = await client.exchange_code(
            code="code",
            client_id="client",
            client_secret="secret",
            redirect_uri="https://fyralis.test/callback",
        )
        long_token = await client.exchange_long_lived_user_token(
            short_lived_user_access_token=token["access_token"],
            client_id="client",
            client_secret="secret",
        )
        pages = await client.list_pages(long_token["access_token"])
        subscribed = await client.subscribe_page(
            page_id="page-1",
            page_access_token="page-token",
        )
        conversations, conversation_cursor = await client.list_conversations(
            page_id="page-1",
        )
        messages, message_cursor = await client.list_messages(
            conversation_id="conversation-1",
        )
        ledger = (
            await http.get(
                "/_lab/ledger",
                params={"source": "facebook_pages"},
            )
        ).json()["entries"]

    assert pages[0]["id"] == "page-1"
    assert subscribed == {"success": True}
    assert conversations[0]["id"] == "conversation-1"
    assert conversation_cursor is None
    assert messages[0]["id"] == "message-1"
    assert message_cursor is None
    assert [entry["route_id"] for entry in ledger] == [
        "facebook_pages.oauth_token",
        "facebook_pages.oauth_token",
        "facebook_pages.accounts",
        "facebook_pages.subscribe",
        "facebook_pages.conversations",
        "facebook_pages.messages",
    ]
