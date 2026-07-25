from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

import httpx
import pytest

from lib.shared.provider_transport import (
    ProviderRetryForbiddenError,
    ProviderTransport,
    RequestContext,
    RequestPolicy,
    RetrySafety,
)
from services.ingest.integrations.provider_transport import (
    ProviderRequestBinding,
)
from services.ingest.integrations.slack.client import SlackClient
from services.ingest.source_contract import effective_request_policy


pytestmark = pytest.mark.asyncio


class _RecordingExecutor:
    def __init__(self) -> None:
        self.contexts: list[RequestContext] = []
        self.policies: list[RequestPolicy] = []

    async def execute(
        self,
        request_context: RequestContext,
        policy: RequestPolicy,
        call: Callable[[], Awaitable[Any]],
    ) -> Any:
        self.contexts.append(request_context)
        self.policies.append(policy)
        return await call()


async def test_binding_resolves_exact_contract_policy_by_default() -> None:
    executor = _RecordingExecutor()
    binding = ProviderRequestBinding(
        source="slack",
        tenant_id="tenant-1",
        installation_id="install-1",
        transport=executor,
        request_policy=None,
        quota_resolver=None,
        allow_unlimited_local=True,
    )

    result = await binding.execute(
        "users.info",
        lambda: _return("ok"),
    )

    assert result == "ok"
    assert executor.policies == [
        effective_request_policy("slack", "users.info")
    ]
    assert executor.policies[0].retry_safety is RetrySafety.IDEMPOTENT


async def test_binding_threads_stable_idempotency_key_to_context() -> None:
    executor = _RecordingExecutor()
    binding = ProviderRequestBinding(
        source="slack",
        tenant_id="tenant-1",
        installation_id="install-1",
        transport=executor,
        request_policy=RequestPolicy(
            retry_safety=RetrySafety.IDEMPOTENCY_KEY,
        ),
        quota_resolver=None,
        allow_unlimited_local=True,
    )

    await binding.execute(
        "chat.postMessage",
        lambda: _return(None),
        idempotency_key="tenant-1:install-1:message-42",
    )

    assert executor.contexts[0].idempotency_key == (
        "tenant-1:install-1:message-42"
    )


async def test_binding_rejects_undeclared_operation_without_provider_call() -> None:
    executor = _RecordingExecutor()
    binding = ProviderRequestBinding(
        source="slack",
        tenant_id="tenant-1",
        installation_id="install-1",
        transport=executor,
        request_policy=None,
        quota_resolver=None,
        allow_unlimited_local=True,
    )

    with pytest.raises(KeyError, match="no request policy"):
        await binding.execute(
            "not.declared",
            lambda: _return(None),
        )

    assert executor.contexts == []


async def test_default_client_policy_retries_declared_idempotent_operation() -> None:
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"ok": True, "user": {"id": "U1"}})

    async def no_sleep(_delay: float) -> None:
        return None

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = SlackClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id="T1",
        http_client=http,
        provider_transport=ProviderTransport(sleep=no_sleep),
    )
    client._bot_token_cache.set("xoxb-test", ttl_seconds=float("inf"))
    try:
        response = await client.users_info("U1")
    finally:
        await client.aclose()

    assert response["user"]["id"] == "U1"
    assert attempts == 2


async def test_default_client_policy_never_replays_unsafe_operation() -> None:
    attempts = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(429, headers={"Retry-After": "0"})

    http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = SlackClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id="T1",
        http_client=http,
        provider_transport=ProviderTransport(),
    )
    client._bot_token_cache.set("xoxb-test", ttl_seconds=float("inf"))
    try:
        with pytest.raises(ProviderRetryForbiddenError) as exc_info:
            await client.chat_post_message(channel="C1", text="hello")
    finally:
        await client.aclose()

    assert attempts == 1
    assert exc_info.value.context["policy_reason"] == "unsafe_operation"


async def _return(value: Any) -> Any:
    return value
