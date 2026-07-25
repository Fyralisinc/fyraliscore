from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from lib.shared.provider_transport import (
    ProviderTransport,
    RequestPolicy,
    RetryLater,
    RetryReason,
)
from services.ingest.integrations.secret_cache import SECRET_CACHE_TTL_ENV
from services.ingest.integrations.slack.client import (
    SlackApiError,
    SlackClient,
    SlackUserClient,
)


def _client(
    handler,
    *,
    max_attempts: int = 1,
    wall_budget_s: float = 1.0,
    max_inline_retry_after_s: float | None = None,
    provider_transport: ProviderTransport | None = None,
) -> SlackClient:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = SlackClient(
        pool=None,  # type: ignore[arg-type]
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id="T123",
        max_attempts=max_attempts,
        wall_budget_s=wall_budget_s,
        max_inline_retry_after_s=max_inline_retry_after_s,
        base_url="https://slack.test/api",
        http_client=http_client,
        provider_transport=provider_transport,
        request_policy=RequestPolicy(
            max_attempts=max_attempts,
            timeout_seconds=15.0,
            max_elapsed_seconds=wall_budget_s,
            max_inline_retry_after_seconds=(
                30.0
                if max_inline_retry_after_s is None
                else max_inline_retry_after_s
            ),
            rate_limit_header_parser_id="http.retry_after",
        ),
    )
    client._bot_token_cache.set(  # type: ignore[attr-defined]
        "xoxb-test", ttl_seconds=float("inf"),
    )
    return client


class _RotatingPool:
    def __init__(self, refs: dict[str, str]) -> None:
        self.refs = refs
        self.calls: list[tuple[object, str]] = []

    async def fetchrow(self, _sql: str, tenant_id, label: str):
        self.calls.append((tenant_id, label))
        ref = self.refs.get(label)
        return None if ref is None else {"id": ref}


class _SecretStore:
    def __init__(self, values: dict[str, object]) -> None:
        self.values = values
        self.calls: list[tuple[str, object]] = []

    async def get(self, ref: str, *, tenant_id):
        self.calls.append((ref, tenant_id))
        return self.values[ref]


async def test_bot_token_reloads_latest_label_when_cache_ttl_is_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SECRET_CACHE_TTL_ENV, "0")
    tenant_id = uuid4()
    label = "slack_bot_token:T123"
    pool = _RotatingPool({label: "bot-ref-1"})
    store = _SecretStore({"bot-ref-1": b"xoxb-one", "bot-ref-2": b"xoxb-two"})
    client = SlackClient(
        pool=pool,  # type: ignore[arg-type]
        secret_store=store,
        tenant_id=tenant_id,
        installation_row_id=uuid4(),
        team_id="T123",
        base_url="https://slack.test/api",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"ok": True}),
        )),
    )

    try:
        first = await client._resolve_bot_token()
        pool.refs[label] = "bot-ref-2"
        second = await client._resolve_bot_token()
    finally:
        await client.aclose()

    assert (first, second) == ("xoxb-one", "xoxb-two")
    assert store.calls == [("bot-ref-1", tenant_id), ("bot-ref-2", tenant_id)]


async def test_user_token_reloads_latest_label_when_cache_ttl_is_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(SECRET_CACHE_TTL_ENV, "0")
    tenant_id = uuid4()
    label = "slack_user_token:T123:U123"
    pool = _RotatingPool({label: "user-ref-1"})
    store = _SecretStore({
        "user-ref-1": bytearray(b"xoxp-one"),
        "user-ref-2": b"xoxp-two",
    })
    client = SlackUserClient(
        pool=pool,  # type: ignore[arg-type]
        secret_store=store,
        tenant_id=tenant_id,
        installation_row_id=uuid4(),
        team_id="T123",
        user_id="U123",
        base_url="https://slack.test/api",
        http_client=httpx.AsyncClient(transport=httpx.MockTransport(
            lambda _request: httpx.Response(200, json={"ok": True}),
        )),
    )

    try:
        first = await client._resolve_token()
        pool.refs[label] = "user-ref-2"
        second = await client._resolve_token()
    finally:
        await client.aclose()

    assert (first, second) == ("xoxp-one", "xoxp-two")
    assert store.calls == [("user-ref-1", tenant_id), ("user-ref-2", tenant_id)]


async def test_429_exhaustion_schedules_retry_later() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "5"})

    client = _client(handler, max_attempts=1)
    try:
        with pytest.raises(RetryLater) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    err = exc_info.value
    assert err.code == "provider_retry_later"
    assert err.recoverable is True
    assert err.request_context.operation == "users.info"
    assert err.retry_after_seconds == 5.0
    assert err.reason is RetryReason.RATE_LIMIT


async def test_short_retry_after_retries_inline() -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "5"})
        return httpx.Response(200, json={"ok": True, "user": {"id": "U123"}})

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = _client(
        handler,
        max_attempts=2,
        wall_budget_s=75.0,
        provider_transport=ProviderTransport(sleep=record_sleep),
    )
    try:
        result = await client.users_info("U123")
    finally:
        await client.aclose()

    assert result["user"]["id"] == "U123"
    assert calls == 2
    assert sleeps == [5.0]


async def test_long_retry_after_schedules_without_sleeping_worker() -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "60"})

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = _client(
        handler,
        max_attempts=3,
        wall_budget_s=75.0,
        provider_transport=ProviderTransport(sleep=record_sleep),
    )
    try:
        with pytest.raises(RetryLater) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    assert calls == 1
    assert sleeps == []
    assert exc_info.value.retry_after_seconds == 60.0
    assert exc_info.value.reason is RetryReason.RATE_LIMIT


async def test_inline_retry_after_limit_is_independent_of_wall_budget() -> None:
    calls = 0
    sleeps: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(429, headers={"Retry-After": "2"})

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = _client(
        handler,
        max_attempts=3,
        wall_budget_s=75.0,
        max_inline_retry_after_s=1.0,
        provider_transport=ProviderTransport(sleep=record_sleep),
    )
    try:
        with pytest.raises(RetryLater) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    assert calls == 1
    assert sleeps == []
    assert exc_info.value.retry_after_seconds == 2.0


async def test_5xx_schedules_transient_retry_later() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="busy")

    client = _client(handler)
    try:
        with pytest.raises(RetryLater) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    err = exc_info.value
    assert err.code == "provider_retry_later"
    assert err.recoverable is True
    assert err.reason is RetryReason.TRANSIENT


async def test_4xx_maps_to_permanent_slack_api_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="bad")

    client = _client(handler)
    try:
        with pytest.raises(SlackApiError) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    err = exc_info.value
    assert err.code == "slack_api_error"
    assert err.recoverable is False
    assert err.context["http_status"] == 400


async def test_transport_error_after_retries_schedules_retry_later() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("network down", request=request)

    client = _client(handler)
    try:
        with pytest.raises(RetryLater) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    err = exc_info.value
    assert err.code == "provider_retry_later"
    assert err.recoverable is True
    assert err.reason is RetryReason.TRANSIENT


async def test_ok_false_response_is_permanent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"ok": False, "error": "not_in_channel"},
        )

    client = _client(handler)
    try:
        with pytest.raises(SlackApiError) as exc_info:
            await client.conversations_history(channel="C123")
    finally:
        await client.aclose()

    err = exc_info.value
    assert err.code == "slack_api_error"
    assert err.recoverable is False
    assert err.context["slack_error"] == "not_in_channel"
