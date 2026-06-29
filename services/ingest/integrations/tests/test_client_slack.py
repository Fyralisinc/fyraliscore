from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

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
        base_url="https://slack.test/api",
        http_client=http_client,
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


async def test_429_exhaustion_is_recoverable_rate_limit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "5"})

    client = _client(handler, max_attempts=1)
    try:
        with pytest.raises(SlackApiError) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    err = exc_info.value
    assert err.code == "slack_api_rate_limited"
    assert err.recoverable is True
    assert err.context["endpoint"] == "users.info"
    assert err.context["retry_after"] == 5.0


async def test_5xx_maps_to_recoverable_slack_api_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="busy")

    client = _client(handler)
    try:
        with pytest.raises(SlackApiError) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    err = exc_info.value
    assert err.code == "slack_api_error"
    assert err.recoverable is True
    assert err.context["http_status"] == 503


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


async def test_transport_error_after_retries_is_recoverable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("network down", request=request)

    client = _client(handler)
    try:
        with pytest.raises(SlackApiError) as exc_info:
            await client.users_info("U123")
    finally:
        await client.aclose()

    err = exc_info.value
    assert err.code == "slack_api_error"
    assert err.recoverable is True
    assert err.context["error_type"] == "ConnectError"


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
