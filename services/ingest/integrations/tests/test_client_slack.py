from __future__ import annotations

from uuid import uuid4

import httpx
import pytest

from services.ingest.integrations.slack.client import SlackApiError, SlackClient


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
    client._bot_token = "xoxb-test"  # type: ignore[attr-defined]
    return client


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
