from __future__ import annotations

import pytest

import services.ingest.ingestion.fetchers._clients as clients
from lib.shared.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitOpenError,
    CircuitState,
)
from lib.shared.errors import JiraApiError


def _breaker(name: str, *, min_samples: int = 2) -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker(
        name=name,
        failure_threshold=0.5,
        min_samples=min_samples,
        open_duration=30.0,
        record_exception=clients._record_source_api_breaker_exception,
    )


class _FakeSourceClient:
    def __init__(self) -> None:
        self.calls = 0
        self.marker = "raw"

    async def recoverable_read(self) -> None:
        self.calls += 1
        raise JiraApiError(
            "upstream unavailable",
            code="jira_api_error",
            context={"http_status": 503},
        )

    async def permanent_read(self) -> None:
        self.calls += 1
        raise JiraApiError(
            "object not found",
            code="jira_api_not_found",
            context={"http_status": 404},
        )

    async def aclose(self) -> str:
        return "closed"


async def test_source_api_proxy_opens_on_recoverable_failures() -> None:
    raw = _FakeSourceClient()
    breaker = _breaker("source_api_proxy_open_test", min_samples=2)
    wrapped = await clients._wrap_source_client(
        "jira",
        raw,
        breaker=breaker,
    )

    for _ in range(2):
        with pytest.raises(JiraApiError):
            await wrapped.recoverable_read()

    assert breaker.state == CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        await wrapped.recoverable_read()

    assert raw.calls == 2


async def test_source_api_proxy_does_not_count_permanent_failures() -> None:
    raw = _FakeSourceClient()
    breaker = _breaker("source_api_proxy_permanent_test", min_samples=1)
    wrapped = await clients._wrap_source_client(
        "jira",
        raw,
        breaker=breaker,
    )

    with pytest.raises(JiraApiError):
        await wrapped.permanent_read()

    assert raw.calls == 1
    assert breaker.state == CircuitState.CLOSED
    assert breaker.status()["samples"] == 0


async def test_source_api_proxy_delegates_attributes_and_lifecycle() -> None:
    raw = _FakeSourceClient()
    breaker = _breaker("source_api_proxy_delegate_test")
    wrapped = await clients._wrap_source_client(
        "jira",
        raw,
        breaker=breaker,
    )

    assert wrapped.marker == "raw"
    wrapped.marker = "updated"
    assert raw.marker == "updated"
    assert await wrapped.aclose() == "closed"
    assert breaker.status()["samples"] == 0
