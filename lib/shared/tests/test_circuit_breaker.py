from __future__ import annotations

import asyncio

import pytest

from lib.observability.metrics import render_default, reset_default_for_tests
from lib.shared.circuit_breaker import (
    AsyncCircuitBreaker,
    CircuitOpenError,
    CircuitState,
)


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    reset_default_for_tests()
    yield
    reset_default_for_tests()


async def _ok() -> str:
    return "ok"


async def _fail() -> str:
    raise RuntimeError("dependency outage")


async def test_breaker_opens_and_fast_fails_without_calling_dependency() -> None:
    breaker = AsyncCircuitBreaker(
        name="shared_breaker_test",
        failure_threshold=0.5,
        min_samples=2,
        open_duration=30.0,
    )
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_fail)

    assert breaker.state == CircuitState.OPEN
    called = 0

    async def _counting() -> str:
        nonlocal called
        called += 1
        return "should-not-run"

    with pytest.raises(CircuitOpenError) as exc_info:
        await breaker.call(_counting)

    assert called == 0
    assert exc_info.value.context["breaker"] == "shared_breaker_test"


async def test_half_open_success_closes_breaker() -> None:
    breaker = AsyncCircuitBreaker(
        name="shared_half_open_test",
        failure_threshold=0.5,
        min_samples=1,
        open_duration=0.01,
    )
    with pytest.raises(RuntimeError):
        await breaker.call(_fail)

    assert breaker.state == CircuitState.OPEN
    await asyncio.sleep(0.02)

    assert await breaker.call(_ok) == "ok"
    assert breaker.state == CircuitState.CLOSED
    assert breaker.status()["samples"] == 0


async def test_record_exception_predicate_skips_permanent_errors() -> None:
    breaker = AsyncCircuitBreaker(
        name="shared_predicate_test",
        failure_threshold=0.5,
        min_samples=1,
        record_exception=lambda exc: not isinstance(exc, ValueError),
    )

    async def _permanent() -> str:
        raise ValueError("caller supplied a bad request")

    with pytest.raises(ValueError):
        await breaker.call(_permanent)

    assert breaker.state == CircuitState.CLOSED
    assert breaker.status()["samples"] == 0


async def test_open_state_is_published_to_metrics() -> None:
    breaker = AsyncCircuitBreaker(
        name="shared_metric_test",
        failure_threshold=0.5,
        min_samples=1,
    )
    with pytest.raises(RuntimeError):
        await breaker.call(_fail)

    text = render_default()

    assert 'circuit_breaker_open{name="shared_metric_test"} 1' in text
    assert (
        'circuit_breaker_state{name="shared_metric_test",state="open"} 1'
        in text
    )
    assert (
        'circuit_breaker_calls_total{name="shared_metric_test",result="failure"} 1'
        in text
    )
