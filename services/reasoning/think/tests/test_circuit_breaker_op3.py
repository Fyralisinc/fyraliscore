"""services/reasoning/think/tests/test_circuit_breaker_op3.py — OP-3 tests.

THINK-DESIGN-AUDIT §8.2. Verifies:
  * state machine: CLOSED → OPEN → HALF_OPEN → CLOSED
  * OPEN fast-fails with CircuitOpenError
  * half-open success closes; half-open failure re-opens with clock reset
  * min_samples prevents a single failure from tripping the breaker
  * integration: a forced-failing provider opens the breaker after the
    threshold and subsequent calls fast-fail
"""
from __future__ import annotations

import asyncio

import pytest

from lib.observability.metrics import (
    LLM_CIRCUIT_BREAKER_STATE,
    reset_default_for_tests,
)
from services.reasoning.think.circuit_breaker import (
    CircuitOpenError,
    CircuitState,
    LLMCircuitBreaker,
    all_breaker_states,
    get_breaker,
    register_breaker,
    reset_breakers,
)


# ---------------------------------------------------------------------
# State-machine unit tests (pure, no DB)
# ---------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _fresh_breakers():
    reset_breakers()
    reset_default_for_tests()
    yield
    reset_breakers()
    reset_default_for_tests()


def _gauge_for(provider: str) -> dict[str, float]:
    """Read the (provider, state) gauge into a {state: value} dict."""
    return {
        state.value: LLM_CIRCUIT_BREAKER_STATE.get(
            provider=provider, state=state.value
        )
        for state in CircuitState
    }


def _assert_active_state(provider: str, expected: CircuitState) -> None:
    """The active state series reads 1, the other two read 0 (G3 invariant)."""
    g = _gauge_for(provider)
    for state in CircuitState:
        assert g[state.value] == (1.0 if state == expected else 0.0), (
            f"provider={provider} expected active {expected.value}, gauge={g}"
        )


async def _ok() -> str:
    return "ok"


async def _fail(message: str = "boom"):
    raise RuntimeError(message)


async def test_closed_passes_through():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, window_seconds=60.0,
        open_duration=0.05, min_samples=3, name="test",
    )
    result = await b.call(_ok)
    assert result == "ok"
    assert b.state == CircuitState.CLOSED


async def test_single_failure_does_not_trip_below_min_samples():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, min_samples=10, name="test",
    )
    with pytest.raises(RuntimeError):
        await b.call(_fail)
    assert b.state == CircuitState.CLOSED


async def test_threshold_trips_to_open():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, window_seconds=60.0,
        open_duration=0.05, min_samples=10, name="test",
    )
    # 10 failures in a row → 100% rate → OPEN.
    for _ in range(10):
        with pytest.raises(RuntimeError):
            await b.call(_fail)
    assert b.state == CircuitState.OPEN


async def test_open_fast_fails():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, open_duration=10.0,
        min_samples=5, name="test",
    )
    for _ in range(5):
        with pytest.raises(RuntimeError):
            await b.call(_fail)
    assert b.state == CircuitState.OPEN
    # Next call short-circuits without running the function.
    called = {"n": 0}

    async def _counting():
        called["n"] += 1
        return "nope"

    with pytest.raises(CircuitOpenError):
        await b.call(_counting)
    assert called["n"] == 0


async def test_open_transitions_to_half_open_after_duration():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, open_duration=0.05,
        min_samples=5, name="test",
    )
    for _ in range(5):
        with pytest.raises(RuntimeError):
            await b.call(_fail)
    assert b.state == CircuitState.OPEN
    # Wait past open_duration; next call should be allowed as HALF_OPEN probe.
    await asyncio.sleep(0.08)
    result = await b.call(_ok)
    assert result == "ok"
    # After a successful probe → CLOSED.
    assert b.state == CircuitState.CLOSED


async def test_half_open_failure_reopens():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, open_duration=0.05,
        min_samples=5, name="test",
    )
    for _ in range(5):
        with pytest.raises(RuntimeError):
            await b.call(_fail)
    assert b.state == CircuitState.OPEN
    await asyncio.sleep(0.08)
    # Probe fails → back to OPEN.
    with pytest.raises(RuntimeError):
        await b.call(_fail)
    assert b.state == CircuitState.OPEN


async def test_mixed_failures_below_threshold_stays_closed():
    """10 calls, 4 failures → 40% < 50% threshold → stays CLOSED."""
    b = LLMCircuitBreaker(
        failure_threshold=0.5, min_samples=10,
        open_duration=10.0, name="test",
    )
    for i in range(10):
        try:
            if i < 4:
                await b.call(_fail)
            else:
                await b.call(_ok)
        except RuntimeError:
            pass
    assert b.state == CircuitState.CLOSED


async def test_window_eviction():
    """Failures older than window_seconds should be evicted from the
    rolling window so they no longer count toward the failure rate."""
    b = LLMCircuitBreaker(
        failure_threshold=0.5,
        window_seconds=0.05,
        open_duration=10.0,
        min_samples=5,           # 4 failures is below min_samples, so
                                 # the breaker never trips.
        name="test",
    )
    for _ in range(4):
        with pytest.raises(RuntimeError):
            await b.call(_fail)
    assert b.state == CircuitState.CLOSED  # not enough samples to trip
    # Wait past the window; events should have aged out.
    await asyncio.sleep(0.08)
    status = b.status()
    assert status["samples"] == 0


async def test_registry_singleton():
    a = get_breaker("deepseek")
    b = get_breaker("deepseek")
    assert a is b


async def test_registered_breaker_used_by_name():
    custom = LLMCircuitBreaker(
        failure_threshold=0.1, min_samples=1, name="test_custom",
    )
    register_breaker("test_custom", custom)
    assert get_breaker("test_custom") is custom


async def test_all_breaker_states_snapshot():
    get_breaker("deepseek")
    get_breaker("anthropic")
    snap = all_breaker_states()
    assert "deepseek" in snap
    assert "anthropic" in snap
    assert snap["deepseek"]["state"] in {"closed", "open", "half_open"}


# ---------------------------------------------------------------------
# Provider integration — forced-failing scripted provider
# ---------------------------------------------------------------------


async def test_integration_scripted_provider_opens_circuit():
    """A ScriptedProvider that always raises should open the circuit
    on the 'deepseek' breaker after the threshold is hit. Subsequent
    calls fast-fail with CircuitOpenError."""
    from lib.llm.provider import LLMConfig, LLMProvider

    # Register a breaker with low threshold + small min_samples so we
    # only need a handful of failures to trip.
    breaker = LLMCircuitBreaker(
        failure_threshold=0.5, min_samples=3,
        open_duration=10.0, name="test_provider",
    )
    register_breaker("test_provider", breaker)

    class _FailingProvider(LLMProvider):
        async def _raw_call(self, **kwargs):
            async def _call():
                raise RuntimeError("provider outage")
            # Route through the breaker.
            return await get_breaker("test_provider").call(_call)

    provider = _FailingProvider(LLMConfig(
        provider="test_provider", api_key="x", model="test",
    ))

    # Three failures should trip the 50% threshold at min_samples=3.
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await provider._raw_call(
                system="s", user="u", temperature=0.0,
                max_tokens=64, schema_hint="{}",
            )
    assert breaker.state == CircuitState.OPEN

    # Next call fast-fails.
    with pytest.raises(CircuitOpenError):
        await provider._raw_call(
            system="s", user="u", temperature=0.0,
            max_tokens=64, schema_hint="{}",
        )


# ---------------------------------------------------------------------
# BYOC §12 G3 — the breaker transitions drive the fleet state gauge
# (fyralis_llm_circuit_breaker_state{provider,state}), one series per state:
# the active state reads 1, the other two read 0. state="open" is then
# directly alertable as "provider down, all Think runs fast-failing".
# ---------------------------------------------------------------------


async def test_gauge_published_eagerly_on_breaker_creation():
    """A never-tripped provider still shows up as CLOSED so the control plane
    tells 'closed/healthy' apart from 'absent/unscraped'. A unique name forces
    the first-creation publish path (get_breaker only publishes on create)."""
    get_breaker("gauge_eager_provider")
    _assert_active_state("gauge_eager_provider", CircuitState.CLOSED)


async def test_gauge_tracks_closed_to_open_transition():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, window_seconds=60.0,
        open_duration=10.0, min_samples=5, name="gauge_open",
    )
    register_breaker("gauge_open", b)
    _assert_active_state("gauge_open", CircuitState.CLOSED)

    for _ in range(5):
        with pytest.raises(RuntimeError):
            await b.call(_fail)

    assert b.state == CircuitState.OPEN
    # The gauge now reads open=1, closed=0, half_open=0.
    _assert_active_state("gauge_open", CircuitState.OPEN)


async def test_gauge_tracks_open_half_open_closed_recovery():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, open_duration=0.05,
        min_samples=5, name="gauge_recover",
    )
    register_breaker("gauge_recover", b)
    for _ in range(5):
        with pytest.raises(RuntimeError):
            await b.call(_fail)
    _assert_active_state("gauge_recover", CircuitState.OPEN)

    # Wait past open_duration; the next call probes as HALF_OPEN, then a
    # success closes the breaker — the gauge must land on closed=1.
    await asyncio.sleep(0.08)
    result = await b.call(_ok)
    assert result == "ok"
    assert b.state == CircuitState.CLOSED
    _assert_active_state("gauge_recover", CircuitState.CLOSED)


async def test_gauge_tracks_half_open_failure_reopen():
    b = LLMCircuitBreaker(
        failure_threshold=0.5, open_duration=0.05,
        min_samples=5, name="gauge_reopen",
    )
    register_breaker("gauge_reopen", b)
    for _ in range(5):
        with pytest.raises(RuntimeError):
            await b.call(_fail)
    await asyncio.sleep(0.08)
    # A failing probe re-opens; the gauge must return to open=1.
    with pytest.raises(RuntimeError):
        await b.call(_fail)
    assert b.state == CircuitState.OPEN
    _assert_active_state("gauge_reopen", CircuitState.OPEN)


async def test_gauge_renders_open_series_on_default_scrape():
    from lib.observability.metrics import render_default

    b = LLMCircuitBreaker(
        failure_threshold=0.5, open_duration=10.0,
        min_samples=3, name="gauge_render",
    )
    register_breaker("gauge_render", b)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await b.call(_fail)
    assert b.state == CircuitState.OPEN

    text = render_default()
    assert "# TYPE fyralis_llm_circuit_breaker_state gauge" in text
    assert (
        'fyralis_llm_circuit_breaker_state'
        '{provider="gauge_render",state="open"} 1' in text
    )
    assert (
        'fyralis_llm_circuit_breaker_state'
        '{provider="gauge_render",state="closed"} 0' in text
    )
