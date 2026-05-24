"""FU-2 integration tests — circuit breaker auto-wrap on real providers.

AUDIT-FIXES-IMPLEMENTATION-PLAN FU-2: verify that `AnthropicProvider`,
`OpenAIProvider`, and `DeepSeekProvider` thread their real SDK calls
through `services.think.circuit_breaker.get_breaker(<name>)`. After N
failures the named breaker must open; subsequent calls must fast-fail
with `CircuitOpenError`.

We stub the SDK client layer (not `_raw_call` itself) so the real
`_raw_call` / `_structured_raw` body executes including the breaker
wrap. `CircuitOpenError` is verified NOT to self-count against the
breaker (the breaker raises it before invoking the wrapped fn).
"""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from lib.llm.provider import (
    AnthropicProvider,
    DeepSeekProvider,
    LLMConfig,
    OpenAIProvider,
)
from services.think.circuit_breaker import (
    CircuitOpenError,
    CircuitState,
    LLMCircuitBreaker,
    get_breaker,
    register_breaker,
    reset_breakers,
)


pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _fresh_breakers():
    reset_breakers()
    yield
    reset_breakers()


@pytest.fixture(autouse=True)
def _breaker_enabled(monkeypatch):
    """Default tests run with the breaker active. Individual tests
    flip the disable env var as needed."""
    monkeypatch.delenv("LLM_CIRCUIT_BREAKER_DISABLED", raising=False)
    yield


# ---------------------------------------------------------------------
# Anthropic provider
# ---------------------------------------------------------------------


async def test_fu2_anthropic_failures_open_breaker(monkeypatch):
    """AnthropicProvider._raw_call wraps `client.messages.create` in
    the 'anthropic' breaker. After 5 failing calls (min_samples=5,
    100% failure rate) the breaker must be OPEN."""
    breaker = LLMCircuitBreaker(
        failure_threshold=0.5, min_samples=5, open_duration=10.0,
    )
    register_breaker("anthropic", breaker)

    provider = AnthropicProvider(LLMConfig(
        provider="anthropic", api_key="dummy", model="claude-opus-4-7",
    ))

    # Stub anthropic.AsyncAnthropic so client.messages.create always raises.
    class _FakeMessages:
        async def create(self, **kwargs):
            raise RuntimeError("anthropic outage")

    class _FakeClient:
        def __init__(self, **kwargs):
            self.messages = _FakeMessages()

    import anthropic
    monkeypatch.setattr(anthropic, "AsyncAnthropic", _FakeClient)

    for _ in range(5):
        with pytest.raises(RuntimeError):
            await provider._raw_call(
                system="s", user="u", temperature=0.0,
                max_tokens=64, schema_hint="{}",
            )

    assert breaker.state == CircuitState.OPEN
    # Subsequent call fast-fails.
    with pytest.raises(CircuitOpenError):
        await provider._raw_call(
            system="s", user="u", temperature=0.0,
            max_tokens=64, schema_hint="{}",
        )


async def test_fu2_anthropic_breaker_disabled_env_bypasses_wrap(monkeypatch):
    """LLM_CIRCUIT_BREAKER_DISABLED=1 — failures do NOT feed the
    breaker; state stays CLOSED regardless of failure count."""
    breaker = LLMCircuitBreaker(
        failure_threshold=0.5, min_samples=3, open_duration=10.0,
    )
    register_breaker("anthropic", breaker)

    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_DISABLED", "1")

    provider = AnthropicProvider(LLMConfig(
        provider="anthropic", api_key="dummy", model="claude-opus-4-7",
    ))

    class _FakeMessages:
        async def create(self, **kwargs):
            raise RuntimeError("outage")

    class _FakeClient:
        def __init__(self, **kwargs):
            self.messages = _FakeMessages()

    import anthropic
    monkeypatch.setattr(anthropic, "AsyncAnthropic", _FakeClient)

    for _ in range(10):
        with pytest.raises(RuntimeError):
            await provider._raw_call(
                system="s", user="u", temperature=0.0,
                max_tokens=64, schema_hint="{}",
            )

    # Breaker never saw these failures.
    assert breaker.state == CircuitState.CLOSED
    assert len(breaker.events) == 0


# ---------------------------------------------------------------------
# OpenAI provider
# ---------------------------------------------------------------------


async def test_fu2_openai_failures_open_breaker(monkeypatch):
    """OpenAIProvider._raw_call wraps the SDK call in the 'openai'
    breaker. Failures open the named breaker."""
    breaker = LLMCircuitBreaker(
        failure_threshold=0.5, min_samples=5, open_duration=10.0,
    )
    register_breaker("openai", breaker)

    provider = OpenAIProvider(LLMConfig(
        provider="openai", api_key="dummy", model="gpt-4o",
    ))

    class _FakeCompletions:
        async def create(self, **kwargs):
            raise RuntimeError("openai outage")

    class _FakeChat:
        def __init__(self):
            self.completions = _FakeCompletions()

    class _FakeOpenAI:
        def __init__(self, **kwargs):
            self.chat = _FakeChat()

    import openai
    monkeypatch.setattr(openai, "AsyncOpenAI", _FakeOpenAI)

    for _ in range(5):
        with pytest.raises(RuntimeError):
            await provider._raw_call(
                system="s", user="u", temperature=0.0,
                max_tokens=64, schema_hint="{}",
            )
    assert breaker.state == CircuitState.OPEN

    with pytest.raises(CircuitOpenError):
        await provider._raw_call(
            system="s", user="u", temperature=0.0,
            max_tokens=64, schema_hint="{}",
        )


# ---------------------------------------------------------------------
# DeepSeek provider (via OpenAIProvider inheritance)
# ---------------------------------------------------------------------


async def test_fu2_deepseek_failures_open_breaker(monkeypatch):
    """DeepSeekProvider inherits _raw_call from OpenAIProvider and
    uses the 'deepseek' breaker. Failures via the non-strict path
    (schema with no registered strict variant) also route through the
    breaker."""
    breaker = LLMCircuitBreaker(
        failure_threshold=0.5, min_samples=5, open_duration=10.0,
    )
    register_breaker("deepseek", breaker)

    provider = DeepSeekProvider(LLMConfig(
        provider="deepseek", api_key="dummy", model="deepseek-chat",
    ))

    class _FakeCompletions:
        async def create(self, **kwargs):
            raise RuntimeError("deepseek outage")

    class _FakeChat:
        def __init__(self):
            self.completions = _FakeCompletions()

    class _FakeOpenAI:
        def __init__(self, **kwargs):
            self.chat = _FakeChat()

    import openai
    monkeypatch.setattr(openai, "AsyncOpenAI", _FakeOpenAI)

    for _ in range(5):
        with pytest.raises(RuntimeError):
            await provider._raw_call(
                system="s", user="u", temperature=0.0,
                max_tokens=64, schema_hint="{}",
            )
    assert breaker.state == CircuitState.OPEN


async def test_fu2_circuit_open_does_not_count_against_breaker(monkeypatch):
    """CircuitOpenError is raised BEFORE the wrapped fn runs. It must
    not feed back into the breaker's success/failure window — otherwise
    a single OPEN state would keep extending itself."""
    breaker = LLMCircuitBreaker(
        failure_threshold=0.5, min_samples=5, open_duration=60.0,
    )
    register_breaker("deepseek", breaker)

    provider = DeepSeekProvider(LLMConfig(
        provider="deepseek", api_key="dummy", model="deepseek-chat",
    ))

    class _FakeCompletions:
        async def create(self, **kwargs):
            raise RuntimeError("outage")

    class _FakeChat:
        def __init__(self):
            self.completions = _FakeCompletions()

    class _FakeOpenAI:
        def __init__(self, **kwargs):
            self.chat = _FakeChat()

    import openai
    monkeypatch.setattr(openai, "AsyncOpenAI", _FakeOpenAI)

    # Trip the breaker open.
    for _ in range(5):
        with pytest.raises(RuntimeError):
            await provider._raw_call(
                system="s", user="u", temperature=0.0,
                max_tokens=64, schema_hint="{}",
            )
    assert breaker.state == CircuitState.OPEN
    samples_at_trip = len(breaker.events)

    # Subsequent calls fast-fail with CircuitOpenError — and must NOT
    # grow the breaker's event window (they didn't hit the SDK).
    for _ in range(10):
        with pytest.raises(CircuitOpenError):
            await provider._raw_call(
                system="s", user="u", temperature=0.0,
                max_tokens=64, schema_hint="{}",
            )
    assert len(breaker.events) == samples_at_trip, (
        "CircuitOpenError should not feed the breaker's own window"
    )
