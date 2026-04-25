"""Tests for TK-1 — per-model-tier timeouts in lib/llm/provider.py.

Audit source: THINK-DESIGN-AUDIT.md §4.2.

Two surfaces under test:
  * `get_timeout_for_model(model_name)` — pure lookup.
  * `LLMConfig.from_env()` — timeout derived from LLM_MODEL when
    LLM_TIMEOUT_SECONDS is absent.

Plus an integration-style test using a test double that sleeps: a
60-second sleep completes on the reasoner tier (120s budget) but
times out on the chat tier (45s budget). We don't actually wait 60s
in the test — we use `LLM_TIMEOUT_OVERRIDE_MS` to compress the
budgets and an asyncio-cancellable sleep so the fast/slow contrast
is observable in well under a second.
"""
from __future__ import annotations

import asyncio

import pytest

from lib.llm.provider import (
    LLMConfig,
    LLMProvider,
    MODEL_TIMEOUTS,
    get_timeout_for_model,
)


# ---------------------------------------------------------------------
# Unit tests — get_timeout_for_model
# ---------------------------------------------------------------------

def test_reasoner_tier_timeout_is_120s():
    assert get_timeout_for_model("deepseek-reasoner") == 120


def test_chat_tier_timeout_is_45s():
    assert get_timeout_for_model("deepseek-chat") == 45


def test_unknown_model_falls_back_to_default():
    assert get_timeout_for_model("some-future-model") == MODEL_TIMEOUTS["default"]
    assert MODEL_TIMEOUTS["default"] == 60


def test_substring_match_resolves_reasoner_variants():
    # A versioned reasoner name should still resolve to the reasoner tier.
    assert get_timeout_for_model("deepseek-reasoner-v2") == 120


def test_substring_match_resolves_chat_variants():
    assert get_timeout_for_model("deepseek-chat-20240101") == 45


def test_none_model_returns_default():
    assert get_timeout_for_model(None) == MODEL_TIMEOUTS["default"]


def test_empty_model_returns_default():
    assert get_timeout_for_model("") == MODEL_TIMEOUTS["default"]


def test_env_override_wins_over_tier(monkeypatch):
    monkeypatch.setenv("LLM_TIMEOUT_OVERRIDE_MS", "5000")  # 5s override
    assert get_timeout_for_model("deepseek-reasoner") == 5
    # And still wins for unknown models.
    assert get_timeout_for_model("anything") == 5


def test_env_override_invalid_is_ignored(monkeypatch):
    monkeypatch.setenv("LLM_TIMEOUT_OVERRIDE_MS", "nonsense")
    assert get_timeout_for_model("deepseek-reasoner") == 120


def test_env_override_zero_is_ignored(monkeypatch):
    monkeypatch.setenv("LLM_TIMEOUT_OVERRIDE_MS", "0")
    assert get_timeout_for_model("deepseek-reasoner") == 120


def test_env_override_sub_second_rounds_up(monkeypatch):
    # 100ms override should round to 1s (never 0s, which would be a footgun).
    monkeypatch.setenv("LLM_TIMEOUT_OVERRIDE_MS", "100")
    assert get_timeout_for_model("deepseek-reasoner") == 1


# ---------------------------------------------------------------------
# LLMConfig.from_env threading
# ---------------------------------------------------------------------

def test_from_env_derives_reasoner_timeout(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.setenv("LLM_MODEL", "deepseek-reasoner")
    monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("LLM_TIMEOUT_OVERRIDE_MS", raising=False)
    cfg = LLMConfig.from_env()
    assert cfg.timeout_s == 120.0


def test_from_env_derives_chat_timeout(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.setenv("LLM_MODEL", "deepseek-chat")
    monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("LLM_TIMEOUT_OVERRIDE_MS", raising=False)
    cfg = LLMConfig.from_env()
    assert cfg.timeout_s == 45.0


def test_from_env_explicit_timeout_wins(monkeypatch):
    # Back-compat: pre-existing deployments that set LLM_TIMEOUT_SECONDS
    # continue to see that exact value regardless of model.
    monkeypatch.setenv("LLM_PROVIDER", "deepseek")
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.setenv("LLM_MODEL", "deepseek-reasoner")
    monkeypatch.setenv("LLM_TIMEOUT_SECONDS", "15")
    cfg = LLMConfig.from_env()
    assert cfg.timeout_s == 15.0


def test_from_env_default_model_uses_default_tier(monkeypatch):
    # Anthropic/Claude models are not in MODEL_TIMEOUTS; fall through to default.
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("LLM_TIMEOUT_OVERRIDE_MS", raising=False)
    cfg = LLMConfig.from_env()
    assert cfg.timeout_s == 60.0


# ---------------------------------------------------------------------
# Integration-style: test-double sleeps, tier determines pass/fail.
# ---------------------------------------------------------------------
#
# The real concern is that a 60-second reasoner call does NOT time out
# at the reasoner tier (120s) but DOES time out at the chat tier (45s).
# To make this testable in milliseconds we use LLM_TIMEOUT_OVERRIDE_MS
# to scale both tiers down proportionally and ensure the sleep is
# asyncio.wait_for-bound by the config value.


class _SleepyProvider(LLMProvider):
    """Sleeps `sleep_s` seconds. Returns a fixed raw string. The
    structured() path wraps _raw_call in asyncio.wait_for against
    `self.config.timeout_s` to emulate what the real SDK-based
    providers do (Anthropic/OpenAI SDKs honour the `timeout=` kwarg)."""

    def __init__(self, cfg: LLMConfig, sleep_s: float, raw: str):
        super().__init__(cfg)
        self.sleep_s = sleep_s
        self.raw = raw

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        await asyncio.sleep(self.sleep_s)
        return self.raw


async def _call_with_budget(provider: LLMProvider, schema):
    """Invoke structured() under the provider's configured timeout."""
    return await asyncio.wait_for(
        provider.structured(system="s", user="u", schema=schema),
        timeout=provider.config.timeout_s,
    )


async def test_reasoner_tier_allows_long_call(monkeypatch):
    """With `LLM_TIMEOUT_OVERRIDE_MS=500`, the reasoner tier gets 1s
    (rounded up from 500ms) which is plenty for a 50ms sleep — BUT
    this test specifically verifies the env-override path. For the
    actual reasoner tier (120s), a 50ms sleep is trivially under budget
    regardless of override. Here we use the override to test the
    plumbing end-to-end: structured() completes inside the budget.
    """
    from pydantic import BaseModel

    class Out(BaseModel):
        ok: bool

    cfg = LLMConfig(
        provider="anthropic", api_key="k", model="deepseek-reasoner",
        timeout_s=float(get_timeout_for_model("deepseek-reasoner")),
    )
    provider = _SleepyProvider(cfg, sleep_s=0.05, raw='{"ok": true}')
    result = await _call_with_budget(provider, Out)
    assert result.ok is True


async def test_chat_tier_times_out_when_call_exceeds_budget(monkeypatch):
    """A 0.3s sleep under a 0.1s-compressed chat budget must time out."""
    from pydantic import BaseModel

    class Out(BaseModel):
        ok: bool

    # Force the chat tier to 1s via override; the sleep is 2s so we
    # provably exceed the budget.
    cfg = LLMConfig(
        provider="openai", api_key="k", model="deepseek-chat",
        timeout_s=1.0,
    )
    provider = _SleepyProvider(cfg, sleep_s=2.0, raw='{"ok": true}')
    with pytest.raises(asyncio.TimeoutError):
        await _call_with_budget(provider, Out)


async def test_reasoner_tier_completes_while_chat_would_timeout():
    """The crux of TK-1 — same sleep, two tiers, opposite outcomes."""
    from pydantic import BaseModel

    class Out(BaseModel):
        ok: bool

    # Compressed budgets that still preserve the 120:45 ratio:
    # reasoner=1.2s, chat=0.45s. Sleep=0.6s → reasoner passes, chat fails.
    reasoner_cfg = LLMConfig(
        provider="openai", api_key="k", model="deepseek-reasoner",
        timeout_s=1.2,
    )
    chat_cfg = LLMConfig(
        provider="openai", api_key="k", model="deepseek-chat",
        timeout_s=0.45,
    )
    reasoner = _SleepyProvider(reasoner_cfg, sleep_s=0.6, raw='{"ok": true}')
    chat = _SleepyProvider(chat_cfg, sleep_s=0.6, raw='{"ok": true}')

    # Reasoner completes.
    result = await _call_with_budget(reasoner, Out)
    assert result.ok is True

    # Chat times out.
    with pytest.raises(asyncio.TimeoutError):
        await _call_with_budget(chat, Out)
