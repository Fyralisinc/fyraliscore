"""Tests for lib/llm/provider.py."""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Literal

import pytest
from pydantic import BaseModel, Field

from lib.llm.provider import (
    AnthropicProvider,
    DeepSeekProvider,
    LLMConfig,
    LLMConfigError,
    LLMParseError,
    LLMProvider,
    OpenAIProvider,
    build_provider,
)


# ---------------------------------------------------------------------
# A simple Pydantic schema the LLM is expected to produce.
# ---------------------------------------------------------------------

class Claim(BaseModel):
    """A single claim with confidence in [0.05, 0.95]."""
    claim: str
    confidence: float = Field(ge=0.05, le=0.95)
    kind: Literal["state", "prediction"]


# ---------------------------------------------------------------------
# Test double: a Provider whose _raw_call is scripted.
# ---------------------------------------------------------------------

class ScriptedProvider(LLMProvider):
    """
    Replays a list of canned responses (or exceptions) in order.
    Each call to `_raw_call` pops the next item from `responses`.
    """

    def __init__(self, responses: list[str | Exception], cfg: LLMConfig | None = None):
        super().__init__(cfg or LLMConfig(provider="anthropic", api_key="test", model="m"))
        self.responses = list(responses)
        self.calls: list[dict] = []

    async def _raw_call(self, *, system, user, temperature, max_tokens, schema_hint):
        self.calls.append({
            "system": system, "user": user,
            "temperature": temperature, "max_tokens": max_tokens,
            "schema_hint": schema_hint,
        })
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def _valid_payload() -> str:
    return json.dumps({"claim": "Alice ships fast", "confidence": 0.7, "kind": "state"})


# =====================================================================
# Config
# =====================================================================

def test_config_from_env_defaults(monkeypatch):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.setenv("LLM_API_KEY", "k")
    cfg = LLMConfig.from_env()
    assert cfg.provider == "anthropic"
    assert cfg.api_key == "k"
    assert cfg.model == "claude-opus-4-7"
    assert cfg.timeout_s == 30.0


def test_config_from_env_openai(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_API_KEY", "k")
    monkeypatch.delenv("LLM_MODEL", raising=False)
    cfg = LLMConfig.from_env()
    assert cfg.provider == "openai"
    assert cfg.model == "gpt-4o"


def test_config_from_env_unknown_provider(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "my-llm")
    monkeypatch.setenv("LLM_API_KEY", "k")
    with pytest.raises(LLMConfigError):
        LLMConfig.from_env()


def test_build_provider_anthropic(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("LLM_API_KEY", "k")
    provider = build_provider()
    assert isinstance(provider, AnthropicProvider)


def test_build_provider_openai(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    monkeypatch.setenv("LLM_API_KEY", "k")
    provider = build_provider()
    assert isinstance(provider, OpenAIProvider)


def test_deepseek_reasoner_uses_json_mode_not_strict_tools(monkeypatch):
    """DeepSeek reasoner rejects tool_choice; chat models keep strict tools."""
    from lib.llm.provider import _deepseek_supports_strict_tool_calling

    assert not _deepseek_supports_strict_tool_calling("deepseek-reasoner")
    assert not _deepseek_supports_strict_tool_calling("deepseek-reasoner-v2")
    assert _deepseek_supports_strict_tool_calling("deepseek-chat")


async def test_deepseek_strict_parse_failure_falls_back_to_json_mode(
    monkeypatch,
):
    """Malformed strict tool args should not be the final failure mode."""
    from services.think.diff_schema import RawDiff

    monkeypatch.setenv("LLM_CIRCUIT_BREAKER_DISABLED", "1")

    strict_calls = 0
    fallback_call: dict = {}

    class FakeCompletions:
        async def create(self, **_kwargs):
            nonlocal strict_calls
            strict_calls += 1
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        arguments='{"trigger_ref": "x" "tenant_id": "y"}'
                                    )
                                )
                            ]
                        )
                    )
                ],
                usage=SimpleNamespace(prompt_tokens=11, completion_tokens=7),
            )

    class FakeOpenAIClient:
        def __init__(self, **_kwargs):
            self.chat = SimpleNamespace(
                completions=FakeCompletions()
            )

    async def fake_json_mode(self, **kwargs):
        fallback_call.update(kwargs)
        return '{"fallback": true}'

    import openai

    monkeypatch.setattr(openai, "AsyncOpenAI", FakeOpenAIClient)
    monkeypatch.setattr(OpenAIProvider, "_structured_raw", fake_json_mode)

    provider = DeepSeekProvider(LLMConfig(
        provider="deepseek",
        api_key="k",
        model="deepseek-chat",
        max_retries=1,
    ))

    raw = await provider._structured_raw(
        system="s",
        user="u",
        schema=RawDiff,
        temperature=0.0,
        max_tokens=128,
    )

    assert raw == '{"fallback": true}'
    assert strict_calls == 2
    assert "Prior strict tool-call output failed validation" in fallback_call["user"]


# =====================================================================
# Happy-path structured()
# =====================================================================

async def test_structured_happy_path():
    p = ScriptedProvider([_valid_payload()])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert isinstance(out, Claim)
    assert out.claim == "Alice ships fast"
    assert out.confidence == 0.7


async def test_structured_strips_code_fences():
    raw = "```json\n" + _valid_payload() + "\n```"
    p = ScriptedProvider([raw])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert out.claim == "Alice ships fast"


async def test_structured_schema_hint_included_in_first_call():
    p = ScriptedProvider([_valid_payload()])
    await p.structured(system="s", user="u", schema=Claim)
    assert "confidence" in p.calls[0]["schema_hint"]
    assert "claim" in p.calls[0]["schema_hint"]


# =====================================================================
# Retry-on-parse-failure
# =====================================================================

async def test_structured_retries_on_bad_json_then_succeeds():
    p = ScriptedProvider([
        "not json at all",
        _valid_payload(),
    ])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert out.confidence == 0.7
    assert len(p.calls) == 2
    # Second call includes a repair note.
    assert "Prior attempt failed validation" in p.calls[1]["user"]


async def test_structured_retries_on_schema_validation_failure():
    bad = json.dumps({"claim": "x", "confidence": 2.0, "kind": "state"})  # out of range
    p = ScriptedProvider([bad, _valid_payload()])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert out.confidence == 0.7
    assert len(p.calls) == 2


async def test_structured_exhausts_max_retries():
    # TK-5: default max_retries=1 → 2 total attempts (simplified from
    # the legacy 3 now that strict-mode makes parse errors rare).
    p = ScriptedProvider(["junk"] * 2)
    with pytest.raises(LLMParseError) as exc:
        await p.structured(system="s", user="u", schema=Claim)
    assert len(p.calls) == 2
    assert exc.value.context["schema"] == "Claim"


async def test_structured_respects_custom_max_retries():
    cfg = LLMConfig(provider="anthropic", api_key="k", model="m", max_retries=1)
    p = ScriptedProvider(["bad", "bad"], cfg=cfg)
    with pytest.raises(LLMParseError):
        await p.structured(system="s", user="u", schema=Claim)
    assert len(p.calls) == 2


async def test_structured_accepts_prose_prefixed_json_only_when_fenced():
    """
    The repair-aware parser tolerates code fences but NOT arbitrary
    prose prefixes. Prose-before-JSON should fail parse and trigger
    a retry.
    """
    bad = "Here is my answer:\n" + _valid_payload()
    p = ScriptedProvider([bad, _valid_payload()])
    out = await p.structured(system="s", user="u", schema=Claim)
    assert out.confidence == 0.7
    assert len(p.calls) == 2


async def test_structured_passes_temperature_and_max_tokens():
    p = ScriptedProvider([_valid_payload()])
    await p.structured(
        system="s", user="u", schema=Claim,
        temperature=0.35, max_tokens=128,
    )
    call = p.calls[0]
    assert call["temperature"] == 0.35
    assert call["max_tokens"] == 128


async def test_structured_propagates_raw_call_errors():
    class Boom(Exception):
        pass

    p = ScriptedProvider([Boom("server down")])
    with pytest.raises(Boom):
        await p.structured(system="s", user="u", schema=Claim)


async def test_structured_error_rejects_invalid_literal_field():
    # TK-5: default retry budget is now 1 (2 total attempts).
    bad_kind = json.dumps({"claim": "x", "confidence": 0.5, "kind": "not_a_kind"})
    p = ScriptedProvider([bad_kind, bad_kind])
    with pytest.raises(LLMParseError):
        await p.structured(system="s", user="u", schema=Claim)
    assert len(p.calls) == 2


async def test_anthropic_requires_api_key(monkeypatch):
    monkeypatch.delenv("LLM_API_KEY", raising=False)
    cfg = LLMConfig(provider="anthropic", api_key="", model="m")
    p = AnthropicProvider(cfg)
    with pytest.raises(LLMConfigError):
        await p._raw_call(
            system="s", user="u", temperature=0.0,
            max_tokens=10, schema_hint="{}",
        )


async def test_openai_requires_api_key():
    cfg = LLMConfig(provider="openai", api_key="", model="m")
    p = OpenAIProvider(cfg)
    with pytest.raises(LLMConfigError):
        await p._raw_call(
            system="s", user="u", temperature=0.0,
            max_tokens=10, schema_hint="{}",
        )


def test_schema_hint_is_json_valid():
    """The inlined schema hint must itself be valid JSON."""
    from lib.llm.provider import _schema_hint
    hint = _schema_hint(Claim)
    parsed = json.loads(hint)
    assert "properties" in parsed
    assert "claim" in parsed["properties"]


def test_strip_code_fences():
    from lib.llm.provider import _strip_code_fences
    assert _strip_code_fences("```json\n{\"a\": 1}\n```") == '{"a": 1}'
    assert _strip_code_fences("```\n{\"a\": 1}\n```") == '{"a": 1}'
    assert _strip_code_fences('{"a": 1}') == '{"a": 1}'


def test_try_parse_handles_plain_json():
    from lib.llm.provider import _try_parse
    parsed, err = _try_parse(_valid_payload(), Claim)
    assert err is None
    assert isinstance(parsed, Claim)


def test_try_parse_returns_error_on_bad_json():
    from lib.llm.provider import _try_parse
    parsed, err = _try_parse("not json", Claim)
    assert parsed is None
    assert err is not None


async def test_structured_different_schema_per_call():
    class Other(BaseModel):
        topic: str

    raw = json.dumps({"topic": "ship"})
    p = ScriptedProvider([raw])
    out = await p.structured(system="s", user="u", schema=Other)
    assert out.topic == "ship"
