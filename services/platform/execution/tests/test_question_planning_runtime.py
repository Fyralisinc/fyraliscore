from __future__ import annotations

from lib.llm.provider import LLMConfig, LLMProvider
from services.platform.execution import inquiry, question_planning_runtime
from services.platform.execution.config import InquiryConfig


class _Provider(LLMProvider):
    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        return "{}"


def _provider(
    *,
    provider: str = "codex",
    model: str = "gpt-5.3-codex-spark",
    timeout_s: float = 24.0,
) -> LLMProvider:
    return _Provider(
        LLMConfig(
            provider=provider,
            api_key="test",
            model=model,
            timeout_s=timeout_s,
        )
    )


def test_question_planning_runtime_helpers_keep_legacy_inquiry_identity() -> None:
    assert (
        inquiry._question_planning_max_tokens
        is question_planning_runtime.question_planning_max_tokens
    )
    assert (
        inquiry._question_planning_schema_name
        is question_planning_runtime.question_planning_schema_name
    )
    assert (
        inquiry._question_planning_timeout_seconds
        is question_planning_runtime.question_planning_timeout_seconds
    )
    assert (
        inquiry._use_compact_question_planning_schema
        is question_planning_runtime.use_compact_question_planning_schema
    )


def test_codex_spark_uses_compact_schema_and_token_cap(monkeypatch) -> None:
    monkeypatch.delenv("INQUIRY_CODEX_QUESTION_MAX_TOKENS", raising=False)
    monkeypatch.delenv("INQUIRY_CODEX_COMPACT_QUESTION_SCHEMA", raising=False)
    provider = _provider(model="gpt-5.3-codex-spark")
    cfg = InquiryConfig(llm_question_max_tokens=900)

    assert question_planning_runtime.use_compact_question_planning_schema(provider)
    assert question_planning_runtime.question_planning_schema_name(provider) == (
        "compact_v1"
    )
    assert question_planning_runtime.question_planning_max_tokens(cfg, provider) == 420


def test_question_planning_env_overrides_are_bounded(monkeypatch) -> None:
    provider = _provider(timeout_s=24.0)
    cfg = InquiryConfig(llm_question_max_tokens=900)

    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_MAX_TOKENS", "12")
    monkeypatch.setenv("INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS", "0.2")

    assert question_planning_runtime.question_planning_max_tokens(cfg, provider) == 320
    assert question_planning_runtime.question_planning_timeout_seconds(provider) == 1.0


def test_non_codex_provider_uses_full_schema_and_generic_timeout(monkeypatch) -> None:
    provider = _provider(provider="openai", model="gpt-4.1", timeout_s=18.0)
    cfg = InquiryConfig(llm_question_max_tokens=700)

    monkeypatch.setenv("INQUIRY_LLM_QUESTION_TIMEOUT_SECONDS", "42")

    assert not question_planning_runtime.use_compact_question_planning_schema(provider)
    assert (
        question_planning_runtime.question_planning_schema_name(provider) == "full_v1"
    )
    assert question_planning_runtime.question_planning_max_tokens(cfg, provider) == 700
    assert question_planning_runtime.question_planning_timeout_seconds(provider) == 42.0
