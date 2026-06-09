"""Provider selection for inquiry question planning.

Question planning is latency-sensitive and does not need the same reasoning
budget as Think's main model-update pass. When the app-wide provider is Codex,
use a dedicated small Codex provider configured for low effort.
"""
from __future__ import annotations

import os

from lib.llm.provider import LLMConfig, LLMProvider, build_provider


_QUESTION_PLANNING_PROVIDER_CACHE: LLMProvider | None = None
_QUESTION_PLANNING_PROVIDER_CACHE_KEY: tuple[str, str, str, float, int] | None = None
DEFAULT_CODEX_QUESTION_MODEL = "gpt-5.3-codex-spark"
DEFAULT_CODEX_SPARK_QUESTION_TIMEOUT_SECONDS = 24.0


def select_question_planning_provider(
    llm_provider: LLMProvider | None,
) -> LLMProvider | None:
    """Return the provider that should plan inquiry questions."""

    if llm_provider is None:
        return None
    if not _codex_low_effort_question_planning_enabled():
        return llm_provider
    if getattr(llm_provider.config, "provider", None) != "codex":
        return llm_provider
    return _codex_low_effort_provider(llm_provider)


def question_planning_provider_metadata(
    llm_provider: LLMProvider | None,
) -> dict[str, str | bool | None]:
    """Small telemetry payload for inquiry notes and stress reports."""

    if llm_provider is None:
        return {
            "llm_provider": None,
            "llm_model": None,
            "llm_reasoning_effort": None,
            "uses_codex_low_effort": False,
        }
    provider_name = getattr(llm_provider.config, "provider", None)
    effort = getattr(llm_provider.config, "reasoning_effort", None)
    return {
        "llm_provider": provider_name,
        "llm_model": getattr(llm_provider.config, "model", None),
        "llm_reasoning_effort": effort,
        "uses_codex_low_effort": provider_name == "codex" and effort == "low",
    }


def _codex_low_effort_question_planning_enabled() -> bool:
    raw = os.environ.get("INQUIRY_CODEX_LOW_EFFORT_QUESTION_PLANNING", "1")
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _codex_low_effort_provider(source: LLMProvider) -> LLMProvider:
    global _QUESTION_PLANNING_PROVIDER_CACHE
    global _QUESTION_PLANNING_PROVIDER_CACHE_KEY

    source_config = source.config
    model = os.environ.get("INQUIRY_CODEX_QUESTION_MODEL") or DEFAULT_CODEX_QUESTION_MODEL
    default_timeout_s = (
        DEFAULT_CODEX_SPARK_QUESTION_TIMEOUT_SECONDS
        if "spark" in model.casefold()
        else source_config.timeout_s
    )
    timeout_s = float(
        os.environ.get(
            "INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS",
            str(default_timeout_s),
        )
    )
    default_retries = 0 if "spark" in model.casefold() else source_config.max_retries
    max_retries = int(
        os.environ.get(
            "INQUIRY_CODEX_QUESTION_MAX_RETRIES",
            str(default_retries),
        )
    )
    key = (source_config.provider, source_config.api_key, model, timeout_s, max_retries)
    cached = _QUESTION_PLANNING_PROVIDER_CACHE
    if cached is not None and _QUESTION_PLANNING_PROVIDER_CACHE_KEY == key:
        return cached

    _QUESTION_PLANNING_PROVIDER_CACHE = build_provider(
        LLMConfig(
            provider="codex",
            api_key=source_config.api_key,
            model=model,
            timeout_s=timeout_s,
            max_retries=max_retries,
            reasoning_effort="low",
        )
    )
    _QUESTION_PLANNING_PROVIDER_CACHE_KEY = key
    return _QUESTION_PLANNING_PROVIDER_CACHE
