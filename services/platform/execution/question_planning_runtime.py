"""Runtime option helpers for inquiry question planning."""

from __future__ import annotations

import os

from lib.llm.provider import LLMProvider

from .config import InquiryConfig


def question_planning_max_tokens(
    config: InquiryConfig,
    llm_provider: LLMProvider,
) -> int:
    provider_name = getattr(llm_provider.config, "provider", "")
    if provider_name == "codex":
        raw = os.environ.get("INQUIRY_CODEX_QUESTION_MAX_TOKENS")
        if raw:
            try:
                return max(320, int(raw))
            except ValueError:
                pass
        if use_compact_question_planning_schema(llm_provider):
            return min(config.llm_question_max_tokens, 420)
        model_name = str(getattr(llm_provider.config, "model", "") or "").casefold()
        if "spark" in model_name:
            return min(config.llm_question_max_tokens, 650)
    return config.llm_question_max_tokens


def question_planning_schema_name(llm_provider: LLMProvider) -> str:
    if use_compact_question_planning_schema(llm_provider):
        return "compact_v1"
    return "full_v1"


def use_compact_question_planning_schema(llm_provider: LLMProvider) -> bool:
    provider_name = str(getattr(llm_provider.config, "provider", "") or "")
    if provider_name != "codex":
        return False
    raw = os.environ.get("INQUIRY_CODEX_COMPACT_QUESTION_SCHEMA", "1")
    if raw.strip().lower() in {"0", "false", "no", "off"}:
        return False
    model_name = str(getattr(llm_provider.config, "model", "") or "").casefold()
    return "spark" in model_name


def question_planning_timeout_seconds(llm_provider: LLMProvider) -> float:
    provider_name = str(getattr(llm_provider.config, "provider", "") or "")
    if provider_name == "codex":
        raw = os.environ.get("INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS")
        if raw:
            try:
                return max(1.0, float(raw))
            except ValueError:
                pass
        return float(getattr(llm_provider.config, "timeout_s", 30) or 30)
    raw = os.environ.get("INQUIRY_LLM_QUESTION_TIMEOUT_SECONDS")
    if raw:
        try:
            return max(1.0, float(raw))
        except ValueError:
            pass
    return float(getattr(llm_provider.config, "timeout_s", 30) or 30)


__all__ = [
    "question_planning_max_tokens",
    "question_planning_schema_name",
    "question_planning_timeout_seconds",
    "use_compact_question_planning_schema",
]
