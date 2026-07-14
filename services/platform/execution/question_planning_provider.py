"""Provider selection for inquiry question planning.

Question planning is latency-sensitive and does not need the same reasoning
budget as Think's main model-update pass. The production path is Codex-only:
Think uses the app's Codex provider, while question planning uses a dedicated
low-effort Codex provider.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

from lib.llm.provider import LLMConfig, LLMProvider, build_provider


_QUESTION_PLANNING_PROVIDER_CACHE: LLMProvider | None = None
_QUESTION_PLANNING_PROVIDER_CACHE_KEY: tuple[str, str, str, float, int] | None = None
_QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE: LLMProvider | None = None
_QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE_KEY: (
    tuple[str, str, str, float, int] | None
) = None
DEFAULT_CODEX_QUESTION_MODEL = "gpt-5.3-codex-spark"
DEFAULT_CODEX_QUESTION_FALLBACK_MODEL = "gpt-5.4-mini"
DEFAULT_CODEX_SPARK_QUESTION_TIMEOUT_SECONDS = 24.0
DEFAULT_CODEX_FALLBACK_QUESTION_TIMEOUT_SECONDS = 36.0
DEFAULT_CODEX_QUESTION_FAILURE_BACKOFF_SECONDS = 90.0
DEFAULT_CODEX_QUESTION_QUOTA_BACKOFF_SECONDS = 6 * 60 * 60.0
DEFAULT_CODEX_QUESTION_TIMEOUT_BACKOFF_SECONDS = 10 * 60.0


@dataclass(slots=True)
class _ProviderBackoff:
    blocked_until: float
    failure_count: int
    reason: str
    detail: str
    backoff_kind: str
    backoff_seconds: float


_QUESTION_PLANNING_PROVIDER_BACKOFFS: dict[tuple[Any, ...], _ProviderBackoff] = {}


def select_question_planning_provider(
    llm_provider: LLMProvider | None,
) -> LLMProvider | None:
    """Return the provider that should plan inquiry questions."""

    if llm_provider is None:
        return None
    config = getattr(llm_provider, "config", None)
    if getattr(config, "provider", None) != "codex":
        return None
    if getattr(llm_provider, "_is_question_planning_provider", False):
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


def question_planning_provider_backoff_seconds() -> float:
    return _env_float(
        "INQUIRY_CODEX_QUESTION_FAILURE_BACKOFF_SECONDS",
        DEFAULT_CODEX_QUESTION_FAILURE_BACKOFF_SECONDS,
    )


def question_planning_provider_quota_backoff_seconds() -> float:
    return _env_float(
        "INQUIRY_CODEX_QUESTION_QUOTA_BACKOFF_SECONDS",
        DEFAULT_CODEX_QUESTION_QUOTA_BACKOFF_SECONDS,
    )


def question_planning_provider_timeout_backoff_seconds() -> float:
    return _env_float(
        "INQUIRY_CODEX_QUESTION_TIMEOUT_BACKOFF_SECONDS",
        DEFAULT_CODEX_QUESTION_TIMEOUT_BACKOFF_SECONDS,
    )


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw:
        try:
            return max(0.0, float(raw))
        except ValueError:
            pass
    return default


def question_planning_provider_backoff_note(
    llm_provider: LLMProvider | None,
    *,
    now: float | None = None,
) -> dict[str, Any] | None:
    key = _question_planning_provider_health_key(llm_provider)
    if key is None:
        return None
    if now is None:
        now = time.monotonic()
    entry = _QUESTION_PLANNING_PROVIDER_BACKOFFS.get(key)
    if entry is None:
        return None
    remaining = entry.blocked_until - now
    if remaining <= 0:
        _QUESTION_PLANNING_PROVIDER_BACKOFFS.pop(key, None)
        return None
    return {
        **question_planning_provider_metadata(llm_provider),
        "reason": entry.reason,
        "detail": entry.detail,
        "failure_count": entry.failure_count,
        "backoff_kind": entry.backoff_kind,
        "backoff_seconds": entry.backoff_seconds,
        "backoff_remaining_ms": int(remaining * 1000),
    }


def record_question_planning_provider_failure(
    llm_provider: LLMProvider | None,
    exc: Exception,
    *,
    now: float | None = None,
) -> dict[str, Any] | None:
    key = _question_planning_provider_health_key(llm_provider)
    backoff_kind, backoff_s = _question_planning_provider_backoff_for_failure(exc)
    if key is None or backoff_s <= 0:
        return None
    if now is None:
        now = time.monotonic()
    prior = _QUESTION_PLANNING_PROVIDER_BACKOFFS.get(key)
    failure_count = 1 if prior is None else prior.failure_count + 1
    detail = _compact_provider_error_detail(exc)
    entry = _ProviderBackoff(
        blocked_until=now + backoff_s,
        failure_count=failure_count,
        reason=type(exc).__name__,
        detail=detail,
        backoff_kind=backoff_kind,
        backoff_seconds=backoff_s,
    )
    _QUESTION_PLANNING_PROVIDER_BACKOFFS[key] = entry
    return question_planning_provider_backoff_note(llm_provider, now=now)


def record_question_planning_provider_success(llm_provider: LLMProvider | None) -> None:
    key = _question_planning_provider_health_key(llm_provider)
    if key is not None:
        _QUESTION_PLANNING_PROVIDER_BACKOFFS.pop(key, None)


def reset_question_planning_provider_health() -> None:
    _QUESTION_PLANNING_PROVIDER_BACKOFFS.clear()


def _question_planning_provider_backoff_for_failure(
    exc: Exception,
) -> tuple[str, float]:
    message = f"{type(exc).__name__} {exc}".casefold()
    if any(
        marker in message
        for marker in (
            "usage limit",
            "rate limit",
            "too many requests",
            "insufficient quota",
            "quota",
        )
    ):
        return "quota", question_planning_provider_quota_backoff_seconds()
    if isinstance(exc, TimeoutError) or any(
        marker in message
        for marker in (
            "timeout",
            "timed out",
        )
    ):
        return "timeout", question_planning_provider_timeout_backoff_seconds()
    return "generic", question_planning_provider_backoff_seconds()


def select_question_planning_fallback_provider(
    source: LLMProvider | None,
    failed_provider: LLMProvider | None,
) -> LLMProvider | None:
    """Return a quota fallback provider for Codex question planning.

    The normal question planner uses the fastest low-effort Codex model. If that
    model is quota-limited, retry once with the source Think model at low effort
    so ambiguity planning remains LLM-backed instead of silently degrading.
    """

    if source is None:
        return None
    source_config = getattr(source, "config", None)
    if getattr(source_config, "provider", None) != "codex":
        return None
    failed_config = getattr(failed_provider, "config", None)

    model = (
        os.environ.get("INQUIRY_CODEX_QUESTION_FALLBACK_MODEL")
        or DEFAULT_CODEX_QUESTION_FALLBACK_MODEL
        or getattr(source_config, "model", None)
    )
    if not model:
        return None

    timeout_s = float(
        os.environ.get(
            "INQUIRY_CODEX_QUESTION_FALLBACK_TIMEOUT_SECONDS",
            os.environ.get(
                "INQUIRY_CODEX_QUESTION_TIMEOUT_SECONDS",
                str(
                    DEFAULT_CODEX_FALLBACK_QUESTION_TIMEOUT_SECONDS
                    if model == DEFAULT_CODEX_QUESTION_FALLBACK_MODEL
                    else max(24.0, min(float(source_config.timeout_s), 60.0))
                ),
            ),
        )
    )
    max_retries = int(
        os.environ.get("INQUIRY_CODEX_QUESTION_FALLBACK_MAX_RETRIES", "0")
    )

    if (
        getattr(failed_config, "provider", None) == "codex"
        and getattr(failed_config, "model", None) == model
        and getattr(failed_config, "reasoning_effort", None) == "low"
        and float(getattr(failed_config, "timeout_s", timeout_s)) == timeout_s
        and int(getattr(failed_config, "max_retries", max_retries)) == max_retries
    ):
        return None

    return _cached_fallback_provider(source_config, model, timeout_s, max_retries)


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


def _question_planning_provider_health_key(
    llm_provider: LLMProvider | None,
) -> tuple[Any, ...] | None:
    if llm_provider is None:
        return None
    config = getattr(llm_provider, "config", None)
    if getattr(config, "provider", None) != "codex":
        return None
    return (
        getattr(config, "provider", None),
        getattr(config, "api_key", None),
        getattr(config, "model", None),
        getattr(config, "reasoning_effort", None),
        float(getattr(config, "timeout_s", 0) or 0),
        int(getattr(config, "max_retries", 0) or 0),
    )


def _compact_provider_error_detail(exc: Exception) -> str:
    lines = [
        line.strip()
        for line in str(exc).splitlines()
        if line.strip()
        and ":loader:" not in line
        and "codex_core_skills::loader" not in line
        and "ignoring interface.icon_" not in line
    ]
    detail = "\n".join(lines) if lines else str(exc)
    if len(detail) <= 240:
        return detail
    return detail[-240:]


def _cached_fallback_provider(
    source_config: LLMConfig,
    model: str,
    timeout_s: float,
    max_retries: int,
) -> LLMProvider:
    global _QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE
    global _QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE_KEY

    key = (source_config.provider, source_config.api_key, model, timeout_s, max_retries)
    cached = _QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE
    if cached is not None and _QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE_KEY == key:
        return cached

    _QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE = build_provider(
        LLMConfig(
            provider="codex",
            api_key=source_config.api_key,
            model=model,
            timeout_s=timeout_s,
            max_retries=max_retries,
            reasoning_effort="low",
        )
    )
    _QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE_KEY = key
    return _QUESTION_PLANNING_FALLBACK_PROVIDER_CACHE
