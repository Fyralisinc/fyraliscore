"""
lib/llm/provider.py — provider-agnostic structured LLM abstraction.

Shape:

    class SomeOutput(BaseModel):
        claim: str
        confidence: float

    provider = build_provider()
    out: SomeOutput = await provider.structured(
        system="You are a reasoning engine ...",
        user="What is the claim here? ...",
        schema=SomeOutput,
        temperature=0.2,
        max_tokens=1024,
    )

The provider picks up config from env:
    LLM_PROVIDER   = "codex" for the Fyralis app path
    CODEX_API_KEY  = ...
    CODEX_MODEL    = "gpt-5.5" | "gpt-5.3-codex" | ...
    CODEX_REASONING_EFFORT = "medium"
    LLM_TIMEOUT_SECONDS = 30
    LLM_MAX_RETRIES = 1   # parse/schema repair retries; 0 disables repair calls

Implements retry-on-parse-failure per Prompt 0.2 + TK-5. After
strict mode, parse errors are rare; the default `max_retries=1` yields
one repair attempt (2 total calls) rather than the legacy 3. The provider
is the sole retry owner: parse and retryable transport failures share a
hard ceiling of 3 physical attempts and a 240-second logical deadline.

If the model returns invalid JSON or JSON that doesn't validate
against the supplied Pydantic schema, the prompt is augmented
with a repair instruction and the call is retried.

Wave 0 note: the actual Anthropic / OpenAI calls are wrapped in
thin shims that tests mock via patching. This keeps the library
testable without a live API key.
"""
from __future__ import annotations

import abc
import asyncio
import contextvars
import enum
import json
import os
import tempfile
import time
import tomllib
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

import structlog
from pydantic import BaseModel, ValidationError as PydanticValidationError

from lib.contracts.kernel import canonical_sha256
from lib.llm.telemetry import (
    LLMReceiptSink,
    LogicalCallReceipt,
    PhysicalAttemptReceipt,
    utc_now,
)
from lib.shared.errors import CompanyOSError

_log = structlog.get_logger(__name__)


def _llm_strict_config() -> bool:
    """Cost-plan §3.1: when set, config footguns raise instead of warning."""
    return os.environ.get("LLM_STRICT_CONFIG", "0").strip().lower() in {
        "1", "on", "true", "yes",
    }


def _config_footgun(message: str, **fields: Any) -> None:
    """Surface a provider-config footgun. Logs a warning so it is visible (it
    was silent before), and raises `LLMConfigError` when `LLM_STRICT_CONFIG` is
    set for environments that prefer fail-fast."""
    _log.warning("llm.config_footgun", message=message, **fields)
    if _llm_strict_config():
        raise LLMConfigError(message, **fields)


# ---------------------------------------------------------------------
# OP-2 — Per-model pricing + token usage tracking.
#
# THINK-DESIGN-AUDIT §9.3: no cost tracking today. Retry-heavy triggers
# silently consume budget. Extract token counts from provider responses
# and compute per-call cost via a per-model pricing table.
#
# Prices are USD per 1M tokens (input / output). Designed to be easy to
# edit: add a new model row + its pricing and all callers pick it up.
# Unknown models fall through to `default`.
# ---------------------------------------------------------------------

# OP-2 cost-plan §0.1: each row may carry optional cache-tier rates so
# `compute_cost_usd` stops pricing cache-read/-write tokens at the full input
# rate. `cache_read_per_mtok` = price of a prompt-cache *hit* token;
# `cache_write_per_mtok` = price of a cache *creation* token (Anthropic only;
# OpenAI/DeepSeek caching is automatic with no write premium). When a rate is
# absent, the full `input_per_mtok` is used — conservative (never understates).
MODEL_PRICING: dict[str, dict[str, float]] = {
    # DeepSeek — on-demand tiers; cache-hit ≈ 0.1× input (approximate, repriced
    # periodically — DeepSeek is not the production provider, verify before use).
    "deepseek-reasoner": {
        "input_per_mtok": 0.55, "output_per_mtok": 2.19,
        "cache_read_per_mtok": 0.14,
    },
    "deepseek-chat": {
        "input_per_mtok": 0.27, "output_per_mtok": 1.10,
        "cache_read_per_mtok": 0.07,
    },
    # Anthropic — base tier subject to model-rev drift; non-production path
    # (prod is Codex), so these are dashboards-only. Cache rates follow
    # Anthropic's standard multipliers: read = 0.1× input, write (5-min TTL)
    # = 1.25× input. TODO: pin the base tier to a live quote before relying on
    # absolute Anthropic $ in dashboards.
    "claude-opus-4-7": {
        "input_per_mtok": 15.0, "output_per_mtok": 75.0,
        "cache_read_per_mtok": 1.5, "cache_write_per_mtok": 18.75,
    },
    "claude-sonnet-4-5": {
        "input_per_mtok": 3.0, "output_per_mtok": 15.0,
        "cache_read_per_mtok": 0.30, "cache_write_per_mtok": 3.75,
    },
    # OpenAI — gpt-4o cached input is 50% off standard input.
    "gpt-4o": {
        "input_per_mtok": 2.5, "output_per_mtok": 10.0,
        "cache_read_per_mtok": 1.25,
    },
    # Codex / GPT-5 family — production path. Cached input ≈ 0.1× input for the
    # GPT-5/Codex generation (e.g. gpt-5.5 $5→$0.50, codex $1.75→$0.175). These
    # remain approximate until the deployed table is pinned for Fyralis, but are
    # close enough that 1.1's cache savings show up directionally in dashboards.
    "gpt-5.3-codex": {
        "input_per_mtok": 1.75, "output_per_mtok": 14.0,
        "cache_read_per_mtok": 0.175,
    },
    "gpt-5.3-codex-spark": {
        "input_per_mtok": 1.75, "output_per_mtok": 14.0,
        "cache_read_per_mtok": 0.175,
    },
    "gpt-5.5": {
        "input_per_mtok": 5.0, "output_per_mtok": 30.0,
        "cache_read_per_mtok": 0.50,
    },
    # Fallback — conservative enough that an unknown model is not free.
    "default": {"input_per_mtok": 1.0, "output_per_mtok": 3.0},
}


def get_pricing_for_model(model_name: str | None) -> dict[str, float]:
    """Return `{input_per_mtok, output_per_mtok}` for the model. Exact
    match → substring match → default, same scheme as
    `get_timeout_for_model`."""
    if not model_name:
        return MODEL_PRICING["default"]
    if model_name in MODEL_PRICING:
        return MODEL_PRICING[model_name]
    for key, pricing in MODEL_PRICING.items():
        if key == "default":
            continue
        if key in model_name:
            return pricing
    return MODEL_PRICING["default"]


def compute_cost_usd(
    *,
    input_tokens: int,
    output_tokens: int,
    model_name: str | None,
    cache_read_tokens: int = 0,
    cache_creation_tokens: int = 0,
) -> float:
    """Compute call cost in USD for the given token counts + model.

    `input_tokens` is the *full* prompt-token count (cached + uncached);
    `cache_read_tokens` / `cache_creation_tokens` are the cached subsets, billed
    at the model's discounted cache rates. The uncached remainder is billed at
    the full input rate. Cache rates default to the full input rate when a model
    has no cache tier configured, so this never understates cost. Backward
    compatible: callers that pass only `input_tokens`/`output_tokens` get the
    same number as before (cache subsets default to 0)."""
    pricing = get_pricing_for_model(model_name)
    input_rate = pricing["input_per_mtok"]
    cache_read_rate = pricing.get("cache_read_per_mtok", input_rate)
    cache_write_rate = pricing.get("cache_write_per_mtok", input_rate)
    uncached_input = max(0, input_tokens - cache_read_tokens - cache_creation_tokens)
    return (
        uncached_input * input_rate
        + cache_read_tokens * cache_read_rate
        + cache_creation_tokens * cache_write_rate
        + output_tokens * pricing["output_per_mtok"]
    ) / 1_000_000.0


def _estimate_tokens_from_text(*parts: str | None) -> int:
    """Cheap fallback for transports that return text but no usage block."""
    char_count = sum(len(part or "") for part in parts)
    # The estimate is intentionally conservative enough for dashboards
    # and budget alarms; exact SDK usage still wins whenever available.
    return max(1, (char_count + 3) // 4)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


@dataclass
class LLMUsage:
    """One call's usage + cost accounting. Accumulated across a Think
    run by `LLMUsageAggregator`. Populated when the provider can extract
    token counts from the SDK response (Anthropic + OpenAI both return
    usage metadata; DeepSeek via the OpenAI-compatible endpoint does
    too). Zeroed when not available so callers can always sum safely.

    Cost-plan §0.1: `cache_read_tokens` / `cache_creation_tokens` are the cached
    subsets of `input_tokens` (which stays the *full* input count), so existing
    `total_input_tokens` consumers are unaffected. Populated only on transports
    that surface cache metadata (OpenAI/DeepSeek `prompt_*cache*`, Anthropic
    `cache_*_input_tokens`); the Codex CLI/app-server estimated path leaves them
    at 0."""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    model_name: str | None = None
    cost_usd: float = 0.0
    # Cost-plan §0.1: which slice of a Think run made this call —
    # 'main_reasoning' (default), 'question_planning', or 'parse_repair'.
    # Set from the `_CURRENT_USAGE_PURPOSE` contextvar at record time.
    purpose: str = "main_reasoning"
    # Whether token counts came from the provider or a text estimate.
    usage_exactness: str = "reported"


@dataclass
class LLMUsageAggregator:
    """Accumulator threaded into `LLMProvider` for a single Think run.
    Each provider call appends the call's `LLMUsage`; callers read
    totals via `total_input_tokens`, `total_output_tokens`,
    `total_cost_usd`, `call_count`.

    Installed via `LLMProvider.set_usage_aggregator(agg)` at the start
    of a Think run (inside `llm_reason`). Clearing is cheap — create a
    fresh aggregator per run."""
    calls: list[LLMUsage] = field(default_factory=list)

    def record(self, usage: LLMUsage) -> None:
        self.calls.append(usage)

    @property
    def call_count(self) -> int:
        return len(self.calls)

    @property
    def total_input_tokens(self) -> int:
        return sum(c.input_tokens for c in self.calls)

    @property
    def total_output_tokens(self) -> int:
        return sum(c.output_tokens for c in self.calls)

    @property
    def total_cache_read_tokens(self) -> int:
        return sum(c.cache_read_tokens for c in self.calls)

    @property
    def total_cache_creation_tokens(self) -> int:
        return sum(c.cache_creation_tokens for c in self.calls)

    @property
    def total_cost_usd(self) -> float:
        return sum(c.cost_usd for c in self.calls)

    def by_purpose(self) -> dict[str, "LLMUsage"]:
        """Cost-plan §0.1: aggregate calls per `purpose` so the ledger can
        separate main-reasoning spend from question-planning and parse-repair.
        Returns a `{purpose: summed LLMUsage}` map (cost summed, model_name from
        the last call of that purpose). Empty when no calls were recorded."""
        out: dict[str, LLMUsage] = {}
        for c in self.calls:
            agg = out.get(c.purpose)
            if agg is None:
                out[c.purpose] = LLMUsage(
                    input_tokens=c.input_tokens,
                    output_tokens=c.output_tokens,
                    cache_read_tokens=c.cache_read_tokens,
                    cache_creation_tokens=c.cache_creation_tokens,
                    model_name=c.model_name,
                    cost_usd=c.cost_usd,
                    purpose=c.purpose,
                    usage_exactness=c.usage_exactness,
                )
            else:
                agg.input_tokens += c.input_tokens
                agg.output_tokens += c.output_tokens
                agg.cache_read_tokens += c.cache_read_tokens
                agg.cache_creation_tokens += c.cache_creation_tokens
                agg.cost_usd += c.cost_usd
                agg.model_name = c.model_name or agg.model_name
                if c.usage_exactness == "estimated":
                    agg.usage_exactness = "estimated"
        return out

    def call_count_for(self, purpose: str) -> int:
        return sum(1 for c in self.calls if c.purpose == purpose)

    def reset(self) -> None:
        self.calls.clear()


# ---------------------------------------------------------------------
# Week 5 stabilization — per-task aggregator via ContextVar.
#
# `LLMProvider.set_usage_aggregator` stores the aggregator on the
# provider *instance*. That works for Think, which runs one reasoning
# task per provider use, but it races under concurrent rendering
# calls that share a single `RenderingService._service_singleton`: one
# task's `finally: set_usage_aggregator(None)` can clear the aggregator
# that a sibling task is still depending on, silently dropping its
# usage rows. `view_render_costs` then logs `$0.00` for whichever
# sibling lost the race.
#
# The contextvar path keeps Think's behavior intact while scoping
# rendering's aggregator to the current async task. `_record_usage`
# prefers the contextvar when set, falling back to the instance attr.
# ---------------------------------------------------------------------


_CURRENT_USAGE_AGG: contextvars.ContextVar[LLMUsageAggregator | None] = (
    contextvars.ContextVar("_CURRENT_USAGE_AGG", default=None)
)


_CURRENT_USAGE_PURPOSE: contextvars.ContextVar[str] = contextvars.ContextVar(
    "_CURRENT_USAGE_PURPOSE", default="main_reasoning"
)

_CURRENT_LOGICAL_CALL_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_CURRENT_LOGICAL_CALL_ID", default=None
)
_CURRENT_PHYSICAL_ATTEMPT_COUNT: contextvars.ContextVar[int] = contextvars.ContextVar(
    "_CURRENT_PHYSICAL_ATTEMPT_COUNT", default=0
)
_CURRENT_ATTEMPT_LIMIT: contextvars.ContextVar[int] = contextvars.ContextVar(
    "llm_attempt_limit", default=3,
)
_CURRENT_LOGICAL_DEADLINE: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "llm_logical_deadline", default=None,
)
_CURRENT_RECEIPT_SINK: contextvars.ContextVar[LLMReceiptSink | None] = (
    contextvars.ContextVar("_CURRENT_RECEIPT_SINK", default=None)
)


@contextmanager
def using_usage_purpose(purpose: str) -> Iterator[str]:
    """Cost-plan §0.1: tag every `_record_usage` call made within this block
    with `purpose` (e.g. 'question_planning', 'parse_repair') so the ledger can
    attribute spend per slice of a Think run. Task-local; nests/restores on exit.
    """
    token = _CURRENT_USAGE_PURPOSE.set(purpose)
    try:
        yield purpose
    finally:
        _CURRENT_USAGE_PURPOSE.reset(token)


@contextmanager
def using_usage_aggregator(agg: LLMUsageAggregator) -> Iterator[LLMUsageAggregator]:
    """Context manager: route `_record_usage` to `agg` for the duration.

    Task-local (via ContextVar), so concurrent renders each get their
    own aggregator without racing through a shared provider instance.
    Reset-on-exit is atomic even if the body raises.
    """
    token = _CURRENT_USAGE_AGG.set(agg)
    try:
        yield agg
    finally:
        _CURRENT_USAGE_AGG.reset(token)


@contextmanager
def using_receipt_sink(sink: LLMReceiptSink) -> Iterator[LLMReceiptSink]:
    """Route receipts to one async-task-local collector."""

    token = _CURRENT_RECEIPT_SINK.set(sink)
    try:
        yield sink
    finally:
        _CURRENT_RECEIPT_SINK.reset(token)


def _usage_field(obj: Any, *names: str) -> Any:
    """First truthy value among `names`, supporting dict or attribute access.
    Used to read provider usage blocks that may be SDK objects or plain dicts."""
    for name in names:
        value = obj.get(name) if isinstance(obj, dict) else getattr(obj, name, None)
        if value:
            return value
    return None


def _extract_anthropic_usage(response: Any) -> tuple[int, int, int, int]:
    """Anthropic usage → (full_input, output, cache_read, cache_creation).

    Anthropic reports `input_tokens` *excluding* cached/created tokens, plus
    separate `cache_read_input_tokens` / `cache_creation_input_tokens`. We
    return `full_input` = the sum so callers see total prompt size; the cache
    subsets are priced at their discounted/premium rates in `compute_cost_usd`.
    """
    usage = getattr(response, "usage", None)
    if usage is None:
        return 0, 0, 0, 0
    base_input = int(_usage_field(usage, "input_tokens") or 0)
    output = int(_usage_field(usage, "output_tokens") or 0)
    cache_read = int(_usage_field(usage, "cache_read_input_tokens") or 0)
    cache_creation = int(_usage_field(usage, "cache_creation_input_tokens") or 0)
    return base_input + cache_read + cache_creation, output, cache_read, cache_creation


def _extract_openai_usage(response: Any) -> tuple[int, int, int, int]:
    """OpenAI-compatible usage → (full_input, output, cache_read, cache_creation).

    For OpenAI/Codex/DeepSeek the reported `prompt_tokens` already *includes*
    cached tokens, so `full_input` = prompt_tokens and the cached subset is read
    from `prompt_tokens_details.cached_tokens` (OpenAI/Codex Responses + Chat) or
    the flat `prompt_cache_hit_tokens` field (DeepSeek). There is no cache-write
    premium on these providers, so cache_creation is always 0.
    """
    usage = getattr(response, "usage", None)
    if usage is None:
        return 0, 0, 0, 0
    full_input = int(_usage_field(usage, "prompt_tokens", "input_tokens") or 0)
    output = int(_usage_field(usage, "completion_tokens", "output_tokens") or 0)
    details = (
        usage.get("prompt_tokens_details")
        if isinstance(usage, dict)
        else getattr(usage, "prompt_tokens_details", None)
    )
    cache_read = 0
    if details is not None:
        cache_read = int(_usage_field(details, "cached_tokens") or 0)
    if cache_read == 0:
        # DeepSeek surfaces cache hits as a flat top-level field.
        cache_read = int(_usage_field(usage, "prompt_cache_hit_tokens") or 0)
    # Cached subset can't exceed the full prompt size.
    cache_read = min(cache_read, full_input)
    return full_input, output, cache_read, 0


T = TypeVar("T", bound=BaseModel)


class LLMError(CompanyOSError):
    default_code = "llm_error"


class LLMParseError(LLMError):
    default_code = "llm_parse_error"


class LLMConfigError(LLMError):
    default_code = "llm_config_error"


# ---------------------------------------------------------------------
# TK-5 — Error classification + per-class retry policies.
#
# THINK-DESIGN-AUDIT §4.1: the historical 3-attempt parse-error retry
# predates strict-mode structured output; it's largely dead code now.
# Classify errors by semantic class and apply targeted retry policy.
# ---------------------------------------------------------------------


class LLMRateLimitError(LLMError):
    default_code = "llm_rate_limit"


class LLMTimeoutError(LLMError):
    default_code = "llm_timeout"


class LLMContentViolationError(LLMError):
    default_code = "llm_content_violation"


class LLMTransientError(LLMError):
    default_code = "llm_transient"


class LLMPermanentError(LLMError):
    default_code = "llm_permanent"


class LLMErrorClass(str, enum.Enum):
    RATE_LIMIT = "rate_limit"
    TIMEOUT = "timeout"
    CONTENT_VIOLATION = "content_violation"
    PARSE_ERROR = "parse_error"
    TRANSIENT = "transient"
    PERMANENT = "permanent"


# Provider-error signals we look for in messages / status codes. We
# stay conservative — an unknown error falls into TRANSIENT (retry 2x)
# rather than PERMANENT so a genuine intermittent SDK failure isn't
# mis-bucketed as a dead letter.

_RATE_LIMIT_STATUSES = (429,)
_PERMANENT_STATUSES = (400, 401, 402, 403, 404, 422)
_TRANSIENT_STATUSES = (500, 502, 503, 504)


def _status_code_of(exc: BaseException) -> int | None:
    """Extract an HTTP status code if the exception carries one."""
    for attr in ("status_code", "status", "http_status"):
        code = getattr(exc, attr, None)
        if isinstance(code, int):
            return code
    resp = getattr(exc, "response", None)
    if resp is not None:
        code = getattr(resp, "status_code", None)
        if isinstance(code, int):
            return code
    return None


def _message_of(exc: BaseException) -> str:
    msg = getattr(exc, "message", None)
    if isinstance(msg, str) and msg:
        return msg
    return str(exc)


def classify_error(exc: BaseException) -> LLMErrorClass:
    """
    Classify an exception raised by an LLM call into one of
    `LLMErrorClass`. The classifier inspects subclass first, then HTTP
    status code, then message text. Unknown classes default to
    TRANSIENT so the caller still gets a retry.
    """
    if isinstance(exc, LLMParseError):
        return LLMErrorClass.PARSE_ERROR
    if isinstance(exc, LLMRateLimitError):
        return LLMErrorClass.RATE_LIMIT
    if isinstance(exc, LLMTimeoutError):
        return LLMErrorClass.TIMEOUT
    if isinstance(exc, LLMContentViolationError):
        return LLMErrorClass.CONTENT_VIOLATION
    if isinstance(exc, LLMPermanentError):
        return LLMErrorClass.PERMANENT
    if isinstance(exc, LLMTransientError):
        return LLMErrorClass.TRANSIENT
    if isinstance(exc, asyncio.TimeoutError) or isinstance(exc, TimeoutError):
        return LLMErrorClass.TIMEOUT

    # HTTP status-code bucketing.
    status = _status_code_of(exc)
    if status in _RATE_LIMIT_STATUSES:
        return LLMErrorClass.RATE_LIMIT
    if status in _PERMANENT_STATUSES:
        return LLMErrorClass.PERMANENT
    if status in _TRANSIENT_STATUSES:
        return LLMErrorClass.TRANSIENT

    # Message-text heuristics. Keep narrow.
    msg = _message_of(exc).lower()
    if "rate limit" in msg or "too many requests" in msg:
        return LLMErrorClass.RATE_LIMIT
    if "timeout" in msg or "timed out" in msg:
        return LLMErrorClass.TIMEOUT
    if "content policy" in msg or "content_policy" in msg or "content filter" in msg:
        return LLMErrorClass.CONTENT_VIOLATION
    if (
        "insufficient balance" in msg
        or "insufficient quota" in msg
        or "billing" in msg
        or "payment required" in msg
    ):
        return LLMErrorClass.PERMANENT

    # Unknown — treat as transient (caller gets a retry).
    return LLMErrorClass.TRANSIENT


def parse_retry_after(exc: BaseException) -> float:
    """
    Extract a `Retry-After` delay (seconds) from a rate-limit
    exception. Supports integer-seconds or HTTP-date formats via the
    exception's `response.headers`. Falls back to 1.0 when unparseable
    or absent.
    """
    default = 1.0
    resp = getattr(exc, "response", None)
    headers = None
    if resp is not None:
        headers = getattr(resp, "headers", None)
    if headers is None:
        headers = getattr(exc, "headers", None)
    if headers is None:
        return default
    retry_after = None
    try:
        retry_after = headers.get("Retry-After") if hasattr(headers, "get") else None
    except Exception:
        retry_after = None
    if retry_after is None:
        return default
    try:
        return max(0.0, float(retry_after))
    except (TypeError, ValueError):
        pass
    # HTTP-date form (RFC 7231). Not common; ignore for now.
    return default


@dataclass(frozen=True)
class RetryPolicy:
    """Per-error-class retry budget. `max_attempts` is the number of
    RETRIES after the initial call (so `max_attempts=2` means 3 total
    calls). `base_delay` is a starting back-off (seconds). The actual
    delay at attempt N is `base_delay * backoff_multiplier**N`.

    `requires_prompt_change=True` signals to the caller that retrying
    with an unchanged prompt is futile (content violation → reword;
    parse error → append a repair hint)."""
    max_attempts: int
    base_delay: float = 0.0
    backoff_multiplier: float = 1.0
    requires_prompt_change: bool = False

    def delay_for(self, attempt: int) -> float:
        """Delay (seconds) before retry number `attempt` (1-indexed)."""
        if attempt <= 0:
            return 0.0
        return self.base_delay * (self.backoff_multiplier ** (attempt - 1))


RETRY_POLICIES: dict[LLMErrorClass, RetryPolicy] = {
    LLMErrorClass.RATE_LIMIT: RetryPolicy(
        max_attempts=5, base_delay=1.0, backoff_multiplier=2.0,
    ),
    LLMErrorClass.TIMEOUT: RetryPolicy(
        max_attempts=2, base_delay=0.0, backoff_multiplier=2.0,
    ),
    LLMErrorClass.CONTENT_VIOLATION: RetryPolicy(
        max_attempts=0, requires_prompt_change=True,
    ),
    LLMErrorClass.PARSE_ERROR: RetryPolicy(
        max_attempts=1, requires_prompt_change=True,
    ),
    LLMErrorClass.TRANSIENT: RetryPolicy(
        max_attempts=2, base_delay=1.0, backoff_multiplier=2.0,
    ),
    LLMErrorClass.PERMANENT: RetryPolicy(max_attempts=0),
}


def retry_policy_for(exc: BaseException) -> RetryPolicy:
    """Convenience: classify + look up the policy in one call."""
    return RETRY_POLICIES[classify_error(exc)]


def _is_retryable_transport_error(exc: BaseException) -> bool:
    """Do not mistake arbitrary application bugs for provider failures."""
    return isinstance(exc, (LLMError, TimeoutError, asyncio.TimeoutError)) or (
        _status_code_of(exc) is not None
    )


# ---------------------------------------------------------------------
# Per-model-tier timeouts (TK-1 — THINK-DESIGN-AUDIT §4.2)
# ---------------------------------------------------------------------
#
# The default `timeout_s=30` historically used for every LLM call is
# too aggressive for reasoner-class models (DeepSeek-reasoner routinely
# takes 40-60s). Spurious timeouts triggered unnecessary retries, which
# amplified cost and latency. Look up the per-tier timeout by model
# name prefix/substring; `default` applies to any model the map does
# not recognise.
#
# Override via environment variable `LLM_TIMEOUT_OVERRIDE_MS` — useful
# for tests that need a short timeout regardless of model.

MODEL_TIMEOUTS: dict[str, int] = {
    "deepseek-reasoner": 120,
    "deepseek-chat": 45,
    "gpt-5.3-codex": 180,
    "gpt-5.3-codex-spark": 120,
    "gpt-5.5": 180,
    "gpt-5": 180,
    "default": 60,
}


def get_timeout_for_model(model_name: str | None) -> int:
    """
    Return the per-model timeout (seconds).

    Resolution order:
      1. `LLM_TIMEOUT_OVERRIDE_MS` environment variable (milliseconds),
         if set and parseable as a positive number.
      2. Exact match in `MODEL_TIMEOUTS`.
      3. Substring match: any key in `MODEL_TIMEOUTS` that is contained
         in `model_name` (so e.g. `"deepseek-reasoner-v2"` still
         resolves to the reasoner tier).
      4. `MODEL_TIMEOUTS['default']`.
    """
    override_ms = os.environ.get("LLM_TIMEOUT_OVERRIDE_MS")
    if override_ms:
        try:
            ms = float(override_ms)
            if ms > 0:
                # Round up to the nearest second, minimum 1s.
                return max(1, int((ms + 999) // 1000))
        except ValueError:
            pass
    if not model_name:
        return MODEL_TIMEOUTS["default"]
    if model_name in MODEL_TIMEOUTS:
        return MODEL_TIMEOUTS[model_name]
    for key, secs in MODEL_TIMEOUTS.items():
        if key == "default":
            continue
        if key in model_name:
            return secs
    return MODEL_TIMEOUTS["default"]


@dataclass(frozen=True)
class LLMConfig:
    provider: str                  # Fyralis app path uses "codex"
    api_key: str
    model: str
    timeout_s: float = 30.0
    # TK-5: strict-mode makes parse errors rare. One repair attempt
    # (2 total calls) is the tuned default per
    # `RETRY_POLICIES[LLMErrorClass.PARSE_ERROR]`. Callers that need
    # a custom budget still pass `max_retries=` explicitly.
    max_retries: int = 1
    # Optional per-provider override. Currently used by Codex question
    # planning so retrieval can run low effort without changing the main
    # Think provider's global CODEX_REASONING_EFFORT.
    reasoning_effort: str | None = None
    # Optional circuit-breaker identity. Defaults to `provider`, preserving the
    # historical per-provider breaker. Think workers can set lane-specific names
    # such as `codex:t4:repair` so background maintenance cannot open the T1
    # breaker.
    circuit_breaker_name: str | None = None

    @classmethod
    def from_env(cls) -> "LLMConfig":
        raw_provider = os.environ.get("LLM_PROVIDER")
        provider = (raw_provider or "codex").lower()
        if raw_provider is None:
            _config_footgun(
                "LLM_PROVIDER unset — defaulting to 'codex'. Set "
                "LLM_PROVIDER=codex explicitly in deployment env files.",
                provider=provider,
            )
        if provider == "deepseek":
            api_key = (
                os.environ.get("DEEPSEEK_API_KEY", "")
                or os.environ.get("LLM_API_KEY", "")
            )
        elif provider == "anthropic":
            api_key = (
                os.environ.get("ANTHROPIC_API_KEY", "")
                or os.environ.get("LLM_API_KEY", "")
            )
        elif provider == "openai":
            api_key = (
                os.environ.get("OPENAI_API_KEY", "")
                or os.environ.get("LLM_API_KEY", "")
            )
        elif provider == "codex":
            api_key = _codex_api_key_from_env()
        else:
            api_key = os.environ.get("LLM_API_KEY", "")
        model = _model_from_env(provider)
        if provider == "deepseek" and not os.environ.get("LLM_MODEL"):
            # Compatibility-only path. Fyralis production uses Codex, but old
            # harnesses can still opt into DeepSeek; make its implicit model loud.
            _config_footgun(
                "Compatibility-only LLM_PROVIDER=deepseek without LLM_MODEL "
                f"— defaulting to {model!r}. Set LLM_MODEL explicitly.",
                provider=provider, model=model,
            )
        # TK-1: if LLM_TIMEOUT_SECONDS is explicitly set, honour it (back-compat);
        # otherwise derive from per-model tier. `get_timeout_for_model` itself
        # respects LLM_TIMEOUT_OVERRIDE_MS for test forcing.
        explicit = os.environ.get("LLM_TIMEOUT_SECONDS")
        if explicit is not None:
            timeout = float(explicit)
        else:
            timeout = float(get_timeout_for_model(model))
        if provider not in ("anthropic", "openai", "deepseek", "codex"):
            raise LLMConfigError(
                f"unknown LLM_PROVIDER: {provider!r}",
                provider=provider,
            )
        if provider == "codex" and model.startswith(("deepseek-", "claude-")):
            raise LLMConfigError(
                "LLM_PROVIDER=codex requires an OpenAI/Codex model; "
                "set LLM_MODEL=gpt-5.3-codex or CODEX_MODEL",
                provider=provider,
                model=model,
            )
        return cls(
            provider=provider,
            api_key=api_key,
            model=model,
            timeout_s=timeout,
            max_retries=_env_int("LLM_MAX_RETRIES", cls.max_retries, minimum=0),
            reasoning_effort=(
                _codex_reasoning_effort()
                if provider == "codex"
                else None
            ),
        )


def _default_model(provider: str) -> str:
    return {
        "anthropic": "claude-opus-4-7",
        "openai": "gpt-4o",
        "deepseek": "deepseek-reasoner",
        "codex": _codex_default_model(),
    }.get(provider, "claude-opus-4-7")


def _model_from_env(provider: str) -> str:
    if provider != "codex":
        return os.environ.get("LLM_MODEL") or _default_model(provider)
    return _codex_model_from_env()


def _codex_model_from_env() -> str:
    explicit = os.environ.get("CODEX_MODEL")
    if explicit:
        return explicit
    config_model = _codex_config_model()
    # Local Codex transports should behave like the Codex CLI/IDE: the
    # user's ~/.codex/config.toml selects the subscription-backed model.
    # `LLM_MODEL` is a legacy provider-generic knob and can accidentally pin a
    # platform/API model in local dogfood. Use CODEX_MODEL for an explicit
    # Codex override.
    if _codex_transport() in {"app-server", "cli"} and config_model:
        return config_model
    return os.environ.get("LLM_MODEL") or config_model or _codex_default_model()


def _codex_default_model() -> str:
    return "gpt-5.5"


def _codex_home() -> Path:
    raw = os.environ.get("CODEX_HOME")
    if raw:
        return Path(raw).expanduser()
    return Path.home() / ".codex"


def _codex_auth_path() -> Path:
    raw = os.environ.get("CODEX_AUTH_FILE")
    if raw:
        return Path(raw).expanduser()
    return _codex_home() / "auth.json"


def _read_codex_auth_file() -> dict[str, Any]:
    path = _codex_auth_path()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        raise LLMConfigError(
            f"failed to read Codex auth file at {path}: {exc}",
            path=str(path),
        ) from exc
    return data if isinstance(data, dict) else {}


def _codex_api_key_from_env() -> str:
    """Resolve a credential for `LLM_PROVIDER=codex`.

    Order:
      1. Explicit Fyralis/Codex env keys.
      2. OpenAI platform key envs.
      3. `~/.codex/auth.json` API key.
      4. `~/.codex/auth.json` ChatGPT OAuth access token.

    The final OAuth form lets a local dogfood Think worker reuse the
    same Codex login as the CLI. Production should prefer an API key or
    an access token mounted by secret management.
    """
    for name in ("CODEX_API_KEY", "OPENAI_API_KEY", "LLM_API_KEY"):
        value = os.environ.get(name)
        if value:
            return value
    auth = _read_codex_auth_file()
    api_key = auth.get("OPENAI_API_KEY")
    if isinstance(api_key, str) and api_key:
        return api_key
    tokens = auth.get("tokens")
    if isinstance(tokens, dict):
        access_token = tokens.get("access_token")
        if isinstance(access_token, str) and access_token:
            return access_token
    return ""


def _codex_account_id_from_env() -> str | None:
    value = os.environ.get("CODEX_ACCOUNT_ID")
    if value:
        return value
    if any(
        os.environ.get(name)
        for name in ("CODEX_API_KEY", "OPENAI_API_KEY", "LLM_API_KEY")
    ):
        return None
    tokens = _read_codex_auth_file().get("tokens")
    if isinstance(tokens, dict):
        account_id = tokens.get("account_id")
        if isinstance(account_id, str) and account_id:
            return account_id
    return None


def _codex_transport() -> str:
    transport = os.environ.get("CODEX_TRANSPORT", "auto").strip().lower()
    if transport == "appserver":
        transport = "app-server"
    if transport in {"responses", "app-server", "cli"}:
        return transport
    if transport != "auto":
        raise LLMConfigError(
            "CODEX_TRANSPORT must be one of: auto, responses, app-server, cli",
            transport=transport,
        )
    if any(
        os.environ.get(name)
        for name in ("CODEX_API_KEY", "OPENAI_API_KEY", "LLM_API_KEY")
    ):
        return "responses"
    auth = _read_codex_auth_file()
    api_key = auth.get("OPENAI_API_KEY")
    if isinstance(api_key, str) and api_key:
        return "responses"
    tokens = auth.get("tokens")
    if isinstance(tokens, dict):
        access_token = tokens.get("access_token")
        if isinstance(access_token, str) and access_token:
            return "app-server"
    return "responses"


def _codex_should_use_cli_transport() -> bool:
    return _codex_transport() == "cli"


def _codex_reasoning_effort(value: str | None = None) -> str:
    value = (
        value or os.environ.get("CODEX_REASONING_EFFORT", "medium")
    ).strip().lower()
    allowed = {"low", "medium", "high", "xhigh"}
    if value not in allowed:
        raise LLMConfigError(
            "CODEX_REASONING_EFFORT must be one of: low, medium, high, xhigh",
            effort=value,
        )
    return value


def _codex_config_model() -> str | None:
    path = _codex_home() / "config.toml"
    try:
        with path.open("rb") as f:
            data = tomllib.load(f)
    except (FileNotFoundError, OSError, tomllib.TOMLDecodeError):
        return None
    model = data.get("model") if isinstance(data, dict) else None
    return model if isinstance(model, str) and model else None


# ---------------------------------------------------------------------
# Module-level response cache (set by real-LLM test infrastructure).
# When set, structured() routes through it before invoking _raw_call.
# ---------------------------------------------------------------------

_RESPONSE_CACHE: Any = None


def set_response_cache(cache: Any) -> None:
    """Install a response cache; pass None to clear."""
    global _RESPONSE_CACHE
    _RESPONSE_CACHE = cache


def get_response_cache() -> Any:
    """Return the currently-installed response cache, or None."""
    return _RESPONSE_CACHE


# ---------------------------------------------------------------------
# OP-3 follow-up (FU-2) — circuit-breaker opt-out.
#
# Real provider `_raw_call` / `_do_call` methods route through
# `services.reasoning.think.circuit_breaker.get_breaker(name).call(fn)`. If the
# breaker module itself goes wrong in production, set
# `LLM_CIRCUIT_BREAKER_DISABLED=1` to bypass the wrap per call (the
# envelope below becomes a no-op pass-through). Read on every call so
# operators don't need to restart the worker to re-enable protection.
# ---------------------------------------------------------------------


def _circuit_breaker_enabled() -> bool:
    """Returns False when `LLM_CIRCUIT_BREAKER_DISABLED` is set to a
    truthy value (1, true, yes, on). True otherwise — safest default."""
    raw = os.environ.get("LLM_CIRCUIT_BREAKER_DISABLED")
    if raw is None:
        return True
    return raw.strip().lower() not in ("1", "true", "yes", "on", "y", "t")


async def _through_breaker(name: str, fn: Callable[[], Any]) -> Any:
    """Route `fn` through the named provider circuit breaker. Env var
    `LLM_CIRCUIT_BREAKER_DISABLED=1` bypasses the breaker entirely.

    `CircuitOpenError` bypasses the wrapped function (breaker raises it
    before running fn), so it cannot self-count as a provider failure.
    """
    if not _circuit_breaker_enabled():
        return await fn()
    # Lazy import — keeps lib/llm from hard-depending on services/reasoning/think
    # at module import time.
    from services.reasoning.think.circuit_breaker import get_breaker
    return await get_breaker(name).call(fn)


# ---------------------------------------------------------------------
# Provider protocol
# ---------------------------------------------------------------------

class LLMProvider(abc.ABC):
    """
    Provider-agnostic interface. Concrete implementations below wrap
    the Anthropic / OpenAI SDKs. Tests substitute a subclass.
    """

    def __init__(self, config: LLMConfig) -> None:
        self.config = config
        self._usage_aggregator: LLMUsageAggregator | None = None
        self._receipt_sink: LLMReceiptSink | None = None

    # -----------------------------------------------------------------
    # Usage aggregator hook (OP-2) — callers install an aggregator for
    # the duration of a Think run and read totals afterwards.
    # -----------------------------------------------------------------
    def set_usage_aggregator(self, agg: LLMUsageAggregator | None) -> None:
        self._usage_aggregator = agg

    def set_receipt_sink(self, sink: LLMReceiptSink | None) -> None:
        """Install attempt/call telemetry without changing structured() callers."""
        self._receipt_sink = sink

    @property
    def circuit_breaker_name(self) -> str:
        return self.config.circuit_breaker_name or self.config.provider

    # -----------------------------------------------------------------
    # Cost-plan §1.2 — output-schema enforcement capability.
    # -----------------------------------------------------------------
    def enforces_output_schema(self, schema: type[BaseModel]) -> bool:
        """True only when this provider *server-side enforces* `schema` (so
        prose that merely re-enumerates the JSON shape the schema already
        constrains is redundant and can be leaned out of the prompt).

        Default False: most transports (Anthropic, OpenAI Chat, Codex
        Responses/CLI) send the schema as a non-binding hint, so the prose
        contract is load-bearing and must stay. Only DeepSeek strict
        tool-calling overrides this to True."""
        return False

    def _record_usage(
        self,
        input_tokens: int,
        output_tokens: int,
        *,
        cache_read_tokens: int = 0,
        cache_creation_tokens: int = 0,
        usage_exactness: str = "reported",
    ) -> None:
        # Week 5: prefer a task-local aggregator (set via
        # `using_usage_aggregator`) over the instance-wide one so
        # concurrent callers on a shared provider don't race.
        agg = _CURRENT_USAGE_AGG.get() or self._usage_aggregator
        if agg is None:
            return
        cost = compute_cost_usd(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            model_name=self.config.model,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
        )
        agg.record(
            LLMUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_tokens=cache_read_tokens,
                cache_creation_tokens=cache_creation_tokens,
                model_name=self.config.model,
                cost_usd=cost,
                purpose=_CURRENT_USAGE_PURPOSE.get(),
                usage_exactness=usage_exactness,
            )
        )

    def _record_estimated_text_usage(
        self,
        *,
        system: str,
        user: str,
        schema_hint: str,
        content: str,
    ) -> None:
        """Record approximate usage for transports without token metadata."""
        self._record_usage(
            _estimate_tokens_from_text(system, user, schema_hint),
            _estimate_tokens_from_text(content),
            usage_exactness="estimated",
        )

    @abc.abstractmethod
    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        """
        Return the raw string returned by the model. Implementations
        are responsible for transport, auth, and provider-specific
        structured-output coercion (tool use / response_format).
        Failure raises LLMError.
        """
        raise NotImplementedError

    # -----------------------------------------------------------------
    # Public API with retry-on-parse-failure
    # -----------------------------------------------------------------
    async def structured(
        self,
        *,
        system: str,
        user: str,
        schema: type[T],
        temperature: float = 0.0,
        max_tokens: int = 2048,
        logical_call_id: str | None = None,
        context_digest: str | None = None,
        max_attempts: int | None = None,
        deadline_s: float = 240.0,
    ) -> T:
        """
        Invoke the model and return a validated Pydantic instance.
        Retries up to `max_retries` on JSON parse or schema
        validation failure, appending a repair instruction each time.

        If a module-level response cache is installed via
        `set_response_cache`, the raw model JSON is fetched/stored
        through it (keyed on inputs + schema name) and re-validated
        through the schema on hit.
        """
        logical_call_id = logical_call_id or str(uuid.uuid4())
        requested_attempts = (
            max_attempts if max_attempts is not None
            else self.config.max_retries + 1
        )
        attempt_limit = min(3, requested_attempts)
        if attempt_limit < 1:
            raise ValueError("max_attempts must be at least 1")
        if deadline_s <= 0:
            raise ValueError("deadline_s must be positive")
        logical_started = utc_now()
        prompt_digest = canonical_sha256(
            {
                "system": system,
                "user": user,
                "schema": f"{schema.__module__}.{schema.__qualname__}",
                "temperature": temperature,
                "max_tokens": max_tokens,
            }
        )
        physical_attempt_count = 0
        logical_outcome = "success"
        logical_error_class: str | None = None
        logical_error_message: str | None = None
        logical_token = _CURRENT_LOGICAL_CALL_ID.set(logical_call_id)
        count_token = _CURRENT_PHYSICAL_ATTEMPT_COUNT.set(0)
        limit_token = _CURRENT_ATTEMPT_LIMIT.set(attempt_limit)
        deadline_token = _CURRENT_LOGICAL_DEADLINE.set(
            time.monotonic() + min(240.0, deadline_s)
        )
        usage_token = None
        if (
            (_CURRENT_RECEIPT_SINK.get() or self._receipt_sink) is not None
            and (_CURRENT_USAGE_AGG.get() or self._usage_aggregator) is None
        ):
            # Receipt-only callers still need a call-local aggregator so exact
            # transport usage reaches their physical-attempt receipts.
            usage_token = _CURRENT_USAGE_AGG.set(LLMUsageAggregator())
        try:
            cache = get_response_cache()
            cache_fetched = False
            if cache is not None:
                async def _fetch() -> dict[str, Any]:
                    nonlocal cache_fetched
                    cache_fetched = True
                    raw = await self._structured_raw(
                        system=system,
                        user=user,
                        schema=schema,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    )
                    return {"raw": raw}

                entry = await cache.get_or_fetch(
                    system=system,
                    user=user,
                    model=self.config.model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    schema_name=schema.__name__,
                    fetch_fn=_fetch,
                )
                raw_cached = entry["raw"]
                parsed, err = _try_parse(raw_cached, schema)
                if err is not None:
                    raise LLMParseError(
                        f"cached LLM output failed re-validation against "
                        f"{schema.__name__}: {err}",
                        last_raw=raw_cached[:1000],
                        schema=schema.__name__,
                    ) from err
                if not cache_fetched:
                    logical_outcome = "cache_hit"
                return parsed     # type: ignore[return-value]

            raw = await self._structured_raw(
                system=system,
                user=user,
                schema=schema,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            parsed, err = _try_parse(raw, schema)
            if err is not None:
                raise LLMParseError(
                    f"LLM output did not validate: {err}",
                    last_raw=raw[:1000],
                    schema=schema.__name__,
                ) from err
            return parsed     # type: ignore[return-value]
        except asyncio.CancelledError as exc:
            logical_outcome = "timeout"
            logical_error_class = "timeout"
            logical_error_message = str(exc) or "cancelled"
            raise
        except LLMParseError as exc:
            physical_attempt_count = _CURRENT_PHYSICAL_ATTEMPT_COUNT.get()
            logical_outcome = "exhausted" if physical_attempt_count else "parse_failure"
            logical_error_class = LLMErrorClass.PARSE_ERROR.value
            logical_error_message = str(exc)
            raise
        except BaseException as exc:
            error_class = classify_error(exc)
            logical_outcome = (
                "timeout" if error_class is LLMErrorClass.TIMEOUT else "provider_error"
            )
            logical_error_class = error_class.value
            logical_error_message = str(exc)
            raise
        finally:
            physical_attempt_count = _CURRENT_PHYSICAL_ATTEMPT_COUNT.get()
            receipt_sink = _CURRENT_RECEIPT_SINK.get() or self._receipt_sink
            if receipt_sink is not None:
                receipt_sink.record_logical_call(
                    LogicalCallReceipt(
                        logical_call_id=logical_call_id,
                        provider=self.config.provider,
                        model=self.config.model,
                        purpose=_CURRENT_USAGE_PURPOSE.get(),
                        schema_name=schema.__name__,
                        prompt_digest=prompt_digest,
                        started_at=logical_started,
                        ended_at=utc_now(),
                        outcome=logical_outcome,  # type: ignore[arg-type]
                        physical_attempt_count=physical_attempt_count,
                        error_class=logical_error_class,
                        error_message=logical_error_message,
                        context_digest=context_digest,
                    )
                )
            _CURRENT_PHYSICAL_ATTEMPT_COUNT.reset(count_token)
            _CURRENT_LOGICAL_CALL_ID.reset(logical_token)
            _CURRENT_ATTEMPT_LIMIT.reset(limit_token)
            _CURRENT_LOGICAL_DEADLINE.reset(deadline_token)
            if usage_token is not None:
                _CURRENT_USAGE_AGG.reset(usage_token)

    async def _structured_raw(
        self,
        *,
        system: str,
        user: str,
        schema: type[T],
        temperature: float,
        max_tokens: int,
    ) -> str:
        """
        Run the retry-on-parse-failure loop and return the final raw
        string that successfully validated. Separated so the cache can
        store the raw text, re-parse on hit, and avoid re-running the
        model.
        """
        base_user = user
        last_error: Exception | None = None
        repair_note: str | None = None
        schema_hint = _schema_hint(schema)

        previous_attempt_id: str | None = None
        logical_call_id = _CURRENT_LOGICAL_CALL_ID.get() or str(uuid.uuid4())
        attempt_limit = _CURRENT_ATTEMPT_LIMIT.get()
        deadline = _CURRENT_LOGICAL_DEADLINE.get()
        attempts_already = _CURRENT_PHYSICAL_ATTEMPT_COUNT.get()
        attempts_remaining = max(0, attempt_limit - attempts_already)
        for attempt in range(attempts_remaining):
            physical_attempt_id = str(uuid.uuid4())
            _CURRENT_PHYSICAL_ATTEMPT_COUNT.set(
                _CURRENT_PHYSICAL_ATTEMPT_COUNT.get() + 1
            )
            attempt_started = utc_now()
            agg = _CURRENT_USAGE_AGG.get() or self._usage_aggregator
            usage_offset = len(agg.calls) if agg is not None else 0
            user_msg = (
                base_user if repair_note is None
                else f"{base_user}\n\nPrior attempt failed validation. "
                     f"Fix: {repair_note}. Return ONLY valid JSON matching the schema."
            )
            # Cost-plan §0.1: attribute repair re-calls to the 'parse_repair'
            # purpose so they're distinguishable in the cost ledger from the
            # first (ambient-purpose) attempt.
            attempt_purpose = _CURRENT_USAGE_PURPOSE.get()
            if repair_note is not None:
                attempt_purpose = "parse_repair"
            try:
                if repair_note is None:
                    remaining = (
                        max(0.0, deadline - time.monotonic())
                        if deadline is not None else 240.0
                    )
                    async with asyncio.timeout(remaining):
                        raw = await self._raw_call(
                            system=system,
                            user=user_msg,
                            temperature=temperature,
                            max_tokens=max_tokens,
                            schema_hint=schema_hint,
                        )
                else:
                    with using_usage_purpose("parse_repair"):
                        remaining = (
                            max(0.0, deadline - time.monotonic())
                            if deadline is not None else 240.0
                        )
                        async with asyncio.timeout(remaining):
                            raw = await self._raw_call(
                                system=system,
                                user=user_msg,
                                temperature=temperature,
                                max_tokens=max_tokens,
                                schema_hint=schema_hint,
                            )
            except BaseException as exc:
                error_class = (
                    LLMErrorClass.TIMEOUT
                    if isinstance(exc, asyncio.CancelledError)
                    else classify_error(exc)
                )
                self._record_attempt_receipt(
                    physical_attempt_id=physical_attempt_id,
                    logical_call_id=logical_call_id,
                    parent_attempt_id=previous_attempt_id,
                    ordinal=attempts_already + attempt + 1,
                    purpose=attempt_purpose,
                    started_at=attempt_started,
                    outcome=(
                        "timeout"
                        if error_class is LLMErrorClass.TIMEOUT
                        else "provider_error"
                    ),
                    error_class=error_class.value,
                    error_message=str(exc),
                    retry_scheduled=(
                        not isinstance(exc, asyncio.CancelledError)
                        and attempt + 1 < attempts_remaining
                        and _is_retryable_transport_error(exc)
                        and retry_policy_for(exc).max_attempts > 0
                    ),
                    agg=agg,
                    usage_offset=usage_offset,
                )
                if isinstance(exc, asyncio.CancelledError):
                    raise
                policy = retry_policy_for(exc)
                if (
                    not _is_retryable_transport_error(exc)
                    or policy.max_attempts == 0
                    or attempt + 1 >= attempts_remaining
                ):
                    raise
                previous_attempt_id = physical_attempt_id
                delay = policy.delay_for(attempt + 1)
                remaining = (
                    max(0.0, deadline - time.monotonic())
                    if deadline is not None else 240.0
                )
                if delay >= remaining:
                    raise LLMTimeoutError(
                        "logical LLM deadline exhausted before retry",
                        attempts=attempt + 1,
                    ) from exc
                if delay:
                    await asyncio.sleep(delay)
                continue

            _, err = _try_parse(raw, schema)
            if err is None:
                self._record_attempt_receipt(
                    physical_attempt_id=physical_attempt_id,
                    logical_call_id=logical_call_id,
                    parent_attempt_id=previous_attempt_id,
                    ordinal=attempts_already + attempt + 1,
                    purpose=attempt_purpose,
                    started_at=attempt_started,
                    outcome="success",
                    error_class=None,
                    error_message=None,
                    retry_scheduled=False,
                    agg=agg,
                    usage_offset=usage_offset,
                )
                return raw

            last_error = err
            repair_note = str(err)
            retry_scheduled = attempt + 1 < attempts_remaining
            self._record_attempt_receipt(
                physical_attempt_id=physical_attempt_id,
                logical_call_id=logical_call_id,
                parent_attempt_id=previous_attempt_id,
                ordinal=attempts_already + attempt + 1,
                purpose=attempt_purpose,
                started_at=attempt_started,
                outcome="parse_failure",
                error_class=LLMErrorClass.PARSE_ERROR.value,
                error_message=str(err),
                retry_scheduled=retry_scheduled,
                agg=agg,
                usage_offset=usage_offset,
            )
            previous_attempt_id = physical_attempt_id
            if attempt + 1 == attempts_remaining:
                raise LLMParseError(
                    f"LLM output did not validate after "
                    f"{attempt_limit} total attempts: {err}",
                    last_raw=raw[:1000],
                    schema=schema.__name__,
                ) from err

        # Theoretically unreachable.
        raise LLMParseError(
            f"structured() exhausted retries: {last_error}"
        ) from last_error

    def _record_attempt_receipt(
        self,
        *,
        physical_attempt_id: str,
        logical_call_id: str,
        parent_attempt_id: str | None,
        ordinal: int,
        purpose: str,
        started_at: Any,
        outcome: str,
        error_class: str | None,
        error_message: str | None,
        retry_scheduled: bool,
        agg: LLMUsageAggregator | None,
        usage_offset: int,
    ) -> None:
        receipt_sink = _CURRENT_RECEIPT_SINK.get() or self._receipt_sink
        if receipt_sink is None:
            return
        usages = agg.calls[usage_offset:] if agg is not None else []
        receipt_sink.record_attempt(
            PhysicalAttemptReceipt(
                physical_attempt_id=physical_attempt_id,
                logical_call_id=logical_call_id,
                parent_attempt_id=parent_attempt_id,
                provider=self.config.provider,
                model=self.config.model,
                purpose=purpose,
                ordinal=ordinal,
                started_at=started_at,
                ended_at=utc_now(),
                outcome=outcome,  # type: ignore[arg-type]
                error_class=error_class,
                error_message=error_message,
                retry_scheduled=retry_scheduled,
                input_tokens=sum(item.input_tokens for item in usages),
                output_tokens=sum(item.output_tokens for item in usages),
                cache_tokens=sum(
                    item.cache_read_tokens + item.cache_creation_tokens
                    for item in usages
                ),
                cost_usd=sum(item.cost_usd for item in usages),
                usage_exactness=(
                    "unavailable"
                    if not usages
                    else "estimated"
                    if any(item.usage_exactness == "estimated" for item in usages)
                    else "reported"
                ),
            )
        )


def _schema_hint(schema: type[BaseModel]) -> str:
    """
    Compact JSON schema suitable for inline inclusion in a prompt,
    so the model sees the exact shape it must produce.
    """
    raw = schema.model_json_schema()
    # Strip Pydantic-internal noise to keep the prompt short.
    raw.pop("title", None)
    return json.dumps(raw, separators=(",", ":"))


def _try_parse(
    raw: str, schema: type[T]
) -> tuple[T | None, Exception | None]:
    """
    Try to parse `raw` as JSON then validate against `schema`. Also
    handles the case where the LLM wraps the JSON in ```json fences.
    """
    candidates = [raw, _strip_code_fences(raw)]
    seen = set()
    unique = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        unique.append(c)

    last_err: Exception | None = None
    for text in unique:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            last_err = e
            continue
        try:
            return schema.model_validate(data), None
        except PydanticValidationError as e:
            last_err = e
            continue
    return None, last_err


def _strip_code_fences(raw: str) -> str:
    s = raw.strip()
    if s.startswith("```"):
        lines = s.splitlines()
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    return s


# ---------------------------------------------------------------------
# Concrete providers — thin wrappers. Real SDK calls are deferred to
# Wave 3 when Think actually uses them; Wave 0 exercises the retry
# and parse logic through a subclass override in tests.
# ---------------------------------------------------------------------

class AnthropicProvider(LLMProvider):
    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        # Import lazily so tests that never instantiate this class
        # don't pay the SDK import cost.
        import anthropic

        if not self.config.api_key:
            raise LLMConfigError("LLM_API_KEY is empty")

        client = anthropic.AsyncAnthropic(
            api_key=self.config.api_key,
            timeout=self.config.timeout_s,
            max_retries=0,
        )
        system_full = (
            f"{system}\n\nYou MUST respond with a single JSON object "
            f"matching this schema:\n{schema_hint}\n"
            f"Do not include any prose outside the JSON."
        )

        async def _do_call() -> Any:
            return await client.messages.create(
                model=self.config.model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system_full,
                messages=[{"role": "user", "content": user}],
            )

        # OP-3: circuit breaker — fast-fail on open for the 'anthropic' provider.
        # FU-2: `LLM_CIRCUIT_BREAKER_DISABLED=1` bypasses the wrap.
        response = await _through_breaker(self.circuit_breaker_name, _do_call)
        if not response.content:
            raise LLMError("empty response from anthropic", model=self.config.model)
        # OP-2: record input/output tokens if an aggregator is installed.
        inp, outp, cache_read, cache_creation = _extract_anthropic_usage(response)
        self._record_usage(
            inp, outp,
            cache_read_tokens=cache_read,
            cache_creation_tokens=cache_creation,
        )
        # Concatenate text blocks.
        return "".join(
            getattr(b, "text", "") for b in response.content
            if getattr(b, "type", None) == "text"
        )


class OpenAIProvider(LLMProvider):
    # OpenAI-compatible provider; subclasses may override `base_url`.
    base_url: str | None = None

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        import openai

        if not self.config.api_key:
            raise LLMConfigError("LLM_API_KEY is empty")

        client_kwargs: dict[str, Any] = {
            "api_key": self.config.api_key,
            "timeout": self.config.timeout_s,
            "max_retries": 0,
        }
        if self.base_url:
            client_kwargs["base_url"] = self.base_url
        client = openai.AsyncOpenAI(**client_kwargs)
        # Week 5 stabilization: only request JSON-mode output when the
        # caller actually supplied a schema hint (structured() path).
        # Rendering service calls `_raw_call` with `schema_hint=""` and
        # expects raw HTML prose; forcing `response_format=json_object`
        # caused DeepSeek-chat to wrap prose as `{"greeting_html":"..."}`.
        # Think's path (via `structured()` → `_structured_raw`) still
        # supplies a non-empty schema_hint, so JSON mode is preserved
        # there. Narrow guard, does not change behavior for schema-ful
        # callers.
        if schema_hint:
            system_full = (
                f"{system}\n\nRespond with a single JSON object matching "
                f"this schema:\n{schema_hint}"
            )
            call_kwargs: dict[str, Any] = {"response_format": {"type": "json_object"}}
        else:
            system_full = system
            call_kwargs = {}

        async def _do_call() -> Any:
            return await client.chat.completions.create(
                model=self.config.model,
                max_tokens=max_tokens,
                temperature=temperature,
                messages=[
                    {"role": "system", "content": system_full},
                    {"role": "user", "content": user},
                ],
                **call_kwargs,
            )

        # OP-3: circuit breaker — fast-fail on open.
        # FU-2: `LLM_CIRCUIT_BREAKER_DISABLED=1` bypasses the wrap.
        response = await _through_breaker(self.circuit_breaker_name, _do_call)
        content = response.choices[0].message.content
        if not content:
            raise LLMError("empty response from openai", model=self.config.model)
        # OP-2: record input/output tokens if an aggregator is installed.
        inp, outp, cache_read, cache_creation = _extract_openai_usage(response)
        self._record_usage(
            inp, outp,
            cache_read_tokens=cache_read,
            cache_creation_tokens=cache_creation,
        )
        return content


class CodexProvider(LLMProvider):
    """Codex / GPT-5-family provider.

    API-key auth uses OpenAI's Responses API. Local ChatGPT/Codex auth
    uses a persistent `codex app-server`, because those access tokens are
    valid for Codex but may not carry direct `api.responses.write` scope.
    """

    async def _raw_call(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        transport = _codex_transport()
        if transport == "app-server":
            return await self._raw_call_app_server(
                system=system,
                user=user,
                temperature=temperature,
                max_tokens=max_tokens,
                schema_hint=schema_hint,
            )
        if transport == "cli":
            return await self._raw_call_cli(
                system=system,
                user=user,
                temperature=temperature,
                max_tokens=max_tokens,
                schema_hint=schema_hint,
            )
        return await self._raw_call_responses(
            system=system,
            user=user,
            temperature=temperature,
            max_tokens=max_tokens,
            schema_hint=schema_hint,
        )

    async def _raw_call_responses(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        import openai

        if not self.config.api_key:
            raise LLMConfigError(
                "Codex auth is missing; set CODEX_API_KEY, OPENAI_API_KEY, "
                "LLM_API_KEY, or run `codex login` so ~/.codex/auth.json exists"
            )

        client_kwargs: dict[str, Any] = {
            "api_key": self.config.api_key,
            "timeout": self.config.timeout_s,
            "max_retries": 0,
        }
        account_id = _codex_account_id_from_env()
        if account_id:
            client_kwargs["default_headers"] = {
                "ChatGPT-Account-Id": account_id,
            }

        client = openai.AsyncOpenAI(**client_kwargs)
        system_full = system
        call_kwargs: dict[str, Any] = {}
        if schema_hint:
            system_full = (
                f"{system}\n\nRespond with a single JSON object matching "
                f"this schema:\n{schema_hint}"
            )
            call_kwargs["text"] = {
                "format": {
                    "type": "json_schema",
                    "name": "fyralis_structured_response",
                    "schema": json.loads(schema_hint),
                    # Keep validation client-side too. Some Think schemas
                    # contain flexible unions that are safer as non-strict
                    # Responses JSON schema hints.
                    "strict": False,
                }
            }
        if self.config.reasoning_effort:
            call_kwargs["reasoning"] = {
                "effort": _codex_reasoning_effort(self.config.reasoning_effort)
            }

        async def _do_call() -> Any:
            return await client.responses.create(
                model=self.config.model,
                instructions=system_full,
                input=user,
                max_output_tokens=max_tokens,
                temperature=temperature,
                **call_kwargs,
            )

        response = await _through_breaker(self.circuit_breaker_name, _do_call)
        inp, outp, cache_read, cache_creation = _extract_openai_usage(response)
        self._record_usage(
            inp, outp,
            cache_read_tokens=cache_read,
            cache_creation_tokens=cache_creation,
        )
        content = _responses_output_text(response)
        if not content:
            raise LLMError("empty response from codex", model=self.config.model)
        return content

    async def _raw_call_app_server(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        del temperature, max_tokens  # The app-server turn owns these controls.

        client = _codex_app_server_client()
        content, usage = await _through_breaker(
            self.circuit_breaker_name,
            lambda: client.call(
                model=self.config.model,
                system=system,
                user=user,
                schema_hint=schema_hint,
                timeout_s=self.config.timeout_s,
                reasoning_effort=self.config.reasoning_effort,
            ),
        )
        if usage:
            self._record_usage(
                usage["input_tokens"], usage["output_tokens"],
                cache_read_tokens=usage.get("cached_input_tokens", 0),
                usage_exactness="reported",
            )
        else:
            self._record_estimated_text_usage(
                system=system,
                user=user,
                schema_hint=schema_hint,
                content=content,
            )
        return content

    async def _raw_call_cli(
        self,
        *,
        system: str,
        user: str,
        temperature: float,
        max_tokens: int,
        schema_hint: str,
    ) -> str:
        del temperature, max_tokens  # The CLI owns these controls.

        prompt = (
            "You are serving as the Fyralis structured LLM provider.\n"
            "Follow the system instructions exactly, answer only the user "
            "request, and do not mention this wrapper.\n\n"
            "<system>\n"
            f"{system}\n"
            "</system>\n\n"
            "<user>\n"
            f"{user}\n"
            "</user>\n"
        )
        if schema_hint:
            prompt += (
                "\nYour final answer must be a single JSON object matching "
                "this JSON schema. Do not wrap it in Markdown.\n"
                f"{schema_hint}\n"
            )

        async def _do_call() -> str:
            with tempfile.TemporaryDirectory(prefix="fyralis-codex-") as tmp:
                del tmp
                args = [
                    "codex",
                    "--ask-for-approval",
                    "never",
                    "exec",
                    "--ephemeral",
                    "--ignore-user-config",
                    "--ignore-rules",
                    "--sandbox",
                    "read-only",
                    "--model",
                    self.config.model,
                    "--json",
                ]
                args.append("-")

                try:
                    proc = await asyncio.create_subprocess_exec(
                        *args,
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                    )
                except FileNotFoundError as exc:
                    raise LLMConfigError(
                        "Codex CLI not found; install Codex or set "
                        "CODEX_TRANSPORT=responses with a platform API key"
                    ) from exc

                try:
                    stdout, stderr = await asyncio.wait_for(
                        proc.communicate(prompt.encode("utf-8")),
                        timeout=self.config.timeout_s,
                    )
                except TimeoutError as exc:
                    proc.kill()
                    await proc.communicate()
                    raise LLMError(
                        f"codex cli timed out after {self.config.timeout_s:.0f}s",
                        model=self.config.model,
                    ) from exc

                if proc.returncode != 0:
                    combined = (stderr + stdout).decode("utf-8", errors="replace")
                    raise LLMError(
                        f"codex cli exited {proc.returncode}: {combined[-1200:]}",
                        model=self.config.model,
                    )

                content, usage = _codex_cli_json_result(stdout)
                if not content:
                    raise LLMError("empty response from codex cli", model=self.config.model)
                if not usage:
                    raise LLMError(
                        "codex cli completed without reported token usage",
                        model=self.config.model,
                    )
                self._record_usage(
                    usage.get("input_tokens", 0), usage.get("output_tokens", 0),
                    cache_read_tokens=usage.get("cached_input_tokens", 0),
                    usage_exactness="reported",
                )
                return content

        content = await _through_breaker(self.circuit_breaker_name, _do_call)
        return content


def _codex_cli_json_result(stdout: bytes) -> tuple[str, dict[str, int]]:
    """Extract final agent text and exact provider-reported CLI usage."""

    messages: list[str] = []
    usage: dict[str, int] = {}
    for raw in stdout.splitlines():
        try:
            event = json.loads(raw)
        except json.JSONDecodeError:
            continue
        item = event.get("item") or {}
        if event.get("type") == "item.completed" and item.get("type") == "agent_message":
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                messages.append(text.strip())
        reported = event.get("usage")
        if event.get("type") == "turn.completed" and isinstance(reported, dict):
            usage = {
                key: int(reported[key])
                for key in ("input_tokens", "output_tokens", "cached_input_tokens")
                if isinstance(reported.get(key), int)
            }
    return (messages[-1] if messages else ""), usage


def _responses_output_text(response: Any) -> str:
    direct = getattr(response, "output_text", None)
    if isinstance(direct, str) and direct:
        return direct

    chunks: list[str] = []
    for item in getattr(response, "output", None) or []:
        for content in getattr(item, "content", None) or []:
            text = getattr(content, "text", None)
            if isinstance(text, str):
                chunks.append(text)
                continue
            if isinstance(content, dict):
                text = content.get("text")
                if isinstance(text, str):
                    chunks.append(text)
    return "".join(chunks)


class _CodexAppServerClient:
    """Small stdio JSON-RPC client for `codex app-server`.

    This is intentionally narrow: Fyralis only needs text-in/text-out
    structured reasoning, so we keep one managed app-server process alive
    and serialize turns through it.
    """

    def __init__(self) -> None:
        self._proc: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task | None = None
        self._stderr_tail: list[str] = []
        self._next_id = 1
        self._pending: dict[int, str] = {}
        self._lock = asyncio.Lock()

    async def call(
        self,
        *,
        model: str,
        system: str,
        user: str,
        schema_hint: str,
        timeout_s: float,
        reasoning_effort: str | None,
    ) -> tuple[str, dict[str, int]]:
        async with self._lock:
            try:
                return await asyncio.wait_for(
                    self._call_unlocked(
                        model=model,
                        system=system,
                        user=user,
                        schema_hint=schema_hint,
                        reasoning_effort=reasoning_effort,
                    ),
                    timeout=timeout_s,
                )
            except TimeoutError as exc:
                await self._restart()
                raise LLMError(
                    f"codex app-server timed out after {timeout_s:.0f}s",
                    model=model,
                ) from exc
            except LLMError as exc:
                if _codex_app_server_error_requires_restart(exc):
                    await self._restart()
                raise

    async def _call_unlocked(
        self,
        *,
        model: str,
        system: str,
        user: str,
        schema_hint: str,
        reasoning_effort: str | None,
    ) -> tuple[str, dict[str, int]]:
        await self._ensure_started()
        assert self._proc is not None

        thread = await self._request(
            "thread/start",
            {
                "ephemeral": True,
                "model": model,
                "cwd": os.getcwd(),
                "approvalPolicy": "never",
                "sandbox": "read-only",
                "developerInstructions": _codex_system_prompt(system),
                "baseInstructions": None,
            },
        )
        thread_id = _codex_json_path(thread, "thread", "id")
        if not isinstance(thread_id, str) or not thread_id:
            raise LLMError(
                f"codex app-server returned invalid thread/start response: {thread!r}",
                model=model,
            )

        turn = await self._request(
            "turn/start",
            {
                "threadId": thread_id,
                "input": [
                    {
                        "type": "text",
                        "text": _codex_user_prompt(user, schema_hint),
                    }
                ],
                "model": model,
                "approvalPolicy": "never",
                "sandboxPolicy": {"type": "readOnly"},
                "effort": _codex_reasoning_effort(reasoning_effort),
                "summary": "none",
            },
        )
        turn_id = _codex_json_path(turn, "turn", "id")
        if not isinstance(turn_id, str) or not turn_id:
            raise LLMError(
                f"codex app-server returned invalid turn/start response: {turn!r}",
                model=model,
            )

        content, usage = await self._wait_for_turn(turn_id=turn_id, model=model)
        if not content:
            raise LLMError("empty response from codex app-server", model=model)
        return content, usage

    async def _ensure_started(self) -> None:
        if self._proc is not None and self._proc.returncode is None:
            return
        self._proc = await asyncio.create_subprocess_exec(
            "codex",
            "app-server",
            "--listen",
            "stdio://",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=_codex_app_server_stream_limit(),
        )
        self._stderr_tail.clear()
        self._stderr_task = asyncio.create_task(self._drain_stderr())
        await self._request(
            "initialize",
            {
                "capabilities": {
                    "optOutNotificationMethods": ["item/agentMessage/delta"],
                },
                "clientInfo": {
                    "name": "fyralis",
                    "title": "Fyralis",
                    "version": "0.1",
                },
            },
        )
        await self._notify("initialized", {})

    async def _restart(self) -> None:
        proc = self._proc
        self._proc = None
        self._pending.clear()
        if proc is not None and proc.returncode is None:
            proc.kill()
            try:
                await proc.communicate()
            except Exception:
                pass
        if self._stderr_task is not None:
            self._stderr_task.cancel()
            self._stderr_task = None

    async def _request(self, method: str, params: dict[str, Any]) -> Any:
        request_id = await self._send(method, params, notify=False)
        while True:
            message = await self._read_message()
            if message.get("id") != request_id:
                continue
            self._pending.pop(request_id, None)
            if "error" in message:
                raise LLMError(
                    f"codex app-server {method} failed: {message['error']!r}"
                )
            return message.get("result")

    async def _notify(self, method: str, params: dict[str, Any]) -> None:
        await self._send(method, params, notify=True)

    async def _send(
        self,
        method: str,
        params: dict[str, Any],
        *,
        notify: bool,
    ) -> int:
        assert self._proc is not None
        if self._proc.stdin is None:
            raise LLMError("codex app-server stdin is unavailable")
        message: dict[str, Any] = {"method": method, "params": params}
        request_id = 0
        if not notify:
            request_id = self._next_id
            self._next_id += 1
            message["id"] = request_id
            self._pending[request_id] = method
        self._proc.stdin.write((json.dumps(message) + "\n").encode("utf-8"))
        await self._proc.stdin.drain()
        return request_id

    async def _read_message(self) -> dict[str, Any]:
        assert self._proc is not None
        if self._proc.stdout is None:
            raise LLMError("codex app-server stdout is unavailable")
        line = await self._proc.stdout.readline()
        if not line:
            stderr = "\n".join(self._stderr_tail[-20:])
            await self._restart()
            raise LLMError(f"codex app-server exited unexpectedly: {stderr}")
        try:
            message = json.loads(line)
        except json.JSONDecodeError as exc:
            raise LLMError(
                f"codex app-server returned non-JSON line: {line[:500]!r}"
            ) from exc
        return message if isinstance(message, dict) else {}

    async def _wait_for_turn(
        self, *, turn_id: str, model: str,
    ) -> tuple[str, dict[str, int]]:
        final_text = ""
        usage: dict[str, int] = {}
        while True:
            message = await self._read_message()
            if "id" in message:
                self._pending.pop(int(message["id"]), None)
                continue
            method = message.get("method")
            params = message.get("params") or {}
            if method == "item/completed":
                item = params.get("item") if isinstance(params, dict) else None
                if isinstance(item, dict) and item.get("type") == "agentMessage":
                    text = item.get("text")
                    if isinstance(text, str) and text:
                        final_text = text
            elif method == "thread/tokenUsage/updated":
                if params.get("turnId") not in {None, turn_id}:
                    continue
                token_usage = params.get("tokenUsage")
                last = token_usage.get("last") if isinstance(token_usage, dict) else None
                if isinstance(last, dict):
                    usage = {
                        "input_tokens": int(last["inputTokens"]),
                        "output_tokens": int(last["outputTokens"]),
                        "cached_input_tokens": int(last["cachedInputTokens"]),
                    } if all(
                        isinstance(last.get(key), int)
                        for key in ("inputTokens", "outputTokens", "cachedInputTokens")
                    ) else {}
            elif method == "turn/completed":
                turn = params.get("turn") if isinstance(params, dict) else None
                if isinstance(turn, dict) and turn.get("id") not in {None, turn_id}:
                    continue
                status = turn.get("status") if isinstance(turn, dict) else None
                if status not in {None, "completed"}:
                    raise LLMError(
                        f"codex app-server turn ended with status {status!r}",
                        model=model,
                    )
                return final_text.strip(), usage

    async def _drain_stderr(self) -> None:
        proc = self._proc
        if proc is None or proc.stderr is None:
            return
        while True:
            line = await proc.stderr.readline()
            if not line:
                return
            self._stderr_tail.append(line.decode("utf-8", errors="replace").strip())
            if len(self._stderr_tail) > 50:
                del self._stderr_tail[:25]


def _codex_app_server_error_requires_restart(exc: BaseException) -> bool:
    msg = _message_of(exc).lower()
    if "codex app-server" not in msg:
        return False
    restart_markers = (
        "turn ended with status",
        "exited unexpectedly",
        "returned non-json",
        "stdin is unavailable",
        "stdout is unavailable",
        "returned invalid thread/start response",
        "returned invalid turn/start response",
        "empty response from codex app-server",
    )
    return any(marker in msg for marker in restart_markers)


def _codex_app_server_stream_limit() -> int:
    raw = os.environ.get("CODEX_APP_SERVER_STREAM_LIMIT_BYTES")
    if raw:
        return max(65_536, int(raw))
    return 16 * 1024 * 1024


_CODEX_APP_SERVER_CLIENT: _CodexAppServerClient | None = None
_CODEX_APP_SERVER_LOOP: asyncio.AbstractEventLoop | None = None


def _codex_app_server_client() -> _CodexAppServerClient:
    global _CODEX_APP_SERVER_CLIENT, _CODEX_APP_SERVER_LOOP
    loop = asyncio.get_running_loop()
    if _CODEX_APP_SERVER_CLIENT is None or _CODEX_APP_SERVER_LOOP is not loop:
        # The app-server client owns asyncio locks, subprocess transports,
        # stdout/stderr readers, and background tasks. Those are bound to
        # the loop that creates them, so a module-global client cannot be
        # reused safely across pytest-asyncio's per-test loops or any
        # multi-loop host process.
        _CODEX_APP_SERVER_CLIENT = _CodexAppServerClient()
        _CODEX_APP_SERVER_LOOP = loop
    return _CODEX_APP_SERVER_CLIENT


async def close_codex_app_server_client() -> None:
    """Close the current loop's Codex app-server client, if one exists.

    Primarily used by live LLM tests so each pytest event loop tears down
    its subprocess before the next test starts on a fresh loop.
    """
    global _CODEX_APP_SERVER_CLIENT, _CODEX_APP_SERVER_LOOP
    client = _CODEX_APP_SERVER_CLIENT
    _CODEX_APP_SERVER_CLIENT = None
    _CODEX_APP_SERVER_LOOP = None
    if client is not None:
        await client._restart()


def _codex_system_prompt(system: str) -> str:
    return (
        "You are serving as the Fyralis structured LLM provider. Follow "
        "the system instructions exactly, answer only the user request, "
        "and do not mention this wrapper.\n\n"
        "<system>\n"
        f"{system}\n"
        "</system>"
    )


def _codex_user_prompt(user: str, schema_hint: str) -> str:
    prompt = f"<user>\n{user}\n</user>"
    if schema_hint:
        prompt += (
            "\n\nYour final answer must be a single JSON object matching "
            "this JSON schema. Do not wrap it in Markdown.\n"
            f"{schema_hint}"
        )
    return prompt


def _codex_json_path(obj: Any, *path: str) -> Any:
    cur = obj
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _codex_cli_stdout_fallback(stdout: bytes) -> str:
    lines = [
        line.strip()
        for line in stdout.decode("utf-8", errors="replace").splitlines()
        if line.strip()
    ]
    skip = {"tokens used", "codex"}
    for line in reversed(lines):
        lower = line.lower()
        if lower in skip:
            continue
        if lower.startswith(("openai codex", "workdir:", "model:", "provider:")):
            continue
        if lower.startswith(("approval:", "sandbox:", "reasoning ", "session id:")):
            continue
        if line.replace(",", "").isdigit():
            continue
        return line
    return ""


class DeepSeekProvider(OpenAIProvider):
    """OpenAI-API-compatible provider targeting the DeepSeek endpoint.

    Overrides `_structured_raw` to use DeepSeek's strict tool-calling
    mode on `/beta` for schemas with a registered strict variant. This
    constrains the decoder server-side, eliminating the schema-mismatch
    failures observed with plain `response_format: json_object`.
    """
    base_url = "https://api.deepseek.com"
    strict_base_url = "https://api.deepseek.com"

    def enforces_output_schema(self, schema: type[BaseModel]) -> bool:
        """Cost-plan §1.2: DeepSeek strict tool-calling constrains the decoder
        server-side for schemas with a registered strict variant — the only
        path where the JSON-shape prose is redundant. Mirrors the gate in
        `_structured_raw`."""
        return (
            _strict_schema_for(schema) is not None
            and _deepseek_supports_strict_tool_calling(self.config.model)
        )

    async def _structured_raw(
        self,
        *,
        system: str,
        user: str,
        schema: type[T],
        temperature: float,
        max_tokens: int,
    ) -> str:
        strict_schema = _strict_schema_for(schema)
        if (
            strict_schema is None
            or not _deepseek_supports_strict_tool_calling(self.config.model)
        ):
            return await super()._structured_raw(
                system=system, user=user, schema=schema,
                temperature=temperature, max_tokens=max_tokens,
            )

        import openai

        if not self.config.api_key:
            raise LLMConfigError("LLM_API_KEY is empty")

        tool_name = f"emit_{schema.__name__.lower()}"
        client = openai.AsyncOpenAI(
            api_key=self.config.api_key,
            timeout=self.config.timeout_s,
            base_url=self.strict_base_url,
            max_retries=0,
        )

        last_error: Exception | None = None
        repair_note: str | None = None
        base_user = user
        logical_call_id = _CURRENT_LOGICAL_CALL_ID.get() or str(uuid.uuid4())
        previous_attempt_id: str | None = None
        attempt_limit = _CURRENT_ATTEMPT_LIMIT.get()
        # Reserve the final physical slot for ordinary JSON mode when strict
        # decoding itself is what fails. The fallback consumes the same shared
        # counter; it is not an additional retry domain.
        strict_attempt_limit = attempt_limit if attempt_limit == 1 else attempt_limit - 1
        for attempt in range(strict_attempt_limit):
            physical_attempt_id = str(uuid.uuid4())
            _CURRENT_PHYSICAL_ATTEMPT_COUNT.set(
                _CURRENT_PHYSICAL_ATTEMPT_COUNT.get() + 1
            )
            attempt_started = utc_now()
            agg = _CURRENT_USAGE_AGG.get() or self._usage_aggregator
            usage_offset = len(agg.calls) if agg is not None else 0
            attempt_purpose = (
                _CURRENT_USAGE_PURPOSE.get() if attempt == 0 else "parse_repair"
            )
            user_msg = (
                base_user if repair_note is None
                else f"{base_user}\n\nPrior attempt failed validation. "
                     f"Fix: {repair_note}."
            )
            async def _do_call() -> Any:
                return await client.chat.completions.create(
                    model=self.config.model,
                    max_tokens=max_tokens,
                    temperature=temperature,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user_msg},
                    ],
                    tools=[{
                        "type": "function",
                        "function": {
                            "name": tool_name,
                            "description": f"Return a {schema.__name__}.",
                            "strict": True,
                            "parameters": strict_schema,
                        },
                    }],
                    tool_choice={"type": "function", "function": {"name": tool_name}},
                )

            # OP-3: circuit breaker on the DeepSeek endpoint.
            # FU-2: `LLM_CIRCUIT_BREAKER_DISABLED=1` bypasses the wrap.
            try:
                deadline = _CURRENT_LOGICAL_DEADLINE.get()
                remaining = (
                    max(0.0, deadline - time.monotonic())
                    if deadline is not None else 240.0
                )
                async with asyncio.timeout(remaining):
                    response = await _through_breaker(self.circuit_breaker_name, _do_call)
            except BaseException as exc:
                error_class = (
                    LLMErrorClass.TIMEOUT
                    if isinstance(exc, asyncio.CancelledError)
                    else classify_error(exc)
                )
                self._record_attempt_receipt(
                    physical_attempt_id=physical_attempt_id,
                    logical_call_id=logical_call_id,
                    parent_attempt_id=previous_attempt_id,
                    ordinal=attempt + 1,
                    purpose=attempt_purpose,
                    started_at=attempt_started,
                    outcome=(
                        "timeout"
                        if error_class is LLMErrorClass.TIMEOUT
                        else "provider_error"
                    ),
                    error_class=error_class.value,
                    error_message=str(exc),
                    retry_scheduled=(
                        not isinstance(exc, asyncio.CancelledError)
                        and _is_retryable_transport_error(exc)
                        and retry_policy_for(exc).max_attempts > 0
                        and attempt + 1 < strict_attempt_limit
                    ),
                    agg=agg,
                    usage_offset=usage_offset,
                )
                if isinstance(exc, asyncio.CancelledError):
                    raise
                policy = retry_policy_for(exc)
                if (
                    not _is_retryable_transport_error(exc)
                    or policy.max_attempts == 0
                    or attempt + 1 >= strict_attempt_limit
                ):
                    raise
                previous_attempt_id = physical_attempt_id
                delay = policy.delay_for(attempt + 1)
                deadline = _CURRENT_LOGICAL_DEADLINE.get()
                remaining = (
                    max(0.0, deadline - time.monotonic())
                    if deadline is not None else 240.0
                )
                if delay >= remaining:
                    raise LLMTimeoutError(
                        "logical LLM deadline exhausted before strict retry",
                        attempts=attempt + 1,
                    ) from exc
                if delay:
                    await asyncio.sleep(delay)
                continue
            choice = response.choices[0]
            tool_calls = getattr(choice.message, "tool_calls", None) or []
            if not tool_calls:
                retry_scheduled = attempt + 1 < strict_attempt_limit
                self._record_attempt_receipt(
                    physical_attempt_id=physical_attempt_id,
                    logical_call_id=logical_call_id,
                    parent_attempt_id=previous_attempt_id,
                    ordinal=attempt + 1,
                    purpose=attempt_purpose,
                    started_at=attempt_started,
                    outcome="provider_error",
                    error_class=LLMErrorClass.TRANSIENT.value,
                    error_message="deepseek strict mode returned no tool_calls",
                    retry_scheduled=retry_scheduled,
                    agg=agg,
                    usage_offset=usage_offset,
                )
                if retry_scheduled:
                    previous_attempt_id = physical_attempt_id
                    await asyncio.sleep(
                        RETRY_POLICIES[LLMErrorClass.TRANSIENT].delay_for(attempt + 1)
                    )
                    continue
                raise LLMError(
                    "deepseek strict mode returned no tool_calls",
                    model=self.config.model,
                )
            # OP-2: record usage (strict-mode responses carry the same usage block).
            inp, outp, cache_read, cache_creation = _extract_openai_usage(response)
            self._record_usage(
                inp, outp,
                cache_read_tokens=cache_read,
                cache_creation_tokens=cache_creation,
            )
            raw = tool_calls[0].function.arguments
            # DeepSeek strict mode occasionally drops the closing quote
            # on a key, producing `"key: value` instead of `"key": value`.
            # Try a repair pass before declaring failure.
            repaired = _repair_deepseek_strict_json(raw)
            _, err = _try_parse(repaired, schema)
            if err is None:
                self._record_attempt_receipt(
                    physical_attempt_id=physical_attempt_id,
                    logical_call_id=logical_call_id,
                    parent_attempt_id=previous_attempt_id,
                    ordinal=attempt + 1,
                    purpose=attempt_purpose,
                    started_at=attempt_started,
                    outcome="success",
                    error_class=None,
                    error_message=None,
                    retry_scheduled=False,
                    agg=agg,
                    usage_offset=usage_offset,
                )
                return repaired
            last_error = err
            repair_note = str(err)
            self._record_attempt_receipt(
                physical_attempt_id=physical_attempt_id,
                logical_call_id=logical_call_id,
                parent_attempt_id=previous_attempt_id,
                ordinal=attempt + 1,
                purpose=attempt_purpose,
                started_at=attempt_started,
                outcome="parse_failure",
                error_class=LLMErrorClass.PARSE_ERROR.value,
                error_message=str(err),
                retry_scheduled=True,
                agg=agg,
                usage_offset=usage_offset,
            )
            previous_attempt_id = physical_attempt_id
            if attempt + 1 == strict_attempt_limit:
                # Strict function-calling is usually tighter, but live
                # DeepSeek-chat can still return syntactically malformed
                # tool arguments after repair. Use ordinary JSON mode as
                # a rescue path before failing the whole reasoning run.
                fallback_user = (
                    f"{base_user}\n\nPrior strict tool-call output failed "
                    f"validation. Return ordinary JSON matching the schema. "
                    f"Validation error: {err}."
                )
                if attempt_limit > 1:
                    return await super()._structured_raw(
                        system=system,
                        user=fallback_user,
                        schema=schema,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    )
                raise LLMParseError(
                    "DeepSeek strict output failed validation and exhausted "
                    "the one-attempt budget",
                    last_raw=repaired[:1000],
                    schema=schema.__name__,
                ) from err

        raise LLMParseError(
            f"DeepSeek strict-mode exhausted retries: {last_error}"
        ) from last_error


def _repair_deepseek_strict_json(text: str) -> str:
    """Patch the known DeepSeek strict-mode bug of missing closing quote on keys.

    Pattern: `"key: value` -> `"key": value`. Conservative regex: only
    repairs object keys at `{` or `,` boundaries, and leaves already
    valid JSON untouched so string values like `"owner: alice"` survive.
    """
    import json
    import re
    try:
        json.loads(text)
        return text
    except Exception:
        pass
    return re.sub(
        r'(?P<prefix>[{,]\s*)"(?P<key>[A-Za-z_][A-Za-z0-9_]*):\s',
        r'\g<prefix>"\g<key>": ',
        text,
    )


def _deepseek_supports_strict_tool_calling(model_name: str | None) -> bool:
    """DeepSeek reasoner rejects tool_choice/tool-calling requests.

    Keep strict tool-calling for chat-class models where it constrains
    RawDiff shape server-side, but route reasoner-class models through
    JSON-mode structured output instead. This lets operators choose
    reasoner for deeper cognition without tripping a provider-level
    400 before Think ever reaches validation.
    """
    if not model_name:
        return True
    return "reasoner" not in model_name.lower()


def _strict_schema_for(schema: type[BaseModel]) -> dict | None:
    """Return the registered strict-mode schema for a Pydantic class, or None."""
    try:
        from services.reasoning.think.diff_schema import (
            RawDiff,
            RawDiffClaimsOnly,
            ValidatedDiff,
        )
        from services.reasoning.think.strict_schema import (
            RAW_DIFF_CLAIMS_ONLY_STRICT_SCHEMA,
            RAW_DIFF_STRICT_SCHEMA,
        )
    except ImportError:
        return None
    if schema is RawDiffClaimsOnly:
        return RAW_DIFF_CLAIMS_ONLY_STRICT_SCHEMA
    if schema is RawDiff or schema is ValidatedDiff:
        return RAW_DIFF_STRICT_SCHEMA
    return None


def build_provider(config: LLMConfig | None = None) -> LLMProvider:
    cfg = config or LLMConfig.from_env()
    # Cost-plan §3.1: log the resolved provider/model + its pricing so a
    # misconfigured (e.g. accidentally-Opus) run is visible at startup instead
    # of only showing up as a cost spike days later.
    _log.info(
        "llm.provider_initialized",
        provider=cfg.provider,
        model=cfg.model,
        circuit_breaker_name=cfg.circuit_breaker_name or cfg.provider,
        pricing=get_pricing_for_model(cfg.model),
    )
    if cfg.provider == "anthropic":
        return AnthropicProvider(cfg)
    if cfg.provider == "openai":
        return OpenAIProvider(cfg)
    if cfg.provider == "deepseek":
        return DeepSeekProvider(cfg)
    if cfg.provider == "codex":
        return CodexProvider(cfg)
    raise LLMConfigError(
        f"unknown provider: {cfg.provider!r}", provider=cfg.provider
    )


__all__ = [
    "LLMConfig",
    "LLMProvider",
    "AnthropicProvider",
    "OpenAIProvider",
    "CodexProvider",
    "DeepSeekProvider",
    "build_provider",
    "close_codex_app_server_client",
    "set_response_cache",
    "get_response_cache",
    "get_timeout_for_model",
    "MODEL_TIMEOUTS",
    "LLMError",
    "LLMParseError",
    "LLMConfigError",
    # TK-5 error classification + retry policies.
    "LLMRateLimitError",
    "LLMTimeoutError",
    "LLMContentViolationError",
    "LLMTransientError",
    "LLMPermanentError",
    "LLMErrorClass",
    "RetryPolicy",
    "RETRY_POLICIES",
    "classify_error",
    "retry_policy_for",
    "parse_retry_after",
    # OP-2 cost tracking.
    "MODEL_PRICING",
    "LLMUsage",
    "LLMUsageAggregator",
    "get_pricing_for_model",
    "compute_cost_usd",
    # Week 5: task-local aggregator.
    "using_usage_aggregator",
    "using_receipt_sink",
    # Cost-plan §0.1: per-call purpose attribution.
    "using_usage_purpose",
]
