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
    LLM_PROVIDER   = "anthropic" | "openai"
    LLM_API_KEY    = ...
    LLM_MODEL      = "claude-opus-4-7" | "gpt-4o" | ...
    LLM_TIMEOUT_SECONDS = 30

Implements retry-on-parse-failure per Prompt 0.2 + TK-5. After
strict mode, parse errors are rare; the default `max_retries=1` yields
one repair attempt (2 total calls) rather than the legacy 3. Callers
that need different retry budgets per error class consult
`RETRY_POLICIES` below and drive the loop themselves.

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
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, TypeVar

from pydantic import BaseModel, ValidationError as PydanticValidationError

from lib.shared.errors import CompanyOSError


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

MODEL_PRICING: dict[str, dict[str, float]] = {
    # DeepSeek — May 2025 on-demand tiers.
    "deepseek-reasoner": {"input_per_mtok": 0.55, "output_per_mtok": 2.19},
    "deepseek-chat": {"input_per_mtok": 0.27, "output_per_mtok": 1.10},
    # Anthropic — rough tier; the exact number is subject to model-rev drift,
    # but the scale is right. Edit when a new Claude rev ships.
    "claude-opus-4-7": {"input_per_mtok": 15.0, "output_per_mtok": 75.0},
    "claude-sonnet-4-5": {"input_per_mtok": 3.0, "output_per_mtok": 15.0},
    # OpenAI — rough placeholders; update with the live pricing page before
    # relying on cost numbers in dashboards.
    "gpt-4o": {"input_per_mtok": 2.5, "output_per_mtok": 10.0},
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
) -> float:
    """Compute call cost in USD for the given token counts + model."""
    pricing = get_pricing_for_model(model_name)
    return (
        input_tokens * pricing["input_per_mtok"] / 1_000_000.0
        + output_tokens * pricing["output_per_mtok"] / 1_000_000.0
    )


@dataclass
class LLMUsage:
    """One call's usage + cost accounting. Accumulated across a Think
    run by `LLMUsageAggregator`. Populated when the provider can extract
    token counts from the SDK response (Anthropic + OpenAI both return
    usage metadata; DeepSeek via the OpenAI-compatible endpoint does
    too). Zeroed when not available so callers can always sum safely."""
    input_tokens: int = 0
    output_tokens: int = 0
    model_name: str | None = None
    cost_usd: float = 0.0


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
    def total_cost_usd(self) -> float:
        return sum(c.cost_usd for c in self.calls)

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


def _extract_anthropic_usage(response: Any) -> tuple[int, int]:
    """Anthropic response `.usage.input_tokens` / `.usage.output_tokens`."""
    usage = getattr(response, "usage", None)
    if usage is None:
        return 0, 0
    return int(getattr(usage, "input_tokens", 0) or 0), int(
        getattr(usage, "output_tokens", 0) or 0
    )


def _extract_openai_usage(response: Any) -> tuple[int, int]:
    """OpenAI-compatible response `.usage.prompt_tokens` / `.completion_tokens`."""
    usage = getattr(response, "usage", None)
    if usage is None:
        return 0, 0
    return int(getattr(usage, "prompt_tokens", 0) or 0), int(
        getattr(usage, "completion_tokens", 0) or 0
    )


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
_PERMANENT_STATUSES = (400, 401, 403, 404, 422)
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
    provider: str                  # "anthropic" | "openai"
    api_key: str
    model: str
    timeout_s: float = 30.0
    # TK-5: strict-mode makes parse errors rare. One repair attempt
    # (2 total calls) is the tuned default per
    # `RETRY_POLICIES[LLMErrorClass.PARSE_ERROR]`. Callers that need
    # a custom budget still pass `max_retries=` explicitly.
    max_retries: int = 1

    @classmethod
    def from_env(cls) -> "LLMConfig":
        provider = os.environ.get("LLM_PROVIDER", "anthropic").lower()
        if provider == "deepseek":
            api_key = os.environ.get("DEEPSEEK_API_KEY", "") or os.environ.get("LLM_API_KEY", "")
        else:
            api_key = os.environ.get("LLM_API_KEY", "")
        model = os.environ.get("LLM_MODEL", _default_model(provider))
        # TK-1: if LLM_TIMEOUT_SECONDS is explicitly set, honour it (back-compat);
        # otherwise derive from per-model tier. `get_timeout_for_model` itself
        # respects LLM_TIMEOUT_OVERRIDE_MS for test forcing.
        explicit = os.environ.get("LLM_TIMEOUT_SECONDS")
        if explicit is not None:
            timeout = float(explicit)
        else:
            timeout = float(get_timeout_for_model(model))
        if provider not in ("anthropic", "openai", "deepseek"):
            raise LLMConfigError(
                f"unknown LLM_PROVIDER: {provider!r}",
                provider=provider,
            )
        return cls(
            provider=provider,
            api_key=api_key,
            model=model,
            timeout_s=timeout,
        )


def _default_model(provider: str) -> str:
    return {
        "anthropic": "claude-opus-4-7",
        "openai": "gpt-4o",
        "deepseek": "deepseek-reasoner",
    }.get(provider, "claude-opus-4-7")


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
# `services.think.circuit_breaker.get_breaker(name).call(fn)`. If the
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
    # Lazy import — keeps lib/llm from hard-depending on services/think
    # at module import time.
    from services.think.circuit_breaker import get_breaker
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

    # -----------------------------------------------------------------
    # Usage aggregator hook (OP-2) — callers install an aggregator for
    # the duration of a Think run and read totals afterwards.
    # -----------------------------------------------------------------
    def set_usage_aggregator(self, agg: LLMUsageAggregator | None) -> None:
        self._usage_aggregator = agg

    def _record_usage(
        self, input_tokens: int, output_tokens: int,
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
        )
        agg.record(
            LLMUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                model_name=self.config.model,
                cost_usd=cost,
            )
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
        cache = get_response_cache()
        if cache is not None:
            async def _fetch() -> dict[str, Any]:
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

        for attempt in range(self.config.max_retries + 1):
            user_msg = (
                base_user if repair_note is None
                else f"{base_user}\n\nPrior attempt failed validation. "
                     f"Fix: {repair_note}. Return ONLY valid JSON matching the schema."
            )
            raw = await self._raw_call(
                system=system,
                user=user_msg,
                temperature=temperature,
                max_tokens=max_tokens,
                schema_hint=schema_hint,
            )

            _, err = _try_parse(raw, schema)
            if err is None:
                return raw

            last_error = err
            repair_note = str(err)
            if attempt == self.config.max_retries:
                raise LLMParseError(
                    f"LLM output did not validate after "
                    f"{self.config.max_retries + 1} attempts: {err}",
                    last_raw=raw[:1000],
                    schema=schema.__name__,
                ) from err

        # Theoretically unreachable.
        raise LLMParseError(
            f"structured() exhausted retries: {last_error}"
        ) from last_error


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
        response = await _through_breaker(self.config.provider, _do_call)
        if not response.content:
            raise LLMError("empty response from anthropic", model=self.config.model)
        # OP-2: record input/output tokens if an aggregator is installed.
        inp, outp = _extract_anthropic_usage(response)
        self._record_usage(inp, outp)
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
        response = await _through_breaker(self.config.provider, _do_call)
        content = response.choices[0].message.content
        if not content:
            raise LLMError("empty response from openai", model=self.config.model)
        # OP-2: record input/output tokens if an aggregator is installed.
        inp, outp = _extract_openai_usage(response)
        self._record_usage(inp, outp)
        return content


class DeepSeekProvider(OpenAIProvider):
    """OpenAI-API-compatible provider targeting the DeepSeek endpoint.

    Overrides `_structured_raw` to use DeepSeek's strict tool-calling
    mode on `/beta` for schemas with a registered strict variant. This
    constrains the decoder server-side, eliminating the schema-mismatch
    failures observed with plain `response_format: json_object`.
    """
    base_url = "https://api.deepseek.com"
    strict_base_url = "https://api.deepseek.com/beta"

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
        if strict_schema is None:
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
        )

        last_error: Exception | None = None
        repair_note: str | None = None
        base_user = user
        for attempt in range(self.config.max_retries + 1):
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
            response = await _through_breaker(self.config.provider, _do_call)
            choice = response.choices[0]
            tool_calls = getattr(choice.message, "tool_calls", None) or []
            if not tool_calls:
                raise LLMError(
                    "deepseek strict mode returned no tool_calls",
                    model=self.config.model,
                )
            # OP-2: record usage (strict-mode responses carry the same usage block).
            inp, outp = _extract_openai_usage(response)
            self._record_usage(inp, outp)
            raw = tool_calls[0].function.arguments
            # DeepSeek strict mode occasionally drops the closing quote
            # on a key, producing `"key: value` instead of `"key": value`.
            # Try a repair pass before declaring failure.
            repaired = _repair_deepseek_strict_json(raw)
            _, err = _try_parse(repaired, schema)
            if err is None:
                return repaired
            last_error = err
            repair_note = str(err)
            if attempt == self.config.max_retries:
                raise LLMParseError(
                    f"DeepSeek strict-mode output did not validate after "
                    f"{self.config.max_retries + 1} attempts: {err}",
                    last_raw=raw[:1000],
                    schema=schema.__name__,
                ) from err

        raise LLMParseError(
            f"DeepSeek strict-mode exhausted retries: {last_error}"
        ) from last_error


def _repair_deepseek_strict_json(text: str) -> str:
    """Patch the known DeepSeek strict-mode bug of missing closing quote on keys.

    Pattern: `"key: value` -> `"key": value`. Conservative regex: only
    matches when an opening quote is followed by an identifier-like
    sequence then a colon-space, with no intervening closing quote.
    """
    import re
    return re.sub(r'"([A-Za-z_][A-Za-z0-9_]*):\s', r'"\1": ', text)


def _strict_schema_for(schema: type[BaseModel]) -> dict | None:
    """Return the registered strict-mode schema for a Pydantic class, or None."""
    try:
        from services.think.diff_schema import RawDiff, ValidatedDiff
        from services.think.strict_schema import RAW_DIFF_STRICT_SCHEMA
    except ImportError:
        return None
    if schema is RawDiff or schema is ValidatedDiff:
        return RAW_DIFF_STRICT_SCHEMA
    return None


def build_provider(config: LLMConfig | None = None) -> LLMProvider:
    cfg = config or LLMConfig.from_env()
    if cfg.provider == "anthropic":
        return AnthropicProvider(cfg)
    if cfg.provider == "openai":
        return OpenAIProvider(cfg)
    if cfg.provider == "deepseek":
        return DeepSeekProvider(cfg)
    raise LLMConfigError(
        f"unknown provider: {cfg.provider!r}", provider=cfg.provider
    )


__all__ = [
    "LLMConfig",
    "LLMProvider",
    "AnthropicProvider",
    "OpenAIProvider",
    "DeepSeekProvider",
    "build_provider",
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
]
