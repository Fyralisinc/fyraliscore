"""services/think/circuit_breaker.py — per-provider LLM circuit breaker.

OP-3 (THINK-DESIGN-AUDIT §8.2). When an LLM provider has an outage,
every Think worker retries independently — amplifying the outage into
a retry storm. A circuit breaker short-circuits calls after a window-
based failure threshold, fails fast with `CircuitOpenError`, and
probes recovery via a half-open state.

States:
  CLOSED     — normal. All calls pass through. Successes and failures
               are tracked in a rolling window.
  OPEN       — threshold exceeded. All calls raise `CircuitOpenError`
               immediately. After `open_duration` seconds, transitions
               to HALF_OPEN on the next call.
  HALF_OPEN  — probing. The next call through the breaker is allowed.
               Success → CLOSED (breaker reset). Failure → OPEN with
               the clock reset.

Counts are maintained as a deque of (timestamp, success_bool) tuples,
evicted when older than `window_seconds`. The failure rate is
`failures_in_window / total_in_window` — a constant-time calculation
after eviction.

Per-provider singletons are keyed by provider label. Call via
`get_breaker('deepseek').call(coro)` or register a test breaker via
`register_breaker('deepseek', custom)`.
"""
from __future__ import annotations

import asyncio
import enum
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, TypeVar

import structlog

from lib.shared.errors import CompanyOSError


_log = structlog.get_logger(__name__)


T = TypeVar("T")


class CircuitOpenError(CompanyOSError):
    """Raised when a call is rejected because the breaker is OPEN."""
    default_code = "circuit_open"


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class LLMCircuitBreaker:
    """One circuit breaker instance, typically one per LLM provider.

    Parameters:
      failure_threshold : failure-rate at which CLOSED → OPEN (default 0.5
          meaning 50%+ of the window's calls must fail).
      window_seconds    : rolling window over which the failure rate is
          computed (default 60s).
      open_duration     : time OPEN stays open before allowing a single
          HALF_OPEN probe (default 30s).
      min_samples       : minimum calls in the window before the rate
          is evaluated (default 10). Prevents early-boot noise from
          tripping the breaker on a single failure.
    """
    failure_threshold: float = 0.5
    window_seconds: float = 60.0
    open_duration: float = 30.0
    min_samples: int = 10
    name: str = "default"

    state: CircuitState = field(default=CircuitState.CLOSED)
    events: deque = field(default_factory=deque)  # (timestamp, success)
    opened_at: float | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def _now(self) -> float:
        return time.monotonic()

    def _evict_old(self) -> None:
        cutoff = self._now() - self.window_seconds
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def _failure_rate(self) -> float:
        if not self.events:
            return 0.0
        failures = sum(1 for _, ok in self.events if not ok)
        return failures / len(self.events)

    def _record_success(self) -> None:
        self.events.append((self._now(), True))
        self._evict_old()
        if self.state == CircuitState.HALF_OPEN:
            # Successful probe — close the breaker.
            self.state = CircuitState.CLOSED
            self.opened_at = None
            # Clear the window so the next failure starts from scratch.
            self.events.clear()
            _log.info("circuit_breaker_closed", name=self.name)

    def _record_failure(self) -> None:
        self.events.append((self._now(), False))
        self._evict_old()
        if self.state == CircuitState.HALF_OPEN:
            # Failed probe — re-open with clock reset.
            self.state = CircuitState.OPEN
            self.opened_at = self._now()
            _log.warning(
                "circuit_breaker_reopened",
                name=self.name,
                failure_rate=self._failure_rate(),
            )
            return
        # CLOSED: evaluate threshold if we have enough samples.
        if len(self.events) >= self.min_samples:
            rate = self._failure_rate()
            if rate >= self.failure_threshold:
                self.state = CircuitState.OPEN
                self.opened_at = self._now()
                _log.warning(
                    "circuit_breaker_opened",
                    name=self.name,
                    failure_rate=rate,
                    samples=len(self.events),
                )

    def _check_half_open_transition(self) -> None:
        if self.state == CircuitState.OPEN and self.opened_at is not None:
            if self._now() - self.opened_at >= self.open_duration:
                self.state = CircuitState.HALF_OPEN
                _log.info("circuit_breaker_half_open", name=self.name)

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        """Invoke `fn()` through the breaker.

        CLOSED     — pass through, record outcome.
        OPEN       — raise `CircuitOpenError` immediately (after checking
                     for HALF_OPEN transition first).
        HALF_OPEN  — allow one probe; success closes, failure re-opens.
        """
        async with self._lock:
            self._check_half_open_transition()
            if self.state == CircuitState.OPEN:
                raise CircuitOpenError(
                    f"LLM provider circuit breaker '{self.name}' is OPEN",
                    breaker=self.name,
                    failure_rate=self._failure_rate(),
                    opened_at=self.opened_at,
                )

        try:
            result = await fn()
        except Exception:
            async with self._lock:
                self._record_failure()
            raise
        else:
            async with self._lock:
                self._record_success()
            return result

    def status(self) -> dict[str, Any]:
        """Snapshot current state — used by dashboards and tests."""
        self._evict_old()
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_rate": self._failure_rate(),
            "samples": len(self.events),
            "opened_at": self.opened_at,
        }

    def reset(self) -> None:
        """Force back to CLOSED + drain the window. Used by tests to
        start each scenario fresh."""
        self.state = CircuitState.CLOSED
        self.opened_at = None
        self.events.clear()


# ---------------------------------------------------------------------
# Per-provider singleton registry
# ---------------------------------------------------------------------

_BREAKERS: dict[str, LLMCircuitBreaker] = {}


def get_breaker(name: str) -> LLMCircuitBreaker:
    """Return the breaker for `name` (usually 'deepseek', 'anthropic',
    'openai'). Creates one with default settings on first call."""
    if name not in _BREAKERS:
        _BREAKERS[name] = LLMCircuitBreaker(name=name)
    return _BREAKERS[name]


def register_breaker(name: str, breaker: LLMCircuitBreaker) -> None:
    """Install a pre-configured breaker for `name`. Primarily for tests
    that need custom thresholds / windows without monkey-patching."""
    breaker.name = name
    _BREAKERS[name] = breaker


def reset_breakers() -> None:
    """Reset every registered breaker to CLOSED — used between tests."""
    for b in _BREAKERS.values():
        b.reset()


def all_breaker_states() -> dict[str, dict[str, Any]]:
    """Snapshot of every breaker's status (dashboard consumer)."""
    return {name: b.status() for name, b in _BREAKERS.items()}


__all__ = [
    "CircuitState",
    "CircuitOpenError",
    "LLMCircuitBreaker",
    "get_breaker",
    "register_breaker",
    "reset_breakers",
    "all_breaker_states",
]
