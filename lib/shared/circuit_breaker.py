"""Async circuit breaker for shared outbound dependency clients.

The LLM stack has its own historical breaker under
``services.reasoning.think``. This module is the generic version for HTTP
clients, SDK wrappers, and storage adapters that need the same production
guard without importing across service boundaries.
"""
from __future__ import annotations

import asyncio
import enum
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from lib.observability.metrics import counter, gauge
from lib.shared.errors import CompanyOSError


T = TypeVar("T")


class CircuitOpenError(CompanyOSError):
    """Raised when a breaker rejects a call without touching the dependency."""

    default_code = "circuit_open"
    _recoverable = True


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


def _record_all_exceptions(exc: BaseException) -> bool:
    return True


_breaker_open = gauge(
    "circuit_breaker_open",
    "Whether a named outbound dependency circuit breaker is open.",
    ("name",),
)
_breaker_state = gauge(
    "circuit_breaker_state",
    "State of a named outbound dependency circuit breaker.",
    ("name", "state"),
    allowed_label_values={
        "state": [state.value for state in CircuitState],
    },
)
_breaker_calls = counter(
    "circuit_breaker_calls_total",
    "Outbound dependency calls observed by circuit breakers.",
    ("name", "result"),
    allowed_label_values={
        "result": ("success", "failure", "rejected"),
    },
)


def _publish_state(name: str, state: CircuitState) -> None:
    _breaker_open.set(1.0 if state == CircuitState.OPEN else 0.0, name=name)
    for candidate in CircuitState:
        _breaker_state.set(
            1.0 if candidate == state else 0.0,
            name=name,
            state=candidate.value,
        )


@dataclass
class AsyncCircuitBreaker:
    """Rolling-window async circuit breaker.

    ``record_exception`` lets callers avoid counting permanent caller faults
    such as non-retryable 4xx responses, while still counting retry-exhausted
    dependency outages.
    """

    name: str
    failure_threshold: float = 0.5
    window_seconds: float = 60.0
    open_duration: float = 30.0
    min_samples: int = 5
    record_exception: Callable[[BaseException], bool] = field(
        default=_record_all_exceptions,
        repr=False,
    )

    state: CircuitState = field(default=CircuitState.CLOSED)
    events: deque[tuple[float, bool]] = field(default_factory=deque)
    opened_at: float | None = None
    _lock: asyncio.Lock | None = field(default=None, init=False, repr=False)
    _lock_loop: asyncio.AbstractEventLoop | None = field(
        default=None,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if not 0 < self.failure_threshold <= 1:
            raise ValueError("failure_threshold must be between 0 and 1")
        if self.window_seconds <= 0:
            raise ValueError("window_seconds must be positive")
        if self.open_duration < 0:
            raise ValueError("open_duration must be >= 0")
        if self.min_samples < 1:
            raise ValueError("min_samples must be >= 1")
        _publish_state(self.name, self.state)

    def _now(self) -> float:
        return time.monotonic()

    def _current_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if self._lock is None or self._lock_loop is not loop:
            self._lock = asyncio.Lock()
            self._lock_loop = loop
        return self._lock

    def _evict_old(self) -> None:
        cutoff = self._now() - self.window_seconds
        while self.events and self.events[0][0] < cutoff:
            self.events.popleft()

    def _failure_rate(self) -> float:
        if not self.events:
            return 0.0
        failures = sum(1 for _, success in self.events if not success)
        return failures / len(self.events)

    def _set_state(self, state: CircuitState) -> None:
        if self.state != state:
            self.state = state
        _publish_state(self.name, self.state)

    def _record_success(self) -> None:
        if self.state == CircuitState.HALF_OPEN:
            self.events.clear()
            self.opened_at = None
            self._set_state(CircuitState.CLOSED)
            return
        self.events.append((self._now(), True))
        self._evict_old()
        _publish_state(self.name, self.state)

    def _record_failure(self) -> None:
        self.events.append((self._now(), False))
        self._evict_old()
        if self.state == CircuitState.HALF_OPEN:
            self.opened_at = self._now()
            self._set_state(CircuitState.OPEN)
            return
        if len(self.events) >= self.min_samples:
            if self._failure_rate() >= self.failure_threshold:
                self.opened_at = self._now()
                self._set_state(CircuitState.OPEN)
                return
        _publish_state(self.name, self.state)

    def _check_half_open_transition(self) -> None:
        if self.state != CircuitState.OPEN or self.opened_at is None:
            return
        if self._now() - self.opened_at >= self.open_duration:
            self._set_state(CircuitState.HALF_OPEN)

    async def call(self, fn: Callable[[], Awaitable[T]]) -> T:
        """Run ``fn`` through the breaker."""

        async with self._current_lock():
            self._check_half_open_transition()
            if self.state == CircuitState.OPEN:
                _breaker_calls.inc(1.0, name=self.name, result="rejected")
                raise CircuitOpenError(
                    f"circuit breaker '{self.name}' is OPEN",
                    breaker=self.name,
                    failure_rate=self._failure_rate(),
                    opened_at=self.opened_at,
                )

        try:
            result = await fn()
        except Exception as exc:
            if self.record_exception(exc):
                async with self._current_lock():
                    self._record_failure()
                _breaker_calls.inc(1.0, name=self.name, result="failure")
            raise

        async with self._current_lock():
            self._record_success()
        _breaker_calls.inc(1.0, name=self.name, result="success")
        return result

    def status(self) -> dict[str, Any]:
        self._evict_old()
        _publish_state(self.name, self.state)
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_rate": self._failure_rate(),
            "samples": len(self.events),
            "opened_at": self.opened_at,
        }

    def reset(self) -> None:
        self.state = CircuitState.CLOSED
        self.opened_at = None
        self.events.clear()
        self._lock = None
        self._lock_loop = None
        _publish_state(self.name, self.state)


__all__ = [
    "AsyncCircuitBreaker",
    "CircuitOpenError",
    "CircuitState",
]
