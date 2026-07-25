"""Deterministic state machines used by the Provider Lab ASGI app."""
from __future__ import annotations

import copy
import hashlib
import json
import math
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Mapping

from .protocol import AdapterRegistry


DEFAULT_CLOCK_START = datetime(2025, 1, 1, tzinfo=timezone.utc)
QuotaMode = Literal["disabled", "observe", "enforce"]
FaultAction = Literal["response", "malformed_json", "disconnect"]


def isoformat_z(value: datetime) -> str:
    value = value.astimezone(timezone.utc)
    rendered = value.isoformat(timespec="milliseconds")
    return rendered.replace("+00:00", "Z")


def _json_copy(value: Any) -> Any:
    # Round-tripping gives state isolation and rejects non-JSON values.
    return json.loads(json.dumps(value, sort_keys=True))


class VirtualClock:
    """A manually controlled UTC clock; it never reads wall time."""

    def __init__(self, start: datetime = DEFAULT_CLOCK_START) -> None:
        self._lock = threading.RLock()
        self._initial = self._validate(start)
        self._now = self._initial

    @staticmethod
    def _validate(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("virtual clock values must be timezone-aware")
        return value.astimezone(timezone.utc)

    def now(self) -> datetime:
        with self._lock:
            return self._now

    def set(self, value: datetime) -> datetime:
        with self._lock:
            normalized = self._validate(value)
            if normalized < self._now:
                raise ValueError(
                    "virtual clock cannot move backwards; reset the lab instead"
                )
            self._now = normalized
            return self._now

    def advance(self, *, seconds: float = 0.0, milliseconds: int = 0) -> datetime:
        if seconds < 0 or milliseconds < 0:
            raise ValueError("virtual clock cannot advance by a negative duration")
        with self._lock:
            self._now += timedelta(seconds=seconds, milliseconds=milliseconds)
            return self._now

    def reset(self) -> datetime:
        with self._lock:
            self._now = self._initial
            return self._now

    def snapshot(self) -> dict[str, Any]:
        return {"now": isoformat_z(self.now())}


@dataclass(frozen=True)
class QuotaConfiguration:
    source: str
    scope: str
    bucket: str
    mode: QuotaMode
    capacity: float
    refill_per_second: float
    initial_tokens: float | None = None

    def __post_init__(self) -> None:
        if self.mode not in {"disabled", "observe", "enforce"}:
            raise ValueError(f"unsupported quota mode {self.mode!r}")
        if not self.scope or len(self.scope) > 256:
            raise ValueError("quota scope must contain 1..256 characters")
        if not self.bucket or len(self.bucket) > 128:
            raise ValueError("quota bucket must contain 1..128 characters")
        if self.capacity <= 0:
            raise ValueError("quota capacity must be greater than zero")
        if self.refill_per_second < 0:
            raise ValueError("quota refill_per_second cannot be negative")
        if self.initial_tokens is not None and not (
            0 <= self.initial_tokens <= self.capacity
        ):
            raise ValueError("initial_tokens must be between zero and capacity")


@dataclass
class _TokenBucket:
    config: QuotaConfiguration
    tokens: float
    last_refill: datetime


@dataclass(frozen=True)
class QuotaDecision:
    allowed: bool
    configured: bool
    mode: QuotaMode
    would_limit: bool
    remaining: float | None
    retry_after_seconds: int | None
    headers: Mapping[str, str]


class QuotaManager:
    """Scoped deterministic token buckets driven by ``VirtualClock``."""

    def __init__(self, clock: VirtualClock) -> None:
        self._clock = clock
        self._lock = threading.RLock()
        self._buckets: dict[tuple[str, str, str], _TokenBucket] = {}

    @staticmethod
    def _key(source: str, scope: str, bucket: str) -> tuple[str, str, str]:
        return source, scope, bucket

    def configure(self, config: QuotaConfiguration) -> dict[str, Any]:
        initial = (
            config.capacity
            if config.initial_tokens is None
            else config.initial_tokens
        )
        with self._lock:
            self._buckets[self._key(config.source, config.scope, config.bucket)] = (
                _TokenBucket(
                    config=config,
                    tokens=initial,
                    last_refill=self._clock.now(),
                )
            )
            return self._snapshot_bucket(
                self._buckets[self._key(config.source, config.scope, config.bucket)]
            )

    def remove(self, source: str, scope: str, bucket: str) -> bool:
        with self._lock:
            return (
                self._buckets.pop(self._key(source, scope, bucket), None) is not None
            )

    def clear(self) -> None:
        with self._lock:
            self._buckets.clear()

    def _refill(self, state: _TokenBucket, now: datetime) -> None:
        elapsed = max(0.0, (now - state.last_refill).total_seconds())
        if elapsed:
            state.tokens = min(
                state.config.capacity,
                state.tokens + elapsed * state.config.refill_per_second,
            )
            state.last_refill = now

    def check(
        self,
        *,
        source: str,
        scope: str,
        bucket: str,
        cost: float,
    ) -> QuotaDecision:
        if cost <= 0:
            raise ValueError("quota request cost must be greater than zero")
        key = self._key(source, scope, bucket)
        with self._lock:
            state = self._buckets.get(key)
            if state is None:
                return QuotaDecision(
                    allowed=True,
                    configured=False,
                    mode="disabled",
                    would_limit=False,
                    remaining=None,
                    retry_after_seconds=None,
                    headers={},
                )

            config = state.config
            now = self._clock.now()
            self._refill(state, now)

            if config.mode == "disabled":
                return QuotaDecision(
                    allowed=True,
                    configured=True,
                    mode="disabled",
                    would_limit=False,
                    remaining=state.tokens,
                    retry_after_seconds=None,
                    headers={
                        "X-Provider-Lab-Quota-Mode": "disabled",
                    },
                )

            would_limit = state.tokens + 1e-12 < cost
            retry_after: int | None = None
            if would_limit:
                if config.refill_per_second > 0:
                    retry_after = max(
                        1,
                        math.ceil(
                            (cost - state.tokens) / config.refill_per_second
                        ),
                    )
                else:
                    # A zero-refill bucket remains exhausted until a control
                    # operation resets/reconfigures it.
                    retry_after = 86_400
            else:
                state.tokens -= cost

            remaining = max(0.0, state.tokens)
            headers = {
                "X-RateLimit-Limit": _format_number(config.capacity),
                "X-RateLimit-Remaining": _format_number(remaining),
                "X-Provider-Lab-Quota-Mode": config.mode,
                "X-Provider-Lab-Quota-Scope": scope,
                "X-Provider-Lab-Quota-Bucket": bucket,
            }
            if retry_after is not None:
                headers["Retry-After"] = str(retry_after)
            if config.mode == "observe" and would_limit:
                headers["X-Provider-Lab-Quota-Observed"] = "exceeded"

            return QuotaDecision(
                allowed=not (config.mode == "enforce" and would_limit),
                configured=True,
                mode=config.mode,
                would_limit=would_limit,
                remaining=remaining,
                retry_after_seconds=retry_after,
                headers=headers,
            )

    @staticmethod
    def _snapshot_bucket(state: _TokenBucket) -> dict[str, Any]:
        return {
            "source": state.config.source,
            "scope": state.config.scope,
            "bucket": state.config.bucket,
            "mode": state.config.mode,
            "capacity": state.config.capacity,
            "refill_per_second": state.config.refill_per_second,
            "tokens": round(state.tokens, 9),
            "last_refill": isoformat_z(state.last_refill),
        }

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            now = self._clock.now()
            for state in self._buckets.values():
                self._refill(state, now)
            return [
                self._snapshot_bucket(self._buckets[key])
                for key in sorted(self._buckets)
            ]


def _format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.9f}".rstrip("0").rstrip(".")


@dataclass
class FaultRule:
    rule_id: str
    source: str
    action: FaultAction = "response"
    route_id: str | None = None
    scope: str | None = None
    status_code: int = 503
    body: Any = None
    headers: Mapping[str, str] = None  # type: ignore[assignment]
    after_requests: int = 0
    every: int = 1
    max_hits: int | None = None
    latency_ms: int = 0
    enabled: bool = True
    attempts: int = 0
    hits: int = 0

    def __post_init__(self) -> None:
        if self.action not in {"response", "malformed_json", "disconnect"}:
            raise ValueError(f"unsupported fault action {self.action!r}")
        if not 400 <= self.status_code <= 599:
            raise ValueError("fault status_code must be between 400 and 599")
        if self.after_requests < 0:
            raise ValueError("after_requests cannot be negative")
        if self.every <= 0:
            raise ValueError("every must be greater than zero")
        if self.max_hits is not None and self.max_hits <= 0:
            raise ValueError("max_hits must be greater than zero")
        if self.latency_ms < 0:
            raise ValueError("latency_ms cannot be negative")
        self.headers = dict(self.headers or {})
        _json_copy(self.body)

    def matches(self, *, source: str, route_id: str, scope: str) -> bool:
        return (
            self.enabled
            and self.source == source
            and (self.route_id is None or self.route_id == route_id)
            and (self.scope is None or self.scope == scope)
            and (self.max_hits is None or self.hits < self.max_hits)
        )

    def consider(self) -> bool:
        self.attempts += 1
        ordinal = self.attempts - self.after_requests
        if ordinal <= 0:
            return False
        fire = (ordinal - 1) % self.every == 0
        if fire:
            self.hits += 1
        return fire

    def snapshot(self) -> dict[str, Any]:
        data = asdict(self)
        data["headers"] = dict(sorted(self.headers.items()))
        return data


class FaultManager:
    """Ordered deterministic request fault rules."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._rules: dict[str, FaultRule] = {}
        self._next_id = 1

    def create(
        self,
        *,
        source: str,
        action: FaultAction = "response",
        route_id: str | None = None,
        scope: str | None = None,
        status_code: int = 503,
        body: Any = None,
        headers: Mapping[str, str] | None = None,
        after_requests: int = 0,
        every: int = 1,
        max_hits: int | None = None,
        latency_ms: int = 0,
        enabled: bool = True,
    ) -> dict[str, Any]:
        with self._lock:
            rule_id = f"fault-{self._next_id:06d}"
            rule = FaultRule(
                rule_id=rule_id,
                source=source,
                action=action,
                route_id=route_id,
                scope=scope,
                status_code=status_code,
                body=body,
                headers=dict(headers or {}),
                after_requests=after_requests,
                every=every,
                max_hits=max_hits,
                latency_ms=latency_ms,
                enabled=enabled,
            )
            self._rules[rule_id] = rule
            self._next_id += 1
            return rule.snapshot()

    def remove(self, rule_id: str) -> bool:
        with self._lock:
            return self._rules.pop(rule_id, None) is not None

    def clear(self) -> None:
        with self._lock:
            self._rules.clear()
            self._next_id = 1

    def evaluate(
        self, *, source: str, route_id: str, scope: str
    ) -> FaultRule | None:
        with self._lock:
            fired: FaultRule | None = None
            for rule_id in sorted(self._rules):
                rule = self._rules[rule_id]
                if rule.matches(source=source, route_id=route_id, scope=scope):
                    should_fire = rule.consider()
                    if should_fire and fired is None:
                        fired = rule
            return copy.deepcopy(fired)

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [self._rules[key].snapshot() for key in sorted(self._rules)]


_SENSITIVE_PARTS = (
    "authorization",
    "cookie",
    "credential",
    "password",
    "secret",
    "signature",
    "token",
)


def _is_sensitive(name: str) -> bool:
    lowered = name.lower()
    return (
        lowered in {"x-api-key", "api-key", "set-cookie"}
        or any(part in lowered for part in _SENSITIVE_PARTS)
    )


def sanitize_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {
        key.lower(): ("[REDACTED]" if _is_sensitive(key) else value)
        for key, value in sorted(headers.items(), key=lambda pair: pair[0].lower())
    }


def sanitize_query(
    query_items: tuple[tuple[str, str], ...],
) -> list[dict[str, str]]:
    return [
        {
            "name": key,
            "value": "[REDACTED]" if _is_sensitive(key) else value,
        }
        for key, value in query_items
    ]


def body_fingerprint(body: bytes) -> dict[str, Any]:
    return {
        "size_bytes": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    }


class RequestLedger:
    """Append-only request outcomes, excluding control-plane traffic."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: list[dict[str, Any]] = []
        self._next_id = 1

    def append(self, entry: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            materialized = _json_copy(dict(entry))
            materialized["request_id"] = self._next_id
            self._next_id += 1
            self._entries.append(materialized)
            return _json_copy(materialized)

    def list(
        self,
        *,
        source: str | None = None,
        scope: str | None = None,
        route_id: str | None = None,
        outcome: str | None = None,
        after_id: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        if not 1 <= limit <= 1_000:
            raise ValueError("ledger limit must be between 1 and 1000")
        with self._lock:
            selected = [
                entry
                for entry in self._entries
                if entry["request_id"] > after_id
                and (source is None or entry.get("source") == source)
                and (scope is None or entry.get("scope") == scope)
                and (route_id is None or entry.get("route_id") == route_id)
                and (outcome is None or entry.get("outcome") == outcome)
            ]
            return _json_copy(selected[:limit])

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()
            self._next_id = 1

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)


class InjectedDisconnect(ConnectionError):
    """Raised deliberately when a disconnect fault fires."""


class LabRuntime:
    """All mutable Provider Lab state, owned by one ASGI app instance."""

    def __init__(
        self,
        registry: AdapterRegistry,
        *,
        clock_start: datetime = DEFAULT_CLOCK_START,
    ) -> None:
        self.registry = registry
        self.clock = VirtualClock(clock_start)
        self.quotas = QuotaManager(self.clock)
        self.faults = FaultManager()
        self.ledger = RequestLedger()
        self._lock = threading.RLock()
        self._defaults = {
            source: _json_copy(registry.require(source).default_state())
            for source in registry.sources
        }
        self._source_state = copy.deepcopy(self._defaults)
        self._source_revisions = {source: 0 for source in registry.sources}

    def require_source(self, source: str) -> None:
        if self.registry.get(source) is None:
            raise KeyError(source)

    def get_source_state(self, source: str) -> dict[str, Any]:
        self.require_source(source)
        with self._lock:
            return _json_copy(self._source_state[source])

    def set_source_state(self, source: str, state: Mapping[str, Any]) -> dict[str, Any]:
        self.require_source(source)
        copied = _json_copy(dict(state))
        with self._lock:
            self._source_state[source] = copied
            self._source_revisions[source] += 1
            return {
                "source": source,
                "revision": self._source_revisions[source],
                "state": _json_copy(copied),
            }

    def seed_source_state(self, source: str, state: Mapping[str, Any]) -> None:
        """Set the app's reset baseline before it starts serving requests."""

        self.require_source(source)
        copied = _json_copy(dict(state))
        with self._lock:
            self._defaults[source] = copy.deepcopy(copied)
            self._source_state[source] = copied
            self._source_revisions[source] = 0

    def reset_source_state(self, source: str) -> dict[str, Any]:
        self.require_source(source)
        reset_adapter = getattr(self.registry.require(source), "reset", None)
        if callable(reset_adapter):
            reset_adapter()
        with self._lock:
            self._source_state[source] = copy.deepcopy(self._defaults[source])
            self._source_revisions[source] += 1
            return {
                "source": source,
                "revision": self._source_revisions[source],
                "state": _json_copy(self._source_state[source]),
            }

    def source_state_snapshot(self, source: str) -> dict[str, Any]:
        self.require_source(source)
        with self._lock:
            return {
                "source": source,
                "revision": self._source_revisions[source],
                "state": _json_copy(self._source_state[source]),
            }

    def reset_all(self) -> dict[str, Any]:
        for source in self.registry.sources:
            reset_adapter = getattr(self.registry.require(source), "reset", None)
            if callable(reset_adapter):
                reset_adapter()
        with self._lock:
            self._source_state = copy.deepcopy(self._defaults)
            self._source_revisions = {source: 0 for source in self.registry.sources}
        self.clock.reset()
        self.quotas.clear()
        self.faults.clear()
        self.ledger.clear()
        return self.snapshot()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            revisions = [
                {
                    "source": source,
                    "revision": self._source_revisions[source],
                }
                for source in self.registry.sources
            ]
        return {
            "clock": self.clock.snapshot(),
            "sources": revisions,
            "quota_buckets": len(self.quotas.snapshot()),
            "fault_rules": len(self.faults.snapshot()),
            "ledger_entries": self.ledger.count,
        }


__all__ = [
    "DEFAULT_CLOCK_START",
    "FaultAction",
    "InjectedDisconnect",
    "LabRuntime",
    "QuotaConfiguration",
    "QuotaMode",
    "body_fingerprint",
    "isoformat_z",
    "sanitize_headers",
    "sanitize_query",
]
