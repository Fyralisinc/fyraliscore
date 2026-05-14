"""services/webhooks/metrics.py — verification-failure counters.

Per spec FR-011 every verification failure increments a counter
labeled with `{provider, reason}`. This module provides an in-process
counter that the observability stack (structlog handlers, Prometheus
exporter, etc.) can read or wrap.

The implementation is deliberately minimal — a thread-safe dict —
because the project does not currently ship a Prometheus client and
the constitution's simplicity principle (X) says don't add one until
there's a second caller. Tests read the counter directly to assert
labeling correctness.
"""
from __future__ import annotations

import threading
from typing import Mapping


_lock = threading.Lock()
_counters: dict[tuple[str, str], int] = {}

# ---------------------------------------------------------------------
# Tenant resolver metrics (FR-018) — three named families:
#   webhook_resolver_outcomes_total{provider, outcome}
#   webhook_resolver_cache_total{provider, result}
#   webhook_resolver_duration_seconds{provider}  (sample-based p95)
#
# Labels are bounded by the 5-provider enum × small outcome/result
# enums. installation_id is NEVER a label (FR-015).
# ---------------------------------------------------------------------
_resolver_outcomes: dict[tuple[str, str], int] = {}
_resolver_cache: dict[tuple[str, str], int] = {}
_resolver_samples: dict[str, list[float]] = {}
# Cap stored samples per provider to bound memory. P95 on a rolling
# 1024-sample window is the assertion API the integration test uses.
_RESOLVER_SAMPLE_CAP = 1024


def record_failure(provider: str, reason: str) -> None:
    """Increment the (provider, reason) failure counter by 1."""
    key = (provider, reason)
    with _lock:
        _counters[key] = _counters.get(key, 0) + 1


def get_count(provider: str, reason: str) -> int:
    with _lock:
        return _counters.get((provider, reason), 0)


def snapshot() -> Mapping[tuple[str, str], int]:
    """Read-only snapshot of all counters. Used by tests."""
    with _lock:
        return dict(_counters)


def reset() -> None:
    """Test helper — clear all counters."""
    with _lock:
        _counters.clear()
        _resolver_outcomes.clear()
        _resolver_cache.clear()
        _resolver_samples.clear()


# ---------------------------------------------------------------------
# Resolver metric helpers
# ---------------------------------------------------------------------

def record_resolver_outcome(provider: str, outcome: str) -> None:
    """Increment webhook_resolver_outcomes_total{provider, outcome}.

    outcome ∈ {resolved, unknown_installation, payload_missing}.
    """
    with _lock:
        key = (provider, outcome)
        _resolver_outcomes[key] = _resolver_outcomes.get(key, 0) + 1


def record_resolver_cache(provider: str, result: str) -> None:
    """Increment webhook_resolver_cache_total{provider, result}.

    result ∈ {hit, miss, bypass}.
    """
    with _lock:
        key = (provider, result)
        _resolver_cache[key] = _resolver_cache.get(key, 0) + 1


def observe_resolver_duration(provider: str, seconds: float) -> None:
    """Record one resolver-duration sample for the given provider.

    Sample-based histogram (capped at 1024 entries per provider). The
    integration test reads p95 via `resolver_duration_p95`. Cap is
    intentional — sample-based histograms don't bound memory by
    default.
    """
    with _lock:
        samples = _resolver_samples.setdefault(provider, [])
        samples.append(seconds)
        if len(samples) > _RESOLVER_SAMPLE_CAP:
            # Drop the oldest sample. This is O(n) but n is bounded
            # and resolver invocations are infrequent vs network IO.
            del samples[0]


def get_resolver_outcome_count(provider: str, outcome: str) -> int:
    with _lock:
        return _resolver_outcomes.get((provider, outcome), 0)


def get_resolver_cache_count(provider: str, result: str) -> int:
    with _lock:
        return _resolver_cache.get((provider, result), 0)


def resolver_duration_p95(provider: str) -> float | None:
    """Return p95 of stored samples for this provider, or None if no
    samples exist.

    Uses the nearest-rank method: sort, pick the ⌈0.95 * N⌉-th item.
    """
    with _lock:
        samples = list(_resolver_samples.get(provider, ()))
    if not samples:
        return None
    samples.sort()
    # Nearest-rank for p95: index = ceil(0.95 * N) - 1, clamped.
    n = len(samples)
    idx = max(0, min(n - 1, -(-95 * n // 100) - 1))
    return samples[idx]


def snapshot_resolver() -> dict[str, dict[tuple[str, str], int]]:
    """Read-only snapshot of resolver counters (outcomes + cache).

    Used by integration tests to assert exact counter values.
    """
    with _lock:
        return {
            "outcomes": dict(_resolver_outcomes),
            "cache": dict(_resolver_cache),
        }


__all__ = [
    "record_failure",
    "get_count",
    "snapshot",
    "reset",
    "record_resolver_outcome",
    "record_resolver_cache",
    "observe_resolver_duration",
    "get_resolver_outcome_count",
    "get_resolver_cache_count",
    "resolver_duration_p95",
    "snapshot_resolver",
]
