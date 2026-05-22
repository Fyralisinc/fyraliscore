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


__all__ = ["record_failure", "get_count", "snapshot", "reset"]
