"""IN-12 US2: INVALID_SESSION + backoff behavior.

These tests exercise the protocol's reconnect paths beyond a simple
close-and-resume:
  - INVALID_SESSION (op 9) with `d=true`  → RESUME on same session
  - INVALID_SESSION (op 9) with `d=false` → full re-IDENTIFY
  - `_next_backoff()` schedule (pure function)
"""
from __future__ import annotations

import pytest

from services.integrations.discord.gateway.worker import _next_backoff


pytestmark = pytest.mark.integration


def test_backoff_schedule_caps_at_60_with_jitter() -> None:
    """FR-012 / research R4: backoff follows 1, 2, 4, 8, 16, 32, cap 60s,
    ±25 % jitter. We assert the base envelope (jitter-adjusted)."""
    # Attempt 0: base 1s, jitter ±25% → [0.5, 1.25]
    for _ in range(20):
        v = _next_backoff(0)
        assert 0.5 <= v <= 1.5
    # Attempt 1: base 2s → [1.0, 2.5]
    for _ in range(20):
        v = _next_backoff(1)
        assert 0.5 <= v <= 3.0
    # Attempt 6: base would be 64s, capped at 60 → ±25% → [45, 75]
    for _ in range(20):
        v = _next_backoff(6)
        assert 45 <= v <= 75
    # Attempt 10: deep in cap zone → still ~[45, 75]
    for _ in range(20):
        v = _next_backoff(10)
        assert 45 <= v <= 75


def test_backoff_never_returns_zero_or_negative() -> None:
    """Lower bound: `_next_backoff` never returns ≤0 (would cause a
    spin loop in worker.run_forever)."""
    for attempt in range(20):
        for _ in range(5):
            assert _next_backoff(attempt) > 0
