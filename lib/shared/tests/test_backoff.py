from __future__ import annotations

import pytest

from lib.shared.backoff import (
    exponential_backoff_seconds,
    queue_retry_backoff_seconds,
)


def test_queue_retry_backoff_uses_standard_schedule() -> None:
    assert [queue_retry_backoff_seconds(i) for i in range(0, 8)] == [
        0.0,
        10.0,
        20.0,
        40.0,
        80.0,
        160.0,
        300.0,
        300.0,
    ]


def test_exponential_backoff_supports_deterministic_jitter() -> None:
    low = exponential_backoff_seconds(
        3,
        base_seconds=10.0,
        cap_seconds=300.0,
        jitter_ratio=0.25,
        random_fn=lambda: 0.0,
    )
    high = exponential_backoff_seconds(
        3,
        base_seconds=10.0,
        cap_seconds=300.0,
        jitter_ratio=0.25,
        random_fn=lambda: 1.0,
    )
    assert low == 30.0
    assert high == 50.0


def test_exponential_backoff_rejects_invalid_parameters() -> None:
    with pytest.raises(ValueError):
        exponential_backoff_seconds(1, base_seconds=-1)
    with pytest.raises(ValueError):
        exponential_backoff_seconds(1, multiplier=0.5)
    with pytest.raises(ValueError):
        exponential_backoff_seconds(1, jitter_ratio=-0.1)
