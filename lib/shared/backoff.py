"""Shared retry backoff schedules.

All attempts are 1-indexed: attempt 1 is the first retry delay after the
first failed operation.
"""

from __future__ import annotations

import random
from collections.abc import Callable


QUEUE_RETRY_BACKOFF_BASE_SECONDS = 10.0
QUEUE_RETRY_BACKOFF_CAP_SECONDS = 300.0


def exponential_backoff_seconds(
    attempt: int,
    *,
    base_seconds: float = QUEUE_RETRY_BACKOFF_BASE_SECONDS,
    cap_seconds: float = QUEUE_RETRY_BACKOFF_CAP_SECONDS,
    multiplier: float = 2.0,
    jitter_ratio: float = 0.0,
    minimum_seconds: float = 0.0,
    random_fn: Callable[[], float] | None = None,
) -> float:
    """Return a capped exponential backoff delay.

    `jitter_ratio=0.25` applies +/-25% multiplicative jitter after the cap.
    """
    if attempt <= 0:
        return 0.0
    if base_seconds < 0:
        raise ValueError("base_seconds must be >= 0")
    if cap_seconds < 0:
        raise ValueError("cap_seconds must be >= 0")
    if multiplier < 1:
        raise ValueError("multiplier must be >= 1")
    if jitter_ratio < 0:
        raise ValueError("jitter_ratio must be >= 0")
    if minimum_seconds < 0:
        raise ValueError("minimum_seconds must be >= 0")

    delay = min(
        cap_seconds,
        base_seconds * (multiplier ** max(0, attempt - 1)),
    )
    if jitter_ratio:
        rand = random_fn or random.random
        jitter = delay * jitter_ratio * (2 * rand() - 1)
        delay += jitter
    return max(minimum_seconds, delay)


def queue_retry_backoff_seconds(attempt: int) -> float:
    """Standard durable-queue retry schedule used by Fyralis workers."""
    return exponential_backoff_seconds(attempt)


__all__ = [
    "QUEUE_RETRY_BACKOFF_BASE_SECONDS",
    "QUEUE_RETRY_BACKOFF_CAP_SECONDS",
    "exponential_backoff_seconds",
    "queue_retry_backoff_seconds",
]
