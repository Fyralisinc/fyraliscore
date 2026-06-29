from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import httpx


SleepFn = Callable[[float], Awaitable[None]]
RandomFn = Callable[[], float]


@dataclass(frozen=True)
class HttpRetryConfig:
    max_attempts: int = 3
    initial_backoff_s: float = 0.1
    max_backoff_s: float = 1.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.initial_backoff_s < 0:
            raise ValueError("initial_backoff_s must be >= 0")
        if self.max_backoff_s < self.initial_backoff_s:
            raise ValueError("max_backoff_s must be >= initial_backoff_s")
        if not 0 <= self.jitter_ratio <= 1:
            raise ValueError("jitter_ratio must be between 0 and 1")


RETRYABLE_HTTP_STATUSES = frozenset({408, 409, 425, 429, 500, 502, 503, 504})


def is_retryable_httpx_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_HTTP_STATUSES
    return isinstance(exc, (httpx.TimeoutException, httpx.NetworkError))


def retry_delay_s(
    *,
    attempt_index: int,
    config: HttpRetryConfig,
    random_fn: RandomFn = random.random,
) -> float:
    base = min(
        config.max_backoff_s,
        config.initial_backoff_s * (2**attempt_index),
    )
    if base <= 0 or config.jitter_ratio <= 0:
        return base
    low = 1.0 - config.jitter_ratio
    high = 1.0 + config.jitter_ratio
    return base * (low + ((high - low) * random_fn()))


async def sleep_before_retry(
    *,
    attempt_index: int,
    config: HttpRetryConfig,
    sleep: SleepFn = asyncio.sleep,
    random_fn: RandomFn = random.random,
) -> None:
    delay = retry_delay_s(
        attempt_index=attempt_index,
        config=config,
        random_fn=random_fn,
    )
    if delay > 0:
        await sleep(delay)
