"""Decorator for real-LLM tests with retry, flake tracking, and timeout budget."""

from __future__ import annotations

import inspect
from functools import wraps
from typing import Callable

import pytest

from tests.real_llm.infrastructure import flake_tracker


def real_llm_test(
    attempts: int = 3,
    pass_threshold: int = 2,
    timeout_seconds: int = 300,
    tags: list[str] | None = None,
):
    """Mark a test as real-LLM with retry semantics and flake tracking."""

    def decorator(test_func: Callable):
        clamped_threshold = min(pass_threshold, attempts)
        is_async = inspect.iscoroutinefunction(test_func)

        if is_async:
            @wraps(test_func)
            async def wrapper(*args, **kwargs):
                await _run_with_retries(test_func, args, kwargs, attempts, clamped_threshold, is_async=True)
        else:
            @wraps(test_func)
            def wrapper(*args, **kwargs):
                _run_with_retries_sync(test_func, args, kwargs, attempts, clamped_threshold)

        wrapped = wrapper
        wrapped = pytest.mark.timeout(timeout_seconds * attempts)(wrapped)
        wrapped = pytest.mark.real_llm(wrapped)
        wrapped._real_llm_tags = list(tags) if tags else []
        wrapped._real_llm_attempts = attempts
        wrapped._real_llm_pass_threshold = clamped_threshold
        wrapped._real_llm_timeout_seconds = timeout_seconds
        return wrapped

    return decorator


async def _run_with_retries(test_func, args, kwargs, attempts, pass_threshold, *, is_async):
    """Async retry loop. Mirrors the sync version; kept separate to preserve await semantics."""
    passes = 0
    failures: list[tuple[int, str]] = []
    name = test_func.__name__
    for attempt in range(1, attempts + 1):
        try:
            await test_func(*args, **kwargs)
        except (AssertionError, pytest.fail.Exception) as e:
            failures.append((attempt, str(e)))
            flake_tracker.record_attempt(name, attempt, "fail", str(e))
        except BaseException:
            # Real bug, not a flake. Still flush any buffered attempts.
            flake_tracker.record_final(name, passes, attempts, pass_threshold)
            raise
        else:
            passes += 1
            flake_tracker.record_attempt(name, attempt, "pass")
            if passes >= pass_threshold:
                break

    flake_tracker.record_final(name, passes, attempts, pass_threshold)
    _maybe_fail(name, passes, attempts, pass_threshold, failures)


def _run_with_retries_sync(test_func, args, kwargs, attempts, pass_threshold):
    """Sync retry loop for non-async tests."""
    passes = 0
    failures: list[tuple[int, str]] = []
    name = test_func.__name__
    for attempt in range(1, attempts + 1):
        try:
            test_func(*args, **kwargs)
        except (AssertionError, pytest.fail.Exception) as e:
            failures.append((attempt, str(e)))
            flake_tracker.record_attempt(name, attempt, "fail", str(e))
        except BaseException:
            flake_tracker.record_final(name, passes, attempts, pass_threshold)
            raise
        else:
            passes += 1
            flake_tracker.record_attempt(name, attempt, "pass")
            if passes >= pass_threshold:
                break

    flake_tracker.record_final(name, passes, attempts, pass_threshold)
    _maybe_fail(name, passes, attempts, pass_threshold, failures)


def _maybe_fail(name, passes, attempts, pass_threshold, failures):
    """Call pytest.fail with all per-attempt failures if the threshold was not reached."""
    if passes >= pass_threshold:
        return
    detail = "\n".join(f"  attempt {a}: {msg}" for a, msg in failures) or "  (no failure details)"
    pytest.fail(
        f"{name}: passed {passes}/{attempts}, needed {pass_threshold}.\n"
        f"Per-attempt failures:\n{detail}"
    )
