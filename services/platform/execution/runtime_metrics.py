"""Runtime timing helpers for adaptive inquiry execution."""

from __future__ import annotations

import time
from typing import Any


def elapsed_ms(started: float) -> int:
    return int((time.perf_counter() - started) * 1000)


def append_stage_timing(
    timings: list[dict[str, Any]],
    stage: str,
    started: float,
    **extra: Any,
) -> None:
    note = {
        "stage": stage,
        "elapsed_ms": elapsed_ms(started),
    }
    for key, value in extra.items():
        if value is not None:
            note[key] = value
    timings.append(note)


def sum_elapsed_ms(notes: list[dict[str, Any]]) -> int:
    total = 0
    for note in notes:
        try:
            total += int(note.get("elapsed_ms") or 0)
        except (TypeError, ValueError):
            continue
    return total


def runtime_residual_summary(
    *,
    total_ms: int,
    action_timings: list[dict[str, Any]],
    stage_timings: list[dict[str, Any]],
) -> dict[str, Any]:
    action_total = sum_elapsed_ms(action_timings)
    stage_total = sum_elapsed_ms(stage_timings)
    return {
        "total_ms": total_ms,
        "retrieval_action_timings_ms_total": action_total,
        "retrieval_stage_timings_ms_total": stage_total,
        "measured_ms_total": action_total + stage_total,
        "unaccounted_ms": max(0, total_ms - action_total - stage_total),
    }


__all__ = [
    "append_stage_timing",
    "elapsed_ms",
    "runtime_residual_summary",
    "sum_elapsed_ms",
]
