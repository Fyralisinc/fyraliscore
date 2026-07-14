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
        elapsed = _safe_elapsed_ms(note)
        if elapsed is None:
            continue
        total += elapsed
    return total


def _safe_elapsed_ms(note: dict[str, Any]) -> int | None:
    try:
        return int(note.get("elapsed_ms") or 0)
    except (TypeError, ValueError):
        return None


def _is_in_flight_wait(note: dict[str, Any]) -> bool:
    return (
        bool(note.get("in_flight_wait"))
        or note.get("timing_kind") == "in_flight_wait"
    )


def runtime_residual_summary(
    *,
    total_ms: int,
    action_timings: list[dict[str, Any]],
    stage_timings: list[dict[str, Any]],
) -> dict[str, Any]:
    action_total = sum_elapsed_ms(action_timings)
    action_wait_total = sum_elapsed_ms(
        [note for note in action_timings if _is_in_flight_wait(note)]
    )
    action_work_total = max(0, action_total - action_wait_total)
    stage_total = sum_elapsed_ms(stage_timings)
    work_measured_total = action_work_total + stage_total
    return {
        "total_ms": total_ms,
        "retrieval_action_timings_ms_total": action_total,
        "retrieval_action_work_timings_ms_total": action_work_total,
        "retrieval_action_wait_timings_ms_total": action_wait_total,
        "retrieval_stage_timings_ms_total": stage_total,
        "measured_ms_total": action_total + stage_total,
        "non_wait_measured_ms_total": work_measured_total,
        "parallel_wait_overcount_ms": action_wait_total,
        "unaccounted_ms": max(0, total_ms - action_total - stage_total),
        "work_unaccounted_ms": max(0, total_ms - work_measured_total),
    }


__all__ = [
    "append_stage_timing",
    "elapsed_ms",
    "runtime_residual_summary",
    "sum_elapsed_ms",
]
