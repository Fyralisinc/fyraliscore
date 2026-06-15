from __future__ import annotations

import time

from services.platform.execution import inquiry
from services.platform.execution import runtime_metrics


def test_runtime_metrics_keep_legacy_inquiry_identity() -> None:
    assert inquiry._append_stage_timing is runtime_metrics.append_stage_timing
    assert inquiry._elapsed_ms is runtime_metrics.elapsed_ms
    assert inquiry._runtime_residual_summary is runtime_metrics.runtime_residual_summary


def test_runtime_metrics_append_stage_timing_skips_none_extras() -> None:
    timings: list[dict[str, object]] = []

    runtime_metrics.append_stage_timing(
        timings,
        "question_planning",
        time.perf_counter(),
        round_index=1,
        skipped=None,
    )

    assert timings[0]["stage"] == "question_planning"
    assert "elapsed_ms" in timings[0]
    assert timings[0]["round_index"] == 1
    assert "skipped" not in timings[0]


def test_runtime_metrics_residual_summary_tolerates_bad_elapsed_values() -> None:
    summary = runtime_metrics.runtime_residual_summary(
        total_ms=100,
        action_timings=[
            {"elapsed_ms": "40"},
            {"elapsed_ms": "bad"},
        ],
        stage_timings=[
            {"elapsed_ms": 25},
            {"elapsed_ms": None},
        ],
    )

    assert summary == {
        "total_ms": 100,
        "retrieval_action_timings_ms_total": 40,
        "retrieval_stage_timings_ms_total": 25,
        "measured_ms_total": 65,
        "unaccounted_ms": 35,
    }
