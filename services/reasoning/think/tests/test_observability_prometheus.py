"""Prometheus text exposition for the Think worker — render_prometheus_text().

Pure in-memory tests against the module-level METRICS singleton: no DB,
no worker. Each test starts from a reset METRICS so the rendered lines
are deterministic, and the fixture resets again on teardown so the
process-global singleton never leaks state into other test files.
"""
from __future__ import annotations

import pytest

from services.reasoning.think.observability import (
    _SAMPLE_CAP,
    METRICS,
    render_prometheus_text,
)


@pytest.fixture(autouse=True)
def _reset_metrics():
    METRICS.reset()
    yield
    METRICS.reset()


def test_inc_run_renders_runs_total_family():
    METRICS.inc_run("T1")
    text = render_prometheus_text()
    assert 'think_runs_total{trigger_kind="T1"} 1' in text
    assert "# TYPE think_runs_total counter" in text


def test_record_cost_renders_cost_family():
    METRICS.record_cost(
        "T1",
        cost_usd=0.5,
        input_tokens=1000,
        output_tokens=200,
        llm_calls=2,
    )
    text = render_prometheus_text()
    assert 'think_llm_cost_usd_total{trigger_kind="T1"} 0.5' in text
    assert 'think_llm_calls_total{trigger_kind="T1"} 2' in text
    assert 'think_llm_input_tokens_total{trigger_kind="T1"} 1000' in text
    assert 'think_llm_output_tokens_total{trigger_kind="T1"} 200' in text


def test_set_queue_depth_renders_aggregate_gauge():
    METRICS.set_queue_depth("all", 7)
    text = render_prometheus_text()
    assert "think_queue_depth 7" in text
    assert "# TYPE think_queue_depth gauge" in text


def test_queue_health_metrics_render_without_tenant_labels():
    METRICS.set_stale_trigger_locks(2)
    METRICS.inc_retry_exhausted("think_trigger_queue")
    text = render_prometheus_text()
    assert "think_trigger_stale_locks 2" in text
    assert (
        'think_queue_retry_exhausted_total{queue="think_trigger_queue"} 1'
        in text
    )
    assert "tenant" not in text


def test_observe_latency_renders_p95_line():
    # 100ms samples → 0.1s p95 (nearest-rank over the rolling window).
    for _ in range(10):
        METRICS.observe_latency("T1", 100.0)
    text = render_prometheus_text()
    assert 'think_run_latency_seconds_p95{trigger_kind="T1"} 0.1' in text
    assert (
        'think_run_latency_seconds_window_count{trigger_kind="T1"} 10' in text
    )


def test_inc_dropped_op_renders_both_labels():
    METRICS.inc_dropped_op("inadequate_falsifier", "claim")
    text = render_prometheus_text()
    assert (
        'think_validation_dropped_ops_total'
        '{reason="inadequate_falsifier",op_type="claim"} 1'
    ) in text


def test_latency_samples_trimmed_to_sample_cap():
    for i in range(_SAMPLE_CAP + 10):
        METRICS.observe_latency("T1", float(i))
    samples = METRICS.run_latency_ms["T1"]
    assert len(samples) == _SAMPLE_CAP
    # _trim drops the OLDEST samples: the window starts after the
    # overflow (samples 10.._SAMPLE_CAP+9 remain).
    assert samples[0] == 10.0
    assert samples[-1] == float(_SAMPLE_CAP + 9)
