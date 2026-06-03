"""Tests for the Prometheus text exposition of webhook metrics (FR-011).

The in-process counters were always recorded; this renderer is the
scrape path that was missing. We assert the exposition format (HELP/TYPE
lines, `{provider,reason}` labels, values) and label escaping.
"""
from __future__ import annotations

import pytest

from services.app.webhooks import metrics


@pytest.fixture(autouse=True)
def _clean_metrics():
    metrics.reset()
    yield
    metrics.reset()


def _lines(text: str) -> list[str]:
    return text.splitlines()


def test_verification_failures_rendered_with_provider_reason_labels() -> None:
    metrics.record_failure("slack", "signature_mismatch")
    metrics.record_failure("slack", "signature_mismatch")
    metrics.record_failure("github", "expired_timestamp")

    text = metrics.render_prometheus()
    lines = _lines(text)

    assert "# TYPE webhook_verification_failures_total counter" in lines
    assert any(line.startswith("# HELP webhook_verification_failures_total")
               for line in lines)
    assert (
        'webhook_verification_failures_total'
        '{provider="slack",reason="signature_mismatch"} 2'
    ) in lines
    assert (
        'webhook_verification_failures_total'
        '{provider="github",reason="expired_timestamp"} 1'
    ) in lines


def test_all_counter_families_present() -> None:
    metrics.record_failure("slack", "signature_mismatch")
    metrics.record_resolver_outcome("github", "resolved")
    metrics.record_resolver_cache("github", "hit")
    metrics.record_kafka_path_outcome("stripe", "fallback")
    metrics.observe_resolver_duration("github", 0.012)

    text = metrics.render_prometheus()

    for name, mtype in [
        ("webhook_verification_failures_total", "counter"),
        ("webhook_resolver_outcomes_total", "counter"),
        ("webhook_resolver_cache_total", "counter"),
        ("webhook_router_kafka_path_total", "counter"),
        ("webhook_resolver_duration_p95_seconds", "gauge"),
    ]:
        assert f"# TYPE {name} {mtype}" in text

    assert 'webhook_resolver_outcomes_total{provider="github",outcome="resolved"} 1' in text
    assert 'webhook_resolver_cache_total{provider="github",result="hit"} 1' in text
    assert 'webhook_router_kafka_path_total{provider="stripe",outcome="fallback"} 1' in text
    # p95 over a single 0.012 sample is 0.012.
    assert 'webhook_resolver_duration_p95_seconds{provider="github"} 0.012' in text


def test_empty_metrics_still_emit_help_type_headers() -> None:
    """A freshly-reset registry still advertises every metric name so a
    scraper sees the series exist (valid 0.0.4 — TYPE without samples)."""
    text = metrics.render_prometheus()
    assert "# TYPE webhook_verification_failures_total counter" in text
    # No sample rows, but the family header is present.
    assert "webhook_verification_failures_total{" not in text
    # Always newline-terminated.
    assert text.endswith("\n")


def test_label_values_are_escaped() -> None:
    # Defensive: a backslash/quote/newline in a label value must be
    # escaped so the exposition stays parseable. Providers/reasons are
    # closed enums, but the renderer must not assume that.
    metrics.record_failure('we"ird\\', "line\nbreak")
    text = metrics.render_prometheus()
    assert r'provider="we\"ird\\"' in text
    assert r'reason="line\nbreak"' in text
    # And the rendered line has no raw newline inside the label.
    failure_lines = [
        ln for ln in _lines(text)
        if ln.startswith("webhook_verification_failures_total{")
    ]
    assert len(failure_lines) == 1


def test_render_is_consistent_snapshot_under_lock() -> None:
    # Sanity: render doesn't deadlock on the non-reentrant lock when it
    # also needs the p95 (which has its own lock). Recording samples then
    # rendering must succeed.
    for v in (0.01, 0.02, 0.03, 0.04):
        metrics.observe_resolver_duration("slack", v)
    text = metrics.render_prometheus()
    assert 'webhook_resolver_duration_p95_seconds{provider="slack"}' in text
