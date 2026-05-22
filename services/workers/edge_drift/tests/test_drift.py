"""
services/workers/edge_drift/tests/test_drift.py — unit tests for the
drift checker logic (pure-Python; uses _check_tenant_sample so the
tests don't need a DB).
"""
from __future__ import annotations

from uuid import UUID

from lib.shared.ids import uuid7
from services.workers.edge_drift.worker import _check_tenant_sample


def _row(
    *,
    model_id=None,
    tenant_id=None,
    supporting_array=None,
    supports_edges=None,
    instance_of_targets=None,
    contributing_array=None,
    contributes_edges=None,
):
    """Build a sample row matching get_drift_sample's output shape."""
    return {
        "model_id": model_id or uuid7(),
        "tenant_id": tenant_id or uuid7(),
        "supporting_array": list(supporting_array or []),
        "supports_edges": list(supports_edges or []),
        "instance_of_targets": list(instance_of_targets or []),
        "contributing_array": list(contributing_array or []),
        "contributes_edges": list(contributes_edges or []),
    }


def test_perfect_alignment_no_drift():
    """Array == edges union → no drift."""
    a, b, p = uuid7(), uuid7(), uuid7()
    rows = [
        _row(
            supporting_array=[a, b, p],
            supports_edges=[a, b],
            instance_of_targets=[p],
            contributing_array=[],
            contributes_edges=[],
        )
    ]
    report = _check_tenant_sample(rows)
    assert report.supports_drift_models == 0
    assert report.contributes_drift_models == 0
    assert report.drift_examples == []


def test_drift_when_array_has_extra():
    """An id appears in the array but no edge mirrors it."""
    a = uuid7()
    rows = [
        _row(
            supporting_array=[a],
            supports_edges=[],
            instance_of_targets=[],
        )
    ]
    report = _check_tenant_sample(rows)
    assert report.supports_drift_models == 1
    assert len(report.drift_examples) == 1
    ex = report.drift_examples[0]
    assert ex["kind"] == "supports"
    assert str(a) in ex["missing_in_edges"]


def test_drift_when_edges_have_extra():
    """An edge exists but the array doesn't include the source —
    means a producer wrote the edge without updating the array."""
    a = uuid7()
    rows = [
        _row(
            supporting_array=[],
            supports_edges=[a],
            instance_of_targets=[],
        )
    ]
    report = _check_tenant_sample(rows)
    assert report.supports_drift_models == 1
    ex = report.drift_examples[0]
    assert str(a) in ex["extra_in_edges"]


def test_drift_contributing_path_is_independent():
    """Drift in contributing_models must be reported separately from
    supports drift."""
    a = uuid7()
    rows = [
        _row(
            supporting_array=[],
            supports_edges=[],
            contributing_array=[a],
            contributes_edges=[],
        )
    ]
    report = _check_tenant_sample(rows)
    assert report.contributes_drift_models == 1
    assert report.supports_drift_models == 0


def test_pattern_back_link_does_not_trigger_drift():
    """A pattern id in the array (legacy back-link) with a
    corresponding instance_of edge must NOT be flagged."""
    constituent = uuid7()
    pattern = uuid7()
    rows = [
        _row(
            model_id=constituent,
            supporting_array=[pattern],
            supports_edges=[],
            instance_of_targets=[pattern],
        )
    ]
    report = _check_tenant_sample(rows)
    assert report.supports_drift_models == 0


def test_examples_capped():
    """drift_examples is capped to avoid unbounded payload size."""
    bad_rows = []
    for _ in range(20):
        a = uuid7()
        bad_rows.append(
            _row(
                supporting_array=[a],
                supports_edges=[],
            )
        )
    report = _check_tenant_sample(bad_rows)
    # 20 drifted models, but examples capped at 5.
    assert report.supports_drift_models == 20
    assert len(report.drift_examples) == 5


def test_empty_sample_returns_empty_report():
    report = _check_tenant_sample([])
    assert report.models_sampled == 0
    assert report.supports_drift_models == 0
    assert report.contributes_drift_models == 0
