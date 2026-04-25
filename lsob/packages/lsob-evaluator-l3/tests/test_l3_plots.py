"""Smoke tests for the reliability-diagram plot helper."""

from __future__ import annotations

from pathlib import Path

from lsob_evaluator_l3.metrics import bin_stats
from lsob_evaluator_l3.plots import reliability_diagram


def test_reliability_diagram_produces_nonzero_png(tmp_path: Path):
    preds = [
        (0.1, False),
        (0.2, False),
        (0.3, True),
        (0.5, False),
        (0.6, True),
        (0.8, True),
        (0.9, True),
    ]
    stats = bin_stats(preds, n_bins=4, mode="equal_frequency")
    out = tmp_path / "reliability.png"
    path = reliability_diagram(stats, out)
    assert path.exists()
    assert path.stat().st_size > 0


def test_reliability_diagram_empty_stats_still_writes(tmp_path: Path):
    out = tmp_path / "empty.png"
    path = reliability_diagram([], out, title="empty")
    assert path.exists()
    assert path.stat().st_size > 0
