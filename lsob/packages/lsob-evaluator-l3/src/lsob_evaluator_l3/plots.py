"""Matplotlib plotting helpers for Layer 3.

Always uses the ``Agg`` backend so the module is safe to import during headless tests.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402 - must follow backend selection

from lsob_evaluator_l3.metrics import BinStat, wilson_interval  # noqa: E402


def reliability_diagram(
    bin_stats: Iterable[BinStat],
    output_path: str | os.PathLike[str],
    title: str = "Reliability diagram",
) -> Path:
    """Render a reliability diagram with Wilson 95% CIs per bin.

    Returns the absolute path of the written PNG.
    """
    stats = list(bin_stats)
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    xs: list[float] = []
    ys: list[float] = []
    y_err_low: list[float] = []
    y_err_high: list[float] = []
    counts: list[int] = []
    for s in stats:
        successes = int(round(s.empirical_accuracy * s.count))
        lo, hi = wilson_interval(successes, s.count)
        xs.append(s.mean_confidence)
        ys.append(s.empirical_accuracy)
        y_err_low.append(max(0.0, s.empirical_accuracy - lo))
        y_err_high.append(max(0.0, hi - s.empirical_accuracy))
        counts.append(s.count)

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.plot([0.0, 1.0], [0.0, 1.0], linestyle="--", color="grey", label="perfect")
    if xs:
        ax.errorbar(
            xs,
            ys,
            yerr=[y_err_low, y_err_high],
            fmt="o",
            capsize=4,
            label="observed (95% Wilson CI)",
        )
        for x, y, c in zip(xs, ys, counts, strict=False):
            ax.annotate(f"n={c}", (x, y), textcoords="offset points", xytext=(5, 5), fontsize=8)
    ax.set_xlim(0.0, 1.0)
    ax.set_ylim(0.0, 1.0)
    ax.set_xlabel("Mean asserted confidence")
    ax.set_ylabel("Empirical accuracy")
    ax.set_title(title)
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(path, format="png", dpi=100)
    plt.close(fig)
    return path.resolve()
