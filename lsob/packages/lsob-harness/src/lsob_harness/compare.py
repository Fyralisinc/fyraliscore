"""Side-by-side run comparison rendering."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path

from lsob_harness import db as dbmod


def _aggregate(results_db: Path) -> tuple[str, OrderedDict[tuple[int, str], float]]:
    """Collapse eval_results into a single value per (layer, metric) pair.

    Multiple entries for the same pair (e.g. one per monthly checkpoint) are
    averaged so that ``compare`` can render a stable table.
    """
    with dbmod.open_db(results_db) as conn:
        cur = conn.execute(
            "SELECT run_id, layer_id, metric_name, value FROM eval_results ORDER BY idx"
        )
        run_id = ""
        rows = cur.fetchall()
    acc: dict[tuple[int, str], list[float]] = {}
    for row in rows:
        run_id = row["run_id"]
        key = (int(row["layer_id"]), str(row["metric_name"]))
        acc.setdefault(key, []).append(float(row["value"]))
    merged: OrderedDict[tuple[int, str], float] = OrderedDict()
    for k in sorted(acc):
        merged[k] = sum(acc[k]) / len(acc[k])
    return run_id, merged


def compare_runs(db_a: Path, db_b: Path) -> str:
    """Return a markdown side-by-side comparison table."""
    run_a, a = _aggregate(db_a)
    run_b, b = _aggregate(db_b)
    keys = sorted(set(a) | set(b))
    lines = [
        f"# Run comparison\n",
        f"- **A**: `{run_a}`",
        f"- **B**: `{run_b}`\n",
        "| Layer | Metric | A | B | Δ (B−A) |",
        "|------:|:-------|---:|---:|---:|",
    ]
    for layer, metric in keys:
        va = a.get((layer, metric))
        vb = b.get((layer, metric))
        va_s = f"{va:.4f}" if va is not None else "—"
        vb_s = f"{vb:.4f}" if vb is not None else "—"
        if va is not None and vb is not None:
            delta = vb - va
            delta_s = f"{delta:+.4f}"
        else:
            delta_s = "—"
        lines.append(f"| L{layer} | `{metric}` | {va_s} | {vb_s} | {delta_s} |")
    return "\n".join(lines) + "\n"


def find_run_db(runs_root: Path, run_id: str) -> Path:
    """Resolve a run_id to its ``results.db`` on disk."""
    candidate = runs_root / run_id / "results.db"
    if candidate.exists():
        return candidate
    raise FileNotFoundError(f"no results.db for run {run_id!r} under {runs_root}")
