"""Real-CompanyOS integration test (gated on ``LSOB_COMPANY_OS_REAL``).

Skipped by default. Runs two ``lsob run`` invocations end-to-end and
asserts at least one Layer-3 metric differs between the ``none`` and
``no-calibration`` ablations.

Activation requires BOTH:

    - environment flag ``LSOB_COMPANY_OS_REAL=1``
    - the parent Company OS packages (``services``, ``lib.shared.*``)
      are importable in this environment.
"""

from __future__ import annotations

import importlib
import json
import os
import sqlite3
from pathlib import Path

import pytest


REAL_FLAG = os.environ.get("LSOB_COMPANY_OS_REAL") == "1"


def _parent_importable() -> bool:
    for mod in (
        "services.ingestion.core",
        "services.bridge.queries",
        "services.think.reason",
        "lib.shared.db",
    ):
        try:
            importlib.import_module(mod)
        except Exception:
            return False
    return True


pytestmark = pytest.mark.skipif(
    not REAL_FLAG or not _parent_importable(),
    reason=(
        "real CompanyOS integration skipped: set LSOB_COMPANY_OS_REAL=1 "
        "AND install the parent Company OS package "
        "(see docs/COMPANY_OS_INTEGRATION.md)."
    ),
)


WORKSPACE_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = WORKSPACE_ROOT / "fixtures"


def _layer3_metrics(results_db: Path) -> dict[str, float]:
    """Read every Layer-3 metric from a run's ``results.db``."""
    with sqlite3.connect(results_db) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT metric_name, value FROM eval_results WHERE layer_id = 3"
        ).fetchall()
    return {r["metric_name"]: float(r["value"]) for r in rows}


def _invoke_cli(args: list[str]) -> int:
    from typer.testing import CliRunner

    from lsob_harness.cli import app

    runner = CliRunner()
    result = runner.invoke(app, args)
    if result.exit_code != 0:
        print(result.stdout)
    return result.exit_code


def test_company_os_real_ablation_shifts_layer3(tmp_path: Path) -> None:
    """End-to-end: ``no-calibration`` should move at least one Layer-3 metric."""
    runs_root = tmp_path / "runs"
    # Force the real client path. When LSOB_COMPANY_OS_REAL=1 is set we
    # assume the operator has DATABASE_URL / Ollama / LLM keys wired.
    os.environ["LSOB_COMPANY_OS_CLIENT"] = "local"

    # Baseline (none) run.
    rc_none = _invoke_cli(
        [
            "run",
            "--corpus", str(FIXTURES / "mini_corpus_a.json"),
            "--sut", "company-os",
            "--ablation", "none",
            "--layers", "3",
            "--runs-root", str(runs_root),
        ]
    )
    assert rc_none == 0

    # Ablated (no-calibration) run.
    rc_abl = _invoke_cli(
        [
            "run",
            "--corpus", str(FIXTURES / "mini_corpus_a.json"),
            "--sut", "company-os",
            "--ablation", "no-calibration",
            "--layers", "3",
            "--runs-root", str(runs_root),
        ]
    )
    assert rc_abl == 0

    # Locate the two result dbs; the filenames encode the ablation name.
    run_dirs = sorted(p for p in runs_root.iterdir() if p.is_dir())
    assert len(run_dirs) == 2

    none_db = next(
        (d / "results.db" for d in run_dirs if "-none-" in d.name),
        None,
    )
    abl_db = next(
        (d / "results.db" for d in run_dirs if "-no-calibration-" in d.name),
        None,
    )
    assert none_db is not None and none_db.exists(), run_dirs
    assert abl_db is not None and abl_db.exists(), run_dirs

    metrics_none = _layer3_metrics(none_db)
    metrics_abl = _layer3_metrics(abl_db)

    # Sanity: both runs produced at least one Layer-3 metric.
    assert metrics_none, "baseline run produced no Layer-3 metrics"
    assert metrics_abl, "ablated run produced no Layer-3 metrics"

    # Assertion: at least one metric differs between the two runs.
    shared = set(metrics_none) & set(metrics_abl)
    assert shared, "no overlapping Layer-3 metric names"
    deltas = {
        name: metrics_abl[name] - metrics_none[name]
        for name in shared
    }
    assert any(abs(d) > 1e-9 for d in deltas.values()), (
        f"expected no-calibration to shift at least one Layer-3 metric; "
        f"deltas={deltas}"
    )

    # Emit a small summary for the test log.
    print(json.dumps({"deltas": deltas}, indent=2))
