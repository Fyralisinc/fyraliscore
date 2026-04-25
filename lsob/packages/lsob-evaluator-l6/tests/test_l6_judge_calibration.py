"""Calibration harness smoke test: MockJudge mode always writes a report."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from lsob_evaluator_l6.llm_judge import (
    LLMJudge,
    MockJudge,
    cohens_kappa,
    load_calibration_fixtures,
)
from lsob_evaluator_l6.llm_judge.calibration import run_calibration_sync

_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPT = (
    _REPO_ROOT
    / "packages"
    / "lsob-evaluator-l6"
    / "scripts"
    / "judge_calibration.py"
)


def test_calibration_fixtures_load():
    items = load_calibration_fixtures()
    assert len(items) == 50
    # Distribution matches the brief.
    labels = [i.human_label for i in items]
    assert labels.count("reference_wins") == 20
    assert labels.count("tie") == 20
    assert labels.count("sut_wins") == 10


def test_cohens_kappa_perfect_agreement():
    labels = ["reference_wins", "tie", "sut_wins", "tie", "sut_wins"]
    assert cohens_kappa(labels, labels) == pytest.approx(1.0)


def test_cohens_kappa_chance_agreement_near_zero():
    # If the "judge" always says "tie" and humans are mixed, kappa ~ 0.
    humans = ["reference_wins"] * 10 + ["tie"] * 10 + ["sut_wins"] * 10
    judge = ["tie"] * 30
    k = cohens_kappa(humans, judge)
    # Observed agreement is 1/3 and expected agreement under these marginals
    # equals observed, so kappa collapses to 0.
    assert k == pytest.approx(0.0, abs=1e-6)


def test_cohens_kappa_length_mismatch():
    with pytest.raises(ValueError):
        cohens_kappa(["tie"], ["tie", "tie"])


def test_run_calibration_mock_builds_report():
    judge = LLMJudge(judge_client=MockJudge())
    report = run_calibration_sync(judge)
    assert report.n_items == 50
    # Confusion matrix covers all three labels.
    assert set(report.confusion_matrix.keys()) == {
        "reference_wins",
        "tie",
        "sut_wins",
    }
    # Cost tracked (non-zero since MockJudge reports usage).
    assert report.input_tokens > 0
    assert report.output_tokens > 0
    # Kappa is a finite float (MockJudge is not expected to match humans).
    assert -1.0 <= report.cohens_kappa <= 1.0


def test_calibration_script_mock_writes_report(tmp_path: Path):
    out_dir = tmp_path / "reports"
    result = subprocess.run(
        [
            sys.executable,
            str(_SCRIPT),
            "--out",
            str(out_dir),
        ],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin", "HOME": str(tmp_path), **__import__("os").environ, "LSOB_RUN_REAL_JUDGE": "0"},
    )
    assert result.returncode == 0, result.stderr
    files = list(out_dir.glob("*-mock.json"))
    assert len(files) == 1
    payload = json.loads(files[0].read_text())
    assert payload["mode"] == "mock"
    assert payload["n_items"] == 50
    assert "cohens_kappa" in payload
    assert "confusion_matrix" in payload
    assert "cost" in payload
