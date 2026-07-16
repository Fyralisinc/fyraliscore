from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from lib.evaluation.company_learning import CompanyLearningEvaluationState
from lib.evaluation.proof import (
    InvariantEvidenceBundle,
    InvariantEvidenceManifest,
    InvariantProofMatrixReport,
)


pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[4]


async def test_company_learning_vitals_cli_runs_and_rerenders_from_artifacts(
    tmp_path: Path,
) -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not set — skipping real-Postgres CLI smoke test.")

    report_dir = tmp_path / "company-learning-vitals-cli"
    run_id = "pytest-company-learning-vitals-cli"
    system_version = "pytest-cli-system"
    env = {**os.environ, "DATABASE_URL": database_url}

    first = await _run_cli(
        REPO_ROOT / "scripts" / "run_company_learning_vitals_harness.py",
        "--report-dir",
        str(report_dir),
        "--run-id",
        run_id,
        "--system-version",
        system_version,
        cwd=REPO_ROOT,
        env=env,
    )

    assert first.returncode == 0, first.stderr
    assert f"report_dir={report_dir}" in first.stdout
    assert f"vitals_dir={report_dir / 'vitals'}" in first.stdout
    assert "working_version_status=ok" in first.stdout
    assert "adaptive_lift=1.0" in first.stdout

    pair_path = report_dir / "company_learning_scenario_evidence.json"
    vitals_dir = report_dir / "vitals"
    scorecard_path = vitals_dir / "vitals_scorecard.json"
    summary_path = vitals_dir / "vitals_summary.md"
    evaluation_path = vitals_dir / "company_learning_evaluation.json"
    evidence_bundle_path = (
        vitals_dir / "company_learning_evidence_bundle.json"
    )

    for artifact_path in (
        pair_path,
        scorecard_path,
        summary_path,
        evaluation_path,
        evidence_bundle_path,
    ):
        assert artifact_path.is_file(), artifact_path

    pair = json.loads(pair_path.read_text(encoding="utf-8"))
    scorecard = json.loads(scorecard_path.read_text(encoding="utf-8"))
    evaluation = json.loads(evaluation_path.read_text(encoding="utf-8"))
    evidence_bundle = json.loads(
        evidence_bundle_path.read_text(encoding="utf-8")
    )
    summary = summary_path.read_text(encoding="utf-8")

    pair_metrics = pair["report"]["metrics"]
    experiment = scorecard["company_physics"]["experiments"][
        "corrective_memory_recurrence"
    ]
    vitals_metrics = experiment["metrics"]

    assert pair["report"]["status"] == "observed"
    assert pair_metrics["pair_count"] == 3
    assert pair_metrics["adaptive_correctness_rate"] == 1.0
    assert pair_metrics["frozen_correctness_rate"] == 0.0
    assert pair_metrics["adaptive_minus_frozen_correctness"] == 1.0
    assert vitals_metrics["adaptive_correctness_rate"] == 1.0
    assert vitals_metrics["frozen_correctness_rate"] == 0.0
    assert vitals_metrics["adaptive_minus_frozen_correctness"] == 1.0
    assert evaluation["available"] is True
    assert evaluation["evidence_bundle"] == evidence_bundle
    assert evidence_bundle["evidence"]
    assert scorecard["vitals_measurement_profile"] == "company_learning_only"
    assert scorecard["overall_score"] is None
    assert scorecard["scored_vitals"] == 0
    assert scorecard["score_coverage"] == 0.0
    assert "# Company Understanding Vitals" in summary
    assert "Adaptive vs frozen correctness: 1.0000 vs 0.0000" in summary
    assert "Adaptive correctness lift: 1.0000" in summary

    live_evidence_bundle_bytes = evidence_bundle_path.read_bytes()
    live_evaluation_cutoff = evaluation["evaluation_cutoff"]
    live_state = CompanyLearningEvaluationState.model_validate(
        evaluation["state"]
    )
    live_manifest = InvariantEvidenceManifest.model_validate(
        evaluation["evidence_manifest"]
    )
    live_bundle = InvariantEvidenceBundle.model_validate(
        evaluation["evidence_bundle"]
    )
    live_proof = InvariantProofMatrixReport.model_validate(
        evaluation["invariant_proof"]
    )
    live_company_physics = scorecard["company_physics"]
    live_measurement_profile = scorecard["vitals_measurement_profile"]
    rerender_env = dict(env)
    rerender_env.pop("DATABASE_URL", None)

    rerender = await _run_cli(
        REPO_ROOT / "scripts" / "run_company_vitals_harness.py",
        "--report-dir",
        str(report_dir),
        cwd=REPO_ROOT,
        env=rerender_env,
    )

    assert rerender.returncode == 0, rerender.stderr
    assert f"wrote vitals artifacts to {vitals_dir}" in rerender.stdout
    assert evidence_bundle_path.read_bytes() == live_evidence_bundle_bytes

    rerendered_evaluation = json.loads(
        evaluation_path.read_text(encoding="utf-8")
    )
    rerendered_scorecard = json.loads(
        scorecard_path.read_text(encoding="utf-8")
    )
    assert rerendered_evaluation["available"] is True
    assert rerendered_evaluation["status"] == evaluation["status"]
    assert (
        rerendered_evaluation["observed_slice_health"]
        == evaluation["observed_slice_health"]
    )
    assert (
        rerendered_evaluation["evaluation_cutoff"]
        == live_evaluation_cutoff
    )
    assert CompanyLearningEvaluationState.model_validate(
        rerendered_evaluation["state"]
    ) == live_state
    assert InvariantEvidenceManifest.model_validate(
        rerendered_evaluation["evidence_manifest"]
    ) == live_manifest
    assert InvariantEvidenceBundle.model_validate(
        rerendered_evaluation["evidence_bundle"]
    ) == live_bundle
    assert InvariantProofMatrixReport.model_validate(
        rerendered_evaluation["invariant_proof"]
    ) == live_proof
    rerendered_company_physics = rerendered_scorecard["company_physics"]
    assert rerendered_company_physics["status"] == live_company_physics["status"]
    assert (
        rerendered_company_physics["learning_loop"]
        == live_company_physics["learning_loop"]
    )
    assert (
        rerendered_company_physics["incident_counts"]
        == live_company_physics["incident_counts"]
    )
    assert (
        rerendered_company_physics["experiments"]
        == live_company_physics["experiments"]
    )
    assert rerendered_scorecard["overall_score"] is None
    assert rerendered_scorecard["scored_vitals"] == 0
    assert (
        rerendered_scorecard["vitals_measurement_profile"]
        == live_measurement_profile
    )
    rerendered_summary = summary_path.read_text(encoding="utf-8")
    assert "# Company Understanding Vitals" in rerendered_summary
    assert (
        "Adaptive vs frozen correctness: 1.0000 vs 0.0000"
        in rerendered_summary
    )
    assert "Adaptive correctness lift: 1.0000" in rerendered_summary


async def _run_cli(
    script: Path,
    *arguments: str,
    cwd: Path,
    env: dict[str, str],
) -> _CompletedProcess:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(script),
        *arguments,
        cwd=cwd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return _CompletedProcess(
        returncode=process.returncode,
        stdout=stdout.decode(),
        stderr=stderr.decode(),
    )


@dataclass(frozen=True)
class _CompletedProcess:
    returncode: int
    stdout: str
    stderr: str
