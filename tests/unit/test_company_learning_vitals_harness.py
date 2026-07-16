from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.company_vitals import VitalsArtifacts, build_vitals_from_report_dir
from scripts.run_company_learning_vitals_harness import (
    _working_version_failures,
    write_company_learning_report_shell,
)


def test_focused_report_shell_does_not_score_unmeasured_product_vitals(
    tmp_path: Path,
) -> None:
    report_dir = tmp_path / "focused-company-learning"
    write_company_learning_report_shell(
        report_dir,
        run_id="focused-run",
        system_version="focused-system",
        tenant_id="0190f21f-fcb2-7d75-b235-648ee24c8c92",
        observation_ids=(
            "0190f21f-fcb2-7d75-b235-648ee24c8c93",
            "0190f21f-fcb2-7d75-b235-648ee24c8c94",
        ),
    )

    scorecard = build_vitals_from_report_dir(report_dir)

    assert scorecard["vitals_measurement_profile"] == "company_learning_only"
    assert scorecard["overall_score"] is None
    assert scorecard["scored_vitals"] == 0
    assert {
        vital["status"] for vital in scorecard["vitals"].values()
    } == {"not_observed"}
    assert {
        vital["metrics"]["measurement_status"]
        for vital in scorecard["vitals"].values()
    } == {"not_measured"}


def test_working_version_gate_rejects_missing_and_unavailable_proof(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "vitals"
    output_dir.mkdir()
    result = VitalsArtifacts(
        report_dir=tmp_path,
        output_dir=output_dir,
        scorecard={
            "hard_failures": [],
            "company_physics": {
                "status": "unavailable",
                "scope": {},
                "hard_failures": [],
                "experiments": {},
            },
        },
        signal_metabolism_rows=[],
        db_trace_summary={},
    )

    failures = _working_version_failures(result)

    assert "DB-backed Company Physics evaluation is unavailable" in failures
    assert "typed corrective-memory experiment is unavailable" in failures
    assert (
        "corrective-memory experiment did not reach observed status"
        in failures
    )
    assert "adaptive-versus-frozen correctness lift is missing" in failures
    assert any("artifact is missing" in failure for failure in failures)


def test_unknown_measurement_profile_fails_closed(tmp_path: Path) -> None:
    report_dir = tmp_path / "unknown-profile"
    write_company_learning_report_shell(
        report_dir,
        run_id="unknown-profile-run",
        system_version="unknown-profile-system",
        tenant_id="0190f21f-fcb2-7d75-b235-648ee24c8c92",
        observation_ids=(
            "0190f21f-fcb2-7d75-b235-648ee24c8c93",
            "0190f21f-fcb2-7d75-b235-648ee24c8c94",
        ),
    )
    run_summary_path = report_dir / "run_summary.json"
    run_summary = json.loads(run_summary_path.read_text(encoding="utf-8"))
    run_summary["vitals_measurement_profile"] = "typo"
    run_summary_path.write_text(json.dumps(run_summary), encoding="utf-8")

    with pytest.raises(ValueError, match="unknown Vitals measurement profile"):
        build_vitals_from_report_dir(report_dir)


def test_working_version_gate_accepts_complete_zero_incident_proof(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "vitals"
    output_dir.mkdir()
    for path in (
        tmp_path / "company_learning_scenario_evidence.json",
        output_dir / "company_learning_evaluation.json",
        output_dir / "company_learning_evidence_bundle.json",
        output_dir / "vitals_scorecard.json",
    ):
        path.write_text(json.dumps({}), encoding="utf-8")
    result = VitalsArtifacts(
        report_dir=tmp_path,
        output_dir=output_dir,
        scorecard={
            "hard_failures": [],
            "company_physics": {
                "status": "insufficient",
                "scope": {"tenant_id": "tenant"},
                "hard_failures": [],
                "experiments": {
                    "corrective_memory_recurrence": {
                        "available": True,
                        "status": "observed",
                        "hard_safety_incident_count": 0,
                        "metrics": {
                            "adaptive_minus_frozen_correctness": 1.0,
                        },
                    }
                },
            },
        },
        signal_metabolism_rows=[],
        db_trace_summary={},
    )

    assert _working_version_failures(result) == []
