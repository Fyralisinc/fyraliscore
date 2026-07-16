from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest

from scripts.run_company_learning_assurance_suite import (
    SUMMARY_ARTIFACT_NAME,
    CompanyLearningAssuranceSummary,
)


pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[4]


async def test_company_learning_assurance_suite_cli_writes_one_summary(
    tmp_path: Path,
) -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not set — skipping assurance CLI test.")

    output_dir = tmp_path / "company-learning-assurance"
    result = await _run_cli(
        REPO_ROOT / "scripts" / "run_company_learning_assurance_suite.py",
        "--output-dir",
        str(output_dir),
        "--run-id",
        "pytest-company-learning-assurance",
        "--system-version",
        "pytest-system",
        cwd=REPO_ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
    )

    assert result.returncode == 0, result.stderr
    summary_path = output_dir / SUMMARY_ARTIFACT_NAME
    assert f"summary={summary_path}" in result.stdout
    assert "status=working" in result.stdout
    assert "positive_lift=1.0" in result.stdout
    assert "negative_incidents=0" in result.stdout
    assert "slack_status=observed" in result.stdout

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    summary_digest = payload.pop("summary_digest")
    summary = CompanyLearningAssuranceSummary.model_validate(payload)

    assert summary.status == "working"
    assert summary.blocking_failures == ()
    assert summary.positive.status == "observed"
    assert summary.positive.pair_count == 3
    assert summary.positive.adaptive_correctness_rate == 1.0
    assert summary.positive.frozen_correctness_rate == 0.0
    assert summary.positive.adaptive_minus_frozen_correctness == 1.0
    assert summary.positive.hard_failures == ()
    assert summary.negative.status == "observed"
    assert summary.negative.pair_count == 4
    assert summary.negative.safety_incident_count == 0
    assert summary.negative.adaptive_unsafe_count == 0
    assert summary.negative.frozen_unsafe_count == 0
    assert summary.slack.status == "observed"
    assert summary.slack.diagnostic_only is True
    assert summary.slack.metrics["case_count"] == 9
    assert summary.slack.metrics["correct_case_rate"] == 1.0
    assert summary.slack.metrics["supported_case_rate"] == 1.0
    assert summary.slack.metrics["mean_sufficient_set_recall"] == 1.0
    assert summary.slack.metrics["selected_context_precision"] == 1.0
    assert summary.slack.metrics["contamination_rate"] == 0.0
    assert summary.slack.metrics["mean_topology_recall"] == 1.0
    assert summary.slack.metrics["budget_adherence_rate"] == 1.0
    assert not any(
        gap.startswith("slack: Gold family not yet sealed:")
        for gap in summary.proof_gaps
    )
    assert summary.population is not None
    assert summary.population.status == "observed_with_gaps"
    assert summary.population.registry_pair_count == 60
    assert summary.population.observed_pair_count == 15
    assert summary.population.unsupported_case_count == 45
    assert summary.population.runtime_support_rate == 0.25
    assert summary.population.metrics["pair_count"] == 60
    assert summary.population.metrics["observed_pair_count"] == 15
    assert summary.population.metrics["unsupported_case_count"] == 45
    assert summary.population.metrics["complete_population"] is True
    assert summary.population.unsupported_strata_counts["entity_type"] == {
        "project": 15,
        "system": 15,
        "team": 15,
    }
    assert sum(summary.population.unsupported_reason_counts.values()) == 45
    assert any(
        "Slack reconstruction remains diagnostic and non-blocking"
        in gap
        for gap in summary.proof_gaps
    )
    assert any(
        "does not execute that convergence burn"
        in gap
        for gap in summary.proof_gaps
    )
    assert any(
        "population: runtime coverage observed 15/60 sealed cases"
        in gap
        for gap in summary.proof_gaps
    )
    assert all(
        Path(path).is_file()
        for path in summary.artifact_paths.values()
    )
    assert all(
        len(digest) == 64
        for digest in summary.component_digests.values()
    )
    assert summary_digest == summary.digest

    positive_pair = json.loads(
        Path(summary.artifact_paths["positive_pair"]).read_text(
            encoding="utf-8"
        )
    )
    negative = json.loads(
        Path(summary.artifact_paths["negative_evidence"]).read_text(
            encoding="utf-8"
        )
    )
    slack = json.loads(
        Path(summary.artifact_paths["slack_report"]).read_text(
            encoding="utf-8"
        )
    )
    population = json.loads(
        Path(summary.artifact_paths["population_evidence"]).read_text(
            encoding="utf-8"
        )
    )
    assert (
        positive_pair["report"]["metrics"][
            "adaptive_minus_frozen_correctness"
        ]
        == summary.positive.adaptive_minus_frozen_correctness
    )
    assert len(negative["report"]["incidents"]) == (
        summary.negative.safety_incident_count
    )
    assert slack["report"]["metrics"] == summary.slack.metrics
    assert population["population_report"]["pair_count"] == 60
    assert population["population_report"]["observed_pair_count"] == 15
    assert population["population_report"]["unsupported_case_count"] == 45

    persisted_summary_path = (
        output_dir
        / "positive"
        / "vitals"
        / SUMMARY_ARTIFACT_NAME
    )
    persisted_payload = json.loads(
        persisted_summary_path.read_text(encoding="utf-8")
    )
    persisted_digest = persisted_payload.pop("summary_digest")
    persisted_summary = CompanyLearningAssuranceSummary.model_validate(
        persisted_payload
    )
    assert persisted_summary == summary
    assert persisted_digest == summary.digest

    vitals_scorecard = json.loads(
        (
            output_dir
            / "positive"
            / "vitals"
            / "vitals_scorecard.json"
        ).read_text(encoding="utf-8")
    )
    assurance = vitals_scorecard["company_physics"]["assurance_suite"]
    assert assurance["status"] == "working"
    assert assurance["summary_digest"] == summary.digest
    assert assurance["slack"]["metrics"]["case_count"] == 9
    assert assurance["population"]["registry_pair_count"] == 60
    assert assurance["population"]["observed_pair_count"] == 15


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
