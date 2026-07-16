from __future__ import annotations

import json
from pathlib import Path

import pytest

from lib.evaluation.slack_reconstruction_gold import (
    evaluate_slack_reconstruction,
    load_slack_reconstruction_gold,
)
from scripts.observe_slack_reconstruction_gold import (
    main as observer_main,
)
from scripts.observe_slack_reconstruction_gold import (
    observe_existing_slack_reconstruction,
)


REPO_ROOT = Path(__file__).resolve().parents[4]
GOLD = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "company_learning"
    / "slack_reconstruction_gold_v1.jsonl"
)


async def test_existing_surface_produces_honest_slack_gold_measurement() -> None:
    cases = load_slack_reconstruction_gold(GOLD)
    observations = await observe_existing_slack_reconstruction(cases)
    report = evaluate_slack_reconstruction(
        cases=cases,
        observations=observations,
        run_id="pytest-existing-slack-surface",
        system_version="pytest",
        artifact_refs=("pytest://existing-slack-surface",),
    )

    by_case = {observation.case_id: observation for observation in observations}
    assert len(observations) == 4
    assert report.status == "observed_with_gaps"
    assert report.metrics.correct_case_rate == 0.0
    assert report.metrics.mean_sufficient_set_recall == 1.0
    assert report.metrics.complete_sufficient_set_rate == 1.0
    assert report.metrics.reconstructability_rate == 1.0
    assert report.metrics.contamination_rate == 0.2
    assert report.metrics.selected_context_precision == 0.8
    assert report.metrics.mean_topology_recall == 0.0
    assert report.metrics.edit_delete_correctness_rate == 0.5
    assert report.metrics.long_range_recall == 1.0
    assert report.metrics.budget_adherence_rate == 0.5
    assert report.metrics.abstention_under_insufficiency_rate == 0.0
    assert report.metrics.supported_case_rate == 0.5

    thread = by_case["slack-thread-dependency-v1"]
    assert len(thread.candidate_event_revision_ids) == 4
    assert len(thread.selected_event_revision_ids) == 4
    assert thread.disposition.value == "needs_expansion"
    assert thread.unsupported_reasons == (
        "selected_topology_edges_not_materialized",
    )

    edit = by_case["slack-edit-succession-v1"]
    assert edit.revision_fates[
        "observation:22222222-2222-4222-8222-222222222201:v1"
    ].value == "current"
    assert edit.unsupported_reasons == (
        "selected_topology_edges_not_materialized",
        "edit_supersession_fate_not_materialized",
    )

    contamination = by_case["slack-contamination-abstention-v1"]
    assert contamination.selected_event_revision_ids == (
        contamination.candidate_event_revision_ids[0],
    )
    assert contamination.disposition.value == "operationally_sufficient"


def test_existing_surface_observer_cli_writes_replayable_artifacts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    output_dir = tmp_path / "slack-existing-surface"

    exit_code = observer_main(
        [
            "--gold",
            str(GOLD),
            "--output-dir",
            str(output_dir),
            "--run-id",
            "pytest-slack-existing-surface-cli",
            "--system-version",
            "pytest",
        ]
    )

    assert exit_code == 0
    observations_path = (
        output_dir / "slack_reconstruction_observations.jsonl"
    )
    report_path = (
        output_dir / "slack_reconstruction_existing_surface_report.json"
    )
    assert observations_path.is_file()
    assert report_path.is_file()
    assert len(observations_path.read_text(encoding="utf-8").splitlines()) == 4
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["report"]["status"] == "observed_with_gaps"
    assert payload["report"]["metrics"]["correct_case_rate"] == 0.0
    assert payload["report"]["metrics"]["mean_sufficient_set_recall"] == 1.0
    assert len(payload["report_digest"]) == 64
    output = capsys.readouterr().out
    assert f"observations={observations_path}" in output
    assert f"report={report_path}" in output
