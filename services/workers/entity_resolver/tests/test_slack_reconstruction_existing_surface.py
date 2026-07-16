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
    assert len(observations) == 9
    assert report.status == "observed"
    assert report.metrics.correct_case_rate == 1.0
    assert report.metrics.mean_sufficient_set_recall == 1.0
    assert report.metrics.complete_sufficient_set_rate == 1.0
    assert report.metrics.reconstructability_rate == 1.0
    assert report.metrics.contamination_rate == 0.0
    assert report.metrics.selected_context_precision == 1.0
    assert report.metrics.mean_topology_recall == 1.0
    assert report.metrics.edit_delete_correctness_rate == 1.0
    assert report.metrics.long_range_recall == 1.0
    assert report.metrics.cross_channel_recall == 1.0
    assert report.metrics.budget_adherence_rate == 1.0
    assert report.metrics.abstention_under_insufficiency_rate == 1.0
    assert report.metrics.supported_case_rate == 1.0

    thread = by_case["slack-thread-dependency-v1"]
    assert len(thread.candidate_event_revision_ids) == 4
    assert len(thread.selected_event_revision_ids) == 3
    assert thread.disposition.value == "operationally_sufficient"
    assert thread.selected_topology_edge_ids == (
        "slack-thread:1760000000.100001->1760000000.200001",
        "slack-thread:1760000000.200001->1760000000.300001",
    )
    assert thread.unsupported_reasons == ()

    edit = by_case["slack-edit-succession-v1"]
    assert edit.revision_fates[
        "observation:22222222-2222-4222-8222-222222222201:v1"
    ].value == "superseded"
    assert edit.selected_topology_edge_ids == (
        "slack-edit:1760000100.100001->1760000100.200001",
    )
    assert edit.unsupported_reasons == ()

    contamination = by_case["slack-contamination-abstention-v1"]
    assert contamination.selected_event_revision_ids == (
        contamination.candidate_event_revision_ids[0],
    )
    assert contamination.disposition.value == "needs_clarification"

    long_range = by_case["slack-long-range-recurrence-v1"]
    assert long_range.selected_event_revision_ids == (
        "observation:33333333-3333-4333-8333-333333333302:v1",
        "observation:33333333-3333-4333-8333-333333333301:v1",
    )
    assert long_range.disposition.value == "operationally_sufficient"

    cross_thread = by_case["slack-cross-thread-dependency-v1"]
    assert cross_thread.selected_event_revision_ids == (
        "observation:55555555-5555-4555-8555-555555555503:v1",
        "observation:55555555-5555-4555-8555-555555555501:v1",
    )
    assert cross_thread.disposition.value == "operationally_sufficient"
    assert cross_thread.unsupported_reasons == ()

    pronoun = by_case["slack-pronoun-coreference-v1"]
    assert len(pronoun.selected_event_revision_ids) == 3
    assert pronoun.disposition.value == "operationally_sufficient"
    assert len(pronoun.selected_topology_edge_ids) == 2

    deletion = by_case["slack-deletion-tombstone-v1"]
    assert deletion.disposition.value == "non_identifiable"
    assert deletion.revision_fates[
        "observation:77777777-7777-4777-8777-777777777701:v1"
    ].value == "superseded"
    assert deletion.revision_fates[
        "observation:77777777-7777-4777-8777-777777777702:v1"
    ].value == "tombstone"
    assert deletion.unsupported_reasons == ()

    reaction = by_case["slack-reaction-evidence-v1"]
    assert reaction.disposition.value == "non_identifiable"
    assert reaction.revision_fates[
        "observation:88888888-8888-4888-8888-888888888801:v1"
    ].value == "current"
    assert reaction.revision_fates[
        "observation:88888888-8888-4888-8888-888888888802:v1"
    ].value == "reaction_evidence"
    assert reaction.unsupported_reasons == ()

    cross_channel = by_case["slack-cross-channel-dependency-v1"]
    assert cross_channel.disposition.value == "operationally_sufficient"
    assert cross_channel.selected_event_revision_ids == (
        "observation:99999999-9999-4999-8999-999999999902:v1",
        "observation:99999999-9999-4999-8999-999999999901:v1",
    )
    assert cross_channel.unsupported_reasons == ()


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
    assert len(observations_path.read_text(encoding="utf-8").splitlines()) == 9
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["report"]["status"] == "observed"
    assert payload["report"]["metrics"]["correct_case_rate"] == 1.0
    assert payload["report"]["metrics"]["mean_sufficient_set_recall"] == 1.0
    assert len(payload["report_digest"]) == 64
    output = capsys.readouterr().out
    assert f"observations={observations_path}" in output
    assert f"report={report_path}" in output
