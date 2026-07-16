from __future__ import annotations

import json
from pathlib import Path

import pytest

from lib.contracts.perception import SufficiencyDisposition
from lib.evaluation.slack_reconstruction_gold import (
    SlackGoldFamily,
    SlackReconstructionGoldCase,
    SlackReconstructionObservation,
    evaluate_slack_reconstruction,
    load_slack_reconstruction_gold,
)
from scripts.run_slack_reconstruction_gold import main as slack_gold_main


FIXTURE = (
    Path(__file__).resolve().parents[3]
    / "tests"
    / "fixtures"
    / "company_learning"
    / "slack_reconstruction_gold_v1.jsonl"
)


def test_gold_fixture_seals_all_nine_reconstruction_families() -> None:
    cases = load_slack_reconstruction_gold(FIXTURE)

    assert len(cases) == 9
    assert {case.family for case in cases} == set(SlackGoldFamily)
    assert len({case.case_id for case in cases}) == 9
    assert all(len(case.digest) == 64 for case in cases)


def test_perfect_observations_receive_full_continuous_credit() -> None:
    cases = load_slack_reconstruction_gold(FIXTURE)
    report = evaluate_slack_reconstruction(
        cases=cases,
        observations=tuple(_perfect_observation(case) for case in cases),
        run_id="pytest-slack-gold-perfect",
        system_version="pytest",
        artifact_refs=("pytest://slack-gold-perfect",),
    )

    metrics = report.metrics
    assert report.status == "observed"
    assert metrics.case_count == 9
    assert metrics.supported_case_rate == 1.0
    assert metrics.correct_case_rate == 1.0
    assert metrics.mean_sufficient_set_recall == 1.0
    assert metrics.complete_sufficient_set_rate == 1.0
    assert metrics.selected_context_precision == 1.0
    assert metrics.contamination_rate == 0.0
    assert metrics.reconstructability_rate == 1.0
    assert metrics.edit_delete_correctness_rate == 1.0
    assert metrics.long_range_recall == 1.0
    assert metrics.budget_adherence_rate == 1.0
    assert metrics.abstention_under_insufficiency_rate == 1.0
    assert not any(
        gap.startswith("Gold family not yet sealed:")
        for gap in report.proof_gaps
    )


def test_contamination_and_unsafe_resolution_reduce_metrics_continuously() -> None:
    cases = load_slack_reconstruction_gold(FIXTURE)
    observations = [
        _perfect_observation(case)
        for case in cases
    ]
    thread = cases[0]
    observations[0] = SlackReconstructionObservation(
        case_id=thread.case_id,
        candidate_event_revision_ids=thread.candidate_event_revision_ids,
        selected_event_revision_ids=(
            thread.focal_event_revision_id,
            thread.forbidden_event_revision_ids[0],
        ),
        selected_topology_edge_ids=(),
        revision_fates=thread.expected_revision_fates,
        disposition=SufficiencyDisposition.NEEDS_EXPANSION,
        selected_token_count=13,
        artifact_refs=("pytest://thread-contaminated",),
    )
    insufficient_index = next(
        index
        for index, case in enumerate(cases)
        if case.family is SlackGoldFamily.HIGH_SIMILARITY_CONTAMINATION
    )
    insufficient = cases[insufficient_index]
    observations[insufficient_index] = SlackReconstructionObservation(
        case_id=insufficient.case_id,
        candidate_event_revision_ids=insufficient.candidate_event_revision_ids,
        selected_event_revision_ids=insufficient.candidate_event_revision_ids,
        selected_topology_edge_ids=(),
        revision_fates=insufficient.expected_revision_fates,
        disposition=SufficiencyDisposition.OPERATIONALLY_SUFFICIENT,
        selected_token_count=17,
        artifact_refs=("pytest://unsafe-insufficient-resolution",),
    )

    report = evaluate_slack_reconstruction(
        cases=cases,
        observations=tuple(observations),
        run_id="pytest-slack-gold-degraded",
        system_version="pytest",
        artifact_refs=("pytest://slack-gold-degraded",),
    )

    assessments = {item.case_id: item for item in report.assessments}
    assert report.metrics.correct_case_rate == pytest.approx(7 / 9)
    assert report.metrics.mean_sufficient_set_recall == pytest.approx(5 / 6)
    assert report.metrics.contamination_rate > 0.0
    assert report.metrics.selected_context_precision < 1.0
    assert report.metrics.abstention_under_insufficiency_rate == pytest.approx(
        2 / 3
    )
    assert assessments[thread.case_id].sufficient_set_recall == 0.0
    assert assessments[thread.case_id].contamination_count == 1
    assert assessments[insufficient.case_id].contamination_count == 2
    assert assessments[insufficient.case_id].budget_adherent is False
    assert assessments[insufficient.case_id].correct is False


def test_selective_result_omission_is_rejected() -> None:
    cases = load_slack_reconstruction_gold(FIXTURE)

    with pytest.raises(
        ValueError,
        match="exactly cover the sealed gold population",
    ):
        evaluate_slack_reconstruction(
            cases=cases,
            observations=tuple(
                _perfect_observation(case) for case in cases[:-1]
            ),
            run_id="pytest-slack-gold-omitted",
            system_version="pytest",
            artifact_refs=("pytest://slack-gold-omitted",),
        )


def test_candidate_population_omission_cannot_hide_contamination() -> None:
    cases = load_slack_reconstruction_gold(FIXTURE)
    observations = [
        _perfect_observation(case)
        for case in cases
    ]
    insufficient_index = next(
        index
        for index, case in enumerate(cases)
        if case.family is SlackGoldFamily.HIGH_SIMILARITY_CONTAMINATION
    )
    insufficient = cases[insufficient_index]
    observations[insufficient_index] = SlackReconstructionObservation(
        case_id=insufficient.case_id,
        candidate_event_revision_ids=(
            insufficient.focal_event_revision_id,
        ),
        selected_event_revision_ids=(
            insufficient.focal_event_revision_id,
        ),
        selected_topology_edge_ids=(),
        revision_fates=insufficient.expected_revision_fates,
        disposition=SufficiencyDisposition.NEEDS_CLARIFICATION,
        selected_token_count=insufficient.token_counts[
            insufficient.focal_event_revision_id
        ],
        artifact_refs=("pytest://candidate-omission",),
    )

    report = evaluate_slack_reconstruction(
        cases=cases,
        observations=tuple(observations),
        run_id="pytest-slack-gold-candidate-omission",
        system_version="pytest",
        artifact_refs=("pytest://slack-gold-candidate-omission",),
    )

    assessment = next(
        item
        for item in report.assessments
        if item.case_id == insufficient.case_id
    )
    assert assessment.candidate_population_match is False
    assert assessment.candidate_reconstructable is False
    assert assessment.correct is False
    assert report.metrics.reconstructability_rate == pytest.approx(8 / 9)


def test_cli_writes_report_for_complete_observed_population(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cases = load_slack_reconstruction_gold(FIXTURE)
    observed_path = tmp_path / "observed.jsonl"
    observed_path.write_text(
        "".join(
            json.dumps(
                _perfect_observation(case).model_dump(mode="json"),
                sort_keys=True,
            )
            + "\n"
            for case in cases
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "report"

    exit_code = slack_gold_main(
        [
            "--gold",
            str(FIXTURE),
            "--observed-jsonl",
            str(observed_path),
            "--output-dir",
            str(output_dir),
            "--run-id",
            "pytest-slack-gold-cli",
            "--system-version",
            "pytest",
        ]
    )

    assert exit_code == 0
    output_path = output_dir / "slack_reconstruction_gold_report.json"
    assert output_path.is_file()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["report"]["metrics"]["case_count"] == 9
    assert payload["report"]["metrics"]["correct_case_rate"] == 1.0
    assert len(payload["report_digest"]) == 64
    assert f"report={output_path}" in capsys.readouterr().out


def _perfect_observation(
    case: SlackReconstructionGoldCase,
) -> SlackReconstructionObservation:
    if case.insufficient_evidence:
        selected = (case.focal_event_revision_id,)
        disposition = case.allowed_dispositions[0]
    else:
        selected = tuple(
            dict.fromkeys(
                (
                    case.focal_event_revision_id,
                    *case.acceptable_sufficient_sets[0],
                )
            )
        )
        disposition = SufficiencyDisposition.OPERATIONALLY_SUFFICIENT
    return SlackReconstructionObservation(
        case_id=case.case_id,
        candidate_event_revision_ids=case.candidate_event_revision_ids,
        selected_event_revision_ids=selected,
        selected_topology_edge_ids=case.required_topology_edge_ids,
        revision_fates=case.expected_revision_fates,
        disposition=disposition,
        selected_token_count=sum(
            case.token_counts[event_id] for event_id in selected
        ),
        artifact_refs=(f"pytest://{case.case_id}",),
    )
