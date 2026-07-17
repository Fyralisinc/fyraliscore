from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from lib.evaluation.epistemic_repair.p3_runner import (
    ARTIFACT_SCHEMA_VERSION,
    P3Artifact,
    P3PerceptionRuntime,
    build_p3_population,
    run_p3_perception_grounding,
    write_p3_artifact,
    write_p3_artifact_schema,
)
from services.domain.entity_grounding.episode import (
    ContextObservationInput,
    GroundingCandidateInput,
    build_adjudicated_grounding_decision,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import (
    prepare_entity_mention_detection,
)


ROOT = Path(__file__).resolve().parents[3]


def _runtime() -> P3PerceptionRuntime:
    return P3PerceptionRuntime(
        context_observation_type=ContextObservationInput,
        grounding_candidate_type=GroundingCandidateInput,
        prepare_context_selection=prepare_context_selection,
        prepare_entity_mention_detection=prepare_entity_mention_detection,
        build_grounding_episode=build_grounding_episode,
        build_adjudicated_grounding_decision=build_adjudicated_grounding_decision,
        candidate_id_for_ref=candidate_id_for_ref,
    )


def test_population_is_exactly_the_preregistered_p3_shape() -> None:
    population = build_p3_population()

    assert len(population.signals) == 120
    assert population.family_counts() == {
        "boundary_distractor": 10,
        "cross_source_link": 20,
        "email_document": 20,
        "entity_negative_ambiguity": 10,
        "slack_interleaved": 40,
        "structured_object": 20,
    }
    assert (
        len(
            {
                item.episode_id
                for item in population.signals
                if item.family == "slack_interleaved"
            }
        )
        == 4
    )
    assert sum(item.split_merge_decision for item in population.signals) == 12
    assert sum(bool(item.gold_mention_spans) for item in population.signals) >= 30
    assert sum(item.high_consequence_link for item in population.signals) == 8
    assert sum(item.safe_abstention_or_review for item in population.signals) >= 8
    assert sum(item.correction_replay for item in population.signals) == 5
    assert len({item.signal_id for item in population.signals}) == 120


def test_population_seal_is_stable_and_separates_scenario_from_gold() -> None:
    first = build_p3_population()
    second = build_p3_population()

    assert first.scenario_digest == second.scenario_digest
    assert first.gold_digest == second.gold_digest
    assert first.scenario_digest != first.gold_digest
    assert len(first.scenario_digest) == len(first.gold_digest) == 64


def test_full_runner_emits_complete_member_level_artifact() -> None:
    report = run_p3_perception_grounding(repository_root=ROOT, runtime=_runtime())

    assert report["schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert report["population"]["signal_count"] == 120
    assert len(report["member_receipts"]) == 120
    assert len(report["correction_receipts"]) == 5
    assert report["hard_gates"]["HG-03"]["status"] == "pass"
    assert report["hard_gates"]["HG-02"]["status"] == "not_observed"
    assert report["hard_gates"]["HG-06"]["status"] == "not_observed"
    assert report["hard_gates"]["HG-14"]["status"] == "not_observed"
    assert not report["phase_exit_ready"]
    assert report["missing_evidence"]
    assert all(
        not item["future_context_selected"] for item in report["member_receipts"]
    )
    assert all(item["mention_fate"] for item in report["member_receipts"])
    P3Artifact.model_validate(report)


def test_metrics_are_bounded_denominator_complete_and_context_recall_is_sufficient() -> None:
    report = run_p3_perception_grounding(repository_root=ROOT, runtime=_runtime())
    metrics = report["continuous_metrics"]

    assert metrics["exact_mention_f1"]["value"] == 1.0
    assert metrics["canonical_link_precision"]["value"] == 1.0
    assert metrics["safe_abstention_precision"]["value"] == 1.0
    assert metrics["correction_replay_convergence_coverage"]["value"] == 1.0
    # The production selector must recover required email and cross-source
    # references without changing the sealed oracle or admitting contamination.
    assert metrics["pairwise_boundary_recall"]["threshold_met"] is True
    assert metrics["sufficient_context_recall"]["threshold_met"] is True
    assert metrics["selected_context_contamination"]["value"] == 0.0
    for item in metrics.values():
        assert item["denominator"] > 0
        assert 0.0 <= item["value"] <= 1.0
        low, high = item["confidence_interval"]
        assert 0.0 <= low <= high <= 1.0
        assert item["source_artifact"]


def test_artifact_round_trip_preserves_content_digest(tmp_path: Path) -> None:
    report = run_p3_perception_grounding(repository_root=ROOT, runtime=_runtime())
    path = write_p3_artifact(report, tmp_path / "p3.json")
    reopened = json.loads(path.read_text())

    assert reopened == report
    assert (
        P3Artifact.model_validate(reopened).artifact_content_digest
        == (report["artifact_content_digest"])
    )

    schema_path = write_p3_artifact_schema(tmp_path / "p3.schema.json")
    schema = json.loads(schema_path.read_text())
    assert schema["title"] == "P3Artifact"
    assert "member_receipts" in schema["properties"]


def test_cli_exposes_output_and_runs_without_database(tmp_path: Path) -> None:
    script = ROOT / "scripts/run_epistemic_repair_p3_perception_grounding.py"
    help_result = subprocess.run(
        [sys.executable, str(script), "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert help_result.returncode == 0
    assert "--repository-root" in help_result.stdout
    assert "--output" in help_result.stdout
    assert "--schema-output" in help_result.stdout

    output = tmp_path / "p3-cli.json"
    run_result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--output",
            str(output),
            "--schema-output",
            str(tmp_path / "p3-cli.schema.json"),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert run_result.returncode == 0, run_result.stderr
    assert json.loads(output.read_text())["population"]["signal_count"] == 120
