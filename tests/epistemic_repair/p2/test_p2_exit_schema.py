import json

from lib.evaluation.epistemic_repair.p2_exit import (
    ARTIFACT_SCHEMA_VERSION,
    build_p2_exit_artifact,
    write_p2_exit_artifact,
)


def test_initial_exit_artifact_is_honestly_unrun() -> None:
    report = build_p2_exit_artifact()

    assert report["schema_version"] == ARTIFACT_SCHEMA_VERSION
    assert report["execution_status"] == "unrun"
    assert not report["phase_exit_ready"]
    assert report["population"]["race_scenario_count"] == 5
    assert all(result["status"] == "missing" for result in report["hard_gates"].values())
    assert all(value is None for value in report["continuous_metrics"].values())
    assert report["command_receipts"] == []
    assert report["truth_snapshots"] == []
    assert len(report["artifact_content_digest"]) == 64


def test_artifact_content_digest_ignores_generated_time() -> None:
    first = build_p2_exit_artifact()
    second = build_p2_exit_artifact()

    assert first["artifact_content_digest"] == second["artifact_content_digest"]


def test_exit_artifact_round_trips_as_json(tmp_path) -> None:
    report = build_p2_exit_artifact()
    path = write_p2_exit_artifact(report, tmp_path / "p2.json")

    assert json.loads(path.read_text()) == report
