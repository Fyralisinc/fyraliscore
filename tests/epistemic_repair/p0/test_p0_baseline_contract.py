from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
P0 = ROOT / "docs" / "plans" / "epistemic-repair" / "p0"


def _load(name: str) -> dict[str, object]:
    return json.loads((P0 / name).read_text(encoding="utf-8"))


def test_baseline_counts_match_source_inventories() -> None:
    baseline = _load("epistemic-repair-p0-baseline-v1.json")
    authority = _load("authority-writer-reader-inventory.json")
    truth = _load("truth-state-inventory.json")
    hooks = _load("benchmark-hook-inventory.json")
    telemetry = _load("telemetry-inventory.json")
    evidence = _load("evidence-inventory.json")
    counts = baseline["inventories"]

    assert counts["canonical_tables"] == len(authority["canonical_tables"])
    assert counts["direct_writer_modules"] == len(authority["writer_modules"])
    assert counts["direct_reader_modules"] == len(
        authority["direct_canonical_reader_modules"]
    )
    assert counts["known_authority_bypasses"] == len(
        authority["known_bypasses"]
    )
    assert counts["illegal_truth_state_classes"] == len(truth["illegal_fixtures"])
    assert counts["legal_truth_state_controls"] == len(
        truth["legal_control_fixtures"]
    )
    assert counts["production_reachable_hook_families"] == len(hooks["hooks"])
    assert counts["telemetry_surfaces"] == len(telemetry["surfaces"])
    assert counts["telemetry_reconciliation_failures"] == len(
        telemetry["reconciliation_failures"]
    )
    assert counts["historical_or_bounded_evidence_items"] == len(
        evidence["evidence_items"]
    )
    assert counts["missing_required_populations"] == len(
        evidence["missing_required_populations"]
    )


def test_all_hard_gates_have_owner_enforcement_test_and_evidence() -> None:
    matrix = _load("hard-gate-ownership-matrix.json")
    gates = matrix["gates"]

    assert [gate["id"] for gate in gates] == [f"HG-{index:02d}" for index in range(1, 16)]
    for gate in gates:
        assert gate["current_owner"]
        assert gate["enforcement_seam"]
        assert gate["test_seam"]
        assert gate["evidence"]


def test_every_baseline_artifact_and_test_exists() -> None:
    baseline = _load("epistemic-repair-p0-baseline-v1.json")

    for relative in (*baseline["artifacts"], *baseline["characterization_tests"]):
        assert (ROOT / relative).is_file(), relative


def test_p0_does_not_claim_repairs_or_provider_execution() -> None:
    baseline = _load("epistemic-repair-p0-baseline-v1.json")

    assert baseline["status"] == "characterization_complete_repairs_not_started"
    assert (
        baseline["baseline"]["provider_environment"]["provider_execution_in_p0"]
        == "prohibited"
    )
    assert baseline["current_gate_summary"]["benchmark_blindness"] == "fail"
    assert baseline["current_gate_summary"]["telemetry_reconciliation"] == "fail"
