from pathlib import Path

from lib.evaluation.epistemic_repair.p2_population import build_p2_population


def test_sealed_population_registers_ten_exact_duplicate_attempts() -> None:
    cases = build_p2_population().family("semantic_duplicate")
    assert len(cases) == 10
    assert {case.fact("duplicate_group") for case in cases} == {
        "sealed-exact-identity-1"
    }
    assert all(case.expected_disposition == "accept" for case in cases)


def test_receipt_schema_registers_absorbed_duplicate_outcome() -> None:
    migration = Path(
        "db/migrations/0226_truth_semantic_duplicate_absorption.sql"
    ).read_text(encoding="utf-8")
    assert "absorbed_duplicate" in migration
    assert "truth_command_receipts_outcome_check" in migration
    assert "truth_command_receipts_rejection_code_check" in migration
    assert "outcome IN ('applied', 'absorbed_duplicate')" in migration
    assert "CREATE TABLE IF NOT EXISTS truth_semantic_absorptions" in migration
    assert "attempted_command JSONB NOT NULL" in migration
    assert "truth_semantic_absorptions_immutable" in migration


def test_runner_enforces_required_continuous_threshold() -> None:
    source = Path(
        "lib/evaluation/epistemic_repair/p2_runner.py"
    ).read_text(encoding="utf-8")
    assert '"semantic_duplicate_absorption": 0.90' in source
    assert ">= 0.90" in source
