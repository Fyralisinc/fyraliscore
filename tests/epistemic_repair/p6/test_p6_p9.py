from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_p9 import GATE_IDS, METRIC_SPECS, build_p6_p9_sidecar
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p6_postfreeze_scorer import score_p6_frozen_execution


COMMIT = "a" * 40
ROOT = Path(__file__).resolve().parents[3]


def _write(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


def _score_metric(name: str) -> dict:
    operator, threshold = METRIC_SPECS[name]
    numerator = threshold if operator in {">=", "="} else 0.0
    return {
        "numerator": numerator, "denominator": 1, "value": numerator,
        "operator": operator, "threshold": threshold, "status": "pass",
        "source_ids": [f"source:{name}"], "worst_cases": [],
    }


def _artifacts(tmp_path: Path) -> tuple[Path, Path]:
    execution_path, score_path = tmp_path / "execution.json", tmp_path / "score.json"
    evidence = {"query_receipts": [{
        "query_name": "observations", "row_count": 300, "result_digest": "b" * 64,
    }]}
    evidence["source_digest"] = canonical_sha256(evidence)
    execution = {
        "schema_version": "epistemic-repair-p6-production-think-v1",
        "population_digest": "c" * 64,
        "postfreeze_evidence": evidence,
        "run_provenance": {"git_commit": COMMIT, "worktree_clean": True},
        "expected_llm_configuration": {
            "provider": "codex", "model": "gpt-5.4", "transport": "cli",
        },
        "mixed_llm_attempt_count": 0,
        "llm_attempt_receipts": [{
            "physical_attempt_id": "attempt-1", "think_run_id": "run-1",
            "provider": "codex", "model": "gpt-5.4", "transport": "cli",
        }],
    }
    _write(execution_path, execution)
    payload = {
        "schema_version": "epistemic-repair-p6-postfreeze-score-v1",
        "input_digests": {
            "raw_execution": canonical_sha256(execution),
            "sealed_population": "c" * 64, "preregistration": "d" * 64,
        },
        "hard_gates": {name: True for name in GATE_IDS},
        "continuous_metrics": {name: _score_metric(name) for name in METRIC_SPECS},
        "phase_exit_ready": True,
    }
    _write(score_path, {**payload, "content_digest": canonical_sha256(payload)})
    return execution_path, score_path


def _rewrite_score(path: Path, mutate) -> None:
    score = json.loads(path.read_text())
    score.pop("content_digest")
    mutate(score)
    score["content_digest"] = canonical_sha256(score)
    _write(path, score)


def test_normalizes_exact_p6_contract_and_member_contributions(tmp_path: Path) -> None:
    execution, score = _artifacts(tmp_path)
    artifact = build_p6_p9_sidecar(execution_path=execution, score_path=score)
    assert artifact["phase_exit_ready"] is True
    assert set(artifact["hard_gates"]) == set(GATE_IDS)
    assert {row["name"] for row in artifact["p9_continuous_metrics"]} == set(METRIC_SPECS)
    assert len(artifact["p9_member_contributions"]["gate_members"]) == 17
    assert len(artifact["p9_member_contributions"]["metric_members"]) == 32
    body = dict(artifact); digest = body.pop("content_digest")
    assert digest == canonical_sha256(body)


def test_preregistered_ids_exactly_match_live_postfreeze_scorer() -> None:
    population = build_p6_population()
    report = score_p6_frozen_execution(
        raw_execution={"population_digest": population.population_digest},
        sealed_population=population,
    )
    assert set(report["hard_gates"]) == set(GATE_IDS)
    assert set(report["continuous_metrics"]) == set(METRIC_SPECS)


def test_red_gate_and_metric_remain_explicitly_red(tmp_path: Path) -> None:
    execution, score = _artifacts(tmp_path)
    def mutate(value):
        value["hard_gates"]["complete_execution"] = False
        row = value["continuous_metrics"]["atomic_claim_precision"]
        row.update(numerator=0, value=0, status="fail")
        value["phase_exit_ready"] = False
    _rewrite_score(score, mutate)
    artifact = build_p6_p9_sidecar(execution_path=execution, score_path=score)
    assert artifact["phase_exit_ready"] is False
    assert artifact["hard_gates"]["complete_execution"] is False
    assert next(row for row in artifact["p9_continuous_metrics"] if row["name"] == "atomic_claim_precision")["status"] == "fail"


def test_calibration_insufficient_population_is_preserved_as_policy_state(tmp_path: Path) -> None:
    execution, score = _artifacts(tmp_path)
    def mutate(value):
        for name in ("resolved_outcome_model_ece", "resolved_outcome_model_brier"):
            value["continuous_metrics"][name].update(
                numerator=None, denominator=19, value=None,
                status="insufficient_population", source_ids=[f"outcome-{i}" for i in range(19)],
            )
    _rewrite_score(score, mutate)
    artifact = build_p6_p9_sidecar(execution_path=execution, score_path=score)
    row = next(row for row in artifact["p9_continuous_metrics"] if row["name"] == "resolved_outcome_model_ece")
    assert artifact["phase_exit_ready"] is True
    assert row["status"] == "pass"
    assert row["uncertainty"]["status"] == "insufficient_population"
    assert row["uncertainty"]["value_interpretation"] == "policy_sentinel_not_observed"


@pytest.mark.parametrize("mutation", [
    "extra_gate", "missing_metric", "unmeasured", "missing_sources",
    "missing_input_digest", "bad_score_digest",
])
def test_malformed_or_unmeasured_score_fails_closed(tmp_path: Path, mutation: str) -> None:
    execution, score = _artifacts(tmp_path)
    if mutation == "bad_score_digest":
        value = json.loads(score.read_text()); value["phase_exit_ready"] = False; _write(score, value)
    else:
        def mutate(value):
            if mutation == "extra_gate":
                value["hard_gates"]["invented"] = True
            elif mutation == "missing_metric":
                del value["continuous_metrics"]["scope_recall"]
            elif mutation == "unmeasured":
                value["continuous_metrics"]["scope_recall"].update(
                    numerator=None, denominator=None, value=None, status="unmeasured",
                )
            elif mutation == "missing_sources":
                del value["continuous_metrics"]["scope_recall"]["source_ids"]
            else:
                del value["input_digests"]["preregistration"]
        _rewrite_score(score, mutate)
    with pytest.raises(ValueError):
        build_p6_p9_sidecar(execution_path=execution, score_path=score)


@pytest.mark.parametrize("mutation", ["commit", "provider", "model", "transport", "source_digest", "raw_tamper"])
def test_mixed_identity_or_source_tampering_fails_closed(tmp_path: Path, mutation: str) -> None:
    execution, score = _artifacts(tmp_path)
    raw = json.loads(execution.read_text())
    if mutation == "commit":
        raw["run_provenance"]["git_commit"] = "short"
    elif mutation in {"provider", "model", "transport"}:
        raw["llm_attempt_receipts"][0][mutation] = "mixed"
    elif mutation == "source_digest":
        raw["postfreeze_evidence"]["source_digest"] = "0" * 64
    else:
        raw["new_unscored_field"] = True
    _write(execution, raw)
    with pytest.raises(ValueError):
        build_p6_p9_sidecar(execution_path=execution, score_path=score)


def test_cli_writes_normalized_artifact(tmp_path: Path) -> None:
    execution, score = _artifacts(tmp_path)
    output = tmp_path / "normalized.json"
    result = subprocess.run([
        sys.executable, str(ROOT / "scripts/build_epistemic_repair_p6_p9_sidecar.py"),
        "--execution", str(execution), "--score", str(score), "--output", str(output),
    ], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(output.read_text())["phase_exit_ready"] is True
