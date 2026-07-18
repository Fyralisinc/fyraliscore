from __future__ import annotations

from copy import deepcopy
from argparse import Namespace
import json
from pathlib import Path
import subprocess
import sys

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_p9 import GATE_IDS, METRIC_SPECS, build_p6_p9_sidecar
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from lib.evaluation.epistemic_repair.p6_postfreeze_scorer import score_p6_frozen_execution
from scripts import score_epistemic_repair_p6_think as score_cli


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


def _artifacts(tmp_path: Path) -> tuple[Path, Path, Path]:
    execution_path = tmp_path / "execution.json"
    evidence_path, score_path = tmp_path / "evidence.json", tmp_path / "score.json"
    evidence = {"query_receipts": [{
        "query_name": "observations", "row_count": 300, "result_digest": "b" * 64,
    }]}
    evidence["source_digest"] = canonical_sha256(evidence)
    execution = {
        "schema_version": "epistemic-repair-p6-production-think-v1",
        "population_digest": "c" * 64,
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
    evidence_body = {
        "schema_version": "epistemic-repair-p6-postfreeze-evidence-artifact-v1",
        "commit": COMMIT, "raw_execution_digest": canonical_sha256(execution),
        "postfreeze_evidence": evidence,
    }
    _write(evidence_path, {**evidence_body, "content_digest": canonical_sha256(evidence_body)})
    frozen = {**execution, "postfreeze_evidence": evidence}
    payload = {
        "schema_version": "epistemic-repair-p6-postfreeze-score-v1",
        "input_digests": {
            "raw_execution": canonical_sha256(frozen),
            "sealed_population": "c" * 64, "preregistration": "d" * 64,
        },
        "hard_gates": {name: True for name in GATE_IDS},
        "continuous_metrics": {name: _score_metric(name) for name in METRIC_SPECS},
        "phase_exit_ready": True,
    }
    _write(score_path, {**payload, "content_digest": canonical_sha256(payload)})
    return execution_path, evidence_path, score_path


def _build(execution: Path, evidence: Path, score: Path) -> dict:
    return build_p6_p9_sidecar(
        execution_path=execution, evidence_path=evidence, score_path=score,
    )


def _rewrite_score(path: Path, mutate) -> None:
    score = json.loads(path.read_text())
    score.pop("content_digest")
    mutate(score)
    score["content_digest"] = canonical_sha256(score)
    _write(path, score)


def test_normalizes_exact_p6_contract_and_member_contributions(tmp_path: Path) -> None:
    execution, evidence, score = _artifacts(tmp_path)
    artifact = _build(execution, evidence, score)
    assert artifact["phase_exit_ready"] is True
    assert set(artifact["hard_gates"]) == set(GATE_IDS)
    assert {row["name"] for row in artifact["p9_continuous_metrics"]} == set(METRIC_SPECS)
    assert len(artifact["p9_member_contributions"]["gate_members"]) == 17
    assert len(artifact["p9_member_contributions"]["metric_members"]) == 34
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
    execution, evidence, score = _artifacts(tmp_path)
    def mutate(value):
        value["hard_gates"]["complete_execution"] = False
        row = value["continuous_metrics"]["atomic_claim_precision"]
        row.update(numerator=0, value=0, status="fail")
        value["phase_exit_ready"] = False
    _rewrite_score(score, mutate)
    artifact = _build(execution, evidence, score)
    assert artifact["phase_exit_ready"] is False
    assert artifact["hard_gates"]["complete_execution"] is False
    assert next(row for row in artifact["p9_continuous_metrics"] if row["name"] == "atomic_claim_precision")["status"] == "fail"


def test_calibration_insufficient_population_is_preserved_as_policy_state(tmp_path: Path) -> None:
    execution, evidence, score = _artifacts(tmp_path)
    def mutate(value):
        for name in ("resolved_outcome_model_ece", "resolved_outcome_model_brier"):
            value["continuous_metrics"][name].update(
                numerator=None, denominator=19, value=None,
                status="insufficient_population", source_ids=[f"outcome-{i}" for i in range(19)],
            )
    _rewrite_score(score, mutate)
    artifact = _build(execution, evidence, score)
    row = next(row for row in artifact["p9_continuous_metrics"] if row["name"] == "resolved_outcome_model_ece")
    assert artifact["phase_exit_ready"] is False
    assert row["status"] == "insufficient_population"
    assert row["numerator"] is None and row["value"] is None
    assert row["uncertainty"]["status"] == "insufficient_population"
    assert row["uncertainty"]["value_interpretation"] == "not_observed"


@pytest.mark.parametrize("mutation", [
    "extra_gate", "missing_metric", "unmeasured", "missing_sources",
    "missing_input_digest", "bad_score_digest",
])
def test_malformed_or_unmeasured_score_fails_closed(tmp_path: Path, mutation: str) -> None:
    execution, evidence, score = _artifacts(tmp_path)
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
        _build(execution, evidence, score)


@pytest.mark.parametrize("mutation", ["commit", "provider", "model", "transport", "source_digest", "raw_tamper"])
def test_mixed_identity_or_source_tampering_fails_closed(tmp_path: Path, mutation: str) -> None:
    execution, evidence, score = _artifacts(tmp_path)
    raw = json.loads(execution.read_text())
    if mutation == "commit":
        raw["run_provenance"]["git_commit"] = "short"
    elif mutation in {"provider", "model", "transport"}:
        raw["llm_attempt_receipts"][0][mutation] = "mixed"
    elif mutation == "source_digest":
        evidence_artifact = json.loads(evidence.read_text())
        evidence_artifact["postfreeze_evidence"]["source_digest"] = "0" * 64
        evidence_artifact.pop("content_digest")
        evidence_artifact["content_digest"] = canonical_sha256(evidence_artifact)
        _write(evidence, evidence_artifact)
        with pytest.raises(ValueError):
            _build(execution, evidence, score)
        return
    else:
        raw["new_unscored_field"] = True
    _write(execution, raw)
    with pytest.raises(ValueError):
        _build(execution, evidence, score)


def test_cli_writes_normalized_artifact(tmp_path: Path) -> None:
    execution, evidence, score = _artifacts(tmp_path)
    output = tmp_path / "normalized.json"
    result = subprocess.run([
        sys.executable, str(ROOT / "scripts/build_epistemic_repair_p6_p9_sidecar.py"),
        "--execution", str(execution), "--evidence", str(evidence),
        "--score", str(score), "--output", str(output),
    ], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(output.read_text())["phase_exit_ready"] is True


@pytest.mark.asyncio
async def test_score_cli_persists_sealed_evidence_for_exact_scored_composition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    population = build_p6_population()
    raw = {
        "schema_version": "epistemic-repair-p6-production-think-v1",
        "population_digest": population.population_digest, "complete": True,
        "tenant_id": "00000000-0000-0000-0000-000000000001",
        "run_provenance": {"git_commit": COMMIT, "worktree_clean": True},
        "expected_llm_configuration": {
            "provider": "codex", "model": "gpt-5.4", "transport": "cli",
        },
        "mixed_llm_attempt_count": 0,
        "llm_attempt_receipts": [{
            "physical_attempt_id": "attempt-1", "think_run_id": "run-1",
            "provider": "codex", "model": "gpt-5.4", "transport": "cli",
        }],
        "waves": [],
    }
    evidence = {"query_receipts": [{
        "query_name": "observations", "row_count": 300, "result_digest": "e" * 64,
    }]}
    evidence["source_digest"] = canonical_sha256(evidence)

    class Connection:
        async def close(self) -> None:
            pass

    async def connect(_database_url: str) -> Connection:
        return Connection()

    async def extract(*_args, **_kwargs) -> dict:
        return deepcopy(evidence)

    monkeypatch.setattr(score_cli.asyncpg, "connect", connect)
    monkeypatch.setattr(score_cli, "extract_p6_postfreeze_evidence", extract)
    raw_path, evidence_path, score_path = (
        tmp_path / "raw.json", tmp_path / "evidence.json", tmp_path / "score.json"
    )
    _write(raw_path, raw)
    status = await score_cli._run(Namespace(
        raw=raw_path, evidence_output=evidence_path, output=score_path,
        database_url="postgresql://unused",
    ))
    assert status == 1  # real-shape evidence is persisted even when semantic gates are red
    evidence_artifact = json.loads(evidence_path.read_text())
    score = json.loads(score_path.read_text())
    body = dict(evidence_artifact); digest = body.pop("content_digest")
    assert digest == canonical_sha256(body)
    assert evidence_artifact["raw_execution_digest"] == canonical_sha256(raw)
    assert score["input_digests"]["raw_execution"] == canonical_sha256({
        **raw, "postfreeze_evidence": evidence,
    })
    with pytest.raises(ValueError, match="unmeasured"):
        _build(raw_path, evidence_path, score_path)
