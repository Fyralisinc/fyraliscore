from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_p9 import GATE_IDS, METRIC_CONTRACTS, build_p8_p9_sidecar


COMMIT = "a" * 40
ROOT = Path(__file__).resolve().parents[3]


def _seal(path: Path, value: dict) -> None:
    value = deepcopy(value)
    value.pop("artifact_digest", None)
    value["artifact_digest"] = canonical_sha256(value)
    path.write_text(json.dumps(value, sort_keys=True) + "\n")


def _fixtures(tmp_path: Path) -> dict[str, Path]:
    paths = {name: tmp_path / f"{name}.json" for name in (
        "fault", "scale", "characterization", "contention", "exit",
    )}
    qualification = {
        "observed_member_receipts": 24,
        "every_physical_attempt_has_receipt": True,
        **{name: {"gate": True, "violations": 0} for name in (
            "cross_tenant_effects", "duplicate_relation_transitions",
            "duplicate_lifecycle_transitions", "partial_truth_state", "stale_active_truth",
            "dead_letter_truth_critical_work", "uninterrupted_reference_digest_equality",
        )},
    }
    _seal(paths["fault"], {
        "commit": COMMIT,
        "bound_execution_evidence": {"fault_execution_keys": [f"fault-{i}" for i in range(24)]},
        "provider_fault_slice": {"receipts": [{
            "usage_exactness": "reported", "input_tokens": 10, "output_tokens": 2,
        }]},
        "member_receipt_qualification": qualification,
    })
    _seal(paths["scale"], {
        "commit": COMMIT,
        "execution": {"cells": [{} for _ in range(27)], "physically_isolated_databases": True},
        "evaluation": {
            "max_retrieval_horizon_ratio": 1.2,
            "max_prompt_horizon_ratio": 1.1,
            "max_concurrency_latency_ratio": 1.4,
            "max_semantic_quality_delta": .01,
            "minimum_fairness_ratio": .9,
            "gates": {
                "concurrency_latency_ratio": True,
                "all_production_queue_families_measured": True,
                "resource_sample_every_durable_barrier": True,
                "deterministic_token_status_explicit": True,
                "derived_refresh_pipeline_executed": True,
                "exact_provider_prompt_token_measurement": False,
            },
        },
    })
    ids = ["example-1", "example-2"]
    _seal(paths["characterization"], {
        "commit": COMMIT,
        "executed_metrics": {"boundary_f1": {
            "score": 1.0, "denominator": 2, "source_example_ids": ids,
            "source_artifact_digest": canonical_sha256(ids), "worst_example_ids": [ids[0]],
        }},
    })
    _seal(paths["contention"], {
        "schema_version": "p8-shared-contention-v2", "commit": COMMIT,
        "result": {
            "selected_cell_ids": ["p8-bs10-h12-t1", "p8-bs25-h12-t5", "p8-bs50-h12-t20"],
            "concurrent_cells": 3, "wall_time_ms": 10, "individual_wall_time_sum_ms": 20,
            "contention_ratio": .5, "evidence_digest": "b" * 64,
        },
    })
    hashes = {name: sha256(paths[name].read_bytes()).hexdigest() for name in (
        "fault", "scale", "characterization", "contention",
    )}
    _seal(paths["exit"], {
        "commit": COMMIT,
        "gates": {"authorized_provider_canaries": True, "hash_reopen_review": True},
        "provider_canary_policy": {"status": "completed"},
        "source_artifact_sha256": {str(paths[name]): hashes[name] for name in hashes},
    })
    return paths


def _build(paths: dict[str, Path]) -> dict:
    return build_p8_p9_sidecar(
        exit_path=paths["exit"], fault_path=paths["fault"], scale_path=paths["scale"],
        characterization_path=paths["characterization"], contention_path=paths["contention"],
    )


def _refresh_exit_hash(paths: dict[str, Path], member: str) -> None:
    exit_artifact = json.loads(paths["exit"].read_text())
    exit_artifact["source_artifact_sha256"][str(paths[member])] = sha256(paths[member].read_bytes()).hexdigest()
    _seal(paths["exit"], exit_artifact)


def test_builds_exact_normalized_contract_from_raw_members(tmp_path: Path) -> None:
    artifact = _build(_fixtures(tmp_path))
    assert artifact["phase_exit_ready"] is True
    assert tuple(artifact["hard_gates"]) == GATE_IDS
    assert {row["name"] for row in artifact["p9_continuous_metrics"]} == set(METRIC_CONTRACTS)
    assert all(artifact["p9_member_contributions"]["gate_members"].values())
    body = dict(artifact); digest = body.pop("content_digest")
    assert digest == canonical_sha256(body)


def test_red_latency_is_normalized_as_failed_not_hidden(tmp_path: Path) -> None:
    paths = _fixtures(tmp_path)
    scale = json.loads(paths["scale"].read_text())
    scale["evaluation"]["gates"]["concurrency_latency_ratio"] = False
    scale["evaluation"]["max_concurrency_latency_ratio"] = 3.5
    _seal(paths["scale"], scale); _refresh_exit_hash(paths, "scale")
    artifact = _build(paths)
    assert artifact["phase_exit_ready"] is False
    assert artifact["hard_gates"]["P8-G05-scale-latency"] is False
    assert next(row for row in artifact["p9_continuous_metrics"] if row["name"] == "scale_max_concurrency_latency_ratio")["status"] == "fail"


@pytest.mark.parametrize("case", ["mixed_commit", "missing_receipt", "missing_provenance", "tampered_member"])
def test_fails_closed_on_adversarial_evidence(tmp_path: Path, case: str) -> None:
    paths = _fixtures(tmp_path)
    member = "fault" if case == "missing_receipt" else "characterization"
    artifact = json.loads(paths[member].read_text())
    if case == "mixed_commit":
        artifact["commit"] = "c" * 40
    elif case == "missing_receipt":
        del artifact["member_receipt_qualification"]["partial_truth_state"]
    elif case == "missing_provenance":
        del artifact["executed_metrics"]["boundary_f1"]["source_artifact_digest"]
    else:
        paths[member].write_bytes(paths[member].read_bytes() + b" ")
        with pytest.raises(ValueError, match="hashes do not match"):
            _build(paths)
        return
    _seal(paths[member], artifact); _refresh_exit_hash(paths, member)
    if case == "missing_provenance":
        result = _build(paths)
        assert result["phase_exit_ready"] is False
        assert result["hard_gates"]["P8-G08-characterization"] is False
        return
    with pytest.raises(ValueError):
        _build(paths)


def test_missing_canary_authorization_is_explicit_red_gate(tmp_path: Path) -> None:
    paths = _fixtures(tmp_path)
    exit_artifact = json.loads(paths["exit"].read_text())
    exit_artifact["gates"]["authorized_provider_canaries"] = False
    exit_artifact["provider_canary_policy"]["status"] = "gated_off"
    _seal(paths["exit"], exit_artifact)
    result = _build(paths)
    assert result["phase_exit_ready"] is False
    assert result["hard_gates"]["P8-G09-authorized-canaries"] is False


def test_exit_cli_emits_wired_p9_sidecar(tmp_path: Path) -> None:
    paths = _fixtures(tmp_path)
    canary = tmp_path / "canary.jsonl"
    canary.write_text(json.dumps({"type": "turn.completed", "usage": {"input_tokens": 3, "output_tokens": 1}}) + "\n")
    output, p9_output = tmp_path / "composed.json", tmp_path / "p9.json"
    result = subprocess.run([
        sys.executable, str(ROOT / "scripts/build_epistemic_repair_p8_exit.py"),
        "--fault", str(paths["fault"]), "--scale", str(paths["scale"]),
        "--characterization", str(paths["characterization"]),
        "--contention", str(paths["contention"]), "--provider-canary", str(canary),
        "--output", str(output), "--p9-output", str(p9_output),
    ], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(p9_output.read_text())["phase_exit_ready"] is True
