from __future__ import annotations

from copy import deepcopy

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p7_p9 import (
    P7_ORACLE_GATES,
    P7_P9_GATES,
    build_p7_p9_sidecar,
)


def _score() -> dict:
    endpoints = []
    for world in ("w1", "w2", "w3"):
        for storyline in ("atlas", "beacon", "cobalt", "delta"):
            endpoints.append({
                "world_id": world, "arm_id": "adaptive", "stage_batch": 12,
                "storyline_id": storyline,
                "direct_thesis_accuracy": {"value": 1.0, "measured": True},
                "atomic_claim_f1": {"value": 0.95, "measured": True},
                "boundary_entity_safety": {"value": 1.0, "measured": True},
                "relation_joint_precision": {"value": 1.0, "measured": True},
                "external_outcome_calibration_ece": {"value": 0.05, "measured": True},
                "false_truth_from_noise": 0,
            })
    gate_members = {
        gate: [{
            "member_id": f"{gate}:member", "conforms": True,
            "raw_source_digest": canonical_sha256({"gate": gate, "raw": True}),
        }] for gate in P7_ORACLE_GATES
    }
    payload = {
        "schema_version": "epistemic-repair-p7-postfreeze-oracle-v1",
        "execution_artifact_digest": "e" * 64,
        "world_count": 3, "endpoint_denominator": 3 * 5 * 3 * 4,
        "endpoints": endpoints,
        "hard_gates": {gate: True for gate in P7_ORACLE_GATES},
        "hard_gate_members": gate_members,
        "run_provenance": {
            "git_commit": "a" * 40, "worktree_clean": True,
            "codex_transport": "cli", "worktree_path": "/isolated/p7",
        },
        "memory_earns_decision": {"criteria": {"evidence": True}},
        "strategic_verdict": "primary_memory_earned",
        "phase_exit_ready": True,
    }
    return {**payload, "content_digest": canonical_sha256(payload)}


def _reseal(score: dict) -> dict:
    body = deepcopy(score)
    body.pop("content_digest", None)
    return {**body, "content_digest": canonical_sha256(body)}


def test_p7_sidecar_has_exact_gates_metrics_members_and_decision() -> None:
    sidecar = build_p7_p9_sidecar(_score())
    assert set(sidecar["hard_gates"]) == set(P7_P9_GATES)
    assert len(sidecar["p9_continuous_metrics"]) == 6
    assert all(item["status"] == "pass" for item in sidecar["p9_continuous_metrics"])
    assert sidecar["strategic_decision"] == "primary_memory_earned"
    assert sidecar["phase_exit_ready"]
    assert sidecar["commit"] == "a" * 40
    assert len(sidecar["content_digest"]) == 64


def test_gate_summary_cannot_override_failed_raw_member() -> None:
    score = _score()
    score["hard_gate_members"]["durable_attempt_receipts"][0]["conforms"] = False
    score = _reseal(score)
    with pytest.raises(ValueError, match="contradicts raw members"):
        build_p7_p9_sidecar(score)


@pytest.mark.parametrize("attack", ("dirty", "app_server", "missing_gate", "dropped_endpoint"))
def test_p7_sidecar_adversarial_evidence_fails_closed(attack: str) -> None:
    score = _score()
    if attack == "dirty":
        score["run_provenance"]["worktree_clean"] = False
    elif attack == "app_server":
        score["run_provenance"]["codex_transport"] = "app_server"
    elif attack == "missing_gate":
        score["hard_gate_members"].pop("exact_paired_population")
    else:
        score["endpoints"].pop()
    with pytest.raises(ValueError):
        build_p7_p9_sidecar(_reseal(score))


def test_unmeasured_metric_cannot_be_promoted_by_phase_summary() -> None:
    score = _score()
    score["endpoints"][0]["atomic_claim_f1"] = {"value": None, "measured": False}
    score["phase_exit_ready"] = True
    with pytest.raises(ValueError, match="unmeasured"):
        build_p7_p9_sidecar(_reseal(score))
