from __future__ import annotations

from copy import deepcopy

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p9_contributions import (
    attach_p9_member_evidence,
    metric_contract_digest,
)


PROVENANCE = {
    "git_commit": "a" * 40,
    "worktree_clean": True,
    "repository_root": "/sealed/repository",
}


def _member(member_id: str, *, conforms: bool = True) -> dict[str, object]:
    raw = {"member_id": member_id, "raw_fact": conforms}
    return {
        "member_id": member_id, "conforms": conforms,
        "raw_source_digest": canonical_sha256(raw),
    }


def _metric(member_id: str, numerator: float = 1) -> dict[str, object]:
    raw = {"member_id": member_id, "numerator": numerator}
    return {
        "member_id": member_id, "numerator": numerator, "denominator": 1,
        "raw_source_digest": canonical_sha256(raw),
    }


def _p1_artifact() -> dict[str, object]:
    return {
        "attempt_history": [{"physical_attempt_id": "a"}],
        "batches": [{"batch_id": "b"}],
        "hook_scan": {"findings": []},
        "cost_reconciliation": {"reconciled": True},
        # These summary flags are deliberately contradictory. The normalized
        # result must come from the raw contribution members below.
        "hard_gates": {"HG-01_benchmark_blindness": False},
        "continuous_metrics": {"attempt_receipt_coverage": 0},
    }


def _p1_members():
    gates = {
        "HG-01_benchmark_blindness": [_member("hook")],
        "HG-13_observability_integrity": [_member("receipt")],
    }
    metrics = {
        name: [_metric(name)] for name in (
            "attempt_receipt_coverage", "count_reconciliation",
            "cost_coverage", "timing_reconciliation",
        )
    }
    return gates, metrics


def test_normalized_values_ignore_contradictory_summary_flags() -> None:
    gates, metrics = _p1_members()
    result = attach_p9_member_evidence(
        _p1_artifact(), phase="p1", gate_members=gates,
        metric_members=metrics, run_provenance=PROVENANCE,
    )
    assert all(item["value"] == 1 for item in result["p9_continuous_metrics"])
    assert result["hard_gates"]["HG-01_benchmark_blindness"] is True
    assert result["preregistered_metric_contract_digest"] == metric_contract_digest("p1")
    assert len(result["content_digest"]) == 64


def test_summary_only_artifact_cannot_be_normalized() -> None:
    gates, metrics = _p1_members()
    with pytest.raises(ValueError, match="required raw member evidence"):
        attach_p9_member_evidence(
            {"hard_gates": {}, "continuous_metrics": {}}, phase="p1",
            gate_members=gates, metric_members=metrics,
            run_provenance=PROVENANCE,
        )


@pytest.mark.parametrize("mutation", ("partial", "duplicate", "zero_denominator"))
def test_adversarial_member_contracts_fail_closed(mutation: str) -> None:
    gates, metrics = _p1_members()
    provenance = dict(PROVENANCE)
    if mutation == "partial":
        metrics.pop("cost_coverage")
    elif mutation == "duplicate":
        gates["HG-01_benchmark_blindness"].append(
            deepcopy(gates["HG-01_benchmark_blindness"][0])
        )
    else:
        metrics["cost_coverage"][0]["denominator"] = 0
    with pytest.raises(ValueError):
        attach_p9_member_evidence(
            _p1_artifact(), phase="p1", gate_members=gates,
            metric_members=metrics, run_provenance=provenance,
        )


def test_dirty_worktree_is_recorded_and_cannot_exit() -> None:
    gates, metrics = _p1_members()
    provenance = {**PROVENANCE, "worktree_clean": False}
    artifact = {**_p1_artifact(), "phase_exit_ready": True}
    result = attach_p9_member_evidence(
        artifact, phase="p1", gate_members=gates,
        metric_members=metrics, run_provenance=provenance,
    )
    assert result["run_provenance"]["worktree_clean"] is False
    assert result["phase_exit_ready"] is False
