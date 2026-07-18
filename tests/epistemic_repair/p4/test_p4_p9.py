import pytest

from lib.evaluation.epistemic_repair.p4_p9 import _CONTRACTS, build_p4_p9_sidecar


def _report():
    return {"hard_gates": {key: True for key in ("HG-10", "HG-11", "HG-12", "HG-13")},
            "continuous_metrics": {key: (1.0 if op in {">=", "="} else 0.0)
                                   for key, (op, _) in _CONTRACTS.items()},
            "raw_p9_evidence": {"context_decisions": [{"decision_id": "d1"}],
                                 "outcomes": [{"outcome_id": "o1"}],
                                 "refresh_jobs": [{"job_id": "j1"}],
                                 "barrier_latencies_seconds": [0.1], "queue_counts": [0]}}


def test_p4_sidecar_binds_all_raw_surfaces():
    sidecar = build_p4_p9_sidecar(report=_report(), commit="a" * 40, worktree_clean=True)
    assert len(sidecar["p9_member_contributions"]["gate_members"]) == 4
    assert len(sidecar["p9_member_contributions"]["metric_members"]) == 10


def test_p4_sidecar_rejects_missing_raw_rows_and_extra_metrics():
    report = _report(); report["raw_p9_evidence"].pop("outcomes")
    with pytest.raises(ValueError, match="raw member"):
        build_p4_p9_sidecar(report=report, commit="a" * 40, worktree_clean=True)
    report = _report(); report["continuous_metrics"]["invented"] = 1
    with pytest.raises(ValueError, match="metric set"):
        build_p4_p9_sidecar(report=report, commit="a" * 40, worktree_clean=True)
