import json

import pytest

from lib.evaluation.epistemic_repair.p9_phase_regeneration import PHASE_CONTRACTS, assess_phase


@pytest.mark.parametrize("phase", tuple(f"p{i}" for i in range(6)))
def test_phase_summaries_never_substitute_for_member_evidence(tmp_path, phase):
    path = tmp_path / f"{phase}.json"
    path.write_text(json.dumps({
        "commit": "a" * 40, "run_provenance": {"git_commit": "a" * 40, "worktree_clean": True},
        "phase_exit_ready": True,
        "hard_gates": {gate: True for gate in PHASE_CONTRACTS[phase]["gates"]},
        "continuous_metrics": {metric: 1.0 for metric in PHASE_CONTRACTS[phase]["metrics"]},
    }))
    result = assess_phase(phase=phase, source_path=path, release_commit="a" * 40)
    assert result["status"] == "rerun_required"
    assert "p9_member_contributions" in result["missing_evidence"]
    assert result["summary_flags_trusted"] is False


def test_wrong_commit_and_partial_member_sets_fail_closed(tmp_path):
    path = tmp_path / "p3.json"
    path.write_text(json.dumps({
        "commit": "b" * 40, "run_provenance": {"git_commit": "b" * 40, "worktree_clean": True},
        "member_receipts": [{"id": "one"}], "correction_receipts": [{"id": "two"}],
        "sealed_manifest": {"digest": "x"},
        "p9_member_contributions": {
            "gate_members": {"HG-02": [{"member_id": "one", "conforms": True}]},
            "metric_members": {}, "preregistered_contract_digest": "x" * 64,
        },
    }))
    result = assess_phase(phase="p3", source_path=path, release_commit="a" * 40)
    assert "full_release_commit_provenance" in result["missing_evidence"]
    assert "exact_gate_member_denominators" in result["missing_evidence"]
    assert "exact_metric_member_denominators" in result["missing_evidence"]


def test_full_sha_is_mandatory(tmp_path):
    with pytest.raises(ValueError, match="full lowercase"):
        assess_phase(phase="p0", source_path=tmp_path / "none", release_commit="abc123")
