import pytest

from lib.evaluation.epistemic_repair.p5_p9 import build_p5_p9_sidecar
from tests.epistemic_repair.p5.test_p5_oracle_adversarial import _fabricated_success_inputs
from lib.evaluation.epistemic_repair.p5_oracles import build_p5_artifact


def _artifact():
    population, signals, vertical, barriers, database_evidence = _fabricated_success_inputs()
    return build_p5_artifact(
        population=population, signals=signals, vertical=vertical, barriers=barriers,
        zero_seed_initial_model_count=0, provider_call_count=0,
        database_evidence=database_evidence, timings_ms={},
    )


def test_p5_sidecar_binds_exact_raw_members_and_metrics():
    artifact = _artifact()
    sidecar = build_p5_p9_sidecar(
        artifact=artifact, commit="a" * 40, worktree_clean=True,
    )
    assert sidecar["phase_exit_ready"] is True
    assert len(sidecar["p9_member_contributions"]["gate_members"]) == 10
    assert len(sidecar["p9_member_contributions"]["metric_members"]) == 12
    assert all(row["denominator"] > 0 for row in sidecar["p9_continuous_metrics"])


def test_p5_sidecar_rejects_dirty_or_tampered_artifact():
    artifact = _artifact()
    with pytest.raises(ValueError, match="clean full"):
        build_p5_p9_sidecar(artifact=artifact, commit="a" * 40, worktree_clean=False)
    object.__setattr__(artifact, "content_digest", "0" * 64)
    with pytest.raises(ValueError, match="digest"):
        build_p5_p9_sidecar(artifact=artifact, commit="a" * 40, worktree_clean=True)
