from pathlib import Path

import pytest

from lib.evaluation.epistemic_repair.p3_p9 import build_p3_p9_sidecar
from lib.evaluation.epistemic_repair.p3_runner import run_p3_perception_grounding
from tests.epistemic_repair.p3.test_p3_runner_contract import _runtime


def _report():
    proof = {"hg02_conforms": True, "hg06_conforms": True, "hg14_conforms": True,
             "raw_receipts": [{"probe_id": "p3-proof", "conforms": True}]}
    return run_p3_perception_grounding(
        repository_root=Path.cwd(), runtime=_runtime(), postgres_proof=proof,
    ), proof


def test_p3_sidecar_requires_and_binds_raw_postgres_proof():
    report, proof = _report()
    sidecar = build_p3_p9_sidecar(
        report=report, postgres_proof=proof, commit="a" * 40, worktree_clean=True,
    )
    assert len(sidecar["p9_member_contributions"]["gate_members"]) == 4
    assert len(sidecar["p9_member_contributions"]["metric_members"]) == 14
    assert sidecar["run_provenance"]["worktree_clean"] is True


def test_p3_sidecar_rejects_missing_probe_or_edited_artifact():
    report, proof = _report()
    with pytest.raises(ValueError, match="PostgreSQL probe"):
        build_p3_p9_sidecar(report=report, postgres_proof=None,
                            commit="a" * 40, worktree_clean=True)
    report["member_receipts"] = report["member_receipts"][:-1]
    with pytest.raises(ValueError):
        build_p3_p9_sidecar(report=report, postgres_proof=proof,
                            commit="a" * 40, worktree_clean=True)
