from pathlib import Path
import json

import pytest

from lib.evaluation.epistemic_repair.p3_p9 import build_p3_p9_sidecar
from lib.evaluation.epistemic_repair.p3_runner import run_p3_perception_grounding
from tests.epistemic_repair.p3.test_p3_runner_contract import _runtime
from lib.contracts.kernel import canonical_sha256


def _report():
    proof = {"hg02_conforms": True, "hg06_conforms": True, "hg14_conforms": True,
             "raw_receipts": [{"probe_id": "p3-proof", "conforms": True}]}
    return run_p3_perception_grounding(
        repository_root=Path.cwd(), runtime=_runtime(), postgres_proof=proof,
    ), proof


def _paths(tmp_path: Path, report: dict, proof: dict) -> tuple[Path, Path]:
    report_path, proof_path = tmp_path / "report.json", tmp_path / "proof.json"
    report_path.write_text(json.dumps(report))
    body = {"schema_version": "epistemic-repair-p3-postgres-proof-v1", "commit": "a" * 40,
            "proof": proof}
    proof_path.write_text(json.dumps({**body, "content_digest": canonical_sha256(body)}))
    return report_path, proof_path


def test_p3_sidecar_requires_and_binds_raw_postgres_proof(tmp_path: Path):
    report, proof = _report()
    report_path, proof_path = _paths(tmp_path, report, proof)
    sidecar = build_p3_p9_sidecar(
        report_path=report_path, postgres_proof_path=proof_path,
        commit="a" * 40, worktree_clean=True,
    )
    assert len(sidecar["p9_member_contributions"]["gate_members"]) == 4
    assert len(sidecar["p9_member_contributions"]["metric_members"]) == 14
    assert sidecar["run_provenance"]["worktree_clean"] is True


def test_p3_sidecar_rejects_missing_probe_or_edited_artifact(tmp_path: Path):
    report, proof = _report()
    report_path, proof_path = _paths(tmp_path, report, proof)
    proof_artifact = json.loads(proof_path.read_text())
    proof_artifact["proof"]["hg02_conforms"] = False
    proof_path.write_text(json.dumps(proof_artifact))
    with pytest.raises(ValueError, match="proof digest"):
        build_p3_p9_sidecar(report_path=report_path, postgres_proof_path=proof_path,
                            commit="a" * 40, worktree_clean=True)
    report["member_receipts"] = report["member_receipts"][:-1]
    report_path.write_text(json.dumps(report))
    _, proof_path = _paths(tmp_path, report, proof)
    with pytest.raises(ValueError):
        build_p3_p9_sidecar(report_path=report_path, postgres_proof_path=proof_path,
                            commit="a" * 40, worktree_clean=True)
