from __future__ import annotations

from pathlib import Path

from lib.evaluation.epistemic_repair import p0_runner


ROOT = Path(__file__).resolve().parents[3]


def test_p0_regeneration_reopens_raw_inventory_members(monkeypatch) -> None:
    monkeypatch.setattr(p0_runner, "git_run_provenance", lambda _: {
        "git_commit": "a" * 40,
        "worktree_clean": True,
        "repository_root": str(ROOT),
    })
    monkeypatch.setattr(p0_runner.subprocess, "run", lambda *args, **kwargs: type(
        "Result", (), {"returncode": 0, "stdout": "passed", "stderr": ""}
    )())
    report = p0_runner.run_p0_regeneration(ROOT)
    contributions = report["p9_member_contributions"]
    assert report["phase_exit_ready"]
    assert len(report["inventory_receipts"]) == len(p0_runner.P0_INVENTORIES)
    assert set(contributions["gate_members"]) == {
        "P0-baseline-integrity", "P0-preregistration-integrity",
        "P0-inventory-completeness",
    }
    assert contributions["metric_members"] == {}
    assert report["run_provenance"]["worktree_clean"] is True
