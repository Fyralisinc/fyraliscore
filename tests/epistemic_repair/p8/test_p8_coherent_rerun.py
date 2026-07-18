from pathlib import Path
import subprocess

import pytest

from scripts.prepare_epistemic_repair_p8_coherent_rerun import build_plan


def test_coherent_plan_is_pinned_and_wires_authorized_canary_and_p9(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "p8@example.test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "P8"], cwd=tmp_path, check=True)
    (tmp_path / "tracked").write_text("v1")
    subprocess.run(["git", "add", "tracked"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "baseline"], cwd=tmp_path, check=True)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    plan = build_plan(
        repository=tmp_path, output_dir=tmp_path / "out", expected_head=head,
        canary_authorization_id="review-approved-canary-1",
    )
    assert plan["commit_sha"] == head
    assert plan["requirements"]["separate_provider_canary_authorized"] is True
    flattened = [token for command in plan["commands"] for token in command]
    assert "scripts/run_epistemic_repair_p8_provider_canary.py" in flattened
    assert "--provider-canary" in flattened and "--p9-output" in flattened
    (tmp_path / "tracked").write_text("dirty")
    with pytest.raises(ValueError, match="clean tracked worktree"):
        build_plan(repository=tmp_path, output_dir=tmp_path / "out", expected_head=head,
                   canary_authorization_id="review-approved-canary-1")
