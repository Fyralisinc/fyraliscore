from pathlib import Path
import subprocess

import pytest

from scripts.prepare_epistemic_repair_p8_coherent_rerun import build_plan


def test_coherent_plan_is_pinned_and_never_authorizes_extra_canary(tmp_path: Path) -> None:
    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "p8@example.test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "P8"], cwd=tmp_path, check=True)
    (tmp_path / "tracked").write_text("v1")
    subprocess.run(["git", "add", "tracked"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "baseline"], cwd=tmp_path, check=True)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    plan = build_plan(repository=tmp_path, output_dir=tmp_path / "out", expected_head=head)
    assert plan["commit_sha"] == head
    assert plan["requirements"]["separate_provider_canary_authorized"] is False
    assert all("provider-canary" not in token for command in plan["commands"] for token in command)
    (tmp_path / "tracked").write_text("dirty")
    with pytest.raises(ValueError, match="clean tracked worktree"):
        build_plan(repository=tmp_path, output_dir=tmp_path / "out", expected_head=head)
