from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from lib.evaluation.repository_provenance import (
    capture_repository_provenance,
    require_provenance_safe_output_path,
    verify_repository_provenance,
)


def test_repository_provenance_binds_commit_and_complete_dirty_overlay(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "pytest@example.com")
    _git(tmp_path, "config", "user.name", "Pytest")
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("committed\n", encoding="utf-8")
    _git(tmp_path, "add", "tracked.txt")
    _git(tmp_path, "commit", "-m", "initial")

    clean = capture_repository_provenance(tmp_path)

    assert clean.worktree_state == "clean"
    assert len(clean.head_commit) == 40
    verify_repository_provenance(clean, tmp_path)

    tracked.write_text("unstaged\n", encoding="utf-8")
    unstaged = capture_repository_provenance(tmp_path)
    assert unstaged.worktree_state == "dirty"
    assert unstaged.worktree_digest != clean.worktree_digest

    _git(tmp_path, "add", "tracked.txt")
    staged = capture_repository_provenance(tmp_path)
    assert staged.worktree_digest != unstaged.worktree_digest

    untracked = tmp_path / "untracked.txt"
    untracked.write_text("first\n", encoding="utf-8")
    first_untracked = capture_repository_provenance(tmp_path)
    untracked.write_text("second\n", encoding="utf-8")
    second_untracked = capture_repository_provenance(tmp_path)
    assert (
        first_untracked.worktree_digest
        != second_untracked.worktree_digest
    )

    with pytest.raises(ValueError, match="worktree digest"):
        verify_repository_provenance(first_untracked, tmp_path)


def test_repository_provenance_rejects_a_different_commit(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "pytest@example.com")
    _git(tmp_path, "config", "user.name", "Pytest")
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("one\n", encoding="utf-8")
    _git(tmp_path, "add", "tracked.txt")
    _git(tmp_path, "commit", "-m", "one")
    first = capture_repository_provenance(tmp_path)

    tracked.write_text("two\n", encoding="utf-8")
    _git(tmp_path, "add", "tracked.txt")
    _git(tmp_path, "commit", "-m", "two")

    with pytest.raises(ValueError, match="Git commit"):
        verify_repository_provenance(first, tmp_path)


def test_generated_output_inside_repository_must_be_ignored(
    tmp_path: Path,
) -> None:
    _git(tmp_path, "init")
    _git(tmp_path, "config", "user.email", "pytest@example.com")
    _git(tmp_path, "config", "user.name", "Pytest")
    (tmp_path / ".gitignore").write_text(
        "/ignored-output/\n",
        encoding="utf-8",
    )
    _git(tmp_path, "add", ".gitignore")
    _git(tmp_path, "commit", "-m", "ignore generated output")

    require_provenance_safe_output_path(
        tmp_path / "ignored-output" / "run",
        tmp_path,
    )
    require_provenance_safe_output_path(
        tmp_path.parent / "outside-output",
        tmp_path,
    )
    with pytest.raises(ValueError, match="must be Git-ignored"):
        require_provenance_safe_output_path(
            tmp_path / "visible-output" / "run",
            tmp_path,
        )


def _git(root: Path, *args: str) -> None:
    subprocess.run(
        ("git", *args),
        cwd=root,
        check=True,
        capture_output=True,
    )
