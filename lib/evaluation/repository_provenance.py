"""Cryptographic provenance for the Git repository that produced evidence."""

from __future__ import annotations

import hashlib
import os
import subprocess
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RepositoryProvenance(BaseModel):
    """The immutable commit and exact Git-visible worktree overlay."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    schema_version: Literal["git-repository-provenance-v1"] = (
        "git-repository-provenance-v1"
    )
    head_commit: str = Field(pattern=r"^[0-9a-f]{40,64}$")
    worktree_state: Literal["clean", "dirty"]
    worktree_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    digest_algorithm: Literal["sha256-git-overlay-v1"] = (
        "sha256-git-overlay-v1"
    )

    @model_validator(mode="after")
    def state_matches_digest_contract(self) -> Self:
        if not self.worktree_digest:
            raise ValueError("Git worktree provenance requires a digest")
        return self


def capture_repository_provenance(
    repository_root: Path,
) -> RepositoryProvenance:
    """Capture a stable Git commit plus staged, unstaged and untracked state."""

    root = Path(repository_root).resolve()
    first = _capture_once(root)
    second = _capture_once(root)
    if first != second:
        raise ValueError(
            "Git repository changed while assurance provenance was captured"
        )
    return first


def verify_repository_provenance(
    expected: RepositoryProvenance,
    repository_root: Path,
) -> None:
    """Reject an artifact reopened against a different repository state."""

    observed = capture_repository_provenance(repository_root)
    if observed.head_commit != expected.head_commit:
        raise ValueError(
            "company-learning assurance Git commit does not match repository"
        )
    if observed.worktree_state != expected.worktree_state:
        raise ValueError(
            "company-learning assurance Git worktree state does not match "
            "repository"
        )
    if observed.worktree_digest != expected.worktree_digest:
        raise ValueError(
            "company-learning assurance Git worktree digest does not match "
            "repository"
        )


def require_provenance_safe_output_path(
    output_path: Path,
    repository_root: Path,
) -> None:
    """Require in-repository generated evidence to be Git-ignored."""

    root = Path(repository_root).resolve()
    candidate = Path(output_path).resolve()
    try:
        relative = candidate.relative_to(root)
    except ValueError:
        return
    result = subprocess.run(
        (
            "git",
            "check-ignore",
            "--quiet",
            "--no-index",
            "--",
            os.fspath(relative),
        ),
        cwd=root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise ValueError(
            "company-learning assurance output inside the repository must be "
            "Git-ignored so generated evidence cannot invalidate source "
            "provenance"
        )


def _capture_once(root: Path) -> RepositoryProvenance:
    top_level = Path(
        os.fsdecode(_git(root, "rev-parse", "--show-toplevel")).strip()
    ).resolve()
    try:
        root.relative_to(top_level)
    except ValueError as exc:
        raise ValueError(
            "requested assurance path is not inside the Git worktree "
            f"reported by Git: {top_level}"
        ) from exc
    root = top_level

    head_commit = os.fsdecode(
        _git(root, "rev-parse", "--verify", "HEAD")
    ).strip()
    tracked_status = _git(
        root,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=no",
    )
    staged_diff = _git(
        root,
        "diff",
        "--cached",
        "--binary",
        "--no-ext-diff",
        "--full-index",
        "HEAD",
        "--",
    )
    unstaged_diff = _git(
        root,
        "diff",
        "--binary",
        "--no-ext-diff",
        "--full-index",
        "--",
    )
    untracked_paths = tuple(
        sorted(
            path
            for path in _git(
                root,
                "ls-files",
                "--others",
                "--exclude-standard",
                "-z",
            ).split(b"\0")
            if path
        )
    )

    digest = hashlib.sha256()
    digest.update(b"fyralis-git-overlay-v1\0")
    _digest_chunk(digest, b"tracked-status", tracked_status)
    _digest_chunk(digest, b"staged-diff", staged_diff)
    _digest_chunk(digest, b"unstaged-diff", unstaged_diff)
    for relative_path in untracked_paths:
        path = root / os.fsdecode(relative_path)
        if path.is_symlink():
            kind = b"symlink"
            content_digest = hashlib.sha256(
                os.fsencode(os.readlink(path))
            ).digest()
        elif path.is_file():
            kind = b"file"
            content_digest = _file_sha256(path)
        else:
            raise ValueError(
                "unsupported untracked repository entry while capturing "
                f"provenance: {os.fsdecode(relative_path)}"
            )
        _digest_chunk(digest, b"untracked-path", relative_path)
        _digest_chunk(digest, b"untracked-kind", kind)
        _digest_chunk(digest, b"untracked-content-sha256", content_digest)

    state: Literal["clean", "dirty"] = (
        "dirty" if tracked_status or untracked_paths else "clean"
    )
    return RepositoryProvenance(
        head_commit=head_commit,
        worktree_state=state,
        worktree_digest=digest.hexdigest(),
    )


def _git(root: Path, *args: str) -> bytes:
    result = subprocess.run(
        ("git", *args),
        cwd=root,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        detail = os.fsdecode(result.stderr).strip() or "unknown Git error"
        raise ValueError(
            f"unable to capture assurance repository provenance: {detail}"
        )
    return result.stdout


def _digest_chunk(
    digest: Any,
    label: bytes,
    value: bytes,
) -> None:
    digest.update(len(label).to_bytes(4, "big"))
    digest.update(label)
    digest.update(len(value).to_bytes(8, "big"))
    digest.update(value)


def _file_sha256(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.digest()


__all__ = [
    "RepositoryProvenance",
    "capture_repository_provenance",
    "require_provenance_safe_output_path",
    "verify_repository_provenance",
]
