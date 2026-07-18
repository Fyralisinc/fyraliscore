#!/usr/bin/env python3
"""Seal a P9 manifest against the current clean git HEAD and frozen artifacts."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p9_release import (
    CONTENT_DIGEST_ALGORITHM, ManifestEvidence, seal_manifest,
)


def _git_state(repository: Path) -> str:
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=repository, text=True,
    ).strip()
    if dirty:
        raise RuntimeError("P9 manifest requires a clean tracked worktree")
    return commit


def _entry(spec: dict, commit: str) -> ManifestEvidence:
    path = Path(spec["path"]).resolve()
    artifact = json.loads(path.read_text())
    field = spec["content_digest_field"]
    return ManifestEvidence(
        str(path), spec["schema_version"], spec.get("commit", commit), sha256(path.read_bytes()).hexdigest(),
        str(artifact.get(field, "")), field, CONTENT_DIGEST_ALGORITHM,
        spec["evidence_class"], tuple(spec["required_gate_ids"]),
        tuple(spec["required_metric_ids"]),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--coordinator-id", required=True)
    parser.add_argument("--repository", type=Path, default=ROOT)
    args = parser.parse_args()
    commit = _git_state(args.repository.resolve())
    spec = json.loads(args.spec.read_text())
    required = {key: _entry(value, commit) for key, value in spec["required_current"].items()}
    diagnostics = tuple(_entry(value, commit) for value in spec.get("diagnostics", ()))
    manifest = seal_manifest(
        coordinator_id=args.coordinator_id, release_commit=commit,
        required_current=required, diagnostics=diagnostics,
    )
    args.output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(f"manifest_digest={manifest['manifest_digest']} commit={commit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
