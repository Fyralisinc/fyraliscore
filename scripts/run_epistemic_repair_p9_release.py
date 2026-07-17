#!/usr/bin/env python3
"""Compose the P9 release decision from a sealed evidence manifest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p9_release import (  # noqa: E402
    PhaseEvidence,
    build_release_report,
    write_release_report,
)


def _git(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    commit = _git("rev-parse", "HEAD")
    clean = not _git("status", "--porcelain", "--untracked-files=no")
    evidence = [
        PhaseEvidence(
            phase=item["phase"],
            path=(ROOT / item["path"]).resolve(),
            expected_sha256=item["sha256"],
            evidence_class=item.get("evidence_class", "integrated_current"),
        )
        for item in manifest["evidence"]
    ]
    report = build_release_report(
        release_commit=commit,
        worktree_clean=clean,
        evidence=evidence,
    )
    write_release_report(report, args.output)
    print(args.output)
    return 0 if report["completion_authorized"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
