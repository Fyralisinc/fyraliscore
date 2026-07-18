#!/usr/bin/env python3
"""Build the final P9 report after live git verification and reviewer reproduction."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p9_release import build_release_report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--reviewer-receipt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repository", type=Path, default=ROOT)
    args = parser.parse_args()
    repository = args.repository.resolve()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    clean = not subprocess.check_output(
        ["git", "status", "--porcelain", "--untracked-files=no"], cwd=repository, text=True,
    ).strip()
    report = build_release_report(
        manifest=json.loads(args.manifest.read_text()), verified_release_commit=commit,
        verified_worktree_clean=clean,
        reviewer_receipt=json.loads(args.reviewer_receipt.read_text()),
    )
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"verdict={report['verdict']} completion_authorized={report['completion_authorized']}")
    return 0 if report["completion_authorized"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
