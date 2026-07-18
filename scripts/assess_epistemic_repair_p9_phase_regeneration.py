#!/usr/bin/env python3
"""Emit a typed P0-P5 rerun requirement without upgrading stale summaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p9_phase_regeneration import assess_phase


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=tuple(f"p{i}" for i in range(6)), required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--repository", type=Path, default=ROOT)
    args = parser.parse_args()
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=args.repository.resolve(), text=True,
    ).strip()
    result = assess_phase(phase=args.phase, source_path=args.source, release_commit=commit)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"phase={args.phase} status={result['status']} missing={len(result['missing_evidence'])}")
    return 0 if result["status"] == "normalization_ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
