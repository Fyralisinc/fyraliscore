#!/usr/bin/env python3
"""Build the normalized P0 release artifact without DB or provider access."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p0_runner import run_p0_regeneration  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=ROOT)
    parser.add_argument(
        "--output", type=Path,
        default=Path("artifacts/epistemic-repair/p9/p0.normalized.json"),
    )
    args = parser.parse_args()
    report = run_p0_regeneration(args.repository)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0 if report["phase_exit_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
