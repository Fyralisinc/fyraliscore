#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p1_finalize import finalize_p1_files  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--deterministic", type=Path, required=True)
    parser.add_argument("--real-smoke", type=Path, required=True)
    parser.add_argument("--durability", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    report = finalize_p1_files(
        deterministic_path=args.deterministic,
        real_smoke_path=args.real_smoke,
        durability_path=args.durability,
        commit=commit,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0 if report["phase_exit_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
