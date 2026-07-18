#!/usr/bin/env python3
"""Build strict P9-normalized P7 evidence from the frozen oracle report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p7_p9 import build_p7_p9_sidecar  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--score", type=Path, required=True)
    parser.add_argument("--raw-execution-artifact", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path,
        default=Path("artifacts/epistemic-repair/p9/p7.normalized.json"),
    )
    args = parser.parse_args()
    score = json.loads(args.score.read_text(encoding="utf-8"))
    result = build_p7_p9_sidecar(
        score, raw_execution_artifact_path=args.raw_execution_artifact,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(f"output={args.output} strategic_decision={result['strategic_decision']}")
    return 0 if result["phase_exit_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
