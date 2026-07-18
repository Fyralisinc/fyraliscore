#!/usr/bin/env python3
"""Build strict normalized P6 evidence for the P9 release contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p6_p9 import build_p6_p9_sidecar


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execution", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--score", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact = build_p6_p9_sidecar(
        execution_path=args.execution, evidence_path=args.evidence, score_path=args.score,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    print(f"p6_p9_ready={str(artifact['phase_exit_ready']).lower()} output={args.output}")
    return 0 if artifact["phase_exit_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
