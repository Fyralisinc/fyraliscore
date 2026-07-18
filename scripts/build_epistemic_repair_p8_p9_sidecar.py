#!/usr/bin/env python3
"""Build strict normalized P8 evidence from a coherent same-commit artifact set."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p8_p9 import build_p8_p9_sidecar


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("exit", "fault", "scale", "characterization", "contention", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    sidecar = build_p8_p9_sidecar(
        exit_path=args.exit, fault_path=args.fault, scale_path=args.scale,
        characterization_path=args.characterization, contention_path=args.contention,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
    print(f"phase_exit_ready={sidecar['phase_exit_ready']} output={args.output}")
    return 0 if sidecar["phase_exit_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
