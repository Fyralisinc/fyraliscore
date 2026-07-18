#!/usr/bin/env python3
"""Compose the coherent, fail-closed P8 exit artifact from exact member artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p8_exit import compose_p8_exit
from lib.evaluation.epistemic_repair.p8_p9 import build_p8_p9_sidecar


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fault", type=Path, required=True)
    parser.add_argument("--scale", type=Path, required=True)
    parser.add_argument("--characterization", type=Path, required=True)
    parser.add_argument("--contention", type=Path, required=True)
    parser.add_argument("--provider-canary", type=Path)
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/epistemic-repair-p8-fault-scale-v1.json"))
    parser.add_argument("--p9-output", type=Path)
    args = parser.parse_args()
    artifact = compose_p8_exit(
        fault_path=args.fault, scale_path=args.scale,
        characterization_path=args.characterization,
        contention_path=args.contention, provider_canary_path=args.provider_canary,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    sidecar_ready = True
    if args.p9_output is not None:
        sidecar = build_p8_p9_sidecar(
            exit_path=args.output, fault_path=args.fault, scale_path=args.scale,
            characterization_path=args.characterization, contention_path=args.contention,
        )
        args.p9_output.parent.mkdir(parents=True, exist_ok=True)
        args.p9_output.write_text(json.dumps(sidecar, indent=2, sort_keys=True) + "\n")
        sidecar_ready = sidecar["phase_exit_ready"]
        if not sidecar["phase_exit_ready"]:
            print(f"p8_p9_ready=false output={args.p9_output}")
    print(f"p8_exit_ready={str(artifact['exit_ready']).lower()} digest={artifact['artifact_digest']} output={args.output}")
    return 0 if artifact["exit_ready"] and sidecar_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
