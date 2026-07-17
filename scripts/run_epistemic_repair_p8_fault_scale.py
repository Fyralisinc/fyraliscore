#!/usr/bin/env python3
"""Run deterministic P8 fault, restart, replay, and scale characterization."""

from __future__ import annotations

import argparse
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.p8_runner import (
    reopen_p8_artifact,
    run_p8_deterministic,
    write_p8_artifact,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("docs/plans/epistemic-repair/p8/epistemic-repair-p8-fault-scale-v1.json"))
    args = parser.parse_args()
    artifact = run_p8_deterministic()
    write_p8_artifact(artifact, args.output)
    artifact = reopen_p8_artifact(args.output)
    print(f"evaluator_contract_ready={str(artifact['evaluator_contract_ready']).lower()} deterministic_qualification_ready={str(artifact['deterministic_qualification_ready']).lower()} phase_exit_ready={str(artifact['phase_exit_ready']).lower()} fault_vectors={len(artifact['fault_reference_vectors'])} scale_vectors={len(artifact['scale_reference_vectors'])} provider_canaries={artifact['real_provider_canaries']['status']} output={args.output}")
    return 0 if artifact["evaluator_contract_ready"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
