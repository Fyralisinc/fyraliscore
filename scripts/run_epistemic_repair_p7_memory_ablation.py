#!/usr/bin/env python3
"""Run the provider-free P7 matched memory-ablation mechanics proof."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lib.evaluation.epistemic_repair.p7_runner import build_p7_artifact


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact = build_p7_artifact()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact.model_dump(mode="json"), indent=2) + "\n")
    print(json.dumps({
        "deterministic_mechanical_ready": artifact.deterministic_mechanical_ready,
        "phase_exit_ready": artifact.phase_exit_ready,
        "strategic_verdict": artifact.strategic_verdict,
        "executed_world_count": artifact.executed_world_count,
        "output": str(args.output),
    }))
    return 0 if artifact.deterministic_mechanical_ready else 1


if __name__ == "__main__":
    raise SystemExit(main())
