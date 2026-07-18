#!/usr/bin/env python3
"""Run the preregistered P8 warm-pair diagnostic under coordinator ownership."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from services.evaluation.epistemic_repair.p8_latency_diagnostic import run_repeated_warm_pairs
from lib.contracts.kernel import canonical_sha256


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--repetitions", type=int, default=5)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    artifact = asyncio.run(run_repeated_warm_pairs(args.database_url, repetitions=args.repetitions))
    artifact["commit"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
    ).strip()
    artifact.pop("artifact_digest", None)
    artifact["artifact_digest"] = canonical_sha256(artifact)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(artifact, indent=2, sort_keys=True, default=str) + "\n")
    print(f"diagnostic_complete={artifact['analysis']['diagnostic_complete']} output={args.output}")
    return 0 if artifact["analysis"]["diagnostic_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
