#!/usr/bin/env python3
"""Generate the provider-free P1 observability exit artifact."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from lib.evaluation.epistemic_repair.p1_exit import (
    ARTIFACT_NAME,
    run_p1_exit_evaluation,
    write_p1_exit_artifact,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("reports") / ARTIFACT_NAME)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    report = asyncio.run(run_p1_exit_evaluation(repository_root=root))
    write_p1_exit_artifact(report, args.output)
    print(args.output)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
