#!/usr/bin/env python3
"""Generate the provider-free P1 observability exit artifact."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p1_exit import (  # noqa: E402
    run_p1_exit_evaluation,
    write_p1_exit_artifact,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output", type=Path,
        default=Path("artifacts/epistemic-repair/p9/p1.normalized.json"),
    )
    args = parser.parse_args()
    report = asyncio.run(run_p1_exit_evaluation(repository_root=ROOT))
    write_p1_exit_artifact(report, args.output)
    print(args.output)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
