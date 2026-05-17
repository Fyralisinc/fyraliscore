"""CLI entry for `python -m services.ingestion.normalizer`.

Per M2 work-order §M2.3.

Two modes:
  - default            — run the supervisor (production shape).
  - --single-worker    — run one worker in this process (dev /
                          debugging; mirrors what each spawned child
                          does).
"""
from __future__ import annotations

import argparse
import os

from services.ingestion.normalizer.supervisor import (
    SupervisorConfig,
    run_supervisor,
)
from services.ingestion.normalizer.worker import main as worker_main


def cli() -> None:
    p = argparse.ArgumentParser(prog="services.ingestion.normalizer")
    p.add_argument(
        "--single-worker",
        action="store_true",
        help="Run one worker in this process (dev / debugging).",
    )
    p.add_argument(
        "--num-workers",
        type=int,
        default=int(os.environ.get("NORMALIZER_NUM_WORKERS", "2")),
        help="Number of worker processes when running the supervisor.",
    )
    args = p.parse_args()

    if args.single_worker:
        worker_main()
    else:
        run_supervisor(SupervisorConfig(num_workers=args.num_workers))


if __name__ == "__main__":
    cli()
