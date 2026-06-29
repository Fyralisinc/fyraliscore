#!/usr/bin/env python3
"""Estimate Fyralis launch-profile costs from configurable unit prices."""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from typing import Sequence

from services.platform.performance.cost_model import (
    CostAssumptions,
    estimate_cost_profile,
)
from services.platform.performance.load_profiles import PROFILES


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=sorted(PROFILES))
    parser.add_argument("--scale", type=float, default=1.0)
    parser.add_argument("--gateway-compute-usd-per-hour", type=float)
    parser.add_argument("--worker-compute-usd-per-hour", type=float)
    parser.add_argument("--postgres-compute-usd-per-hour", type=float)
    parser.add_argument("--broker-compute-usd-per-hour", type=float)
    parser.add_argument("--object-storage-usd-per-gb-month", type=float)
    parser.add_argument("--postgres-storage-usd-per-gb-month", type=float)
    parser.add_argument("--observability-usd-per-day", type=float)
    return parser.parse_args(argv)


def _assumptions_from_args(args: argparse.Namespace) -> CostAssumptions:
    assumptions = CostAssumptions()
    replacements = {
        name: value
        for name, value in {
            "gateway_compute_usd_per_hour": args.gateway_compute_usd_per_hour,
            "worker_compute_usd_per_hour": args.worker_compute_usd_per_hour,
            "postgres_compute_usd_per_hour": args.postgres_compute_usd_per_hour,
            "broker_compute_usd_per_hour": args.broker_compute_usd_per_hour,
            "object_storage_usd_per_gb_month": args.object_storage_usd_per_gb_month,
            "postgres_storage_usd_per_gb_month": args.postgres_storage_usd_per_gb_month,
            "observability_usd_per_day": args.observability_usd_per_day,
        }.items()
        if value is not None
    }
    return replace(assumptions, **replacements)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    estimate = estimate_cost_profile(
        args.profile,
        scale=args.scale,
        assumptions=_assumptions_from_args(args),
    )
    print(json.dumps(estimate, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
