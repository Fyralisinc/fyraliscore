#!/usr/bin/env python3
"""Render Fyralis production target load plans."""

from __future__ import annotations

import argparse
import json
from typing import Sequence

from services.platform.performance.load_profiles import (
    PROFILES,
    build_load_plan,
    cutover_env,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=sorted(PROFILES))
    parser.add_argument(
        "--scale",
        type=float,
        default=1.0,
        help="Positive multiplier for local smoke-sized plans.",
    )
    parser.add_argument("--duration-s", type=int, default=3600)
    parser.add_argument(
        "--format",
        choices=("json", "env"),
        default="json",
        help="Render full JSON plan or M-Load environment variables.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    plan = build_load_plan(
        args.profile,
        scale=args.scale,
        duration_s=args.duration_s,
    )
    if args.format == "env":
        for key, value in cutover_env(plan).items():
            print(f"{key}={value}")
    else:
        print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
