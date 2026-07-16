#!/usr/bin/env python3
"""Seal the corrective-memory negative-control population for execution."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.company_learning_recurrence_runtime import (
    DEFAULT_NEGATIVE_CONTROL_FIXTURE,
    build_negative_control_plan,
    load_negative_control_fixture,
    write_negative_control_plan,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    fixture = load_negative_control_fixture(args.fixture)
    plan = build_negative_control_plan(
        fixture,
        run_id=args.run_id,
        system_version=args.system_version,
        fixture_path=args.fixture,
    )
    output_path = args.output_dir / "company_learning_negative_controls_plan.json"
    write_negative_control_plan(plan, output_path)
    print(f"status={plan.status}")
    print(f"fixture_digest={plan.fixture_digest}")
    print(f"spec_digest={plan.spec.digest}")
    print(f"plan_digest={plan.digest}")
    print(f"artifact={output_path}")
    print(
        "next_dependency=execute all sealed cases against real Postgres and "
        "compile typed adaptive/frozen results"
    )
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser = argparse.ArgumentParser(
        description=(
            "Validate and seal the contextual, unrelated, homonym and "
            "conflicting-source corrective-memory controls."
        )
    )
    parser.add_argument(
        "--fixture",
        type=Path,
        default=DEFAULT_NEGATIVE_CONTROL_FIXTURE,
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--run-id",
        default=f"company-learning-negative-controls-{timestamp}",
    )
    parser.add_argument("--system-version", default="local-working-tree")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
