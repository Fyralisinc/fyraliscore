#!/usr/bin/env python3
"""Generate the deterministic held-out exact-alias population fixture."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.company_learning_population import (
    build_exact_alias_heldout_population,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=(
            Path("tests")
            / "fixtures"
            / "company_learning"
            / "held_out_exact_alias_population_v1.jsonl"
        ),
    )
    parser.add_argument("--size", type=int, default=60)
    args = parser.parse_args(argv)
    population = build_exact_alias_heldout_population(size=args.size)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        "".join(
            json.dumps(case.model_dump(mode="json"), sort_keys=True) + "\n"
            for case in population.cases
        ),
        encoding="utf-8",
    )
    print(
        f"wrote {len(population.cases)} cases to {args.output} "
        f"population_digest={population.digest}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
