#!/usr/bin/env python3
"""Evaluate a sealed learned-memory versus frozen-memory company-model run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lib.evaluation.company_model_ablation import evaluate_company_model_ablation


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--learned", type=Path, required=True)
    parser.add_argument("--frozen", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = evaluate_company_model_ablation(
        manifest=_read(args.manifest),
        learned=_read(args.learned),
        frozen=_read(args.frozen),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output)
    if report["verdict"] != "meets_policy":
        raise SystemExit(1)


def _read(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return value


if __name__ == "__main__":
    main()
