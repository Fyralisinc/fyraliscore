"""Compare benchmark metrics against a committed baseline."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


DEFAULT_METRIC_KEYWORDS = (
    "accuracy",
    "answer_support",
    "evidence_precision",
    "evidence_recall",
    "f1",
    "hit_rate",
    "longmemeval_v2",
    "precision_at",
    "recall_at",
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument(
        "--absolute-tolerance",
        type=float,
        default=0.02,
        help="Allowed absolute regression before the check fails.",
    )
    parser.add_argument(
        "--metric",
        action="append",
        dest="metrics",
        default=None,
        help="Specific metric key to compare. May be passed multiple times.",
    )
    args = parser.parse_args(argv)

    baseline = _load_json(args.baseline)
    current = _load_json(args.current)
    metric_keys = args.metrics or _quality_metric_keys(baseline)
    failures: list[str] = []
    skipped: list[str] = []
    checked: list[str] = []
    for key in metric_keys:
        baseline_value = baseline.get(key)
        current_value = current.get(key)
        if not isinstance(baseline_value, int | float):
            skipped.append(f"{key}: non-numeric baseline")
            continue
        if not isinstance(current_value, int | float):
            failures.append(f"{key}: missing numeric current value")
            continue
        floor = float(baseline_value) - args.absolute_tolerance
        checked.append(key)
        if float(current_value) < floor:
            failures.append(
                f"{key}: current {current_value:.4f} below baseline "
                f"{baseline_value:.4f} - tolerance {args.absolute_tolerance:.4f}"
            )

    print(
        json.dumps(
            {
                "baseline": str(args.baseline),
                "current": str(args.current),
                "absolute_tolerance": args.absolute_tolerance,
                "checked": checked,
                "skipped": skipped,
                "failures": failures,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 1 if failures else 0


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return payload


def _quality_metric_keys(metrics: dict[str, Any]) -> list[str]:
    keys: list[str] = []
    for key, value in metrics.items():
        normalized = key.casefold()
        if not isinstance(value, int | float):
            continue
        if any(keyword in normalized for keyword in DEFAULT_METRIC_KEYWORDS):
            keys.append(key)
    return sorted(keys)


if __name__ == "__main__":
    sys.exit(main())
