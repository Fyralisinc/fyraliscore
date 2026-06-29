#!/usr/bin/env python3
"""Validate product workflow query performance budget coverage."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from services.app.gateway.product_workflow_metrics import (
    PRODUCT_WORKFLOWS,
    classify_product_workflow,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUDGET_PATH = REPO_ROOT / "docs/operations/product-query-performance-budgets.json"
REQUIRED_NUMERIC_FIELDS = (
    "beta_p95_seconds",
    "beta_p99_seconds",
    "ga_p95_seconds",
    "ga_p99_seconds",
)


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("budget registry must be a JSON object")
    return data


def validate_budget_registry(path: Path = DEFAULT_BUDGET_PATH) -> list[str]:
    data = _load_json(path)
    workflows = data.get("workflows")
    if not isinstance(workflows, dict):
        return ["workflows must be an object"]

    expected = set(PRODUCT_WORKFLOWS)
    actual = set(str(key) for key in workflows)
    violations: list[str] = []
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        violations.append(f"missing workflow budget(s): {', '.join(missing)}")
    if extra:
        violations.append(f"unknown workflow budget(s): {', '.join(extra)}")

    for workflow, raw_budget in sorted(workflows.items()):
        if not isinstance(raw_budget, dict):
            violations.append(f"{workflow}: budget must be an object")
            continue
        owner = raw_budget.get("owner")
        if not isinstance(owner, str) or not owner:
            violations.append(f"{workflow}: owner is required")
        for field in REQUIRED_NUMERIC_FIELDS:
            value = raw_budget.get(field)
            if not isinstance(value, (int, float)) or value <= 0:
                violations.append(f"{workflow}: {field} must be positive")
        for prefix in ("beta", "ga"):
            p95 = raw_budget.get(f"{prefix}_p95_seconds")
            p99 = raw_budget.get(f"{prefix}_p99_seconds")
            if isinstance(p95, (int, float)) and isinstance(p99, (int, float)):
                if p99 < p95:
                    violations.append(f"{workflow}: {prefix} p99 must be >= p95")
        hot_paths = raw_budget.get("hot_paths")
        if not isinstance(hot_paths, list) or not hot_paths:
            violations.append(f"{workflow}: hot_paths must be a non-empty list")
        else:
            for route in hot_paths:
                if not isinstance(route, str) or not route:
                    violations.append(f"{workflow}: hot_paths entries must be strings")
                    continue
                classified = classify_product_workflow(route)
                if classified != workflow:
                    violations.append(
                        f"{workflow}: hot path {route!r} classifies as {classified!r}"
                    )
        index_review = raw_budget.get("index_review")
        if not isinstance(index_review, list) or not index_review:
            violations.append(
                f"{workflow}: index_review must include query/index evidence"
            )
        elif not all(isinstance(item, str) and item for item in index_review):
            violations.append(f"{workflow}: index_review entries must be strings")
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--budget-path", type=Path, default=DEFAULT_BUDGET_PATH)
    args = parser.parse_args(argv)

    try:
        violations = validate_budget_registry(args.budget_path)
    except Exception as exc:
        print(f"Product query budget validation failed: {exc}", file=sys.stderr)
        return 1
    if violations:
        print("Product query budget violations:", file=sys.stderr)
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        return 1
    print("Product query performance budgets passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
