#!/usr/bin/env python3
"""Validate production concurrency-control configuration coverage."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.check_production_env_contract import (
    DEFAULT_ENV_TEMPLATE,
    REQUIRED_EXACT_VALUES,
    REQUIRED_KEYS,
    REQUIRED_POSITIVE_INTEGER_KEYS,
    parse_env_template,
)
from services.platform.performance.concurrency_controls import (
    CONCURRENCY_CONTROLS,
    EXPENSIVE_WORKER_GATES,
)


def _env_values(path: Path) -> dict[str, str]:
    return {entry.key: entry.value for entry in parse_env_template(path)}


def validate_concurrency_controls(
    *,
    env_template: Path = DEFAULT_ENV_TEMPLATE,
    required_keys: frozenset[str] = REQUIRED_KEYS,
    positive_integer_keys: frozenset[str] = REQUIRED_POSITIVE_INTEGER_KEYS,
    exact_values: dict[str, str] = REQUIRED_EXACT_VALUES,
) -> list[str]:
    values = _env_values(env_template)
    violations: list[str] = []

    for control in CONCURRENCY_CONTROLS:
        if control.env_key not in required_keys:
            violations.append(f"{control.env_key}: missing from REQUIRED_KEYS")
        if control.env_key not in positive_integer_keys:
            violations.append(
                f"{control.env_key}: must be a required positive integer key"
            )
        value = values.get(control.env_key)
        if value is None:
            violations.append(f"{control.env_key}: missing from env template")
            continue
        try:
            parsed = int(value)
        except ValueError:
            violations.append(f"{control.env_key}: env value must be an integer")
            continue
        if parsed <= 0:
            violations.append(f"{control.env_key}: env value must be positive")

    for env_key, expected in EXPENSIVE_WORKER_GATES.items():
        if env_key not in required_keys:
            violations.append(f"{env_key}: missing from REQUIRED_KEYS")
        if exact_values.get(env_key) != expected:
            violations.append(f"{env_key}: expected exact contract value {expected!r}")
        actual = values.get(env_key)
        if actual is None:
            violations.append(f"{env_key}: missing from env template")
        elif actual != expected:
            violations.append(
                f"{env_key}: env template must default to {expected!r}, found {actual!r}"
            )
    return violations


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-template", type=Path, default=DEFAULT_ENV_TEMPLATE)
    args = parser.parse_args(argv)

    violations = validate_concurrency_controls(env_template=args.env_template)
    if violations:
        print("Concurrency-control violations:", file=sys.stderr)
        for violation in violations:
            print(f"  - {violation}", file=sys.stderr)
        return 1
    print("Concurrency controls passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
