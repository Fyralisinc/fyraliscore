#!/usr/bin/env python3
"""Validate the canonical architecture registry and report proof coverage."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lib.architecture_registry import (
    ArchitectureRegistryError,
    load_architecture_registry,
    validate_architecture_registry,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = ROOT / "architecture" / "registry.yaml"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="fail unless every INV-01..42 row is mapped and every contract is implemented",
    )
    args = parser.parse_args()
    try:
        registry = load_architecture_registry(args.registry)
        report = validate_architecture_registry(registry, root=ROOT)
    except ArchitectureRegistryError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}, indent=2))
        return 1

    payload = report.model_dump(mode="json")
    payload["internally_valid"] = report.internally_valid
    payload["production_freeze_ready"] = report.production_freeze_ready
    print(json.dumps(payload, indent=2, sort_keys=True))
    if not report.internally_valid:
        return 1
    if args.require_complete and not report.production_freeze_ready:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
