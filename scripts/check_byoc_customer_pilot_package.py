#!/usr/bin/env python3
"""Validate a sanitized BYOC customer-pilot package manifest."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_customer_pilot_package import (
    ByocCustomerPilotPackageValidationInputs,
    render_customer_pilot_package_validation_json,
    render_customer_pilot_package_validation_yaml,
    validate_byoc_customer_pilot_package,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(
            "tmp/byoc/customer-pilot/byoc-customer-pilot-package-manifest.json"
        ),
        help="BYOC customer-pilot package manifest to validate.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used to resolve manifest artifact paths.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized validation result.",
    )
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="Exit non-zero unless the package is integrity-valid and launch-ready.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    validation = validate_byoc_customer_pilot_package(
        ByocCustomerPilotPackageValidationInputs(
            manifest_path=args.manifest,
            repo_root=args.repo_root,
        )
    )
    rendered = (
        render_customer_pilot_package_validation_json(validation)
        if args.json
        else render_customer_pilot_package_validation_yaml(validation)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    if validation.status == "fail":
        return 1
    if args.require_ready and not validation.customer_pilot_ready:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
