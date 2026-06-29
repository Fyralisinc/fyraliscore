#!/usr/bin/env python3
"""Run the offline BYOC product-health install rehearsal."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_product_health_install_rehearsal import (
    ByocProductHealthInstallRehearsalInputs,
    product_health_install_rehearsal_json_schema,
    render_product_health_install_rehearsal_json,
    render_product_health_install_rehearsal_yaml,
    run_product_health_install_rehearsal,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--install-plan",
        type=Path,
        default=Path("deploy/byoc/product-health-install-rehearsal.example.yaml"),
        help="BYOC product-health install rehearsal plan to validate.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used to resolve referenced local artifacts.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schemas for the install plan and report, then exit.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the install rehearsal report.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))

    if args.schema:
        print(
            json.dumps(
                product_health_install_rehearsal_json_schema(),
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    try:
        repo_root = args.repo_root.resolve()
        report = run_product_health_install_rehearsal(
            ByocProductHealthInstallRehearsalInputs(
                install_plan_path=_resolve_path(args.install_plan, repo_root),
                repo_root=repo_root,
            )
        )
    except ValidationError as exc:
        _print_errors("BYOC product-health install rehearsal schema violations", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        print(
            "Failed to run BYOC product-health install rehearsal:",
            file=sys.stderr,
        )
        print(f"  {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    rendered = (
        render_product_health_install_rehearsal_json(report)
        if args.json
        else render_product_health_install_rehearsal_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.status == "pass" else 1


def _resolve_path(path: Path, repo_root: Path) -> Path:
    return path if path.is_absolute() else repo_root / path


def _print_errors(title: str, exc: ValidationError) -> None:
    print(f"{title}:", file=sys.stderr)
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"]) or "<root>"
        print(f"  {location}: {error['msg']}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
