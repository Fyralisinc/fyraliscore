#!/usr/bin/env python3
"""Generate or check a Fyralis BYOC bootstrap dry-run plan."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import (
    ByocBootstrapPlanManifest,
    ByocBootstrapPlanViolation,
    byoc_bootstrap_plan_json_schema,
    generate_bootstrap_plan,
    load_byoc_bootstrap_plan,
    render_validation_errors,
    validate_bootstrap_plan_contract,
)
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest to consume.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=Path("deploy/byoc/permissions.example.yaml"),
        help="BYOC permissions manifest to consume.",
    )
    parser.add_argument(
        "--bootstrap-bundle",
        type=Path,
        default=Path("deploy/byoc/bootstrap-bundle.example.yaml"),
        help="BYOC bootstrap bundle manifest to consume.",
    )
    parser.add_argument(
        "--check-plan",
        type=Path,
        help="Existing plan to validate and compare to generated output.",
    )
    parser.add_argument(
        "--generated-at",
        type=str,
        help="ISO timestamp to use when generating output.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used for source digest and local file checks.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the plan contract and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(argv or sys.argv[1:]))

    if args.schema:
        print(json.dumps(byoc_bootstrap_plan_json_schema(), indent=2, sort_keys=True))
        return 0

    try:
        dataplane = load_byoc_manifest(args.dataplane_manifest)
        permissions = load_byoc_permissions_manifest(args.permissions_manifest)
        bundle = load_byoc_bootstrap_bundle(args.bootstrap_bundle)
        generated_at = _parse_generated_at(args.generated_at)
        if args.check_plan is not None:
            existing = load_byoc_bootstrap_plan(args.check_plan)
            generated_at = existing.generated_at
        generated = generate_bootstrap_plan(
            dataplane_manifest=dataplane,
            permissions_manifest=permissions,
            bootstrap_bundle=bundle,
            source_paths={
                "dataplane": args.dataplane_manifest,
                "permissions": args.permissions_manifest,
                "bootstrap_bundle": args.bootstrap_bundle,
            },
            generated_at=generated_at,
            repo_root=args.repo_root.resolve(),
        )
    except ValidationError as exc:
        _print_errors(
            "BYOC bootstrap plan schema violations",
            render_validation_errors(exc),
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "Failed to build BYOC bootstrap plan",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    if args.check_plan is not None:
        return _check_existing_plan(
            existing=existing,
            generated=generated,
            dataplane=dataplane,
            permissions=permissions,
            bundle=bundle,
            args=args,
        )

    payload = generated.model_dump(mode="json", exclude_none=True)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        try:
            import yaml  # type: ignore[import-untyped]
        except ImportError as exc:  # pragma: no cover - dev/test installs PyYAML.
            _print_errors("Failed to render YAML", [str(exc)])
            return 1
        print(yaml.safe_dump(payload, sort_keys=False, width=1_000_000))
    return 0


def _check_existing_plan(
    *,
    existing: ByocBootstrapPlanManifest,
    generated: ByocBootstrapPlanManifest,
    dataplane,
    permissions,
    bundle,
    args: argparse.Namespace,
) -> int:
    violations = validate_bootstrap_plan_contract(
        existing,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        bootstrap_bundle=bundle,
        source_paths={
            "dataplane": args.dataplane_manifest,
            "permissions": args.permissions_manifest,
            "bootstrap_bundle": args.bootstrap_bundle,
        },
        repo_root=args.repo_root.resolve(),
    )
    if existing.model_dump(mode="json") != generated.model_dump(mode="json"):
        violations.append(
            ByocBootstrapPlanViolation(
                path="<root>",
                code="generated_plan_drift",
                message="checked-in plan does not match generated plan",
            )
        )
    if violations:
        _print_errors(
            "BYOC bootstrap plan contract violations",
            [violation.render() for violation in violations],
        )
        return 1
    print("BYOC bootstrap plan passed.")
    return 0


def _parse_generated_at(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _print_errors(title: str, errors: list[str]) -> None:
    print(f"{title}:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
