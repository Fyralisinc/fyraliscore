#!/usr/bin/env python3
"""Generate or check a sanitized Fyralis BYOC customer evidence package."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_aws_iac_package import load_byoc_aws_iac_package
from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import load_byoc_bootstrap_plan
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_ledger import load_byoc_evidence_ledger
from services.platform.runtime.byoc_evidence_package import (
    ByocEvidencePackage,
    ByocEvidencePackageViolation,
    byoc_evidence_package_json_schema,
    generate_evidence_package,
    load_byoc_evidence_package,
    package_source_digests,
    render_validation_errors,
    validate_evidence_package_contract,
)
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=Path("deploy/byoc/evidence-ledger.example.yaml"),
        help="Sanitized BYOC evidence ledger to include in the package.",
    )
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest referenced by the evidence package.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=Path("deploy/byoc/permissions.example.yaml"),
        help="BYOC permissions manifest referenced by the evidence package.",
    )
    parser.add_argument(
        "--aws-iac-package",
        type=Path,
        default=Path("deploy/byoc/aws/iac-package.example.yaml"),
        help="AWS BYOC IaC package manifest referenced by the evidence package.",
    )
    parser.add_argument(
        "--bootstrap-bundle",
        type=Path,
        default=Path("deploy/byoc/bootstrap-bundle.example.yaml"),
        help="BYOC bootstrap bundle referenced by the evidence package.",
    )
    parser.add_argument(
        "--plan",
        type=Path,
        default=Path("deploy/byoc/bootstrap-plan.example.yaml"),
        help="BYOC bootstrap dry-run plan referenced by the evidence package.",
    )
    parser.add_argument(
        "--post-deploy-envelope",
        type=Path,
        help="Optional signed envelope to summarize for signed live report evidence.",
    )
    parser.add_argument(
        "--check-package",
        type=Path,
        help="Existing evidence package to validate and compare to generated output.",
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
        help="Repository root used for source digests and local file checks.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the evidence package contract and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))

    if args.schema:
        print(json.dumps(byoc_evidence_package_json_schema(), indent=2, sort_keys=True))
        return 0

    try:
        ledger = load_byoc_evidence_ledger(args.ledger)
        dataplane = load_byoc_manifest(args.dataplane_manifest)
        permissions = load_byoc_permissions_manifest(args.permissions_manifest)
        aws_iac_package = load_byoc_aws_iac_package(args.aws_iac_package)
        bundle = load_byoc_bootstrap_bundle(args.bootstrap_bundle)
        plan = load_byoc_bootstrap_plan(args.plan)
        generated_at = _parse_generated_at(args.generated_at)
        if args.check_package is not None:
            existing = load_byoc_evidence_package(args.check_package)
            generated_at = existing.generated_at
        generated = generate_evidence_package(
            ledger=ledger,
            dataplane_manifest=dataplane,
            permissions_manifest=permissions,
            bootstrap_bundle=bundle,
            plan=plan,
            ledger_path=args.ledger,
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            aws_iac_package_path=args.aws_iac_package,
            bootstrap_bundle_path=args.bootstrap_bundle,
            plan_path=args.plan,
            post_deploy_envelope_path=args.post_deploy_envelope,
            generated_at=generated_at,
            repo_root=args.repo_root.resolve(),
        )
    except ValidationError as exc:
        _print_errors(
            "BYOC evidence package schema violations",
            render_validation_errors(exc),
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "Failed to build BYOC evidence package",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    if args.check_package is not None:
        return _check_existing_package(
            existing=existing,
            generated=generated,
            dataplane=dataplane,
            permissions=permissions,
            aws_iac_package=aws_iac_package,
            bundle=bundle,
            plan=plan,
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
        sys.stdout.write(yaml.safe_dump(payload, sort_keys=False, width=1_000_000))
    return 0


def _check_existing_package(
    *,
    existing: ByocEvidencePackage,
    generated: ByocEvidencePackage,
    dataplane,
    permissions,
    aws_iac_package,
    bundle,
    plan,
    args: argparse.Namespace,
) -> int:
    source_digests = package_source_digests(
        dataplane_manifest_path=args.dataplane_manifest,
        permissions_manifest_path=args.permissions_manifest,
        aws_iac_package_path=args.aws_iac_package,
        bootstrap_bundle_path=args.bootstrap_bundle,
        plan_path=args.plan,
        ledger_path=args.ledger,
        repo_root=args.repo_root.resolve(),
    )
    violations = validate_evidence_package_contract(
        existing,
        dataplane_manifest=dataplane,
        permissions_manifest=permissions,
        aws_iac_package=aws_iac_package,
        bootstrap_bundle=bundle,
        plan=plan,
        source_digests=source_digests,
    )
    if existing.model_dump(mode="json") != generated.model_dump(mode="json"):
        violations.append(
            ByocEvidencePackageViolation(
                path="<root>",
                code="generated_package_drift",
                message="checked-in evidence package does not match generated output",
            )
        )
    if violations:
        _print_errors(
            "BYOC evidence package contract violations",
            [violation.render() for violation in violations],
        )
        return 1
    print("BYOC evidence package passed.")
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
