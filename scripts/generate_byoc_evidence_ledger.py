#!/usr/bin/env python3
"""Generate or check a sanitized Fyralis BYOC evidence ledger."""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

from pydantic import ValidationError

from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_bootstrap_plan import load_byoc_bootstrap_plan
from services.platform.runtime.byoc_contract import load_byoc_manifest
from services.platform.runtime.byoc_evidence_ledger import (
    ByocDeploymentEvidenceLedger,
    ByocEvidenceLedgerViolation,
    byoc_evidence_ledger_json_schema,
    generate_evidence_ledger,
    load_byoc_evidence_ledger,
    render_validation_errors,
    validate_evidence_ledger_contract,
)
from services.platform.runtime.byoc_permissions import load_byoc_permissions_manifest


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--plan",
        type=Path,
        default=Path("deploy/byoc/bootstrap-plan.example.yaml"),
        help="BYOC bootstrap dry-run plan to summarize.",
    )
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest referenced by the plan.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=Path("deploy/byoc/permissions.example.yaml"),
        help="BYOC permissions manifest referenced by the plan.",
    )
    parser.add_argument(
        "--bootstrap-bundle",
        type=Path,
        default=Path("deploy/byoc/bootstrap-bundle.example.yaml"),
        help="BYOC bootstrap bundle manifest referenced by the plan.",
    )
    parser.add_argument(
        "--iac-package",
        type=Path,
        default=Path("deploy/byoc/aws/iac-package.example.yaml"),
        help="BYOC AWS IaC package manifest to validate for Terraform evidence.",
    )
    parser.add_argument(
        "--iam-template",
        type=Path,
        default=Path("deploy/byoc/aws/iam.bootstrap.template.yaml"),
        help="AWS IAM skeleton referenced by the IaC package.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        help="Optional env file for offline post-deploy validation evidence.",
    )
    parser.add_argument(
        "--terraform-validation-report",
        type=Path,
        help=(
            "Optional sanitized Terraform validation report to summarize "
            "without copying report details."
        ),
    )
    parser.add_argument(
        "--post-deploy-report",
        type=Path,
        help=(
            "Optional customer-side post-deploy validator report to summarize "
            "without copying report details."
        ),
    )
    parser.add_argument(
        "--post-deploy-envelope",
        type=Path,
        help="Optional signed evidence envelope for --post-deploy-report.",
    )
    parser.add_argument(
        "--evidence-signing-secret-env",
        default="FYRALIS_BYOC_EVIDENCE_SIGNING_SECRET",
        help="Environment variable containing the local evidence signing secret.",
    )
    parser.add_argument(
        "--max-envelope-age-seconds",
        type=int,
        default=86_400,
        help="Maximum signed evidence envelope age to accept.",
    )
    parser.add_argument(
        "--check-ledger",
        type=Path,
        help="Existing evidence ledger to validate and compare to generated output.",
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
        help="Repository root used for digest and local file checks.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the JSON schema for the ledger contract and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))

    if args.schema:
        print(json.dumps(byoc_evidence_ledger_json_schema(), indent=2, sort_keys=True))
        return 0

    try:
        plan = load_byoc_bootstrap_plan(args.plan)
        dataplane = load_byoc_manifest(args.dataplane_manifest)
        permissions = load_byoc_permissions_manifest(args.permissions_manifest)
        bundle = load_byoc_bootstrap_bundle(args.bootstrap_bundle)
        generated_at = _parse_generated_at(args.generated_at)
        if args.check_ledger is not None:
            existing = load_byoc_evidence_ledger(args.check_ledger)
            generated_at = existing.generated_at
        signing_secret = (
            os.environ.get(args.evidence_signing_secret_env)
            if args.post_deploy_envelope is not None
            else None
        )
        generated = generate_evidence_ledger(
            plan=plan,
            dataplane_manifest=dataplane,
            permissions_manifest=permissions,
            bootstrap_bundle=bundle,
            plan_path=args.plan,
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            bootstrap_bundle_path=args.bootstrap_bundle,
            iac_package_path=args.iac_package,
            iam_template_path=args.iam_template,
            env_path=args.env_file,
            terraform_validation_report_path=args.terraform_validation_report,
            post_deploy_report_path=args.post_deploy_report,
            post_deploy_envelope_path=args.post_deploy_envelope,
            evidence_signing_secret=signing_secret,
            max_envelope_age_seconds=args.max_envelope_age_seconds,
            generated_at=generated_at,
            repo_root=args.repo_root.resolve(),
        )
    except ValidationError as exc:
        _print_errors(
            "BYOC evidence ledger schema violations",
            render_validation_errors(exc),
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "Failed to build BYOC evidence ledger",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    if args.check_ledger is not None:
        return _check_existing_ledger(
            existing=existing,
            generated=generated,
            dataplane=dataplane,
            plan=plan,
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


def _check_existing_ledger(
    *,
    existing: ByocDeploymentEvidenceLedger,
    generated: ByocDeploymentEvidenceLedger,
    dataplane,
    plan,
) -> int:
    violations = validate_evidence_ledger_contract(
        existing,
        dataplane_manifest=dataplane,
        plan=plan,
    )
    if existing.model_dump(mode="json") != generated.model_dump(mode="json"):
        violations.append(
            ByocEvidenceLedgerViolation(
                path="<root>",
                code="generated_ledger_drift",
                message="checked-in evidence ledger does not match generated output",
            )
        )
    if violations:
        _print_errors(
            "BYOC evidence ledger contract violations",
            [violation.render() for violation in violations],
        )
        return 1
    print("BYOC evidence ledger passed.")
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
