#!/usr/bin/env python3
"""Run BYOC Terraform scaffold plan validation in contract-only mode."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_terraform_plan_validation import (
    ByocTerraformPlanValidationInputs,
    render_terraform_plan_validation_json,
    render_terraform_plan_validation_yaml,
    run_byoc_terraform_plan_validation,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--iac-package",
        type=Path,
        default=Path("deploy/byoc/aws/iac-package.example.yaml"),
        help="BYOC AWS IaC package manifest to validate.",
    )
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest referenced by the IaC package.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=Path("deploy/byoc/permissions.example.yaml"),
        help="BYOC permissions manifest referenced by the IaC package.",
    )
    parser.add_argument(
        "--iam-template",
        type=Path,
        default=Path("deploy/byoc/aws/iam.bootstrap.template.yaml"),
        help="AWS IAM skeleton referenced by the IaC package.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used for local scaffold checks.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--run-terraform-init",
        action="store_true",
        help=(
            "Run terraform init -backend=false before validation. Command "
            "stdout/stderr are discarded and never included in the report."
        ),
    )
    parser.add_argument(
        "--run-terraform-validate",
        action="store_true",
        help=(
            "Run terraform validate in the customer-side scaffold root. "
            "Command stdout/stderr are discarded and never included in the report."
        ),
    )
    parser.add_argument(
        "--terraform-bin",
        default="terraform",
        help="Terraform executable to use with --run-terraform-validate.",
    )
    parser.add_argument(
        "--terraform-init-timeout-seconds",
        type=int,
        default=60,
        help="Timeout for --run-terraform-init; accepted range is 1-300.",
    )
    parser.add_argument(
        "--terraform-validate-timeout-seconds",
        type=int,
        default=30,
        help="Timeout for --run-terraform-validate; accepted range is 1-300.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized validation report.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    report = run_byoc_terraform_plan_validation(
        ByocTerraformPlanValidationInputs(
            iac_package_path=args.iac_package,
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            iam_template_path=args.iam_template,
            repo_root=args.repo_root,
            run_terraform_init=args.run_terraform_init,
            terraform_init_timeout_seconds=args.terraform_init_timeout_seconds,
            run_terraform_validate=args.run_terraform_validate,
            terraform_bin=args.terraform_bin,
            terraform_validate_timeout_seconds=(
                args.terraform_validate_timeout_seconds
            ),
        )
    )
    rendered = (
        render_terraform_plan_validation_json(report)
        if args.json
        else render_terraform_plan_validation_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.required_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
