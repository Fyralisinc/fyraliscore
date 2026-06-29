#!/usr/bin/env python3
"""Run the sanitized customer-side BYOC preflight bundle."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_preflight_bundle import (
    ByocPreflightBundleInputs,
    render_preflight_report_json,
    render_preflight_report_yaml,
    run_byoc_preflight_bundle,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest to validate.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=Path("deploy/byoc/permissions.example.yaml"),
        help="BYOC permissions manifest to validate.",
    )
    parser.add_argument(
        "--iam-template",
        type=Path,
        default=Path("deploy/byoc/aws/iam.bootstrap.template.yaml"),
        help="AWS IAM skeleton referenced by the permissions/IaC contracts.",
    )
    parser.add_argument(
        "--iac-package",
        type=Path,
        default=Path("deploy/byoc/aws/iac-package.example.yaml"),
        help="BYOC AWS IaC package manifest to validate.",
    )
    parser.add_argument(
        "--bootstrap-bundle",
        type=Path,
        default=Path("deploy/byoc/bootstrap-bundle.example.yaml"),
        help="BYOC bootstrap bundle manifest to verify.",
    )
    parser.add_argument(
        "--bootstrap-plan",
        type=Path,
        default=Path("deploy/byoc/bootstrap-plan.example.yaml"),
        help="BYOC bootstrap dry-run plan to evaluate.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        help="Optional env file for offline post-deploy validation checks.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used for local file and scaffold checks.",
    )
    parser.add_argument(
        "--skip-local-bundle-file-verification",
        action="store_true",
        help="Skip local hash checks for checked-in bootstrap bundle artifacts.",
    )
    parser.add_argument(
        "--run-terraform-init",
        action="store_true",
        help=(
            "Run terraform init -backend=false in the customer-side scaffold "
            "root. Command stdout/stderr are discarded."
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
        "--run-aws-live-preflight",
        action="store_true",
        help=(
            "Add the read-only AWS live preflight section. By default this runs "
            "STS identity; add the AWS probe/simulation flags for deeper checks."
        ),
    )
    parser.add_argument(
        "--skip-aws-live-preflight-aws",
        action="store_true",
        help=(
            "When --run-aws-live-preflight is set, skip AWS API calls and run "
            "only the local contract/report-shape smoke."
        ),
    )
    parser.add_argument(
        "--run-aws-readonly-api-probes",
        action="store_true",
        help="Run harmless AWS describe/list probes in the live AWS section.",
    )
    parser.add_argument(
        "--run-aws-iam-policy-simulation",
        action="store_true",
        help=(
            "Run IAM SimulatePrincipalPolicy for the selected manifest role. "
            "Requires --aws-simulation-principal-arn."
        ),
    )
    parser.add_argument(
        "--aws-simulation-principal-arn",
        help="AWS role/user ARN to simulate; never serialized into the report.",
    )
    parser.add_argument(
        "--aws-simulation-role-name",
        default="bootstrap_provisioner",
        help="Permissions-manifest role to simulate in AWS IAM.",
    )
    parser.add_argument(
        "--aws-profile",
        help="Optional local AWS profile for live preflight; never serialized.",
    )
    parser.add_argument(
        "--aws-region",
        help="Optional AWS region override for live preflight.",
    )
    parser.add_argument(
        "--expected-aws-account-id",
        help=(
            "Optional expected AWS account override for live preflight; defaults "
            "to the permissions manifest and is never serialized."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized preflight report.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    report = run_byoc_preflight_bundle(
        ByocPreflightBundleInputs(
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            iam_template_path=args.iam_template,
            iac_package_path=args.iac_package,
            bootstrap_bundle_path=args.bootstrap_bundle,
            bootstrap_plan_path=args.bootstrap_plan,
            env_path=args.env_file,
            repo_root=args.repo_root,
            verify_local_bundle_files=not args.skip_local_bundle_file_verification,
            run_terraform_init=args.run_terraform_init,
            terraform_init_timeout_seconds=args.terraform_init_timeout_seconds,
            run_terraform_validate=args.run_terraform_validate,
            terraform_bin=args.terraform_bin,
            terraform_validate_timeout_seconds=(
                args.terraform_validate_timeout_seconds
            ),
            run_aws_live_preflight=args.run_aws_live_preflight,
            skip_aws_live_preflight_aws=args.skip_aws_live_preflight_aws,
            run_aws_readonly_api_probes=args.run_aws_readonly_api_probes,
            run_aws_iam_policy_simulation=args.run_aws_iam_policy_simulation,
            aws_simulation_principal_arn=args.aws_simulation_principal_arn,
            aws_simulation_role_name=args.aws_simulation_role_name,
            aws_profile=args.aws_profile,
            aws_region=args.aws_region,
            expected_aws_account_id=args.expected_aws_account_id,
        )
    )
    rendered = (
        render_preflight_report_json(report)
        if args.json
        else render_preflight_report_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.required_sections_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
