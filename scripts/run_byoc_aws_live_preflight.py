#!/usr/bin/env python3
"""Run read-only AWS live preflight checks for a BYOC data plane."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_aws_live_preflight import (
    ByocAwsLivePreflightInputs,
    render_aws_live_preflight_json,
    render_aws_live_preflight_yaml,
    run_byoc_aws_live_preflight,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest to verify.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=Path("deploy/byoc/permissions.example.yaml"),
        help="BYOC permissions manifest with the expected AWS account contract.",
    )
    parser.add_argument(
        "--iam-template",
        type=Path,
        default=Path("deploy/byoc/aws/iam.bootstrap.template.yaml"),
        help="AWS IAM skeleton referenced by the permissions manifest.",
    )
    parser.add_argument(
        "--aws-profile",
        help="Optional local AWS profile to use; never serialized into the report.",
    )
    parser.add_argument(
        "--aws-region",
        help="Optional AWS region override; defaults to the BYOC manifest region.",
    )
    parser.add_argument(
        "--expected-account-id",
        help=(
            "Optional expected AWS account ID override; defaults to the permissions "
            "manifest and is never serialized into the report."
        ),
    )
    parser.add_argument(
        "--skip-live-aws",
        action="store_true",
        help=(
            "Run only the local report-shape and manifest-contract checks. This is "
            "for CI/contract smoke tests, not customer readiness."
        ),
    )
    parser.add_argument(
        "--run-readonly-api-probes",
        action="store_true",
        help="Run harmless AWS describe/list probes in addition to STS identity.",
    )
    parser.add_argument(
        "--run-iam-policy-simulation",
        action="store_true",
        help=(
            "Use IAM SimulatePrincipalPolicy for the selected manifest role. "
            "Requires --simulation-principal-arn and sufficient customer-side IAM "
            "permissions; no ARN or policy details are serialized."
        ),
    )
    parser.add_argument(
        "--simulation-principal-arn",
        help="AWS role/user ARN to simulate; never serialized into the report.",
    )
    parser.add_argument(
        "--simulation-role-name",
        default="bootstrap_provisioner",
        help="Permissions-manifest role to simulate; defaults to bootstrap_provisioner.",
    )
    parser.add_argument(
        "--aws-connect-timeout-seconds",
        type=int,
        default=3,
        help="AWS client connect timeout.",
    )
    parser.add_argument(
        "--aws-read-timeout-seconds",
        type=int,
        default=5,
        help="AWS client read timeout.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized AWS live preflight report.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    report = run_byoc_aws_live_preflight(
        ByocAwsLivePreflightInputs(
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            iam_template_path=args.iam_template,
            aws_profile=args.aws_profile,
            aws_region=args.aws_region,
            expected_account_id=args.expected_account_id,
            skip_live_aws=args.skip_live_aws,
            run_readonly_api_probes=args.run_readonly_api_probes,
            run_iam_policy_simulation=args.run_iam_policy_simulation,
            simulation_principal_arn=args.simulation_principal_arn,
            simulation_role_name=args.simulation_role_name,
            aws_connect_timeout_seconds=args.aws_connect_timeout_seconds,
            aws_read_timeout_seconds=args.aws_read_timeout_seconds,
        )
    )
    rendered = (
        render_aws_live_preflight_json(report)
        if args.json
        else render_aws_live_preflight_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.required_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
