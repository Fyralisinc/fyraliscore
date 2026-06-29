#!/usr/bin/env python3
"""Run local BYOC live-credential evidence rehearsal."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_live_credential_rehearsal import (
    ByocLiveCredentialRehearsalInputs,
    render_live_credential_rehearsal_json,
    render_live_credential_rehearsal_yaml,
    run_byoc_live_credential_rehearsal,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory where sanitized rehearsal artifacts will be written.",
    )
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
        "--iac-package",
        type=Path,
        default=Path("deploy/byoc/aws/iac-package.example.yaml"),
        help="BYOC AWS IaC package manifest for evidence generation.",
    )
    parser.add_argument(
        "--bootstrap-bundle",
        type=Path,
        default=Path("deploy/byoc/bootstrap-bundle.example.yaml"),
        help="BYOC bootstrap bundle manifest for evidence generation.",
    )
    parser.add_argument(
        "--bootstrap-plan",
        type=Path,
        default=Path("deploy/byoc/bootstrap-plan.example.yaml"),
        help="BYOC bootstrap dry-run plan for evidence generation.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        help="Optional env file for offline post-deploy validation evidence.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used for source digests and local file checks.",
    )
    parser.add_argument(
        "--aws-profile",
        help="Optional local AWS profile to use; never serialized into reports.",
    )
    parser.add_argument(
        "--aws-region",
        help="Optional AWS region override; defaults to the BYOC manifest region.",
    )
    parser.add_argument(
        "--expected-aws-account-id",
        help=(
            "Optional expected AWS account ID override; defaults to the "
            "permissions manifest and is never serialized."
        ),
    )
    parser.add_argument(
        "--skip-live-aws",
        action="store_true",
        help="Run the no-credential AWS report-shape smoke for CI only.",
    )
    parser.add_argument(
        "--require-live-aws-api-calls",
        action="store_true",
        help="Fail unless the AWS preflight executed real AWS API calls.",
    )
    parser.add_argument(
        "--run-readonly-api-probes",
        action="store_true",
        help="Run harmless AWS describe/list probes after STS identity.",
    )
    parser.add_argument(
        "--run-iam-policy-simulation",
        action="store_true",
        help="Run IAM SimulatePrincipalPolicy for the selected manifest role.",
    )
    parser.add_argument(
        "--simulation-principal-arn",
        help="AWS role/user ARN to simulate; never serialized into reports.",
    )
    parser.add_argument(
        "--simulation-role-name",
        default="bootstrap_provisioner",
        help="Permissions-manifest role to simulate.",
    )
    parser.add_argument(
        "--require-live-post-deploy",
        action="store_true",
        help="Require live post-deploy evidence in the generated package gate.",
    )
    parser.add_argument(
        "--require-signed-post-deploy",
        action="store_true",
        help="Require signed live post-deploy evidence in the generated package gate.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized rehearsal summary.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    report = run_byoc_live_credential_rehearsal(
        ByocLiveCredentialRehearsalInputs(
            output_dir=args.output_dir,
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            iam_template_path=args.iam_template,
            iac_package_path=args.iac_package,
            bootstrap_bundle_path=args.bootstrap_bundle,
            bootstrap_plan_path=args.bootstrap_plan,
            env_path=args.env_file,
            repo_root=args.repo_root,
            aws_profile=args.aws_profile,
            aws_region=args.aws_region,
            expected_aws_account_id=args.expected_aws_account_id,
            skip_live_aws=args.skip_live_aws,
            require_live_aws_api_calls=args.require_live_aws_api_calls,
            run_readonly_api_probes=args.run_readonly_api_probes,
            run_iam_policy_simulation=args.run_iam_policy_simulation,
            simulation_principal_arn=args.simulation_principal_arn,
            simulation_role_name=args.simulation_role_name,
            require_live_post_deploy=args.require_live_post_deploy,
            require_signed_post_deploy=args.require_signed_post_deploy,
        )
    )
    rendered = (
        render_live_credential_rehearsal_json(report)
        if args.json
        else render_live_credential_rehearsal_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.required_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
