#!/usr/bin/env python3
"""Build local sanitized BYOC customer-pilot handoff artifacts."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_customer_pilot_package import (
    ByocCustomerPilotPackageInputs,
    build_byoc_customer_pilot_package,
    render_customer_pilot_package_manifest_json,
    render_customer_pilot_package_manifest_yaml,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("tmp/byoc/customer-pilot"),
        help="Repo-local directory where sanitized customer-pilot artifacts are written.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used for bounded relative artifact paths.",
    )
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
        help="AWS IAM skeleton referenced by the permissions contract.",
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
        "--evidence-package",
        type=Path,
        default=Path("deploy/byoc/evidence-package.example.yaml"),
        help="Sanitized BYOC evidence package to include.",
    )
    parser.add_argument(
        "--evidence-ledger",
        type=Path,
        default=Path("deploy/byoc/evidence-ledger.example.yaml"),
        help="Sanitized BYOC evidence ledger to include.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(".env.production.example"),
        help="Env contract file used by offline customer handoff checks.",
    )
    parser.add_argument(
        "--live-test-readiness",
        type=Path,
        help=(
            "Optional pre-generated sanitized live-test readiness report. Use "
            "this after running real AWS credential readiness."
        ),
    )
    parser.add_argument(
        "--customer-handoff-report",
        type=Path,
        help="Optional pre-generated sanitized customer handoff readiness report.",
    )
    smoke_group = parser.add_mutually_exclusive_group()
    smoke_group.add_argument(
        "--control-plane-read-smoke",
        type=Path,
        help=(
            "Optional raw control-plane read smoke output to summarize. When "
            "omitted, the package marks hosted smoke as manual_required."
        ),
    )
    smoke_group.add_argument(
        "--control-plane-read-smoke-summary",
        type=Path,
        help="Optional pre-generated sanitized control-plane read smoke summary.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="Exit non-zero unless the generated launch summary is pass.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    manifest = build_byoc_customer_pilot_package(
        ByocCustomerPilotPackageInputs(
            output_dir=args.output_dir,
            repo_root=args.repo_root,
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            iam_template_path=args.iam_template,
            iac_package_path=args.iac_package,
            bootstrap_bundle_path=args.bootstrap_bundle,
            bootstrap_plan_path=args.bootstrap_plan,
            evidence_package_path=args.evidence_package,
            evidence_ledger_path=args.evidence_ledger,
            env_path=args.env_file,
            live_test_readiness_path=args.live_test_readiness,
            customer_handoff_report_path=args.customer_handoff_report,
            control_plane_read_smoke_path=args.control_plane_read_smoke,
            control_plane_read_smoke_summary_path=args.control_plane_read_smoke_summary,
        )
    )
    rendered = (
        render_customer_pilot_package_manifest_json(manifest)
        if args.json
        else render_customer_pilot_package_manifest_yaml(manifest)
    )
    sys.stdout.write(rendered)
    if manifest.status == "fail":
        return 1
    if args.require_ready and manifest.status != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
