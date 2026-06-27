#!/usr/bin/env python3
"""Check offline readiness for the next live BYOC AWS test."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_live_test_readiness import (
    ByocLiveTestReadinessInputs,
    model_json_schema_bundle,
    render_live_test_readiness_json,
    render_live_test_readiness_yaml,
    run_byoc_live_test_readiness,
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
        help="BYOC permissions manifest with the expected AWS account contract.",
    )
    parser.add_argument(
        "--iam-template",
        type=Path,
        default=Path("deploy/byoc/aws/iam.bootstrap.template.yaml"),
        help="AWS IAM skeleton referenced by the permissions manifest.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used to check operator scripts.",
    )
    parser.add_argument(
        "--aws-profile",
        help="Optional local AWS profile name; never serialized into reports.",
    )
    parser.add_argument(
        "--aws-region",
        help="Optional AWS region override; must match the BYOC manifests.",
    )
    parser.add_argument(
        "--require-aws-access",
        action="store_true",
        help="Fail if AWS CLI/profile/env credentials are not locally configured.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized readiness report.",
    )
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the live-test readiness schema bundle and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.schema:
        print(json.dumps(model_json_schema_bundle(), indent=2, sort_keys=True))
        return 0

    report = run_byoc_live_test_readiness(
        ByocLiveTestReadinessInputs(
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            iam_template_path=args.iam_template,
            repo_root=args.repo_root,
            aws_profile=args.aws_profile,
            aws_region=args.aws_region,
            require_aws_access=args.require_aws_access,
        )
    )
    rendered = (
        render_live_test_readiness_json(report)
        if args.json
        else render_live_test_readiness_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    if not report.required_checks_passed:
        return 1
    if args.require_aws_access and not report.live_aws_ready:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
