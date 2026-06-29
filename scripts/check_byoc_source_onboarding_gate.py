#!/usr/bin/env python3
"""Check whether BYOC evidence allows first source onboarding."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_source_onboarding_gate import (
    ByocSourceOnboardingGateInputs,
    render_source_onboarding_gate_json,
    render_source_onboarding_gate_yaml,
    run_byoc_source_onboarding_gate,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--evidence-package",
        type=Path,
        help=(
            "Sanitized BYOC evidence package to gate. Defaults to "
            "deploy/byoc/evidence-package.example.yaml when no ledger is supplied."
        ),
    )
    source.add_argument(
        "--evidence-ledger",
        type=Path,
        help="Sanitized BYOC evidence ledger to gate when no package is used.",
    )
    parser.add_argument(
        "--require-aws-live-preflight",
        action="store_true",
        help="Require passing aws_live_preflight evidence before source onboarding.",
    )
    parser.add_argument(
        "--require-live-post-deploy",
        action="store_true",
        help="Require live post-deploy evidence instead of offline validator evidence.",
    )
    parser.add_argument(
        "--require-signed-post-deploy",
        action="store_true",
        help="Require signed live post-deploy evidence.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized gate report.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    evidence_package = args.evidence_package
    if evidence_package is None and args.evidence_ledger is None:
        evidence_package = Path("deploy/byoc/evidence-package.example.yaml")
    report = run_byoc_source_onboarding_gate(
        ByocSourceOnboardingGateInputs(
            evidence_package_path=evidence_package,
            evidence_ledger_path=args.evidence_ledger,
            require_aws_live_preflight=args.require_aws_live_preflight,
            require_live_post_deploy=args.require_live_post_deploy,
            require_signed_post_deploy=args.require_signed_post_deploy,
        )
    )
    rendered = (
        render_source_onboarding_gate_json(report)
        if args.json
        else render_source_onboarding_gate_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.source_onboarding_allowed else 1


if __name__ == "__main__":
    raise SystemExit(main())
